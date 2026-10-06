// Copyright (c) 2025-2026, s0up and the autobrr contributors.
// SPDX-License-Identifier: GPL-2.0-or-later

package notifications

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"

	"github.com/autobrr/qui/internal/models"
	"github.com/autobrr/qui/internal/testutil/testdb"
)

type recordingWebhook struct {
	server *httptest.Server
	notify chan struct{}

	// Requests whose body contains slowSubstring sleep slowDelay before
	// responding, simulating a slow target only for that request.
	slowSubstring string
	slowDelay     time.Duration

	mu     sync.Mutex
	bodies []string
}

func newRecordingWebhook(t *testing.T, status int, delay time.Duration) *recordingWebhook {
	t.Helper()

	rw := &recordingWebhook{
		notify:    make(chan struct{}, 1024),
		slowDelay: delay,
	}
	rw.start(t, status)
	return rw
}

func (rw *recordingWebhook) start(t *testing.T, status int) {
	t.Helper()

	rw.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)

		rw.mu.Lock()
		rw.bodies = append(rw.bodies, string(body))
		rw.mu.Unlock()

		select {
		case rw.notify <- struct{}{}:
		default:
		}

		if rw.slowDelay > 0 && (rw.slowSubstring == "" || strings.Contains(string(body), rw.slowSubstring)) {
			time.Sleep(rw.slowDelay)
		}
		w.WriteHeader(status)
	}))
	t.Cleanup(rw.server.Close)
}

func (rw *recordingWebhook) genericURL() string {
	// shoutrrr generic defaults to https; the local test server is http.
	return "generic://" + strings.TrimPrefix(rw.server.URL, "http://") + "/hook?disabletls=yes"
}

func (rw *recordingWebhook) snapshot() []string {
	rw.mu.Lock()
	defer rw.mu.Unlock()
	return append([]string(nil), rw.bodies...)
}

func (rw *recordingWebhook) waitCount(t *testing.T, want int, timeout time.Duration) []string {
	t.Helper()

	deadline := time.After(timeout)
	for {
		if got := len(rw.snapshot()); got >= want {
			return rw.snapshot()
		}
		select {
		case <-rw.notify:
		case <-deadline:
			t.Fatalf("wanted %d delivered requests, got %d", want, len(rw.snapshot()))
		}
	}
}

func newDispatchTestService(t *testing.T, targets ...*models.NotificationTargetCreate) *Service {
	t.Helper()

	db := testdb.NewMigratedSQLite(t, "notifications-dispatcher")
	store := models.NewNotificationTargetStore(db)

	for _, target := range targets {
		_, err := store.Create(context.Background(), target)
		require.NoError(t, err)
	}

	svc := NewService(store, nil, zerolog.New(io.Discard))
	require.NotNil(t, svc)
	svc.Start(context.Background())

	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = svc.Shutdown(ctx)
	})

	return svc
}

func genericTarget(name, rawURL string, enabled bool, eventTypes []string) *models.NotificationTargetCreate {
	return &models.NotificationTargetCreate{
		Name:       name,
		URL:        rawURL,
		Enabled:    enabled,
		EventTypes: eventTypes,
	}
}

func backupTestEvent(eventType EventType, runID int64) Event {
	event := Event{Type: eventType, BackupRunID: runID}
	if eventType == EventBackupFailed {
		event.ErrorMessage = "boom"
	} else {
		event.BackupTorrentCount = 3
	}
	return event
}

func TestDispatchSameRunEventsStayOrderedAtTarget(t *testing.T) {
	t.Parallel()

	// The failure delivery is slow; under parallel fan-out the later success
	// could overtake it. A serialized run lane must keep failure first.
	webhook := &recordingWebhook{
		notify:        make(chan struct{}, 1024),
		slowSubstring: "Error: boom",
		slowDelay:     200 * time.Millisecond,
	}
	webhook.start(t, http.StatusOK)

	svc := newDispatchTestService(t, genericTarget("target", webhook.genericURL(), true, nil))

	svc.Notify(context.Background(), backupTestEvent(EventBackupFailed, 7001))
	svc.Notify(context.Background(), backupTestEvent(EventBackupSucceeded, 7001))

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	require.NoError(t, svc.Shutdown(ctx))

	bodies := webhook.snapshot()
	require.Len(t, bodies, 2)
	require.Contains(t, bodies[0], "Run: 7001")
	require.Contains(t, bodies[0], "Error: boom", "failure must be delivered before success for the same run")
	require.Contains(t, bodies[1], "Torrents: 3")
}

func TestDispatchDifferentRunsSendInParallel(t *testing.T) {
	t.Parallel()

	webhook := newRecordingWebhook(t, http.StatusOK, 250*time.Millisecond)
	svc := newDispatchTestService(t, genericTarget("target", webhook.genericURL(), true, nil))

	start := time.Now()
	svc.Notify(context.Background(), backupTestEvent(EventBackupSucceeded, 7101))
	svc.Notify(context.Background(), backupTestEvent(EventBackupSucceeded, 7102))

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	require.NoError(t, svc.Shutdown(ctx))

	elapsed := time.Since(start)
	bodies := webhook.snapshot()
	require.Len(t, bodies, 2)
	require.Less(t, elapsed, 450*time.Millisecond, "distinct runs must not be serialized at the same target")
}

func TestDispatchFailingTargetDoesNotBlockOtherTargets(t *testing.T) {
	t.Parallel()

	slowFailing := newRecordingWebhook(t, http.StatusInternalServerError, 500*time.Millisecond)
	healthy := newRecordingWebhook(t, http.StatusOK, 0)

	svc := newDispatchTestService(t,
		genericTarget("slow-failing", slowFailing.genericURL(), true, nil),
		genericTarget("healthy", healthy.genericURL(), true, nil),
	)

	start := time.Now()
	svc.Notify(context.Background(), backupTestEvent(EventBackupSucceeded, 7201))

	bodies := healthy.waitCount(t, 1, time.Second)
	require.Len(t, bodies, 1)
	require.Less(t, time.Since(start), 300*time.Millisecond, "the healthy target must not wait on the failing target")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	require.NoError(t, svc.Shutdown(ctx))

	require.Len(t, slowFailing.snapshot(), 1, "the failing target was still attempted")
	require.Len(t, healthy.snapshot(), 1)
}

func TestDispatchKeepsEventTypeFilteringAndEnabledGating(t *testing.T) {
	t.Parallel()

	allEvents := newRecordingWebhook(t, http.StatusOK, 0)
	onlySuccess := newRecordingWebhook(t, http.StatusOK, 0)
	disabled := newRecordingWebhook(t, http.StatusOK, 0)

	svc := newDispatchTestService(t,
		genericTarget("all", allEvents.genericURL(), true, nil),
		genericTarget("only-success", onlySuccess.genericURL(), true, []string{string(EventBackupSucceeded)}),
		genericTarget("disabled", disabled.genericURL(), false, nil),
	)

	svc.Notify(context.Background(), backupTestEvent(EventBackupFailed, 7301))
	allEvents.waitCount(t, 1, time.Second)

	svc.Notify(context.Background(), backupTestEvent(EventBackupSucceeded, 7302))
	onlySuccess.waitCount(t, 1, time.Second)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	require.NoError(t, svc.Shutdown(ctx))

	require.Len(t, allEvents.snapshot(), 2)
	require.Len(t, onlySuccess.snapshot(), 1)
	require.Contains(t, onlySuccess.snapshot()[0], "Run: 7302")
	require.Empty(t, disabled.snapshot())
}

func TestShutdownDeliversEveryAcceptedEvent(t *testing.T) {
	t.Parallel()

	webhook := newRecordingWebhook(t, http.StatusOK, 0)
	svc := newDispatchTestService(t, genericTarget("target", webhook.genericURL(), true, nil))

	const count = 60
	for i := range count {
		svc.Notify(context.Background(), backupTestEvent(EventBackupSucceeded, int64(7400+i)))
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	require.NoError(t, svc.Shutdown(ctx))

	require.Len(t, webhook.snapshot(), count)
}

func TestNotifyAfterShutdownDeliversNothing(t *testing.T) {
	t.Parallel()

	webhook := newRecordingWebhook(t, http.StatusOK, 0)
	svc := newDispatchTestService(t, genericTarget("target", webhook.genericURL(), true, nil))

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	require.NoError(t, svc.Shutdown(ctx))

	svc.Notify(context.Background(), backupTestEvent(EventBackupSucceeded, 7501))

	select {
	case <-webhook.notify:
		t.Fatal("event must not be delivered after shutdown")
	case <-time.After(200 * time.Millisecond):
	}
}

func TestEventLaneKey(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		a    Event
		b    Event
		same bool
	}{
		{
			name: "backup same run",
			a:    Event{Type: EventBackupFailed, BackupRunID: 10},
			b:    Event{Type: EventBackupSucceeded, BackupRunID: 10},
			same: true,
		},
		{
			name: "backup different runs",
			a:    Event{BackupRunID: 10},
			b:    Event{BackupRunID: 11},
			same: false,
		},
		{
			name: "dir scan runs",
			a:    Event{DirScanRunID: 10},
			b:    Event{DirScanRunID: 10},
			same: true,
		},
		{
			name: "orphan scan different runs",
			a:    Event{OrphanScanRunID: 10},
			b:    Event{OrphanScanRunID: 11},
			same: false,
		},
		{
			name: "cross seed run",
			a:    Event{CrossSeed: &CrossSeedEventData{RunID: 5}},
			b:    Event{CrossSeed: &CrossSeedEventData{RunID: 5}},
			same: true,
		},
		{
			name: "same torrent on same instance",
			a:    Event{Type: EventTorrentAdded, InstanceID: 2, TorrentHash: "ABCDEF1234567890"},
			b:    Event{Type: EventTorrentCompleted, InstanceID: 2, TorrentHash: "abcdef1234567890"},
			same: true,
		},
		{
			name: "same torrent hash on different instances",
			a:    Event{InstanceID: 1, TorrentHash: "deadbeef"},
			b:    Event{InstanceID: 2, TorrentHash: "deadbeef"},
			same: false,
		},
		{
			name: "automation events per instance",
			a:    Event{Type: EventAutomationsActionsApplied, InstanceID: 4},
			b:    Event{Type: EventAutomationsRunFailed, InstanceID: 4},
			same: true,
		},
		{
			name: "per torrent cross seed events",
			a:    Event{Type: EventCrossSeedWebhookSucceeded, InstanceID: 4, TorrentName: "Movie", CrossSeed: &CrossSeedEventData{}},
			b:    Event{Type: EventCrossSeedWebhookFailed, InstanceID: 4, TorrentName: "Movie", CrossSeed: &CrossSeedEventData{}},
			same: true,
		},
		{
			name: "unkeyed event",
			a:    Event{Type: EventCrossSeedWebhookFailed},
			b:    Event{Type: EventCrossSeedWebhookFailed},
			same: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			keyA := eventLaneKey(tt.a)
			keyB := eventLaneKey(tt.b)
			if tt.same {
				require.NotEmpty(t, keyA)
				require.Equal(t, keyA, keyB)
			} else if keyA != "" && keyB != "" {
				require.NotEqual(t, keyA, keyB)
			}
		})
	}
}
