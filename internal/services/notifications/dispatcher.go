// Copyright (c) 2025-2026, s0up and the autobrr contributors.
// SPDX-License-Identifier: GPL-2.0-or-later

package notifications

import (
	"context"
	"strconv"
	"strings"

	"github.com/rs/zerolog"

	"github.com/autobrr/qui/internal/models"
	"github.com/autobrr/qui/pkg/redact"
)

// targetLaneBuffer bounds backlog per (target, run/resource) lane. A lane only
// holds events of one run or one torrent, so reaching the cap means the target
// itself stopped draining; the router then applies backpressure to ingress
// instead of growing memory without limit.
const targetLaneBuffer = 256

// targetJob is one delivery of one event to one target. Title and message are
// rendered at routing time so lane workers never touch the database.
type targetJob struct {
	target  *models.NotificationTarget
	event   Event
	title   string
	message string
}

// targetLane serializes jobs for one (target, run/resource) pair. A run's
// progress and completion therefore leave qui in enqueue order at that target,
// while a different run gets a different lane and sends concurrently. The lane
// also isolates targets: one lane's slow or failing webhook cannot delay
// another target's lane for the same event.
type targetLane struct {
	id   string
	ch   chan targetJob
	stop chan struct{}
}

// runRouter is the sole owner of the lanes map. Events come in single-file
// from the ingress queue and are fanned out to per-target lanes here, so lane
// creation and idle retirement never race. It returns only after every event
// accepted before shutdown has been routed and every lane has retired.
func (s *Service) runRouter() {
	defer s.wg.Done()

	for {
		select {
		case <-s.stopped:
			// accepting is already false (see beginShutdown), so every event
			// that can still appear in the queue is already committed there.
			s.drainIngress()
			for len(s.lanes) > 0 {
				s.retireLane(<-s.laneIdle)
			}
			close(s.routed)
			return
		case event := <-s.queue:
			s.route(event)
		case lane := <-s.laneIdle:
			s.retireLane(lane)
		}
	}
}

func (s *Service) drainIngress() {
	for {
		select {
		case event := <-s.queue:
			s.route(event)
		default:
			return
		}
	}
}

// retireLane removes a drained lane only if it is still the registered one and
// its mailbox is empty. Otherwise the worker keeps draining and offers
// retirement again.
func (s *Service) retireLane(lane *targetLane) {
	current, ok := s.lanes[lane.id]
	if !ok || current != lane || len(lane.ch) != 0 {
		return
	}
	delete(s.lanes, lane.id)
	close(lane.stop)
}

// route fans one event out to all subscribing targets. Fan-out is independent
// per target, so a timeout or protocol error on one target is delivered to and
// logged for that lane alone.
func (s *Service) route(event Event) {
	jobs, err := s.jobsForEvent(event)
	if err != nil {
		s.logger.Error().Err(err).Str("event", string(event.Type)).Msg("notifications: failed to list targets")
		return
	}
	if len(jobs) == 0 {
		return
	}

	laneKey := eventLaneKey(event)

	for _, job := range jobs {
		if laneKey == "" {
			// Events with no run or resource identity have no ordering
			// relationship; send them straight out.
			job := job
			s.wg.Add(1)
			go func() {
				defer s.wg.Done()
				s.deliver(job)
			}()
			continue
		}

		id := strconv.Itoa(job.target.ID) + "|" + laneKey
		lane, ok := s.lanes[id]
		if !ok {
			lane = &targetLane{
				id:   id,
				ch:   make(chan targetJob, targetLaneBuffer),
				stop: make(chan struct{}),
			}
			s.lanes[id] = lane
			s.wg.Add(1)
			go s.runLane(lane)
		}

		// Sole sender is this goroutine; a full lane signals a dead target for
		// one run, and blocking here is the backpressure that keeps accepted
		// events observable instead of growing an overflow pile.
		lane.ch <- job
	}
}

// runLane delivers jobs in FIFO order for one (target, run/resource) pair and
// retires through the router when its mailbox has drained.
func (s *Service) runLane(lane *targetLane) {
	defer s.wg.Done()

	for {
		select {
		case job := <-lane.ch:
			s.deliver(job)
			s.deliverReady(lane)
		case <-s.routed:
			return
		}

		// Mailbox drained: offer retirement, then wait for either the router
		// confirming it or a job arriving before the offer was processed.
		select {
		case s.laneIdle <- lane:
		case <-s.routed:
			return
		}

		select {
		case <-lane.stop:
			return
		case job := <-lane.ch:
			s.deliver(job)
		case <-s.routed:
			return
		}
	}
}

func (s *Service) deliverReady(lane *targetLane) {
	for {
		select {
		case job := <-lane.ch:
			s.deliver(job)
		default:
			return
		}
	}
}

// jobsForEvent resolves the enabled targets for an event and renders each
// target's message exactly as the old fan-out did: event-type filtering, empty
// message skipping, and per-scheme formatting are unchanged. Store access uses
// a detached context so a canceled request can no longer take an accepted
// lifecycle event down with it.
func (s *Service) jobsForEvent(event Event) ([]targetJob, error) {
	ctx := context.Background()

	targets, err := s.store.ListEnabled(ctx)
	if err != nil {
		return nil, err
	}

	// The instance label is target-independent; resolve it once here instead
	// of once per target inside formatEvent as the old fan-out did.
	if strings.TrimSpace(event.InstanceName) == "" {
		event.InstanceName = s.resolveInstanceLabel(ctx, event)
	}

	jobs := make([]targetJob, 0, len(targets))
	for _, target := range targets {
		if !allowsEvent(target.EventTypes, event.Type) {
			continue
		}

		title, message := s.formatEvent(ctx, event, targetScheme(target.URL) != "notifiarrapi")
		if strings.TrimSpace(message) == "" {
			continue
		}

		jobs = append(jobs, targetJob{
			target:  target,
			event:   event,
			title:   title,
			message: message,
		})
	}

	return jobs, nil
}

// dispatchWithoutRouter preserves behavior for a Service that was never
// initialized with a queue (zero value in tests): one best-effort, sequential
// fan-out on its own goroutine.
func (s *Service) dispatchWithoutRouter(event Event) {
	if s.store == nil {
		return
	}

	jobs, err := s.jobsForEvent(event)
	if err != nil {
		s.logger.Error().Err(err).Str("event", string(event.Type)).Msg("notifications: failed to list targets")
		return
	}
	for _, job := range jobs {
		s.deliver(job)
	}
}

func (s *Service) deliver(job targetJob) {
	if err := s.send(context.Background(), job.target, job.event, job.title, job.message); err != nil {
		// Redact: shoutrrr errors embed the post URL, which carries the
		// webhook token / bot token for most services.
		logEventContext(s.logger.Error(), job.event).
			Str("error", redact.String(err.Error())).
			Str("target", job.target.Name).
			Str("event", string(job.event.Type)).
			Msg("notifications: send failed")
		return
	}

	logEventContext(s.logger.Debug(), job.event).
		Str("target", job.target.Name).
		Str("event", string(job.event.Type)).
		Msg("notifications: delivered")
}

// eventLaneKey identifies the run or resource whose events must stay ordered
// at a target. Events sharing a key serialize per target; distinct keys send in
// parallel. An empty key means the event belongs to no tracked run or resource.
func eventLaneKey(event Event) string {
	switch {
	case event.BackupRunID > 0:
		return "backup:" + strconv.FormatInt(event.BackupRunID, 10)
	case event.DirScanRunID > 0:
		return "dirscan:" + strconv.FormatInt(event.DirScanRunID, 10)
	case event.OrphanScanRunID > 0:
		return "orphanscan:" + strconv.FormatInt(event.OrphanScanRunID, 10)
	case event.CrossSeed != nil && event.CrossSeed.RunID > 0:
		return "crossseed:" + strconv.FormatInt(event.CrossSeed.RunID, 10)
	}

	// Torrent lifecycle is per torrent on an instance.
	if hash := strings.ToLower(strings.TrimSpace(event.TorrentHash)); hash != "" {
		return "torrent:" + strconv.Itoa(event.InstanceID) + ":" + hash
	}

	// Automation evaluations run per instance; keep actions-applied and
	// run-failed messages for one instance ordered.
	if event.InstanceID > 0 {
		switch event.Type {
		case EventAutomationsActionsApplied, EventAutomationsRunFailed:
			return "automation:" + strconv.Itoa(event.InstanceID)
		}
	}

	// Per-torrent cross-seed search and webhook events carry the torrent name
	// rather than a run id.
	if event.CrossSeed != nil {
		if name := strings.ToLower(strings.TrimSpace(event.TorrentName)); name != "" {
			return "crossseed-torrent:" + strconv.Itoa(event.InstanceID) + ":" + name
		}
	}

	return ""
}

// logEventContext attaches the run or resource identity of an event to a log
// entry so accepted events have an observable outcome and rejections can name
// the start or completion that did not go out.
func logEventContext(entry *zerolog.Event, event Event) *zerolog.Event {
	if event.InstanceID > 0 {
		entry.Int("instance", event.InstanceID)
	}
	if event.BackupRunID > 0 {
		entry.Int64("backup_run", event.BackupRunID)
	}
	if event.DirScanRunID > 0 {
		entry.Int64("dir_scan_run", event.DirScanRunID)
	}
	if event.OrphanScanRunID > 0 {
		entry.Int64("orphan_scan_run", event.OrphanScanRunID)
	}
	if event.CrossSeed != nil && event.CrossSeed.RunID > 0 {
		entry.Int64("cross_seed_run", event.CrossSeed.RunID)
	}
	if hash := strings.TrimSpace(event.TorrentHash); hash != "" {
		entry.Str("torrent", hash)
	}
	return entry
}
