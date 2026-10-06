// Copyright (c) 2025-2026, s0up and the autobrr contributors.
// SPDX-License-Identifier: GPL-2.0-or-later

package logstream

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestHub_Write(t *testing.T) {
	hub := NewHub(10)

	// Write some lines
	for i := range 5 {
		hub.Write("line " + string(rune('0'+i)))
	}

	if hub.Count() != 5 {
		t.Errorf("expected count 5, got %d", hub.Count())
	}
}

func TestHub_RingBuffer(t *testing.T) {
	hub := NewHub(5)

	// Write more lines than buffer size
	for i := range 10 {
		hub.Write("line " + string(rune('0'+i)))
	}

	// Should only have last 5 lines
	if hub.Count() != 5 {
		t.Errorf("expected count 5, got %d", hub.Count())
	}

	history := hub.History(10) // Request more than available
	if len(history) != 5 {
		t.Errorf("expected 5 lines, got %d", len(history))
	}

	// Verify the content is the last 5 lines
	for i, entry := range history {
		expected := "line " + string(rune('5'+i))
		if entry.Line != expected {
			t.Errorf("expected %q, got %q", expected, entry.Line)
		}
	}

	// Sequence numbers stay contiguous even after wrap.
	for i, entry := range history {
		if want := uint64(6 + i); entry.Seq != want {
			t.Errorf("entry %d: expected seq %d, got %d", i, want, entry.Seq)
		}
	}
}

func TestHub_HistoryPartial(t *testing.T) {
	hub := NewHub(100)

	// Write 10 lines
	for i := range 10 {
		hub.Write("line " + string(rune('0'+i)))
	}

	// Request only 3 lines
	history := hub.History(3)
	if len(history) != 3 {
		t.Errorf("expected 3 lines, got %d", len(history))
	}

	// Should be the last 3 lines
	expected := []string{"line 7", "line 8", "line 9"}
	for i, entry := range history {
		if entry.Line != expected[i] {
			t.Errorf("expected %q, got %q", expected[i], entry.Line)
		}
	}
}

func TestHub_OpenReceivesLiveEntries(t *testing.T) {
	hub := NewHub(100)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	sub, replay, _ := hub.Open(ctx, nil, 100)
	if len(replay) != 0 {
		t.Fatalf("expected empty replay, got %d", len(replay))
	}

	// Write a line after opening
	hub.Write("test line")

	select {
	case entry := <-sub.Channel():
		if entry.Line != "test line" {
			t.Errorf("expected 'test line', got %q", entry.Line)
		}
		if entry.Seq != 1 {
			t.Errorf("expected seq 1, got %d", entry.Seq)
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("timeout waiting for line")
	}
}

func TestHub_Unsubscribe(t *testing.T) {
	hub := NewHub(100)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sub, _, _ := hub.Open(ctx, nil, 100)

	if hub.SubscriberCount() != 1 {
		t.Errorf("expected 1 subscriber, got %d", hub.SubscriberCount())
	}

	hub.Unsubscribe(sub)

	if hub.SubscriberCount() != 0 {
		t.Errorf("expected 0 subscribers, got %d", hub.SubscriberCount())
	}

	// Channel should be closed
	_, ok := <-sub.Channel()
	if ok {
		t.Error("expected channel to be closed")
	}
}

func TestHub_ContextCancel(t *testing.T) {
	hub := NewHub(100)
	ctx, cancel := context.WithCancel(context.Background())

	sub, _, _ := hub.Open(ctx, nil, 100)

	if hub.SubscriberCount() != 1 {
		t.Errorf("expected 1 subscriber, got %d", hub.SubscriberCount())
	}

	cancel()

	// Give time for the goroutine to process the cancellation
	time.Sleep(50 * time.Millisecond)

	if hub.SubscriberCount() != 0 {
		t.Errorf("expected 0 subscribers after context cancel, got %d", hub.SubscriberCount())
	}

	// Verify Done channel is closed
	select {
	case <-sub.Done():
		// Expected
	default:
		t.Error("expected Done channel to be closed")
	}
}

func TestHub_SlowConsumerSignalsGap(t *testing.T) {
	hub := NewHub(100)

	sub, _, _ := hub.Open(t.Context(), nil, 100)

	// Fill up the subscriber's buffer (100 entries) plus more
	for range 200 {
		hub.Write("line")
	}

	select {
	case <-sub.Gap():
		// Expected: overflow is observable instead of a silent drop.
	case <-time.After(100 * time.Millisecond):
		t.Error("expected gap signal for slow consumer")
	}

	// Drain whatever made it into the channel.
	drained := 0
	for {
		select {
		case <-sub.Channel():
			drained++
		default:
			hub.Unsubscribe(sub)
			if drained > DefaultSubscriberBuffer {
				t.Errorf("expected at most %d messages, got %d", DefaultSubscriberBuffer, drained)
			}
			return
		}
	}
}

func TestHub_ConcurrentWrite(t *testing.T) {
	hub := NewHub(1000)

	sub, _, _ := hub.Open(t.Context(), nil, 1000)
	defer hub.Unsubscribe(sub)

	var wg sync.WaitGroup
	numWriters := 10
	linesPerWriter := 100

	for range numWriters {
		wg.Go(func() {
			for range linesPerWriter {
				hub.Write("line")
			}
		})
	}

	wg.Wait()

	// Verify count
	if hub.Count() != 1000 {
		t.Errorf("expected count 1000, got %d", hub.Count())
	}
}

func TestHub_HistoryEmpty(t *testing.T) {
	hub := NewHub(100)

	history := hub.History(10)
	if history != nil {
		t.Errorf("expected nil history for empty hub, got %v", history)
	}
}

func TestHub_DefaultSize(t *testing.T) {
	hub := NewHub(0)

	// Should use default size
	for range DefaultBufferSize + 100 {
		hub.Write("line")
	}

	if hub.Count() != DefaultBufferSize {
		t.Errorf("expected count %d, got %d", DefaultBufferSize, hub.Count())
	}
}

func TestHub_FreshOpenReplaysHistoryAndHandsOff(t *testing.T) {
	hub := NewHub(100)
	for range 10 {
		hub.Write("line")
	}

	sub, replay, reset := hub.Open(t.Context(), nil, 5)
	defer hub.Unsubscribe(sub)
	if reset != "" {
		t.Fatalf("fresh open must not reset, got %q", reset)
	}
	if len(replay) != 5 {
		t.Fatalf("expected 5 replay entries, got %d", len(replay))
	}
	lastReplaySeq := replay[len(replay)-1].Seq

	// The first live entry must be exactly one past the last replayed one:
	// no duplicate and no gap at the handoff.
	hub.Write("after open")
	select {
	case entry := <-sub.Channel():
		if entry.Seq != lastReplaySeq+1 {
			t.Fatalf("expected first live seq %d, got %d", lastReplaySeq+1, entry.Seq)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("timeout waiting for live entry")
	}
}

func TestHub_ResumeFromCursor(t *testing.T) {
	hub := NewHub(100)
	for range 20 {
		hub.Write("line")
	}

	after := &Cursor{Epoch: hub.Epoch(), Seq: 10}
	sub, replay, reset := hub.Open(t.Context(), after, 100)
	defer hub.Unsubscribe(sub)
	if reset != "" {
		t.Fatalf("valid cursor must not reset, got %q", reset)
	}
	if len(replay) != 10 {
		t.Fatalf("expected 10 replay entries (seq 11..20), got %d", len(replay))
	}
	for i, entry := range replay {
		if want := uint64(11 + i); entry.Seq != want {
			t.Fatalf("replay entry %d: expected seq %d, got %d", i, want, entry.Seq)
		}
	}

	// Live continues right after replay.
	hub.Write("line")
	select {
	case entry := <-sub.Channel():
		if entry.Seq != 21 {
			t.Fatalf("expected live seq 21, got %d", entry.Seq)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("timeout waiting for live entry")
	}
}

func TestHub_ResumeFromLatestCursorReplaysNothing(t *testing.T) {
	hub := NewHub(100)
	for range 5 {
		hub.Write("line")
	}

	after := &Cursor{Epoch: hub.Epoch(), Seq: 5}
	sub, replay, reset := hub.Open(t.Context(), after, 100)
	defer hub.Unsubscribe(sub)
	if reset != "" {
		t.Fatalf("resume at latest must not reset, got %q", reset)
	}
	if len(replay) != 0 {
		t.Fatalf("expected empty replay, got %d", len(replay))
	}
}

func TestHub_ResumeFromExpiredCursorResets(t *testing.T) {
	hub := NewHub(5)
	for i := range 10 {
		hub.Write("line " + string(rune('0'+i)))
	}
	// Ring now holds seq 6..10; seq 2 is gone.
	after := &Cursor{Epoch: hub.Epoch(), Seq: 2}

	sub, replay, reset := hub.Open(t.Context(), after, 3)
	defer hub.Unsubscribe(sub)
	if reset != ResetExpired {
		t.Fatalf("expected %q, got %q", ResetExpired, reset)
	}
	if len(replay) != 3 {
		t.Fatalf("expected fresh 3-entry snapshot, got %d", len(replay))
	}
	if replay[0].Seq != 8 || replay[2].Seq != 10 {
		t.Fatalf("expected seq 8..10, got %d..%d", replay[0].Seq, replay[2].Seq)
	}
}

func TestHub_ResumeFromForeignEpochResets(t *testing.T) {
	hub := NewHub(100)
	for range 5 {
		hub.Write("line")
	}
	after := &Cursor{Epoch: "0123456789abcdef", Seq: 5}

	_, replay, reset := hub.Open(t.Context(), after, 100)
	if reset != ResetEpoch {
		t.Fatalf("expected %q, got %q", ResetEpoch, reset)
	}
	if len(replay) != 5 {
		t.Fatalf("expected full history snapshot, got %d", len(replay))
	}
}

func TestHub_ResumeFromFutureCursorResets(t *testing.T) {
	hub := NewHub(100)
	for range 5 {
		hub.Write("line")
	}
	after := &Cursor{Epoch: hub.Epoch(), Seq: 999}

	_, replay, reset := hub.Open(t.Context(), after, 100)
	if reset != ResetFuture {
		t.Fatalf("expected %q, got %q", ResetFuture, reset)
	}
	if len(replay) != 5 {
		t.Fatalf("expected full history snapshot, got %d", len(replay))
	}
}

func TestParseCursor(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want Cursor
		ok   bool
	}{
		{"valid", "0123456789abcdef-42", Cursor{Epoch: "0123456789abcdef", Seq: 42}, true},
		{"empty", "", Cursor{}, false},
		{"no separator", "0123456789abcdef42", Cursor{}, false},
		{"short epoch", "0123-42", Cursor{}, false},
		{"non-hex epoch", "g123456789abcdef-42", Cursor{}, false},
		{"zero seq", "0123456789abcdef-0", Cursor{}, false},
		{"bad seq", "0123456789abcdef-x", Cursor{}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := ParseCursor(tt.in)
			if ok != tt.ok {
				t.Fatalf("ok = %v, want %v", ok, tt.ok)
			}
			if ok && got != tt.want {
				t.Fatalf("got %+v, want %+v", got, tt.want)
			}
		})
	}
}

func TestCursorStringRoundTrip(t *testing.T) {
	c := Cursor{Epoch: NewHub(1).Epoch(), Seq: 1234}
	parsed, ok := ParseCursor(c.String())
	if !ok {
		t.Fatal("expected cursor to parse")
	}
	if parsed != c {
		t.Fatalf("round trip mismatch: %+v vs %+v", parsed, c)
	}
}
