// Copyright (c) 2025-2026, s0up and the autobrr contributors.
// SPDX-License-Identifier: GPL-2.0-or-later

// Package logstream provides a thread-safe log broadcasting system with a ring buffer
// for log history and SSE-based streaming to subscribers.
package logstream

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"strconv"
	"strings"
	"sync"
)

const (
	// DefaultBufferSize is the default number of log lines to keep in the ring buffer.
	DefaultBufferSize = 1000
	// DefaultSubscriberBuffer is the buffer size for each subscriber's channel.
	DefaultSubscriberBuffer = 100
)

// Entry is one buffered log line with its monotonically increasing sequence number.
type Entry struct {
	Seq  uint64
	Line string
}

// Cursor identifies an exact position in a hub's event stream: the hub's epoch
// (unique per process/hub) plus the sequence number of the last delivered entry.
type Cursor struct {
	Epoch string
	Seq   uint64
}

// String renders the cursor as "epoch-seq", the wire form used in SSE id fields.
func (c Cursor) String() string {
	return c.Epoch + "-" + strconv.FormatUint(c.Seq, 10)
}

// ParseCursor parses the "epoch-seq" wire form. The epoch must be 16 hex
// characters and the sequence number must be positive.
func ParseCursor(s string) (Cursor, bool) {
	epoch, seqStr, ok := strings.Cut(s, "-")
	if !ok || len(epoch) != 16 {
		return Cursor{}, false
	}
	if _, err := hex.DecodeString(epoch); err != nil {
		return Cursor{}, false
	}
	seq, err := strconv.ParseUint(seqStr, 10, 64)
	if err != nil || seq == 0 {
		return Cursor{}, false
	}
	return Cursor{Epoch: epoch, Seq: seq}, true
}

func newEpoch() string {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		// crypto/rand failure means the runtime is broken; fall back to a
		// fixed epoch rather than panicking inside the log writer.
		return "0000000000000000"
	}
	return hex.EncodeToString(b[:])
}

// ResetReason explains why a subscriber cannot continue from the requested
// cursor and receives a fresh history snapshot instead.
type ResetReason string

const (
	// ResetEpoch means the cursor comes from a different hub (e.g. server restart).
	ResetEpoch ResetReason = "restart"
	// ResetExpired means the cursor's entries have already left the ring buffer.
	ResetExpired ResetReason = "cursor_expired"
	// ResetFuture means the cursor's sequence number is ahead of anything written.
	ResetFuture ResetReason = "cursor_ahead"
)

// Hub manages log broadcasting to subscribers with a ring buffer for history.
type Hub struct {
	mu          sync.RWMutex
	epoch       string
	buffer      []Entry
	bufferSize  int
	writePos    int
	count       int
	nextSeq     uint64
	subscribers map[*Subscriber]struct{}
}

// Subscriber represents a log stream subscriber with a buffered channel.
type Subscriber struct {
	ch      chan Entry
	gap     chan struct{}
	gapOnce sync.Once
	ctx     context.Context
	cancel  context.CancelFunc
}

// NewHub creates a new Hub with the specified buffer size.
// If size <= 0, DefaultBufferSize is used.
func NewHub(size int) *Hub {
	if size <= 0 {
		size = DefaultBufferSize
	}
	return &Hub{
		epoch:       newEpoch(),
		buffer:      make([]Entry, size),
		bufferSize:  size,
		subscribers: make(map[*Subscriber]struct{}),
	}
}

// Epoch returns the hub's epoch identifier. A new hub (e.g. after a server
// restart) has a different epoch, so clients can detect an unrecoverable cursor.
func (h *Hub) Epoch() string {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.epoch
}

// Write appends a log line to the ring buffer and broadcasts it to subscribers.
// Lines that exceed a subscriber's channel capacity mark that subscriber as
// gapped instead of being dropped silently.
func (h *Hub) Write(line string) {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.nextSeq++
	entry := Entry{Seq: h.nextSeq, Line: line}

	// Write to ring buffer
	h.buffer[h.writePos] = entry
	h.writePos = (h.writePos + 1) % h.bufferSize
	if h.count < h.bufferSize {
		h.count++
	}

	// Broadcast to subscribers (non-blocking, signal gap on full).
	// Must be inside lock to prevent send-on-closed-channel panic
	// when Unsubscribe closes a channel concurrently.
	for sub := range h.subscribers {
		select {
		case sub.ch <- entry:
		default:
			// Slow consumer: the missed range is unrecoverable on this stream;
			// the subscriber reconnects and replays from the ring if possible.
			sub.markGap()
		}
	}
}

// History returns the last n entries from the ring buffer in write order.
// If n <= 0 or n > count, returns all available entries.
func (h *Hub) History(n int) []Entry {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.snapshotLocked(n)
}

func (h *Hub) snapshotLocked(n int) []Entry {
	if n <= 0 || n > h.count {
		n = h.count
	}
	if n == 0 {
		return nil
	}

	result := make([]Entry, n)
	start := (h.writePos - n + h.bufferSize) % h.bufferSize
	for i := range n {
		result[i] = h.buffer[(start+i)%h.bufferSize]
	}
	return result
}

// entriesSinceLocked returns buffered entries with sequence numbers greater
// than seq, in write order. Caller must hold h.mu.
func (h *Hub) entriesSinceLocked(seq uint64) []Entry {
	if h.count == 0 {
		return nil
	}
	start := (h.writePos - h.count + h.bufferSize) % h.bufferSize
	result := make([]Entry, 0, h.count)
	for i := range h.count {
		entry := h.buffer[(start+i)%h.bufferSize]
		if entry.Seq > seq {
			result = append(result, entry)
		}
	}
	return result
}

// Open atomically registers a subscriber and computes the entries to replay
// so the replay-to-live handoff has no duplicates and no gaps: entries in
// replay end at the exact sequence the live channel starts at.
//
// after == nil replays the last limit entries (fresh connection). A valid
// cursor from the current epoch replays every buffered entry after it; when
// the cursor cannot be honoured (different epoch, expired, or ahead) a reset
// reason is returned together with a fresh last-limit snapshot. The caller
// must tell the client to discard its previous state before sending replay.
func (h *Hub) Open(ctx context.Context, after *Cursor, limit int) (*Subscriber, []Entry, ResetReason) {
	sub := h.newSubscriber(ctx)

	h.mu.Lock()
	defer h.mu.Unlock()

	h.subscribers[sub] = struct{}{}

	latest := h.nextSeq
	var reset ResetReason
	switch {
	case after == nil:
		// Fresh connection: no prior client state to invalidate.
	case after.Epoch != h.epoch:
		reset = ResetEpoch
	case after.Seq > latest:
		reset = ResetFuture
	case h.count == 0 || after.Seq < latest-uint64(h.count)+1:
		// The entry right after the cursor is no longer in the ring.
		reset = ResetExpired
	}

	if reset != "" {
		return sub, h.snapshotLocked(limit), reset
	}
	if after == nil {
		return sub, h.snapshotLocked(limit), ""
	}
	return sub, h.entriesSinceLocked(after.Seq), ""
}

// Unsubscribe removes a subscriber and closes its channel.
func (h *Hub) Unsubscribe(sub *Subscriber) {
	h.mu.Lock()
	if _, ok := h.subscribers[sub]; ok {
		delete(h.subscribers, sub)
		sub.cancel()
		close(sub.ch)
	}
	h.mu.Unlock()
}

func (h *Hub) newSubscriber(ctx context.Context) *Subscriber {
	subCtx, cancel := context.WithCancel(ctx)
	sub := &Subscriber{
		ch:     make(chan Entry, DefaultSubscriberBuffer),
		gap:    make(chan struct{}),
		ctx:    subCtx,
		cancel: cancel,
	}

	// Auto-unsubscribe when context is done
	go func() {
		<-subCtx.Done()
		h.Unsubscribe(sub)
	}()

	return sub
}

func (s *Subscriber) markGap() {
	s.gapOnce.Do(func() {
		close(s.gap)
	})
}

// Channel returns the subscriber's log entry channel.
func (s *Subscriber) Channel() <-chan Entry {
	return s.ch
}

// Gap is closed once when entries could not be delivered to this subscriber.
// The current stream can no longer be made contiguous; the client must
// reconnect from its last cursor.
func (s *Subscriber) Gap() <-chan struct{} {
	return s.gap
}

// Done returns the subscriber's context done channel.
func (s *Subscriber) Done() <-chan struct{} {
	return s.ctx.Done()
}

// SubscriberCount returns the current number of subscribers.
func (h *Hub) SubscriberCount() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.subscribers)
}

// Count returns the number of lines currently in the buffer.
func (h *Hub) Count() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.count
}
