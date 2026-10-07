// Copyright (c) 2025-2026, s0up and the autobrr contributors.
// SPDX-License-Identifier: GPL-2.0-or-later

package backups

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	qbt "github.com/autobrr/go-qbittorrent"
	"github.com/rs/zerolog/log"
	"golang.org/x/sync/errgroup"

	"github.com/autobrr/qui/internal/models"
	"github.com/autobrr/qui/internal/qbittorrent"
	"github.com/autobrr/qui/internal/services/activity"
	"github.com/autobrr/qui/internal/services/notifications"
	"github.com/autobrr/qui/pkg/torrentname"
)

var (
	// ErrInstanceBusy is returned when a backup is already running for the instance.
	ErrInstanceBusy = errors.New("backup already running for this instance")
	// ErrImportRunNotRetryable is returned when retry is requested for a run
	// that was not created by a manifest import.
	ErrImportRunNotRetryable = errors.New("run is not a manifest import")
	// ErrImportRecoveryActive is returned while an import's blob recovery is
	// still running; a second recovery for the same run is not allowed.
	ErrImportRecoveryActive = errors.New("torrent recovery is already running for this import")
	removeFile              = os.Remove
)

// Config controls background backup scheduling.
type Config struct {
	DataDir         string
	BackupDir       string // backup root; empty means <DataDir>/backups
	PollInterval    time.Duration
	WorkerCount     int
	FailureCooldown time.Duration
	ExportThrottle  time.Duration
}

type BackupProgress struct {
	Current    int
	Total      int
	Percentage float64
}

type Service struct {
	store          *models.BackupStore
	reader         backupReader
	tracker        backupTrackerSource
	categoryWriter backupCategoryMutator
	tagWriter      backupTagMutator
	torrentWriter  backupTorrentMutator
	notifier       notifications.Notifier
	cfg            Config
	root           string // backup root directory; stored paths resolve against it
	cacheDir       string

	jobs   chan job
	wg     sync.WaitGroup
	ctx    context.Context
	cancel context.CancelFunc
	once   sync.Once

	inflight   map[int]int64
	inflightMu sync.Mutex

	progress   map[int64]*BackupProgress
	progressMu sync.RWMutex

	// recovering tracks import runs whose background blob recovery goroutine
	// is live in this process, so a retry cannot start a second writer for
	// the same run. The DB blob_status columns are the durable counterpart;
	// this map only guards in-process concurrency.
	recovering   map[int64]struct{}
	recoveringMu sync.Mutex

	now func() time.Time

	activityPublisher activity.Publisher
}

type backupReader interface {
	GetAllTorrents(ctx context.Context, instanceID int) ([]qbt.Torrent, error)
	GetCategories(ctx context.Context, instanceID int) (map[string]qbt.Category, error)
	GetTags(ctx context.Context, instanceID int) ([]string, error)
	GetInstanceWebAPIVersion(ctx context.Context, instanceID int) (string, error)
	ExportTorrent(ctx context.Context, instanceID int, hash string) ([]byte, string, string, error)
}

type backupCategoryMutator interface {
	CreateCategory(ctx context.Context, instanceID int, name string, path string) error
	EditCategory(ctx context.Context, instanceID int, name string, path string) error
	RemoveCategories(ctx context.Context, instanceID int, categories []string) error
}

type backupTagMutator interface {
	CreateTags(ctx context.Context, instanceID int, tags []string) error
	DeleteTags(ctx context.Context, instanceID int, tags []string) error
}

type backupTorrentMutator interface {
	AddTorrent(ctx context.Context, instanceID int, fileContent []byte, options map[string]string) (*qbt.TorrentAddResponse, error)
	SetCategory(ctx context.Context, instanceID int, hashes []string, category string) error
	SetTags(ctx context.Context, instanceID int, hashes []string, tags string) error
	ResumeWhenComplete(instanceID int, hashes []string, opts qbittorrent.ResumeWhenCompleteOptions)
	BulkAction(ctx context.Context, instanceID int, hashes []string, action string) error
}

type job struct {
	runID      int64
	instanceID int
	kind       models.BackupRunKind
}

// Manifest captures details about a backup run and its contents for API responses and archived metadata.
type Manifest struct {
	InstanceID   int                                `json:"instanceId"`
	Kind         string                             `json:"kind"`
	GeneratedAt  time.Time                          `json:"generatedAt"`
	TorrentCount int                                `json:"torrentCount"`
	Categories   map[string]models.CategorySnapshot `json:"categories,omitempty"`
	Tags         []string                           `json:"tags,omitempty"`
	Items        []ManifestItem                     `json:"items"`
}

// ManifestItem describes a single torrent contained in a backup archive.
type ManifestItem struct {
	Hash        string   `json:"hash"`
	Name        string   `json:"name"`
	Category    *string  `json:"category,omitempty"`
	SizeBytes   int64    `json:"sizeBytes"`
	ArchivePath string   `json:"archivePath"`
	InfoHashV1  *string  `json:"infohashV1,omitempty"`
	InfoHashV2  *string  `json:"infohashV2,omitempty"`
	Tags        []string `json:"tags,omitempty"`
	TorrentBlob string   `json:"torrentBlob,omitempty"`
	SavePath    string   `json:"savePath,omitempty"`
	// BlobStatus/BlobError are populated on import responses from the
	// per-item recovery state. They are never trusted when importing a
	// manifest: availability is recomputed from the archive and disk.
	BlobStatus string  `json:"blobStatus,omitempty"`
	BlobError  *string `json:"blobError,omitempty"`
}

func NewService(store *models.BackupStore, reader backupReader, cfg Config, notifier notifications.Notifier) *Service {
	if cfg.WorkerCount <= 0 {
		cfg.WorkerCount = 1
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = time.Minute
	}
	if cfg.FailureCooldown <= 0 {
		cfg.FailureCooldown = 10 * time.Minute
	}
	if cfg.ExportThrottle <= 0 {
		cfg.ExportThrottle = 100 * time.Millisecond
	}

	root := strings.TrimSpace(cfg.BackupDir)
	if root == "" && strings.TrimSpace(cfg.DataDir) != "" {
		root = filepath.Join(cfg.DataDir, "backups")
	}

	cacheDir := ""
	if root != "" {
		cacheDir = filepath.Join(root, "torrents")
		if err := os.MkdirAll(cacheDir, 0o755); err != nil {
			log.Warn().Err(err).Str("cacheDir", cacheDir).Msg("Failed to prepare torrent cache directory")
		} else {
			go sweepStaleBlobTemps(cacheDir)
		}
	}

	svc := &Service{
		store:      store,
		reader:     reader,
		notifier:   notifier,
		cfg:        cfg,
		root:       root,
		cacheDir:   cacheDir,
		jobs:       make(chan job, cfg.WorkerCount*2),
		inflight:   make(map[int]int64),
		progress:   make(map[int64]*BackupProgress),
		recovering: make(map[int64]struct{}),
		now:        func() time.Time { return time.Now().UTC() },

		activityPublisher: activity.NopPublisher{},
	}
	if tracker, ok := reader.(backupTrackerSource); ok {
		svc.tracker = tracker
	}
	if writer, ok := reader.(backupCategoryMutator); ok {
		svc.categoryWriter = writer
	}
	if writer, ok := reader.(backupTagMutator); ok {
		svc.tagWriter = writer
	}
	if writer, ok := reader.(backupTorrentMutator); ok {
		svc.torrentWriter = writer
	}

	return svc
}

// SetActivityPublisher wires the qui server-event hub so backup run status
// changes are pushed to connected clients instead of polled. Safe to call once
// at startup.
func (s *Service) SetActivityPublisher(publisher activity.Publisher) {
	if s == nil || publisher == nil {
		return
	}
	s.activityPublisher = publisher
}

// emitRunActivity signals connected clients that a backup run's status changed
// so they refetch instead of polling. Must be called after the state transition
// is persisted and any held lock released.
func (s *Service) emitRunActivity(instanceID int, runID int64) {
	if s == nil || s.activityPublisher == nil {
		return
	}
	s.activityPublisher.Publish(activity.Event{
		Kind:       activity.KindBackupRun,
		InstanceID: instanceID,
		ResourceID: strconv.FormatInt(runID, 10),
	})
}

func normalizeBackupSettings(settings *models.BackupSettings) bool {
	if settings == nil {
		return false
	}

	changed := false

	if settings.CustomPath != nil {
		settings.CustomPath = nil
		changed = true
	}

	if settings.KeepHourly < 0 {
		settings.KeepHourly = 0
		changed = true
	}
	if settings.KeepDaily < 0 {
		settings.KeepDaily = 0
		changed = true
	}
	if settings.KeepWeekly < 0 {
		settings.KeepWeekly = 0
		changed = true
	}
	if settings.KeepMonthly < 0 {
		settings.KeepMonthly = 0
		changed = true
	}
	if settings.HourlyEnabled && settings.KeepHourly < 1 {
		settings.KeepHourly = 1
		changed = true
	}
	if settings.DailyEnabled && settings.KeepDaily < 1 {
		settings.KeepDaily = 1
		changed = true
	}
	if settings.WeeklyEnabled && settings.KeepWeekly < 1 {
		settings.KeepWeekly = 1
		changed = true
	}
	if settings.MonthlyEnabled && settings.KeepMonthly < 1 {
		settings.KeepMonthly = 1
		changed = true
	}

	return changed
}

func (s *Service) normalizeAndPersistSettings(ctx context.Context, settings *models.BackupSettings) bool {
	if settings == nil {
		return false
	}

	changed := normalizeBackupSettings(settings)
	if !changed {
		return false
	}

	if err := s.store.UpsertSettings(ctx, settings); err != nil {
		log.Warn().Err(err).Int("instanceID", settings.InstanceID).Msg("Failed to persist normalized backup settings")
	}

	return true
}

func (s *Service) Start(ctx context.Context) {
	ctx, cancel := context.WithCancel(ctx)
	s.ctx = ctx
	s.cancel = cancel

	// Recover any incomplete backup runs from previous session
	if err := s.recoverIncompleteRuns(ctx); err != nil {
		log.Warn().Err(err).Msg("Failed to recover incomplete backup runs")
	}

	// Reclaim cache blobs stranded by failed runs before workers start
	// writing, but off the startup path so a large cache cannot hold up the
	// HTTP listener. Workers only spawn once the sweep finishes; a canceled
	// ctx stops the sweep mid-walk, and the pre-registered wg count plus the
	// tracked sweep goroutine keep Stop waiting for both.
	s.wg.Add(s.cfg.WorkerCount)
	s.wg.Go(func() {
		s.cleanupOrphanedBlobs(ctx)
		for i := 0; i < s.cfg.WorkerCount; i++ {
			go s.worker(ctx)
		}
	})

	// Check for missed backups and queue exactly one if applicable
	s.wg.Go(func() {
		if err := s.checkMissedBackups(ctx); err != nil {
			if ctx.Err() != nil || errors.Is(err, context.Canceled) {
				log.Debug().Msg("Missed-backup check canceled")
			} else {
				log.Warn().Err(err).Msg("Failed to check for missed backups")
			}
		}
	})

	s.wg.Add(1)
	go s.scheduler(ctx)
}

// recoverIncompleteRuns settles pending/running runs left by a previous
// process. Live backups cannot resume, so they fail; manifest imports can,
// because every item's blob state is persisted and the missing blobs are
// fetched from qBittorrent, so their background recovery is resumed from
// that state instead of failing the run or dropping the missing items.
func (s *Service) recoverIncompleteRuns(ctx context.Context) error {
	incompleteRuns, err := s.store.FindIncompleteRuns(ctx)
	if err != nil {
		return fmt.Errorf("failed to find incomplete runs: %w", err)
	}

	if len(incompleteRuns) == 0 {
		return nil
	}

	log.Info().Int("count", len(incompleteRuns)).Msg("Recovering incomplete backup runs from previous session")

	now := s.now()
	errorMsg := "Backup interrupted by application restart"

	var interruptedRunIDs []int64
	for _, run := range incompleteRuns {
		if run.Kind == models.BackupRunKindImport {
			run := run
			s.wg.Go(func() {
				s.resumeImportRecovery(ctx, run)
			})
			continue
		}
		interruptedRunIDs = append(interruptedRunIDs, run.ID)
	}

	// Process runIDs in chunks to avoid SQLite bind parameter limits
	const chunkSize = 1000
	for i := 0; i < len(interruptedRunIDs); i += chunkSize {
		end := min(i+chunkSize, len(interruptedRunIDs))
		chunk := interruptedRunIDs[i:end]

		if err := s.store.UpdateMultipleRunsStatus(ctx, chunk, models.BackupRunStatusFailed, &now, &errorMsg); err != nil {
			return fmt.Errorf("failed to update incomplete runs: %w", err)
		}
	}

	// Notify connected clients that the interrupted live runs transitioned.
	for _, run := range incompleteRuns {
		if run.Kind == models.BackupRunKindImport {
			continue
		}
		s.emitRunActivity(run.InstanceID, run.ID)
	}
	return nil
}

// resumeImportRecovery continues an import run interrupted mid-recovery.
// Persisted blob_status drives the outcome: pending items are fetched again,
// previously failed items stay failed, available items stay untouched. Runs
// that crashed before their items were inserted have nothing to resume and
// fail deterministically.
func (s *Service) resumeImportRecovery(ctx context.Context, run *models.BackupRun) {
	itemCount, err := s.store.CountItems(ctx, run.ID)
	if err != nil {
		log.Error().Err(err).Int64("runID", run.ID).Msg("Failed to count items while resuming import recovery")
		return
	}
	if itemCount == 0 {
		now := s.now()
		msg := "Import interrupted by application restart before items were saved"
		if err := s.store.UpdateRunMetadata(ctx, run.ID, func(r *models.BackupRun) error {
			r.Status = models.BackupRunStatusFailed
			r.CompletedAt = &now
			r.ErrorMessage = &msg
			return nil
		}); err != nil {
			log.Error().Err(err).Int64("runID", run.ID).Msg("Failed to fail interrupted import run")
		}
		s.emitRunActivity(run.InstanceID, run.ID)
		return
	}

	// Imports from versions before per-item blob_status have every row
	// defaulted to "available", including blobs that were never fetched.
	// Re-verify those files once so a missing blob moves back to pending
	// instead of letting the run report success with files still missing.
	if err := s.reverifyAvailableBlobs(ctx, run.InstanceID, run.ID); err != nil {
		log.Error().Err(err).Int64("runID", run.ID).Msg("Failed to re-verify blobs while resuming import recovery")
	}

	s.runImportRecovery(run.ID, run.InstanceID, []models.BackupItemBlobStatus{models.BackupBlobPending})
}

func (s *Service) isBackupMissed(ctx context.Context, instanceID int, kind models.BackupRunKind, enabled bool, now time.Time) bool {
	if !enabled {
		return false
	}

	// We only consider the most recent successful run as the reference point. Failed/running/pending
	// runs do not count toward the schedule — i.e. a failed run doesn't reset the schedule.
	runs, err := s.store.ListRunsByKind(ctx, instanceID, kind, 10)
	if err != nil {
		if !errors.Is(err, sql.ErrNoRows) {
			log.Warn().Err(err).Int("instanceID", instanceID).Msg("Failed to list runs for missed backup check")
		}
		// On DB error treat as not missed to avoid accidental scheduling
		return false
	}

	// Short-circuit if the latest run is still in flight or within failure cooldown.
	for _, r := range runs {
		if r == nil {
			continue
		}
		switch r.Status {
		case models.BackupRunStatusPending, models.BackupRunStatusRunning:
			return false
		case models.BackupRunStatusFailed, models.BackupRunStatusCanceled:
			if s.cfg.FailureCooldown > 0 {
				ref := r.CompletedAt
				if ref == nil {
					ref = &r.RequestedAt
				}
				if ref != nil && now.Before(ref.Add(s.cfg.FailureCooldown)) {
					return false
				}
			}
		case models.BackupRunStatusSuccess:
			// Success is handled below when selecting schedule reference.
		}
		break
	}

	// Find the most recent successful run
	var refTime *time.Time
	var foundSuccess bool
	for _, r := range runs {
		if r == nil {
			continue
		}
		if strings.EqualFold(string(r.Status), string(models.BackupRunStatusSuccess)) {
			if r.CompletedAt != nil {
				refTime = r.CompletedAt
			} else {
				refTime = &r.RequestedAt
			}
			foundSuccess = true
			break
		}
	}

	// If we found no successful run, consider it missed (first-run semantics)
	if !foundSuccess || refTime == nil {
		return true
	}

	ref := *refTime

	var interval time.Duration
	switch kind {
	case models.BackupRunKindHourly:
		interval = time.Hour
	case models.BackupRunKindDaily:
		interval = 24 * time.Hour
	case models.BackupRunKindWeekly:
		interval = 7 * 24 * time.Hour
	case models.BackupRunKindMonthly:
		next := ref.AddDate(0, 1, 0)
		return !now.Before(next)
	default:
		// Unknown kind — don't consider it missed
		return false
	}

	return !ref.Add(interval).After(now)
}

func (s *Service) checkMissedBackups(ctx context.Context) error {
	settings, err := s.store.ListEnabledSettings(ctx)
	if err != nil {
		return err
	}

	now := s.now()

	for _, cfg := range settings {
		s.normalizeAndPersistSettings(ctx, cfg)

		if !cfg.Enabled {
			continue
		}

		var missedKinds []models.BackupRunKind

		if s.isBackupMissed(ctx, cfg.InstanceID, models.BackupRunKindHourly, cfg.HourlyEnabled, now) {
			missedKinds = append(missedKinds, models.BackupRunKindHourly)
		}
		if s.isBackupMissed(ctx, cfg.InstanceID, models.BackupRunKindDaily, cfg.DailyEnabled, now) {
			missedKinds = append(missedKinds, models.BackupRunKindDaily)
		}
		if s.isBackupMissed(ctx, cfg.InstanceID, models.BackupRunKindWeekly, cfg.WeeklyEnabled, now) {
			missedKinds = append(missedKinds, models.BackupRunKindWeekly)
		}
		if s.isBackupMissed(ctx, cfg.InstanceID, models.BackupRunKindMonthly, cfg.MonthlyEnabled, now) {
			missedKinds = append(missedKinds, models.BackupRunKindMonthly)
		}

		// Queue the first missed backup if any are missed
		if len(missedKinds) > 0 {
			kind := missedKinds[0]
			if _, err := s.QueueRun(ctx, cfg.InstanceID, kind, "startup-recovery"); err != nil {
				if !errors.Is(err, ErrInstanceBusy) {
					log.Warn().Err(err).Int("instanceID", cfg.InstanceID).Str("kind", string(kind)).Msg("Failed to queue missed backup on startup")
				}
			} else {
				log.Info().Int("instanceID", cfg.InstanceID).Str("kind", string(kind)).Msg("Queued missed backup on startup")
			}
		}
	}

	return nil
}

func (s *Service) Stop() {
	s.once.Do(func() {
		if s.cancel != nil {
			s.cancel()
		}
		s.wg.Wait()
	})
}

func (s *Service) worker(ctx context.Context) {
	defer s.wg.Done()

	for {
		select {
		case <-ctx.Done():
			return
		case job := <-s.jobs:
			s.handleJob(ctx, job)
		}
	}
}

func (s *Service) handleJob(ctx context.Context, j job) {
	if s.reader == nil {
		now := s.now()
		msg := "sync manager not configured"
		_ = s.store.UpdateRunMetadata(ctx, j.runID, func(run *models.BackupRun) error {
			run.Status = models.BackupRunStatusFailed
			run.CompletedAt = &now
			run.ErrorMessage = &msg
			return nil
		})
		s.clearInstance(j.instanceID, j.runID)
		log.Error().Int("instanceID", j.instanceID).Msg("Backup run failed: sync manager not configured")
		s.notify(ctx, notifications.Event{
			Type:         notifications.EventBackupFailed,
			InstanceID:   j.instanceID,
			BackupKind:   j.kind,
			BackupRunID:  j.runID,
			ErrorMessage: msg,
			CompletedAt:  &now,
		})
		s.emitRunActivity(j.instanceID, j.runID)
		return
	}

	start := s.now()
	err := s.store.UpdateRunMetadata(ctx, j.runID, func(run *models.BackupRun) error {
		run.Status = models.BackupRunStatusRunning
		run.ErrorMessage = nil
		run.StartedAt = &start
		return nil
	})
	if err != nil {
		s.clearInstance(j.instanceID, j.runID)
		log.Error().Err(err).Int("instanceID", j.instanceID).Msg("Failed to mark backup run as running")
		return
	}
	s.emitRunActivity(j.instanceID, j.runID)

	result, execErr := s.executeBackup(ctx, j)
	if execErr != nil {
		msg := execErr.Error()
		now := s.now()
		_ = s.store.UpdateRunMetadata(ctx, j.runID, func(run *models.BackupRun) error {
			run.Status = models.BackupRunStatusFailed
			run.CompletedAt = &now
			run.ErrorMessage = &msg
			return nil
		})
		log.Error().Err(execErr).Int("instanceID", j.instanceID).Int64("runID", j.runID).Msg("Backup run failed")
		s.notify(ctx, notifications.Event{
			Type:         notifications.EventBackupFailed,
			InstanceID:   j.instanceID,
			BackupKind:   j.kind,
			BackupRunID:  j.runID,
			ErrorMessage: msg,
			StartedAt:    &start,
			CompletedAt:  &now,
		})
		s.emitRunActivity(j.instanceID, j.runID)
	} else {
		now := s.now()
		_ = s.store.UpdateRunMetadata(ctx, j.runID, func(run *models.BackupRun) error {
			run.Status = models.BackupRunStatusSuccess
			run.CompletedAt = &now
			if result.manifestRelPath != nil {
				run.ManifestPath = result.manifestRelPath
			}
			run.TotalBytes = result.totalBytes
			run.TorrentCount = result.torrentCount
			run.CategoryCounts = result.categoryCounts
			run.Categories = result.categories
			run.Tags = result.tags
			run.ErrorMessage = nil
			return nil
		})

		if len(result.items) > 0 {
			if err := s.store.InsertItems(ctx, j.runID, result.items); err != nil {
				log.Warn().Err(err).Int64("runID", j.runID).Msg("Failed to persist backup manifest items")
			}
		}

		if result.settings != nil {
			if err := s.applyRetention(ctx, j.instanceID, result.settings); err != nil {
				log.Warn().Err(err).Int("instanceID", j.instanceID).Msg("Failed to apply backup retention")
			}
		}
		s.notify(ctx, notifications.Event{
			Type:               notifications.EventBackupSucceeded,
			InstanceID:         j.instanceID,
			BackupKind:         j.kind,
			BackupRunID:        j.runID,
			BackupTorrentCount: result.torrentCount,
			StartedAt:          &start,
			CompletedAt:        &now,
		})
		s.emitRunActivity(j.instanceID, j.runID)
	}

	s.clearInstance(j.instanceID, j.runID)
}

func (s *Service) notify(ctx context.Context, event notifications.Event) {
	if s == nil || s.notifier == nil {
		return
	}
	s.notifier.Notify(ctx, event)
}

type backupResult struct {
	manifestRelPath *string
	totalBytes      int64
	torrentCount    int
	categoryCounts  map[string]int
	items           []models.BackupItem
	settings        *models.BackupSettings
	categories      map[string]models.CategorySnapshot
	tags            []string
}

func shouldSkipLiveExportForBackup(torrent qbt.Torrent, hasCachedBlob bool, cacheErr error) bool {
	if hasCachedBlob || cacheErr != nil {
		return false
	}

	return strings.TrimSpace(torrent.InfohashV1) != "" && strings.TrimSpace(torrent.InfohashV2) != ""
}

func (s *Service) executeBackup(ctx context.Context, j job) (*backupResult, error) {
	settings, err := s.store.GetSettings(ctx, j.instanceID)
	if err != nil {
		return nil, fmt.Errorf("failed to load backup settings: %w", err)
	}
	s.normalizeAndPersistSettings(ctx, settings)

	torrents, err := s.reader.GetAllTorrents(ctx, j.instanceID)
	if err != nil {
		return nil, fmt.Errorf("failed to load torrents: %w", err)
	}

	if len(torrents) == 0 {
		return &backupResult{torrentCount: 0, totalBytes: 0, categoryCounts: map[string]int{}, items: nil, settings: settings}, nil
	}

	baseAbs, baseRel, err := s.resolveBasePaths(ctx, settings, j.instanceID)
	if err != nil {
		return nil, err
	}

	if err := os.MkdirAll(baseAbs, 0o755); err != nil {
		return nil, fmt.Errorf("failed to prepare backup directory: %w", err)
	}

	var snapshotCategories map[string]models.CategorySnapshot
	if settings.IncludeCategories {
		categories, err := s.reader.GetCategories(ctx, j.instanceID)
		if err != nil {
			return nil, fmt.Errorf("failed to load categories: %w", err)
		}
		if len(categories) > 0 {
			snapshotCategories = make(map[string]models.CategorySnapshot, len(categories))
			for name, cat := range categories {
				snapshotCategories[name] = models.CategorySnapshot{SavePath: strings.TrimSpace(cat.SavePath)}
			}
		}
	}

	var snapshotTags []string
	if settings.IncludeTags {
		tags, err := s.reader.GetTags(ctx, j.instanceID)
		if err != nil {
			return nil, fmt.Errorf("failed to load tags: %w", err)
		}
		if len(tags) > 0 {
			snapshotTags = append(snapshotTags, tags...)
		}
	}
	if len(snapshotTags) > 1 {
		sort.Strings(snapshotTags)
	}

	webAPIVersion := ""
	patchTrackers := false
	if version, err := s.reader.GetInstanceWebAPIVersion(ctx, j.instanceID); err != nil {
		log.Debug().Err(err).Int("instanceID", j.instanceID).Msg("Unable to determine qBittorrent API version for tracker patching")
	} else {
		webAPIVersion = version
		patchTrackers = shouldInjectTrackerMetadata(version)
	}

	timestamp := s.now().UTC().Format("20060102T150405Z")
	baseSegment := filepath.Base(baseRel)
	baseSegment = strings.TrimSpace(baseSegment)
	if baseSegment == "" || baseSegment == "." || baseSegment == string(filepath.Separator) {
		baseSegment = fmt.Sprintf("instance-%d", j.instanceID)
	}

	slug := safeSegment(baseSegment)
	if slug == "" || slug == "uncategorized" {
		slug = fmt.Sprintf("instance-%d", j.instanceID)
	}

	manifestFileName := fmt.Sprintf("qui-backup_%s_%s_%s_manifest.json", slug, j.kind, timestamp)
	manifestAbsPath := filepath.Join(baseAbs, manifestFileName)
	manifestRelPath := filepath.Join(baseRel, manifestFileName)

	items := make([]models.BackupItem, 0, len(torrents))
	manifestItems := make([]ManifestItem, 0, len(torrents))
	usedPaths := make(map[string]int)
	categoryCounts := make(map[string]int)
	var totalBytes int64

	// Initialize progress tracking
	s.progressMu.Lock()
	s.progress[j.runID] = &BackupProgress{
		Total:      len(torrents),
		Current:    0,
		Percentage: 0,
	}
	s.progressMu.Unlock()

	// Exports run concurrently: qBittorrent serves its WebAPI from a single
	// thread, so a handful of workers lets fast exports drain while one waits
	// out a stall behind another client's large response. Results land in a
	// slice indexed by input position; all order-dependent bookkeeping
	// (usedPaths, categoryCounts, items) happens afterwards in input order so
	// the output is identical to a serial run.
	results := make([]exportedTorrent, len(torrents))
	var exportedCount atomic.Int64

	g, gctx := errgroup.WithContext(ctx)
	indexes := make(chan int)
	g.Go(func() error {
		defer close(indexes)
		for i := range torrents {
			select {
			case indexes <- i:
			case <-gctx.Done():
				return gctx.Err()
			}
		}
		return nil
	})
	for range exportWorkers {
		g.Go(func() error {
			// Per-worker adaptive delay keeps the aggregate request rate
			// bounded by exportWorkers regardless of how the pool schedules.
			var lastExportElapsed time.Duration
			for idx := range indexes {
				res, err := s.exportBackupTorrent(gctx, j, torrents[idx], patchTrackers, webAPIVersion, &lastExportElapsed)
				if err != nil {
					return err
				}
				results[idx] = res
				s.updateProgress(j.runID, int(exportedCount.Add(1)))
			}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}

	for idx, torrent := range torrents {
		res := results[idx]
		if res.skipped {
			continue
		}

		category := strings.TrimSpace(torrent.Category)
		var categoryPtr *string
		if category != "" {
			categoryPtr = &category
			categoryCounts[category]++
		} else {
			categoryCounts["(uncategorized)"]++
		}

		rawTags := ""
		if settings.IncludeTags {
			rawTags = strings.TrimSpace(torrent.Tags)
		}

		archivePath := res.filename
		if settings.IncludeCategories && category != "" {
			archivePath = filepath.ToSlash(filepath.Join(safeSegment(category), res.filename))
		}

		uniquePath := ensureUniquePath(archivePath, usedPaths)
		blobRelPath := res.blobRelPath

		totalBytes += int64(res.dataLen)

		infohashV1 := strings.TrimSpace(torrent.InfohashV1)
		infohashV2 := strings.TrimSpace(torrent.InfohashV2)

		// Capture the per-torrent save path only when it diverges from the
		// category (cross-seed hardlinks, manual relocations, Auto TMM off).
		// Category-managed torrents store nothing here and are placed by their
		// recreated category on restore.
		storeSavePath := ""
		if settings.IncludeSavePaths {
			storeSavePath = resolveBackupSavePath(torrent.SavePath, category, snapshotCategories)
		}

		item := models.BackupItem{
			RunID:       j.runID,
			TorrentHash: torrent.Hash,
			Name:        torrent.Name,
			SizeBytes:   torrent.TotalSize,
		}
		if categoryPtr != nil {
			item.Category = categoryPtr
		}
		if uniquePath != "" {
			rel := uniquePath
			item.ArchiveRelPath = &rel
		}
		if infohashV1 != "" {
			item.InfoHashV1 = &infohashV1
		}
		if infohashV2 != "" {
			item.InfoHashV2 = &infohashV2
		}
		if rawTags != "" {
			item.Tags = &rawTags
		}
		if blobRelPath != nil {
			item.TorrentBlobPath = blobRelPath
		}
		if storeSavePath != "" {
			sp := storeSavePath
			item.SavePath = &sp
		}
		items = append(items, item)

		manifestItem := ManifestItem{
			Hash:        torrent.Hash,
			Name:        torrent.Name,
			ArchivePath: uniquePath,
			SizeBytes:   torrent.TotalSize,
		}
		if categoryPtr != nil {
			manifestItem.Category = categoryPtr
		}
		if infohashV1 != "" {
			manifestItem.InfoHashV1 = &infohashV1
		}
		if infohashV2 != "" {
			manifestItem.InfoHashV2 = &infohashV2
		}
		if rawTags != "" {
			manifestItem.Tags = splitTags(rawTags)
		}
		if blobRelPath != nil {
			manifestItem.TorrentBlob = *blobRelPath
		}
		if storeSavePath != "" {
			manifestItem.SavePath = storeSavePath
		}
		manifestItems = append(manifestItems, manifestItem)
	}

	manifest := Manifest{
		InstanceID:   j.instanceID,
		Kind:         string(j.kind),
		GeneratedAt:  s.now().UTC(),
		TorrentCount: len(manifestItems),
		Categories:   snapshotCategories,
		Tags:         snapshotTags,
		Items:        manifestItems,
	}

	manifestData, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("marshal manifest: %w", err)
	}

	manifestPointer := &manifestRelPath
	if err := os.WriteFile(manifestAbsPath, manifestData, 0o600); err != nil {
		log.Warn().Err(err).Str("path", manifestAbsPath).Msg("Failed to write manifest to disk")
		manifestPointer = nil
	}

	return &backupResult{
		manifestRelPath: manifestPointer,
		totalBytes:      totalBytes,
		torrentCount:    len(manifestItems),
		categoryCounts:  categoryCounts,
		categories:      snapshotCategories,
		tags:            snapshotTags,
		items:           items,
		settings:        settings,
	}, nil
}

// exportWorkers bounds concurrent torrents/export calls during a backup run.
// Kept small on purpose: enough to overlap stalls on qBittorrent's
// single-threaded WebAPI without flooding a struggling instance (#1101).
const exportWorkers = 4

// maxAdaptiveExportDelay caps the adaptive back-pressure delay. A slow export
// usually means the request queued behind another client's large response, not
// that qBittorrent is melting down; sleeping the full stall duration again
// would just double the cost of every stall.
const maxAdaptiveExportDelay = 500 * time.Millisecond

// exportedTorrent is the order-independent result of exporting one torrent.
type exportedTorrent struct {
	skipped     bool
	dataLen     int
	filename    string
	blobRelPath *string
}

// exportBackupTorrent produces the .torrent payload for one torrent (cached
// blob or live export), patches trackers if needed, and persists the blob to
// the cache. It touches no order-dependent backup state, so callers may run it
// concurrently for distinct torrents. lastExportElapsed carries the adaptive
// delay state between consecutive calls on the same worker.
func (s *Service) exportBackupTorrent(ctx context.Context, j job, torrent qbt.Torrent, patchTrackers bool, webAPIVersion string, lastExportElapsed *time.Duration) (exportedTorrent, error) {
	select {
	case <-ctx.Done():
		return exportedTorrent{}, ctx.Err()
	default:
	}

	var (
		data          []byte
		suggestedName string
		trackerDomain string
		blobRelPath   *string
	)

	cachedTorrent, cacheErr := s.loadCachedTorrent(ctx, j.instanceID, torrent.Hash)
	if cacheErr != nil {
		log.Warn().Err(cacheErr).Str("hash", torrent.Hash).Msg("Failed to load cached torrent blob")
	}
	if cachedTorrent != nil {
		data = cachedTorrent.data
		suggestedName = torrent.Name
		trackerDomain = trackerDomainFromTorrent(torrent)
		rel := cachedTorrent.relPath
		blobRelPath = &rel
	}

	if data == nil {
		if shouldSkipLiveExportForBackup(torrent, cachedTorrent != nil, cacheErr) {
			log.Warn().
				Str("hash", torrent.Hash).
				Str("name", torrent.Name).
				Int("instanceID", j.instanceID).
				Msg("Skipping torrent export; live qBittorrent export disabled for hybrid torrents")
			return exportedTorrent{skipped: true}, nil
		}
		if err := adaptiveExportDelay(ctx, s.cfg.ExportThrottle, *lastExportElapsed); err != nil {
			return exportedTorrent{}, err
		}
		exportStart := time.Now()
		var tracker string
		var err error
		data, suggestedName, tracker, err = s.reader.ExportTorrent(ctx, j.instanceID, torrent.Hash)
		*lastExportElapsed = time.Since(exportStart)
		if err != nil {
			if isExportMetadataUnavailable(err) {
				log.Warn().
					Err(err).
					Str("hash", torrent.Hash).
					Str("name", torrent.Name).
					Int("instanceID", j.instanceID).
					Msg("Skipping torrent export; metadata not downloaded yet")
				return exportedTorrent{skipped: true}, nil
			}
			return exportedTorrent{}, fmt.Errorf("export torrent %s: %w", torrent.Hash, err)
		}
		trackerDomain = tracker
	}

	if patchTrackers {
		trackers := gatherTrackerURLs(ctx, s.tracker, j.instanceID, torrent)
		if patched, changed, err := patchTorrentTrackers(data, trackers); err != nil {
			log.Warn().Err(err).Str("hash", torrent.Hash).Int("instanceID", j.instanceID).Msg("Failed to patch exported torrent trackers")
		} else if changed {
			data = patched
			// ensure cached entry is rebuilt with the corrected payload
			blobRelPath = nil
			log.Debug().Str("hash", torrent.Hash).Int("instanceID", j.instanceID).Str("webAPIVersion", webAPIVersion).Msg("Injected tracker metadata into exported torrent")
		}
	}

	// The fallbacks above follow qBittorrent's currently working tracker,
	// which flips between runs for torrents with several announce hosts.
	if domain := announceDomain(data); domain != "" {
		trackerDomain = domain
	}

	if blobRelPath == nil && s.cacheDir != "" {
		sum := sha256.Sum256(data)
		hash := hex.EncodeToString(sum[:])
		blobName := hash + ".torrent"
		subdir := ""
		if len(hash) >= 6 {
			subdir = filepath.Join(hash[0:2], hash[2:4], hash[4:6])
		}
		if err := cacheTorrentBlob(s.cacheDir, filepath.Join(subdir, blobName), data); err != nil {
			return exportedTorrent{}, err
		}
		rel := filepath.ToSlash(filepath.Join("backups", "torrents", subdir, blobName))
		blobRelPath = &rel
	}

	return exportedTorrent{
		dataLen:     len(data),
		filename:    torrentname.SanitizeExportFilename(suggestedName, torrent.Hash, trackerDomain, torrent.Hash),
		blobRelPath: blobRelPath,
	}, nil
}

// blobTmpSeq distinguishes temp file names when several writers cache the
// same payload at once.
var blobTmpSeq atomic.Int64

// blobTmpMinAge shields temp files a live writer is about to rename into
// place from the sweep; anything older is crash litter.
const blobTmpMinAge = time.Hour

// sweepStaleBlobTemps removes *.tmp-* files a crashed run may have left in
// the blob cache. They are never trusted or served; this is litter
// collection, so every failure is best-effort. Runs in the background so a
// large cache does not stall startup; the age guard keeps it clear of temp
// files concurrent writers are about to rename into place.
func sweepStaleBlobTemps(cacheDir string) {
	root, err := os.OpenRoot(cacheDir)
	if err != nil {
		log.Warn().Err(err).Str("cacheDir", cacheDir).Msg("Failed to open torrent cache for temp sweep")
		return
	}
	defer root.Close()

	cutoff := time.Now().Add(-blobTmpMinAge)
	removed := 0
	_ = fs.WalkDir(root.FS(), ".", func(p string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() || !strings.Contains(d.Name(), ".tmp-") {
			return nil
		}
		if info, err := d.Info(); err != nil || info.ModTime().After(cutoff) {
			return nil
		}
		if root.Remove(filepath.FromSlash(p)) == nil {
			removed++
		}
		return nil
	})
	if removed > 0 {
		log.Info().Int("removed", removed).Str("cacheDir", cacheDir).Msg("Removed stale torrent cache temp files")
	}
}

// cacheTorrentBlob persists data at the content-addressed path relBlob inside
// rootDir via a temp file plus rename, so a crash mid-write can never leave a
// truncated file at a path later runs would trust (#2187). An existing
// destination is kept as-is: same address means same content. All access goes
// through os.Root, which guarantees the path cannot escape rootDir. Every
// writer of the blob cache (live export, temp-dir import, background import
// download) must go through this helper.
func cacheTorrentBlob(rootDir, relBlob string, data []byte) error {
	root, err := os.OpenRoot(rootDir)
	if err != nil {
		return fmt.Errorf("open torrent cache: %w", err)
	}
	defer root.Close()

	if _, err := root.Stat(relBlob); err == nil {
		return nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("cache torrent blob: %w", err)
	}
	if dir := filepath.Dir(relBlob); dir != "." {
		if err := root.MkdirAll(dir, 0o755); err != nil {
			return fmt.Errorf("create torrent cache subdir: %w", err)
		}
	}

	// 0o644 matches the manifest and archive writes in this file.
	tmpName := fmt.Sprintf("%s.tmp-%d-%d", relBlob, os.Getpid(), blobTmpSeq.Add(1))
	tmp, err := root.OpenFile(tmpName, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return fmt.Errorf("cache torrent blob: %w", err)
	}
	// Best-effort cleanup: after a successful rename the temp name is gone
	// and this remove is a no-op.
	defer func() { _ = root.Remove(tmpName) }()

	_, err = tmp.Write(data)
	if err == nil {
		err = tmp.Close()
	} else {
		tmp.Close()
	}
	if err != nil {
		return fmt.Errorf("cache torrent blob: %w", err)
	}

	if err := root.Rename(tmpName, relBlob); err != nil {
		// The destination can only exist here if a concurrent worker
		// published the identical payload after the existence check above,
		// so losing the rename race is success. Rename atomically replaces
		// existing files on POSIX and Windows alike; the stat covers any
		// filesystem that refuses the replace regardless.
		if _, statErr := root.Stat(relBlob); statErr == nil {
			return nil
		}
		return fmt.Errorf("cache torrent blob: %w", err)
	}
	return nil
}

// orphanBlobMinAge shields just-written blobs from the startup orphan sweep.
const orphanBlobMinAge = 24 * time.Hour

// cleanupOrphanedBlobs removes cache files no backup item references. Blobs
// are written to the cache during export but only referenced in the database
// once the whole run succeeds, so every failed run strands its already-written
// blobs and nothing else ever deletes them. Runs in the background at startup,
// before any backup workers spawn; the age guard keeps it clear of files
// written around the sweep. All access goes through os.Root so paths cannot
// escape the cache.
func (s *Service) cleanupOrphanedBlobs(ctx context.Context) {
	if s.cacheDir == "" {
		return
	}

	refs, err := s.store.ListTorrentBlobPaths(ctx)
	if err != nil {
		log.Warn().Err(err).Msg("Failed to list referenced torrent blobs; skipping cache cleanup")
		return
	}
	// Stored paths carry the legacy "backups/" prefix or not; ResolveBackupPath
	// maps both forms to the same absolute path under the backup root.
	referenced := make(map[string]struct{}, len(refs))
	for _, rel := range refs {
		if abs := s.ResolveBackupPath(rel); abs != "" {
			referenced[abs] = struct{}{}
		}
	}

	root, err := os.OpenRoot(s.cacheDir)
	if err != nil {
		log.Warn().Err(err).Str("cacheDir", s.cacheDir).Msg("Failed to open torrent cache for cleanup")
		return
	}
	defer root.Close()

	cutoff := s.now().Add(-orphanBlobMinAge)
	removed := 0
	var freed int64
	walkErr := fs.WalkDir(root.FS(), ".", func(p string, d fs.DirEntry, entryErr error) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if entryErr != nil || d.IsDir() {
			return nil
		}
		if _, ok := referenced[filepath.Join(s.cacheDir, filepath.FromSlash(p))]; ok {
			return nil
		}
		info, err := d.Info()
		if err != nil || info.ModTime().After(cutoff) {
			return nil
		}
		if root.Remove(filepath.FromSlash(p)) == nil {
			removed++
			freed += info.Size()
		}
		return nil
	})
	if walkErr != nil {
		log.Warn().Err(walkErr).Str("cacheDir", s.cacheDir).Msg("Torrent cache cleanup stopped early")
	}
	if removed > 0 {
		log.Info().Int("removed", removed).Int64("freedBytes", freed).Str("cacheDir", s.cacheDir).Msg("Removed orphaned torrent cache blobs")
	}
}

// adaptiveExportDelay waits between export API calls with back-pressure.
// The delay is at least minDelay and extends to match the previous export's
// response time when qBittorrent is under load, capped at
// maxAdaptiveExportDelay so a stalled request is not double-charged.
func adaptiveExportDelay(ctx context.Context, minDelay, lastExportDuration time.Duration) error {
	if minDelay <= 0 {
		return nil
	}

	delay := max(minDelay, min(lastExportDuration, maxAdaptiveExportDelay))
	t := time.NewTimer(delay)
	defer t.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

func (s *Service) resolveBasePaths(ctx context.Context, _ *models.BackupSettings, instanceID int) (string, string, error) {
	var baseSegment string
	if name, err := s.store.GetInstanceName(ctx, instanceID); err == nil {
		if trimmed := strings.TrimSpace(name); trimmed != "" {
			baseSegment = safeSegment(trimmed)
		}
	} else if !errors.Is(err, models.ErrInstanceNotFound) {
		return "", "", err
	}

	if baseSegment == "" {
		baseSegment = fmt.Sprintf("instance-%d", instanceID)
	}

	if s.root == "" {
		return "", "", errors.New("backup directory not configured")
	}

	base := filepath.Join("backups", baseSegment)
	abs := filepath.Join(s.root, baseSegment)
	return abs, base, nil
}

func ensureUniquePath(path string, used map[string]int) string {
	if _, exists := used[path]; !exists {
		used[path] = 1
		return path
	}

	ext := filepath.Ext(path)
	base := strings.TrimSuffix(path, ext)

	idx := used[path]
	for {
		candidate := fmt.Sprintf("%s_%d%s", base, idx, ext)
		if _, exists := used[candidate]; !exists {
			used[path] = idx + 1
			used[candidate] = 1
			return candidate
		}
		idx++
	}
}

func safeSegment(input string) string {
	cleaned := strings.TrimSpace(input)
	if cleaned == "" {
		return "uncategorized"
	}

	sanitized := strings.Map(func(r rune) rune {
		switch {
		case r == '/', r == '\\', r == ':', r == '*', r == '?', r == '"', r == '<', r == '>', r == '|':
			return '_'
		case r < 32 || r == 127:
			return -1
		}
		return r
	}, cleaned)

	sanitized = strings.Trim(sanitized, " .")
	if sanitized == "" {
		return "uncategorized"
	}

	sanitized = torrentname.TruncateUTF8(sanitized, 100)
	return sanitized
}

func splitTags(raw string) []string {
	if strings.TrimSpace(raw) == "" {
		return nil
	}
	parts := strings.Split(raw, ",")
	result := make([]string, 0, len(parts))
	for _, part := range parts {
		trimmed := strings.TrimSpace(part)
		if trimmed != "" {
			result = append(result, trimmed)
		}
	}
	if len(result) == 0 {
		return nil
	}
	if len(result) > 1 {
		sort.Strings(result)
	}
	return result
}

func (s *Service) clearInstance(instanceID int, runID int64) {
	s.inflightMu.Lock()
	defer s.inflightMu.Unlock()
	if current, ok := s.inflight[instanceID]; ok && current == runID {
		delete(s.inflight, instanceID)
	}

	s.progressMu.Lock()
	delete(s.progress, runID)
	s.progressMu.Unlock()
}

// updateProgress updates the progress for a run. Progress only moves forward:
// concurrent export workers may report completion counts out of order.
func (s *Service) updateProgress(runID int64, current int) {
	s.progressMu.Lock()
	defer s.progressMu.Unlock()
	if p := s.progress[runID]; p != nil && current > p.Current {
		p.Current = current
		p.Percentage = float64(current) / float64(p.Total) * 100
	}
}

func isExportMetadataUnavailable(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, qbt.ErrTorrentMetadataNotDownloadedYet) {
		return true
	}
	return strings.Contains(err.Error(), "status code: 409")
}

func (s *Service) GetProgress(runID int64) *BackupProgress {
	s.progressMu.RLock()
	defer s.progressMu.RUnlock()
	if p, ok := s.progress[runID]; ok {
		return &BackupProgress{
			Current:    p.Current,
			Total:      p.Total,
			Percentage: p.Percentage,
		}
	}
	return nil
}

func (s *Service) markInstance(instanceID int, runID int64) bool {
	s.inflightMu.Lock()
	defer s.inflightMu.Unlock()
	if _, exists := s.inflight[instanceID]; exists {
		return false
	}
	s.inflight[instanceID] = runID
	return true
}

func (s *Service) QueueRun(ctx context.Context, instanceID int, kind models.BackupRunKind, requestedBy string) (*models.BackupRun, error) {
	if !s.markInstance(instanceID, 0) {
		return nil, ErrInstanceBusy
	}

	run := &models.BackupRun{
		InstanceID:  instanceID,
		Kind:        kind,
		Status:      models.BackupRunStatusPending,
		RequestedBy: requestedBy,
		RequestedAt: s.now(),
	}

	if err := s.store.CreateRun(ctx, run); err != nil {
		s.clearInstance(instanceID, 0)
		return nil, err
	}

	s.inflightMu.Lock()
	s.inflight[instanceID] = run.ID
	s.inflightMu.Unlock()

	select {
	case <-ctx.Done():
		s.clearInstance(instanceID, run.ID)

		cleanupCtx, cancelCleanup := context.WithTimeout(context.Background(), 5*time.Second)
		if err := s.store.DeleteRun(cleanupCtx, run.ID); err != nil {
			log.Warn().Err(err).Int("instanceID", instanceID).Int64("runID", run.ID).Msg("Failed to remove canceled backup run")
		}
		cancelCleanup()
		return nil, ctx.Err()
	case s.jobs <- job{runID: run.ID, instanceID: instanceID, kind: kind}:
	}

	s.emitRunActivity(instanceID, run.ID)
	return run, nil
}

func (s *Service) scheduler(ctx context.Context) {
	defer s.wg.Done()

	ticker := time.NewTicker(s.cfg.PollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := s.scheduleDueBackups(ctx); err != nil {
				log.Warn().Err(err).Msg("Backup scheduler tick failed")
			}
		}
	}
}

func (s *Service) scheduleDueBackups(ctx context.Context) error {
	settings, err := s.store.ListEnabledSettings(ctx)
	if err != nil {
		return err
	}

	now := s.now()

	for _, cfg := range settings {
		s.normalizeAndPersistSettings(ctx, cfg)

		if !cfg.Enabled {
			continue
		}

		evaluate := func(kind models.BackupRunKind, enabled bool) {
			if s.isBackupMissed(ctx, cfg.InstanceID, kind, enabled, now) {
				if _, err := s.QueueRun(ctx, cfg.InstanceID, kind, "scheduler"); err != nil {
					if !errors.Is(err, ErrInstanceBusy) {
						log.Warn().Err(err).Int("instanceID", cfg.InstanceID).Msg("Failed to queue scheduled backup")
					}
				}
			}
		}

		evaluate(models.BackupRunKindHourly, cfg.HourlyEnabled)
		evaluate(models.BackupRunKindDaily, cfg.DailyEnabled)
		evaluate(models.BackupRunKindWeekly, cfg.WeeklyEnabled)
		evaluate(models.BackupRunKindMonthly, cfg.MonthlyEnabled)
	}

	return nil
}

func (s *Service) applyRetention(ctx context.Context, instanceID int, settings *models.BackupSettings) error {
	kinds := []struct {
		kind models.BackupRunKind
		keep int
	}{
		{models.BackupRunKindHourly, settings.KeepHourly},
		{models.BackupRunKindDaily, settings.KeepDaily},
		{models.BackupRunKindWeekly, settings.KeepWeekly},
		{models.BackupRunKindMonthly, settings.KeepMonthly},
	}

	for _, cfg := range kinds {
		runIDs, err := s.store.DeleteRunsOlderThan(ctx, instanceID, cfg.kind, cfg.keep)
		if err != nil {
			return err
		}
		if err := s.cleanupRunFiles(ctx, runIDs); err != nil {
			log.Warn().Err(err).Int("instanceID", instanceID).Msg("Failed to cleanup old backup files")
		}
	}

	return nil
}

func (s *Service) cleanupRunFiles(ctx context.Context, runIDs []int64) error {
	if len(runIDs) == 0 {
		return nil
	}

	// Get all runs in one query
	runs, err := s.store.GetRuns(ctx, runIDs)
	if err != nil {
		return err
	}

	// Create a map for quick lookup
	runMap := make(map[int64]*models.BackupRun)
	for _, run := range runs {
		if run != nil {
			runMap[run.ID] = run
		}
	}

	// Get all items for all runs in one query
	items, err := s.store.ListItemsForRuns(ctx, runIDs)
	if err != nil {
		log.Warn().Err(err).Msg("Failed to list backup items for cleanup")
		items = nil
	}

	var filesToDelete []string
	var itemsToCleanup []*models.BackupItem

	for _, runID := range runIDs {
		run, exists := runMap[runID]
		if !exists {
			// Run was already deleted or not found
			continue
		}

		// Collect items for this run
		var runItems []*models.BackupItem
		for _, item := range items {
			if item.RunID == runID {
				runItems = append(runItems, item)
			}
		}
		itemsToCleanup = append(itemsToCleanup, runItems...)

		if run.ManifestPath != nil {
			if abs := s.ResolveBackupPath(*run.ManifestPath); abs != "" {
				filesToDelete = append(filesToDelete, abs)
			} else {
				log.Warn().Str("path", *run.ManifestPath).Msg("Skipping manifest with unresolvable path during cleanup")
			}
		}
		if run.ArchivePath != nil {
			if abs := s.ResolveBackupPath(*run.ArchivePath); abs != "" {
				filesToDelete = append(filesToDelete, abs)
			} else {
				log.Warn().Str("path", *run.ArchivePath).Msg("Skipping archive with unresolvable path during cleanup")
			}
		}
	}

	// Delete files in parallel
	s.deleteFilesParallel(ctx, filesToDelete)

	// Batch cleanup all runs in database
	if err := s.store.CleanupRuns(ctx, runIDs); err != nil {
		log.Warn().Err(err).Msg("Failed to cleanup runs from database")
	}

	s.cleanupTorrentBlobs(ctx, itemsToCleanup)

	return nil
}

func (s *Service) GetSettings(ctx context.Context, instanceID int) (*models.BackupSettings, error) {
	settings, err := s.store.GetSettings(ctx, instanceID)
	if err != nil {
		return nil, err
	}

	s.normalizeAndPersistSettings(ctx, settings)
	settings.CustomPath = nil

	return settings, nil
}

func (s *Service) UpdateSettings(ctx context.Context, settings *models.BackupSettings) error {
	settings.CustomPath = nil
	normalizeBackupSettings(settings)
	return s.store.UpsertSettings(ctx, settings)
}

func (s *Service) ListRuns(ctx context.Context, instanceID int, limit, offset int) ([]*models.BackupRun, error) {
	return s.store.ListRuns(ctx, instanceID, limit, offset)
}

func (s *Service) GetRun(ctx context.Context, runID int64) (*models.BackupRun, error) {
	return s.store.GetRun(ctx, runID)
}

func (s *Service) GetItem(ctx context.Context, runID int64, hash string) (*models.BackupItem, error) {
	return s.store.GetItemByHash(ctx, runID, hash)
}

func (s *Service) DeleteRun(ctx context.Context, runID int64) error {
	run, err := s.store.GetRun(ctx, runID)
	if err != nil {
		return err
	}

	items, err := s.store.ListItems(ctx, runID)
	if err != nil {
		log.Warn().Err(err).Int64("runID", runID).Msg("Failed to list backup items before delete")
		items = nil
	}

	var filesToDelete []string
	if run.ManifestPath != nil {
		if abs := s.ResolveBackupPath(*run.ManifestPath); abs != "" {
			filesToDelete = append(filesToDelete, abs)
		} else {
			log.Warn().Str("path", *run.ManifestPath).Msg("Skipping manifest with unresolvable path during delete")
		}
	}
	if run.ArchivePath != nil {
		if abs := s.ResolveBackupPath(*run.ArchivePath); abs != "" {
			filesToDelete = append(filesToDelete, abs)
		} else {
			log.Warn().Str("path", *run.ArchivePath).Msg("Skipping archive with unresolvable path during delete")
		}
	}

	s.deleteFilesParallel(ctx, filesToDelete)

	if err := s.store.CleanupRun(ctx, runID); err != nil {
		return err
	}

	s.cleanupTorrentBlobs(ctx, items)

	return nil
}

func (s *Service) DeleteAllRuns(ctx context.Context, instanceID int) error {
	runIDs, err := s.store.ListRunIDs(ctx, instanceID)
	if err != nil {
		return err
	}
	if len(runIDs) == 0 {
		return nil
	}
	for _, runID := range runIDs {
		if err := s.DeleteRun(ctx, runID); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				continue
			}
			return err
		}
	}
	return nil
}

func (s *Service) LoadManifest(ctx context.Context, runID int64) (*Manifest, error) {
	items, err := s.store.ListItems(ctx, runID)
	if err != nil {
		return nil, err
	}

	run, err := s.store.GetRun(ctx, runID)
	if err != nil {
		return nil, err
	}

	manifest := &Manifest{
		InstanceID:   run.InstanceID,
		Kind:         string(run.Kind),
		GeneratedAt:  run.RequestedAt,
		TorrentCount: len(items),
		Categories:   run.Categories,
		Tags:         run.Tags,
		Items:        make([]ManifestItem, 0, len(items)),
	}

	for _, item := range items {
		entry := ManifestItem{
			Hash:        item.TorrentHash,
			Name:        item.Name,
			ArchivePath: "",
			SizeBytes:   item.SizeBytes,
		}
		if item.Category != nil {
			entry.Category = item.Category
		}
		if item.ArchiveRelPath != nil {
			entry.ArchivePath = *item.ArchiveRelPath
		}
		if item.InfoHashV1 != nil {
			entry.InfoHashV1 = item.InfoHashV1
		}
		if item.InfoHashV2 != nil {
			entry.InfoHashV2 = item.InfoHashV2
		}
		if item.Tags != nil {
			entry.Tags = splitTags(*item.Tags)
		}
		if item.TorrentBlobPath != nil {
			entry.TorrentBlob = *item.TorrentBlobPath
		}
		if item.BlobStatus != "" {
			entry.BlobStatus = string(item.BlobStatus)
		}
		entry.BlobError = item.BlobError
		if item.SavePath != nil {
			entry.SavePath = *item.SavePath
		}
		manifest.Items = append(manifest.Items, entry)
	}

	return manifest, nil
}

// ImportManifestFromDir imports a backup manifest with torrent files from
// temp paths. torrentPaths is a map of archivePath -> absolute temp file path
// on disk. The caller is responsible for cleaning up the temp files after
// this returns.
//
// Every item that references a torrent blob is classified up front: shipped
// in the archive, already present on disk, or reusable from another run's
// cache becomes available immediately; anything still missing is persisted
// as pending and recovered from the source qBittorrent instance in the
// background. The run only reports success once no pending or failed items
// remain, so the returned run may be "running"; poll it or wait for the
// activity event for the terminal state.
func (s *Service) ImportManifestFromDir(ctx context.Context, instanceID int, manifestData []byte, requestedBy string, torrentPaths map[string]string) (*models.BackupRun, error) {
	rootDir := s.normalizedImportRoot()

	log.Info().Int("instanceID", instanceID).Str("requestedBy", requestedBy).Int("dataSize", len(manifestData)).Int("torrentPaths", len(torrentPaths)).Str("backupDir", rootDir).Msg("Starting manifest import from dir")

	var manifest Manifest
	if err := json.Unmarshal(manifestData, &manifest); err != nil {
		log.Error().Err(err).Msg("Failed to parse manifest JSON")
		return nil, fmt.Errorf("failed to parse manifest: %w", err)
	}

	log.Info().Int("manifestItemCount", len(manifest.Items)).Int("manifestTorrentCount", manifest.TorrentCount).Msg("Manifest parsed successfully")

	started := s.now()
	run := &models.BackupRun{
		InstanceID:  instanceID,
		Kind:        models.BackupRunKindImport,
		Status:      models.BackupRunStatusRunning,
		RequestedBy: requestedBy,
		RequestedAt: manifest.GeneratedAt,
		StartedAt:   &started,
		Categories:  manifest.Categories,
		Tags:        manifest.Tags,
	}

	if err := s.store.CreateRun(ctx, run); err != nil {
		log.Error().Err(err).Int("instanceID", instanceID).Msg("Failed to create import run")
		return nil, fmt.Errorf("failed to create import run: %w", err)
	}

	log.Info().Int64("runID", run.ID).Msg("Backup run created successfully")
	s.emitRunActivity(instanceID, run.ID)

	items := make([]models.BackupItem, 0, len(manifest.Items))
	pendingCount := 0

	for i, item := range manifest.Items {
		// Skip items with invalid required fields
		if strings.TrimSpace(item.Hash) == "" || strings.TrimSpace(item.Name) == "" {
			continue
		}

		if i > 0 && i%100 == 0 {
			log.Info().Int("processed", i).Int("total", len(manifest.Items)).Int("validSoFar", len(items)).Msg("Processing manifest items progress")
		}

		backupItem := models.BackupItem{
			RunID:       run.ID,
			TorrentHash: item.Hash,
			Name:        item.Name,
			SizeBytes:   item.SizeBytes,
		}

		if item.Category != nil {
			backupItem.Category = item.Category
		}

		if item.ArchivePath != "" {
			backupItem.ArchiveRelPath = &item.ArchivePath
		}

		if item.InfoHashV1 != nil {
			backupItem.InfoHashV1 = item.InfoHashV1
		}

		if item.InfoHashV2 != nil {
			backupItem.InfoHashV2 = item.InfoHashV2
		}

		if len(item.Tags) > 0 {
			tagsStr := strings.Join(item.Tags, ",")
			backupItem.Tags = &tagsStr
		}

		if savePath := strings.TrimSpace(item.SavePath); savePath != "" {
			backupItem.SavePath = &savePath
		}

		if strings.TrimSpace(item.TorrentBlob) != "" {
			status, storedPath := s.classifyImportedBlob(ctx, instanceID, rootDir, item, torrentPaths)
			if storedPath == "" {
				log.Warn().Str("hash", item.Hash).Str("blob", item.TorrentBlob).Msg("Ignoring unsafe TorrentBlob path from manifest")
			} else {
				stored := storedPath
				backupItem.TorrentBlobPath = &stored
				backupItem.BlobStatus = status
				if status == models.BackupBlobPending {
					pendingCount++
				}
			}
		}

		items = append(items, backupItem)
	}

	log.Info().Int("validItems", len(items)).Int("pendingBlobs", pendingCount).Msg("Finished processing manifest items")

	// Validate that we have valid items if the manifest claimed to have any
	if len(items) == 0 && len(manifest.Items) > 0 {
		log.Error().Int("manifestItems", len(manifest.Items)).Msg("Manifest contains items but none are valid")
		return nil, fmt.Errorf("manifest contains %d items but none are valid (missing required hash or name)", len(manifest.Items))
	}

	if len(items) > 0 {
		if err := s.store.InsertItems(ctx, run.ID, items); err != nil {
			log.Error().Err(err).Int64("runID", run.ID).Msg("Failed to insert backup items")
			return nil, fmt.Errorf("failed to insert backup items: %w", err)
		}
		log.Info().Int("insertedItems", len(items)).Int64("runID", run.ID).Msg("Successfully inserted backup items")
	}

	if err := s.store.UpdateRunMetadata(ctx, run.ID, func(r *models.BackupRun) error {
		r.TorrentCount = len(items)
		return nil
	}); err != nil {
		log.Warn().Err(err).Int64("runID", run.ID).Msg("Failed to update torrent count for imported run")
	}

	if pendingCount == 0 {
		// Everything the manifest references is already available: finish now.
		s.finalizeImportRun(instanceID, run.ID)
	} else {
		log.Info().Int("pendingCount", pendingCount).Int64("runID", run.ID).Msg("Starting background recovery of missing torrent blobs")
		s.progressMu.Lock()
		s.progress[run.ID] = &BackupProgress{Current: 0, Total: pendingCount}
		s.progressMu.Unlock()
		s.wg.Go(func() {
			s.runImportRecovery(run.ID, instanceID, []models.BackupItemBlobStatus{models.BackupBlobPending})
		})
	}

	return s.store.GetRun(ctx, run.ID)
}

// normalizedImportRoot returns the backup root with the Windows Git Bash
// "/c/..." mount form translated to a native Windows path.
func (s *Service) normalizedImportRoot() string {
	rootDir := s.root
	if runtime.GOOS == "windows" && strings.HasPrefix(rootDir, "/c/") {
		rootDir = "C:" + strings.ReplaceAll(strings.TrimPrefix(rootDir, "/c"), "/", "\\")
		log.Info().Str("normalizedBackupDir", rootDir).Msg("Normalized backup directory for Windows")
	}
	return rootDir
}

// classifyImportedBlob decides the initial recovery state for one manifest
// item's blob. The returned storedPath is the blob reference to persist; ""
// means the manifest path was unsafe and must not be referenced. Resolution
// order: the file the archive shipped, the file already at the declared
// location, a blob cached for the hash by another run, and finally background
// recovery from qBittorrent.
func (s *Service) classifyImportedBlob(ctx context.Context, instanceID int, rootDir string, item ManifestItem, torrentPaths map[string]string) (models.BackupItemBlobStatus, string) {
	slashRel := backupRelPath(item.TorrentBlob)
	if slashRel == "" {
		return models.BackupBlobAvailable, ""
	}

	declaredStored := path.Join("backups", slashRel)
	rel := filepath.FromSlash(slashRel)
	absPath := filepath.Join(rootDir, rel)

	// 1. Torrent shipped inside the uploaded archive.
	if torrentPaths != nil && item.ArchivePath != "" {
		if tempPath, ok := torrentPaths[item.ArchivePath]; ok {
			if err := s.copyTorrentFromTemp(tempPath, rootDir, rel); err == nil {
				log.Debug().Str("hash", item.Hash).Str("archivePath", item.ArchivePath).Msg("Imported torrent from uploaded archive")
				return models.BackupBlobAvailable, declaredStored
			} else {
				log.Warn().Err(err).Str("hash", item.Hash).Msg("Failed to copy torrent from archive, resolving it another way")
			}
		}
	}

	// 2. Already present at the declared location (idempotent re-import).
	if info, err := os.Stat(absPath); err == nil && !info.IsDir() {
		log.Debug().Str("hash", item.Hash).Str("path", absPath).Msg("Torrent blob already on disk, reusing it")
		return models.BackupBlobAvailable, declaredStored
	}

	// 3. Another run of this instance already cached the same hash.
	// Reference that blob instead of writing a duplicate file.
	if cached, err := s.loadCachedTorrent(ctx, instanceID, item.Hash); err != nil {
		log.Warn().Err(err).Str("hash", item.Hash).Msg("Failed to look up cached torrent blob")
	} else if cached != nil {
		log.Debug().Str("hash", item.Hash).Str("blobPath", cached.relPath).Msg("Reusing cached torrent blob for import")
		return models.BackupBlobAvailable, cached.relPath
	}

	// 4. Missing: background recovery fetches it from qBittorrent.
	return models.BackupBlobPending, declaredStored
}

// copyTorrentFromTemp validates a torrent from the import temp dir and caches
// it at the final blob location through the same atomic write as live exports.
func (s *Service) copyTorrentFromTemp(srcPath, rootDir, relPath string) error {
	data, err := os.ReadFile(srcPath)
	if err != nil {
		return fmt.Errorf("read temp file: %w", err)
	}
	// Valid torrents are at least ~50 bytes and start with 'd' (bencoded dict).
	if len(data) < 50 {
		return fmt.Errorf("invalid torrent data: too small (%d bytes)", len(data))
	}
	if data[0] != 'd' {
		return errors.New("invalid torrent data: not a bencoded dict")
	}

	return cacheTorrentBlob(rootDir, relPath, data)
}

func (s *Service) serviceContext() context.Context {
	if s.ctx != nil {
		return s.ctx
	}
	return context.Background()
}

func (s *Service) tryBeginRecovery(runID int64) bool {
	s.recoveringMu.Lock()
	defer s.recoveringMu.Unlock()
	if _, ok := s.recovering[runID]; ok {
		return false
	}
	s.recovering[runID] = struct{}{}
	return true
}

func (s *Service) endRecovery(runID int64) {
	s.recoveringMu.Lock()
	delete(s.recovering, runID)
	s.recoveringMu.Unlock()
}

// runImportRecovery fetches every unresolved blob of an import run. The work
// set comes from the DB in the given statuses, so retries and post-restart
// resumes never rewrite completed items. It finalizes the run when the set is
// exhausted; on context cancellation it leaves the run running and the
// unprocessed rows pending for the next start to resume.
func (s *Service) runImportRecovery(runID int64, instanceID int, statuses []models.BackupItemBlobStatus) {
	if !s.tryBeginRecovery(runID) {
		log.Info().Int64("runID", runID).Msg("Import blob recovery already in progress, skipping duplicate start")
		return
	}
	defer s.endRecovery(runID)

	// Local DB writes must survive shutdown: a fetched blob whose state flip
	// were canceled would look missing forever. Only the qBittorrent export
	// and the between-items stop observe the shutdown context.
	ctx := s.serviceContext()
	dbCtx := context.Background()

	items, err := s.store.ListItemsForBlobRecovery(dbCtx, runID, statuses)
	if err != nil {
		s.failImportRecovery(instanceID, runID, fmt.Errorf("load pending torrent files: %w", err))
		return
	}
	if len(items) == 0 {
		s.finalizeImportRun(instanceID, runID)
		return
	}

	// A retry flips a failed run back to running; clear terminal markers.
	now := s.now()
	if err := s.store.UpdateRunMetadata(dbCtx, runID, func(r *models.BackupRun) error {
		r.Status = models.BackupRunStatusRunning
		r.CompletedAt = nil
		r.ErrorMessage = nil
		if r.StartedAt == nil {
			started := now
			r.StartedAt = &started
		}
		return nil
	}); err != nil {
		s.failImportRecovery(instanceID, runID, fmt.Errorf("mark import running: %w", err))
		return
	}
	s.emitRunActivity(instanceID, runID)

	s.progressMu.Lock()
	s.progress[runID] = &BackupProgress{Current: 0, Total: len(items)}
	s.progressMu.Unlock()

	log.Info().Int("total", len(items)).Int64("runID", runID).Int("instanceID", instanceID).Msg("Recovering missing torrent blobs")

	for i, item := range items {
		if err := ctx.Err(); err != nil {
			log.Info().Err(err).Int("processed", i).Int("total", len(items)).Int64("runID", runID).Msg("Import blob recovery interrupted; pending items resume on next start")
			return
		}

		status, errMsg, repoint := s.acquireImportedBlob(ctx, instanceID, item)
		if err := s.store.UpdateItemBlobState(dbCtx, item.ID, status, errMsg, repoint); err != nil {
			// Without the persisted result the item still reads pending, so
			// stop with a visible failure instead of silently dropping it.
			s.failImportRecovery(instanceID, runID, fmt.Errorf("persist recovery state for torrent %s: %w", item.TorrentHash, err))
			return
		}

		switch status {
		case models.BackupBlobAvailable:
			log.Debug().Int("current", i+1).Int("total", len(items)).Int64("runID", runID).Str("hash", item.TorrentHash).Msg("Recovered torrent blob")
		default:
			log.Warn().Str("error", derefString(errMsg)).Int("current", i+1).Int("total", len(items)).Int64("runID", runID).Str("hash", item.TorrentHash).Msg("Failed to recover torrent blob")
		}
		s.updateProgress(runID, i+1)
	}

	s.finalizeImportRun(instanceID, runID)
}

// acquireImportedBlob resolves one item's blob without consulting its current
// status; the caller only invokes it for unresolved rows. A non-nil repoint
// path means the row should reference an already cached blob rather than the
// declared (missing) location.
func (s *Service) acquireImportedBlob(ctx context.Context, instanceID int, item *models.BackupItem) (models.BackupItemBlobStatus, *string, *string) {
	if item.TorrentBlobPath == nil || strings.TrimSpace(*item.TorrentBlobPath) == "" {
		return models.BackupBlobAvailable, nil, nil
	}

	// The blob appeared at the referenced location meanwhile (another import
	// or a previous attempt that wrote the file before the crash).
	if absPath := s.ResolveBackupPath(*item.TorrentBlobPath); absPath != "" {
		if info, err := os.Stat(absPath); err == nil && !info.IsDir() {
			return models.BackupBlobAvailable, nil, nil
		}
	}

	// Reuse a cached blob belonging to any run of this instance.
	if cached, err := s.loadCachedTorrent(ctx, instanceID, item.TorrentHash); err != nil {
		log.Warn().Err(err).Str("hash", item.TorrentHash).Msg("Failed to look up cached torrent blob")
	} else if cached != nil {
		repoint := cached.relPath
		return models.BackupBlobAvailable, nil, &repoint
	}

	if s.reader == nil {
		msg := "qBittorrent connection unavailable; torrent export is not configured"
		return models.BackupBlobFailed, &msg, nil
	}

	data, _, _, err := s.reader.ExportTorrent(ctx, instanceID, item.TorrentHash)
	if err != nil {
		msg := sanitizeBlobError(err)
		return models.BackupBlobFailed, &msg, nil
	}

	rel := filepath.FromSlash(backupRelPath(*item.TorrentBlobPath))
	if rel == "" || rel == "." {
		msg := "unsafe torrent blob path in manifest"
		return models.BackupBlobFailed, &msg, nil
	}
	if err := cacheTorrentBlob(s.root, rel, data); err != nil {
		msg := sanitizeBlobError(err)
		return models.BackupBlobFailed, &msg, nil
	}

	return models.BackupBlobAvailable, nil, nil
}

const maxBlobErrorLen = 500

// sanitizeBlobError turns a client/write error into a compact one-line
// per-item failure reason suitable for storage and display.
func sanitizeBlobError(err error) string {
	msg := strings.Join(strings.Fields(err.Error()), " ")
	if len(msg) > maxBlobErrorLen {
		msg = msg[:maxBlobErrorLen]
	}
	return msg
}

func derefString(v *string) string {
	if v == nil {
		return ""
	}
	return *v
}

// finalizeImportRun moves an import run to its terminal state based on the
// persisted per-item results. Pending items keep the run running (recovery
// was interrupted); any failure fails the whole run with a visible summary;
// only full availability reports success.
func (s *Service) finalizeImportRun(instanceID int, runID int64) {
	ctx := context.Background()
	counts, err := s.store.CountItemsByBlobStatus(ctx, runID)
	if err != nil {
		s.failImportRecovery(instanceID, runID, fmt.Errorf("count torrent recovery state: %w", err))
		return
	}
	if counts.Pending > 0 {
		log.Info().Int("pending", counts.Pending).Int64("runID", runID).Msg("Import still has unresolved torrent blobs; run remains running")
		return
	}

	now := s.now()
	status := models.BackupRunStatusSuccess
	var errMsg *string
	if counts.Failed > 0 {
		status = models.BackupRunStatusFailed
		msg := fmt.Sprintf("%d of %d torrent file(s) could not be recovered from qBittorrent; retry the import once the instance is reachable.", counts.Failed, counts.Tracked)
		errMsg = &msg
	}

	totalBytes := s.sumAvailableBlobBytes(ctx, runID)

	var kind models.BackupRunKind
	var torrentCount int
	if err := s.store.UpdateRunMetadata(ctx, runID, func(r *models.BackupRun) error {
		kind = r.Kind
		torrentCount = r.TorrentCount
		r.Status = status
		r.CompletedAt = &now
		r.ErrorMessage = errMsg
		r.TotalBytes = totalBytes
		return nil
	}); err != nil {
		log.Error().Err(err).Int64("runID", runID).Msg("Failed to finalize import run")
		return
	}

	s.progressMu.Lock()
	delete(s.progress, runID)
	s.progressMu.Unlock()

	log.Info().Str("status", string(status)).Int("failed", counts.Failed).Int("tracked", counts.Tracked).Int64("runID", runID).Msg("Import run finalized")
	s.emitRunActivity(instanceID, runID)

	if status == models.BackupRunStatusFailed {
		s.notify(ctx, notifications.Event{
			Type:         notifications.EventBackupFailed,
			InstanceID:   instanceID,
			BackupKind:   kind,
			BackupRunID:  runID,
			ErrorMessage: derefString(errMsg),
			CompletedAt:  &now,
		})
		return
	}
	s.notify(ctx, notifications.Event{
		Type:               notifications.EventBackupSucceeded,
		InstanceID:         instanceID,
		BackupKind:         kind,
		BackupRunID:        runID,
		BackupTorrentCount: torrentCount,
		CompletedAt:        &now,
	})
}

// failImportRecovery forces an import run to a visible failed state when the
// recovery machinery itself cannot continue.
func (s *Service) failImportRecovery(instanceID int, runID int64, cause error) {
	ctx := context.Background()
	log.Error().Err(cause).Int64("runID", runID).Msg("Import blob recovery failed")
	now := s.now()
	msg := cause.Error()

	var kind models.BackupRunKind
	_ = s.store.UpdateRunMetadata(ctx, runID, func(r *models.BackupRun) error {
		kind = r.Kind
		r.Status = models.BackupRunStatusFailed
		r.CompletedAt = &now
		r.ErrorMessage = &msg
		return nil
	})

	s.progressMu.Lock()
	delete(s.progress, runID)
	s.progressMu.Unlock()

	s.emitRunActivity(instanceID, runID)
	s.notify(ctx, notifications.Event{
		Type:         notifications.EventBackupFailed,
		InstanceID:   instanceID,
		BackupKind:   kind,
		BackupRunID:  runID,
		ErrorMessage: msg,
		CompletedAt:  &now,
	})
}

// sumAvailableBlobBytes sums on-disk sizes of the run's available blobs.
// Distinct stored paths are counted once because several items may share a
// content-addressed blob.
func (s *Service) sumAvailableBlobBytes(ctx context.Context, runID int64) int64 {
	items, err := s.store.ListItems(ctx, runID)
	if err != nil {
		log.Warn().Err(err).Int64("runID", runID).Msg("Failed to list items while computing import size")
		return 0
	}

	seen := make(map[string]struct{}, len(items))
	var total int64
	for _, item := range items {
		if item.TorrentBlobPath == nil {
			continue
		}
		if item.BlobStatus != "" && item.BlobStatus != models.BackupBlobAvailable {
			continue
		}
		stored := *item.TorrentBlobPath
		if _, ok := seen[stored]; ok {
			continue
		}
		seen[stored] = struct{}{}

		absPath := s.ResolveBackupPath(stored)
		if absPath == "" {
			continue
		}
		if info, err := os.Stat(absPath); err == nil && !info.IsDir() {
			total += info.Size()
		}
	}
	return total
}

// reverifyAvailableBlobs re-checks "available" items against the disk and
// moves any whose blob is missing back to pending (repointing at a cached
// blob when one exists). It exists for imports created before per-item
// blob_status existed, whose rows all defaulted to available regardless of
// whether the background fetch ever finished.
func (s *Service) reverifyAvailableBlobs(ctx context.Context, instanceID int, runID int64) error {
	items, err := s.store.ListItems(ctx, runID)
	if err != nil {
		return err
	}
	for _, item := range items {
		if item.TorrentBlobPath == nil {
			continue
		}
		if item.BlobStatus == models.BackupBlobPending || item.BlobStatus == models.BackupBlobFailed {
			continue
		}
		if absPath := s.ResolveBackupPath(*item.TorrentBlobPath); absPath != "" {
			if info, err := os.Stat(absPath); err == nil && !info.IsDir() {
				continue
			}
		}

		if cached, err := s.loadCachedTorrent(ctx, instanceID, item.TorrentHash); err == nil && cached != nil {
			repoint := cached.relPath
			if err := s.store.UpdateItemBlobState(ctx, item.ID, models.BackupBlobAvailable, nil, &repoint); err != nil {
				return err
			}
			continue
		}

		if err := s.store.UpdateItemBlobState(ctx, item.ID, models.BackupBlobPending, nil, nil); err != nil {
			return err
		}
	}
	return nil
}

// RetryImportRun resumes recovery for an existing import run. Only items not
// already available are reprocessed: failed rows reset to pending, previously
// pending rows stay pending, and completed rows are never touched. The run
// returns in "running" while recovery proceeds; it settles to success or
// failed like a fresh import.
func (s *Service) RetryImportRun(ctx context.Context, runID int64) (*models.BackupRun, error) {
	run, err := s.store.GetRun(ctx, runID)
	if err != nil {
		return nil, err
	}
	if run.Kind != models.BackupRunKindImport {
		return nil, ErrImportRunNotRetryable
	}
	if run.Status == models.BackupRunStatusPending || run.Status == models.BackupRunStatusRunning {
		return nil, ErrImportRecoveryActive
	}

	if _, err := s.store.ResetFailedBlobItems(ctx, runID); err != nil {
		return nil, fmt.Errorf("reset failed torrent files: %w", err)
	}

	counts, err := s.store.CountItemsByBlobStatus(ctx, runID)
	if err != nil {
		return nil, err
	}
	if counts.Pending == 0 && run.Status == models.BackupRunStatusFailed {
		// Legacy imports predate blob_status; their rows look available even
		// when the background fetch never wrote the files. Re-verify once.
		if err := s.reverifyAvailableBlobs(ctx, run.InstanceID, runID); err != nil {
			return nil, err
		}
		counts, err = s.store.CountItemsByBlobStatus(ctx, runID)
		if err != nil {
			return nil, err
		}
	}

	if counts.Pending == 0 {
		// Nothing left to fetch: recompute the terminal state from disk.
		s.finalizeImportRun(run.InstanceID, runID)
	} else {
		// Flip synchronously so callers never observe the pre-retry failed
		// state while the recovery goroutine is still starting.
		now := s.now()
		if err := s.store.UpdateRunMetadata(ctx, runID, func(r *models.BackupRun) error {
			r.Status = models.BackupRunStatusRunning
			r.CompletedAt = nil
			r.ErrorMessage = nil
			if r.StartedAt == nil {
				started := now
				r.StartedAt = &started
			}
			return nil
		}); err != nil {
			return nil, fmt.Errorf("mark import running: %w", err)
		}
		s.progressMu.Lock()
		s.progress[runID] = &BackupProgress{Current: 0, Total: counts.Pending}
		s.progressMu.Unlock()
		s.emitRunActivity(run.InstanceID, runID)
		s.wg.Go(func() {
			s.runImportRecovery(run.ID, run.InstanceID, []models.BackupItemBlobStatus{models.BackupBlobPending})
		})
	}

	return s.store.GetRun(ctx, runID)
}

// backupRelPath normalizes a stored backup-relative path to a slash form
// without the legacy "backups/" prefix. Returns "" for unsafe paths:
// absolute (POSIX or Windows), drive-prefixed, UNC, or containing a ".."
// path segment.
func backupRelPath(rel string) string {
	raw := strings.ReplaceAll(strings.TrimSpace(rel), `\`, "/")
	if slices.Contains(strings.Split(raw, "/"), "..") {
		return ""
	}
	slash := path.Clean(raw)
	if slash == "." || strings.HasPrefix(slash, "/") || (len(slash) >= 2 && slash[1] == ':') {
		return ""
	}
	return strings.TrimPrefix(slash, "backups/")
}

// ResolveBackupPath maps a stored backup-relative path (with or without the
// legacy "backups/" prefix) to an absolute path under the backup root.
// Returns "" when no root is configured or the path is unsafe.
func (s *Service) ResolveBackupPath(rel string) string {
	slash := backupRelPath(rel)
	if slash == "" || s.root == "" {
		return ""
	}
	return filepath.Join(s.root, filepath.FromSlash(slash))
}

type cachedTorrent struct {
	data    []byte
	relPath string
}

func (s *Service) loadCachedTorrent(ctx context.Context, instanceID int, hash string) (*cachedTorrent, error) {
	if s.cacheDir == "" {
		return nil, nil
	}

	rel, err := s.store.FindCachedTorrentBlob(ctx, instanceID, hash)
	if err != nil {
		return nil, err
	}
	if rel == nil {
		return nil, nil
	}

	absPath := s.ResolveBackupPath(*rel)
	if absPath == "" {
		return nil, nil
	}
	data, err := os.ReadFile(absPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}

	// Keep the stored spelling so rows that share a blob agree on the rel
	// path; blob refcounts compare these strings.
	return &cachedTorrent{data: data, relPath: *rel}, nil
}

func (s *Service) cleanupTorrentBlobs(ctx context.Context, items []*models.BackupItem) {
	if len(items) == 0 {
		return
	}

	// Rows can reference the same blob under the canonical "backups/"-prefixed
	// spelling or the legacy unprefixed one; both resolve to the same file, so
	// count remaining references across both spellings before deleting.
	seen := make(map[string]struct{})
	var uniqueBlobs, lookup []string

	for _, item := range items {
		if item == nil || item.TorrentBlobPath == nil {
			continue
		}

		canon := backupRelPath(*item.TorrentBlobPath)
		if canon == "" {
			continue
		}
		if _, ok := seen[canon]; ok {
			continue
		}

		seen[canon] = struct{}{}
		uniqueBlobs = append(uniqueBlobs, canon)
		lookup = append(lookup, canon, "backups/"+canon)
	}

	if len(uniqueBlobs) == 0 {
		return
	}

	// Item rows are committed only at run end, so a live run's blob reuse is
	// invisible to the refcount, and blobs are content-addressed and shared
	// across instances. Defer to the startup sweep while any run is active.
	if active, err := s.store.FindIncompleteRuns(ctx); err != nil {
		log.Warn().Err(err).Msg("Failed to check for active backup runs; skipping blob cleanup")
		return
	} else if len(active) > 0 {
		log.Debug().Int("activeRuns", len(active)).Msg("Backup run in progress; deferring blob cleanup to the startup sweep")
		return
	}

	refCounts, err := s.store.CountBlobReferencesBatch(ctx, lookup)
	if err != nil {
		// Unknown counts must keep the blob: deleting on error can lose a file
		// another run still references. Startup's cleanupOrphanedBlobs sweeps
		// anything truly unreferenced later.
		log.Warn().Err(err).Msg("Failed to count torrent blob references; skipping blob cleanup")
		return
	}

	var blobsToDelete []string

	for _, canon := range uniqueBlobs {
		if refCounts[canon]+refCounts["backups/"+canon] > 0 {
			continue
		}

		abs := s.ResolveBackupPath(canon)
		if abs == "" {
			log.Warn().Str("blob", canon).Msg("Cannot cleanup torrent blob without backup directory")
			continue
		}
		blobsToDelete = append(blobsToDelete, abs)
	}

	s.deleteFilesParallel(ctx, blobsToDelete)
}

func (s *Service) deleteFilesParallel(ctx context.Context, paths []string) {
	if len(paths) == 0 {
		return
	}

	for _, path := range paths {
		select {
		case <-ctx.Done():
			return
		default:
		}
		if err := removeFile(path); err != nil && !os.IsNotExist(err) {
			log.Warn().Err(err).Str("path", path).Msg("Failed to remove file during bulk cleanup")
		}
	}
}

func trackerDomainFromTorrent(t qbt.Torrent) string {
	if host := hostFromURL(t.Tracker); host != "" {
		return host
	}

	for _, tracker := range t.Trackers {
		if host := hostFromURL(tracker.Url); host != "" {
			return host
		}
	}

	return ""
}

func hostFromURL(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}

	if !strings.Contains(raw, "://") {
		raw = "http://" + raw
	}

	u, err := url.Parse(raw)
	if err != nil {
		return ""
	}

	return u.Hostname()
}
