// Copyright (c) 2025-2026, s0up and the autobrr contributors.
// SPDX-License-Identifier: GPL-2.0-or-later

package backups

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/autobrr/qui/internal/models"
)

// newImportTestService builds a backups service backed by a migrated SQLite
// database and an on-disk backup root under the test temp dir.
func newImportTestService(t *testing.T, reader backupReader) (*Service, int, string) {
	t.Helper()

	db := setupTestBackupDB(t)
	instanceID := insertTestInstance(t, db, "import-instance")
	store := models.NewBackupStore(db)

	dataDir := t.TempDir()
	svc := NewService(store, reader, Config{
		WorkerCount:    1,
		DataDir:        dataDir,
		ExportThrottle: time.Millisecond,
	}, nil)
	svc.now = func() time.Time { return time.Unix(1_700_000_000, 0).UTC() }

	return svc, instanceID, dataDir
}

func marshalImportManifest(t *testing.T, instanceID int, items []ManifestItem) []byte {
	t.Helper()
	manifest := Manifest{
		InstanceID:   instanceID,
		Kind:         string(models.BackupRunKindManual),
		GeneratedAt:  time.Unix(1_700_000_000, 0).UTC(),
		TorrentCount: len(items),
		Items:        items,
	}
	data, err := json.Marshal(manifest)
	require.NoError(t, err)
	return data
}

func importTestItems(hashes ...string) []ManifestItem {
	items := make([]ManifestItem, 0, len(hashes))
	for i, hash := range hashes {
		items = append(items, ManifestItem{
			Hash:        hash,
			Name:        "Torrent " + hash,
			ArchivePath: "cat/" + hash + ".torrent",
			SizeBytes:   int64(1000 + i),
			TorrentBlob: "backups/torrents/" + hash[:2] + "/" + hash + ".torrent",
		})
	}
	return items
}

// waitForTerminalRun polls the run until it leaves pending/running.
func waitForTerminalRun(t *testing.T, store *models.BackupStore, runID int64) *models.BackupRun {
	t.Helper()
	var run *models.BackupRun
	require.Eventually(t, func() bool {
		r, err := store.GetRun(context.Background(), runID)
		if err != nil {
			return false
		}
		run = r
		return r.Status != models.BackupRunStatusRunning && r.Status != models.BackupRunStatusPending
	}, 5*time.Second, 5*time.Millisecond)
	return run
}

func waitForRunStatus(t *testing.T, store *models.BackupStore, runID int64, want models.BackupRunStatus) *models.BackupRun {
	t.Helper()
	run := waitForTerminalRun(t, store, runID)
	require.Equal(t, want, run.Status, "error: %v", run.ErrorMessage)
	return run
}

func blobExists(svc *Service, item *models.BackupItem) bool {
	if item.TorrentBlobPath == nil {
		return false
	}
	abs := svc.ResolveBackupPath(*item.TorrentBlobPath)
	if abs == "" {
		return false
	}
	info, err := os.Stat(abs)
	return err == nil && !info.IsDir()
}

// A complete archive import needs no qBittorrent calls and reports success
// with every item available immediately.
func TestImportManifestCompleteArchiveSucceedsWithoutFetches(t *testing.T) {
	t.Parallel()

	stub := &concurrentExportStub{errs: map[string]error{}}
	svc, instanceID, _ := newImportTestService(t, stub)

	payload := []byte("d" + strings.Repeat("x", 63))
	tempDir := t.TempDir()
	torrentPaths := map[string]string{}
	for _, item := range importTestItems("hashaa", "hashbb") {
		tempPath := filepath.Join(tempDir, filepath.Base(item.ArchivePath))
		require.NoError(t, os.WriteFile(tempPath, payload, 0o600))
		torrentPaths[item.ArchivePath] = tempPath
	}

	items := importTestItems("hashaa", "hashbb")
	data := marshalImportManifest(t, instanceID, items)

	run, err := svc.ImportManifestFromDir(context.Background(), instanceID, data, "tester", torrentPaths)
	require.NoError(t, err)
	require.Equal(t, models.BackupRunKindImport, run.Kind)
	require.Equal(t, models.BackupRunStatusSuccess, run.Status, "complete archives settle synchronously")
	require.NotNil(t, run.CompletedAt)

	stored, err := svc.store.ListItems(context.Background(), run.ID)
	require.NoError(t, err)
	require.Len(t, stored, 2)
	for _, item := range stored {
		require.Equal(t, models.BackupBlobAvailable, item.BlobStatus)
		require.Nil(t, item.BlobError)
		require.True(t, blobExists(svc, item))
	}

	stub.mu.Lock()
	require.Zero(t, stub.calls, "no blob must be fetched from qBittorrent when the archive is complete")
	stub.mu.Unlock()
}

// A manifest-only import recovers every missing blob in the background and
// only then reports success.
func TestImportManifestRecoversMissingBlobs(t *testing.T) {
	t.Parallel()

	stub := &concurrentExportStub{errs: map[string]error{}}
	svc, instanceID, _ := newImportTestService(t, stub)

	data := marshalImportManifest(t, instanceID, importTestItems("hashaa", "hashbb"))
	run, err := svc.ImportManifestFromDir(context.Background(), instanceID, data, "tester", nil)
	require.NoError(t, err)
	require.Equal(t, models.BackupRunStatusRunning, run.Status)

	run = waitForRunStatus(t, svc.store, run.ID, models.BackupRunStatusSuccess)
	require.Nil(t, run.ErrorMessage)
	require.Greater(t, run.TotalBytes, int64(0))

	stored, err := svc.store.ListItems(context.Background(), run.ID)
	require.NoError(t, err)
	for _, item := range stored {
		require.Equal(t, models.BackupBlobAvailable, item.BlobStatus)
		require.True(t, blobExists(svc, item))
	}

	stub.mu.Lock()
	require.Equal(t, 2, stub.calls)
	stub.mu.Unlock()
}

// When the instance cannot provide a blob, the item and the run must end in a
// visible failed state; a retry after the instance recovers processes only
// the unresolved items and never rewrites completed blobs.
func TestImportRunFailureVisibleAndRetryRecoversOnlyFailed(t *testing.T) {
	t.Parallel()

	boom := errors.New("client returned status code: 500")
	stub := &concurrentExportStub{errs: map[string]error{"hashaa": boom}}
	svc, instanceID, _ := newImportTestService(t, stub)

	data := marshalImportManifest(t, instanceID, importTestItems("hashaa", "hashbb"))
	run, err := svc.ImportManifestFromDir(context.Background(), instanceID, data, "tester", nil)
	require.NoError(t, err)

	run = waitForRunStatus(t, svc.store, run.ID, models.BackupRunStatusFailed)
	require.NotNil(t, run.ErrorMessage)
	require.Contains(t, *run.ErrorMessage, "1 of 2")
	require.NotNil(t, run.CompletedAt)

	stored, err := svc.store.ListItems(context.Background(), run.ID)
	require.NoError(t, err)
	byHash := map[string]*models.BackupItem{}
	for _, item := range stored {
		byHash[item.TorrentHash] = item
	}
	require.Equal(t, models.BackupBlobFailed, byHash["hashaa"].BlobStatus)
	require.NotNil(t, byHash["hashaa"].BlobError)
	require.Contains(t, *byHash["hashaa"].BlobError, "500")
	require.Equal(t, models.BackupBlobAvailable, byHash["hashbb"].BlobStatus)
	require.True(t, blobExists(svc, byHash["hashbb"]))
	require.False(t, blobExists(svc, byHash["hashaa"]))

	// The completed blob must not be rewritten by the retry.
	completedAbs := svc.ResolveBackupPath(*byHash["hashbb"].TorrentBlobPath)
	require.NoError(t, os.WriteFile(completedAbs, []byte("sentinel-bytes"), 0o600))

	// Instance is back: clear the failing export.
	stub.mu.Lock()
	delete(stub.errs, "hashaa")
	callsBeforeRetry := stub.calls
	stub.mu.Unlock()

	retried, err := svc.RetryImportRun(context.Background(), run.ID)
	require.NoError(t, err)
	require.Equal(t, models.BackupRunStatusRunning, retried.Status)
	require.Nil(t, retried.ErrorMessage)

	waitForRunStatus(t, svc.store, run.ID, models.BackupRunStatusSuccess)

	stored, err = svc.store.ListItems(context.Background(), run.ID)
	require.NoError(t, err)
	for _, item := range stored {
		require.Equal(t, models.BackupBlobAvailable, item.BlobStatus, item.TorrentHash)
		require.Nil(t, item.BlobError, item.TorrentHash)
	}
	require.True(t, blobExists(svc, byHash["hashaa"]))

	kept, err := os.ReadFile(completedAbs)
	require.NoError(t, err)
	require.Equal(t, "sentinel-bytes", string(kept), "retry must not rewrite an already available blob")

	stub.mu.Lock()
	require.Equal(t, callsBeforeRetry+1, stub.calls, "retry fetches only the unresolved item")
	stub.mu.Unlock()
}

// A blob cached for the hash by an earlier run is referenced as-is: no fetch,
// no duplicate file at the manifest-declared location.
func TestImportManifestReusesExistingCachedBlob(t *testing.T) {
	t.Parallel()

	stub := &concurrentExportStub{errs: map[string]error{}}
	svc, instanceID, _ := newImportTestService(t, stub)

	// Seed an earlier successful run that already cached the blob elsewhere.
	cachedStored := "backups/torrents/ex/isting/cached.torrent"
	cachedAbs := svc.ResolveBackupPath(cachedStored)
	require.NoError(t, os.MkdirAll(filepath.Dir(cachedAbs), 0o755))
	require.NoError(t, os.WriteFile(cachedAbs, []byte("cached-payload"), 0o600))

	seedRun := &models.BackupRun{
		InstanceID:  instanceID,
		Kind:        models.BackupRunKindManual,
		Status:      models.BackupRunStatusSuccess,
		RequestedBy: "test",
	}
	require.NoError(t, svc.store.CreateRun(context.Background(), seedRun))
	cachedPath := cachedStored
	require.NoError(t, svc.store.InsertItems(context.Background(), seedRun.ID, []models.BackupItem{{
		TorrentHash:     "hashaa",
		Name:            "Seed Torrent",
		TorrentBlobPath: &cachedPath,
	}}))

	// Imported manifest declares a different, currently missing location.
	items := importTestItems("hashaa")
	data := marshalImportManifest(t, instanceID, items)
	run, err := svc.ImportManifestFromDir(context.Background(), instanceID, data, "tester", nil)
	require.NoError(t, err)
	require.Equal(t, models.BackupRunStatusSuccess, run.Status)

	stored, err := svc.store.ListItems(context.Background(), run.ID)
	require.NoError(t, err)
	require.Len(t, stored, 1)
	require.Equal(t, cachedStored, *stored[0].TorrentBlobPath, "item must reference the existing cached blob")
	require.True(t, blobExists(svc, stored[0]))

	// The manifest-declared location must not be duplicated.
	declaredAbs := filepath.Join(svc.root, "torrents", "ha", "hashaa.torrent")
	_, err = os.Stat(declaredAbs)
	require.ErrorIs(t, err, os.ErrNotExist)

	stub.mu.Lock()
	require.Zero(t, stub.calls)
	stub.mu.Unlock()
}

// A process exit mid-recovery leaves rows pending; on the next start recovery
// resumes from persisted state and reaches a deterministic result.
func TestImportRecoveryResumesAfterRestart(t *testing.T) {
	t.Parallel()

	stub := &concurrentExportStub{errs: map[string]error{}}
	svc, instanceID, _ := newImportTestService(t, stub)

	// Simulate a crash mid-recovery: an import run with a pending item on disk.
	run := &models.BackupRun{
		InstanceID:  instanceID,
		Kind:        models.BackupRunKindImport,
		Status:      models.BackupRunStatusRunning,
		RequestedBy: "tester",
	}
	require.NoError(t, svc.store.CreateRun(context.Background(), run))
	require.NoError(t, svc.store.InsertItems(context.Background(), run.ID, []models.BackupItem{{
		TorrentHash:     "hashaa",
		Name:            "Torrent hashaa",
		BlobStatus:      models.BackupBlobPending,
		TorrentBlobPath: strPtr("backups/torrents/ha/hashaa.torrent"),
	}}))

	// A process killed during shutdown keeps the run running in the DB; a
	// canceled context must not finalize or drop it.
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	svc.ctx = canceled
	svc.resumeImportRecovery(context.Background(), run)

	mid, err := svc.store.GetRun(context.Background(), run.ID)
	require.NoError(t, err)
	require.Equal(t, models.BackupRunStatusRunning, mid.Status, "interrupted recovery leaves the run running")
	item, err := svc.store.GetItemByHash(context.Background(), run.ID, "hashaa")
	require.NoError(t, err)
	require.Equal(t, models.BackupBlobPending, item.BlobStatus)

	// New process over the same database resumes and finishes.
	svc2, _, _ := newImportTestService(t, stub)
	svc2.store = svc.store
	svc2.root = svc.root
	svc2.cacheDir = svc.cacheDir
	svc2.resumeImportRecovery(context.Background(), mid)

	waitForRunStatus(t, svc2.store, run.ID, models.BackupRunStatusSuccess)
	item, err = svc.store.GetItemByHash(context.Background(), run.ID, "hashaa")
	require.NoError(t, err)
	require.Equal(t, models.BackupBlobAvailable, item.BlobStatus)
	require.Nil(t, item.BlobError)
}

// An import whose process died before items were inserted fails
// deterministically instead of resuming into an empty success.
func TestImportRecoveryWithNoItemsFailsAfterRestart(t *testing.T) {
	t.Parallel()

	stub := &concurrentExportStub{errs: map[string]error{}}
	svc, instanceID, _ := newImportTestService(t, stub)

	run := &models.BackupRun{
		InstanceID:  instanceID,
		Kind:        models.BackupRunKindImport,
		Status:      models.BackupRunStatusRunning,
		RequestedBy: "tester",
	}
	require.NoError(t, svc.store.CreateRun(context.Background(), run))

	svc.resumeImportRecovery(context.Background(), run)

	final, err := svc.store.GetRun(context.Background(), run.ID)
	require.NoError(t, err)
	require.Equal(t, models.BackupRunStatusFailed, final.Status)
	require.NotNil(t, final.ErrorMessage)
}

// Without a qBittorrent connection the run fails visibly and can be retried
// once a reader is configured on a later service instance.
func TestImportWithoutReaderFailsVisibleThenSucceedsOnRetry(t *testing.T) {
	t.Parallel()

	svc, instanceID, _ := newImportTestService(t, nil)
	data := marshalImportManifest(t, instanceID, importTestItems("hashaa"))
	run, err := svc.ImportManifestFromDir(context.Background(), instanceID, data, "tester", nil)
	require.NoError(t, err)
	run = waitForRunStatus(t, svc.store, run.ID, models.BackupRunStatusFailed)
	require.Contains(t, *run.ErrorMessage, "1 of 1")

	stub := &concurrentExportStub{errs: map[string]error{}}
	svc2, _, _ := newImportTestService(t, stub)
	svc2.store = svc.store
	svc2.root = svc.root
	svc2.cacheDir = svc.cacheDir

	retried, err := svc2.RetryImportRun(context.Background(), run.ID)
	require.NoError(t, err)
	require.Equal(t, models.BackupRunStatusRunning, retried.Status)
	waitForRunStatus(t, svc2.store, run.ID, models.BackupRunStatusSuccess)
}

// Retrying a non-import (live backup) run is rejected.
func TestRetryRejectsNonImportRun(t *testing.T) {
	t.Parallel()

	stub := &concurrentExportStub{errs: map[string]error{}}
	svc, instanceID, _ := newImportTestService(t, stub)

	liveRun := &models.BackupRun{
		InstanceID:  instanceID,
		Kind:        models.BackupRunKindManual,
		Status:      models.BackupRunStatusFailed,
		RequestedBy: "test",
	}
	require.NoError(t, svc.store.CreateRun(context.Background(), liveRun))

	_, err := svc.RetryImportRun(context.Background(), liveRun.ID)
	require.ErrorIs(t, err, ErrImportRunNotRetryable)
}

// A second retry while recovery is running is rejected rather than starting a
// second writer.
func TestRetryWhileRunningIsRejected(t *testing.T) {
	t.Parallel()

	block := make(chan struct{})
	release := make(chan struct{})
	stub := &blockingExportStub{
		errs:    map[string]error{},
		block:   block,
		release: release,
	}
	svc, instanceID, _ := newImportTestService(t, stub)

	run := &models.BackupRun{
		InstanceID:  instanceID,
		Kind:        models.BackupRunKindImport,
		Status:      models.BackupRunStatusFailed,
		RequestedBy: "tester",
	}
	require.NoError(t, svc.store.CreateRun(context.Background(), run))
	require.NoError(t, svc.store.InsertItems(context.Background(), run.ID, []models.BackupItem{{
		TorrentHash:     "hashaa",
		Name:            "Torrent hashaa",
		BlobStatus:      models.BackupBlobFailed,
		TorrentBlobPath: strPtr("backups/torrents/ha/hashaa.torrent"),
	}}))

	_, err := svc.RetryImportRun(context.Background(), run.ID)
	require.NoError(t, err)
	<-block

	_, err = svc.RetryImportRun(context.Background(), run.ID)
	require.ErrorIs(t, err, ErrImportRecoveryActive)

	close(release)
	waitForRunStatus(t, svc.store, run.ID, models.BackupRunStatusSuccess)
}

type blockingExportStub struct {
	concurrentExportStub
	block   chan struct{}
	release chan struct{}
}

func (s *blockingExportStub) ExportTorrent(ctx context.Context, _ int, hash string) ([]byte, string, string, error) {
	select {
	case s.block <- struct{}{}:
	default:
	}
	select {
	case <-s.release:
		return []byte("payload-" + hash), "name-" + hash, "tracker.example", nil
	case <-ctx.Done():
		return nil, "", "", ctx.Err()
	}
}
