-- Copyright (c) 2025-2026, s0up and the autobrr contributors.
-- SPDX-License-Identifier: GPL-2.0-or-later

-- Per-item torrent blob recovery state for manifest imports (Postgres mirror
-- of SQLite migration 104). See that file for the status semantics; blob_error
-- is plain TEXT for the same reason instance_backup_items.save_path is.
ALTER TABLE instance_backup_items ADD COLUMN IF NOT EXISTS blob_status TEXT NOT NULL DEFAULT 'available';
ALTER TABLE instance_backup_items ADD COLUMN IF NOT EXISTS blob_error TEXT;

-- Recovery and retry select pending/failed rows per run.
CREATE INDEX IF NOT EXISTS idx_backup_items_blob_status
    ON instance_backup_items(run_id, blob_status);

-- Recreate the items view to expose the new columns.
DROP VIEW IF EXISTS instance_backup_items_view;
CREATE VIEW instance_backup_items_view AS
SELECT
    ibi.id,
    ibi.run_id,
    sp_hash.value as torrent_hash,
    sp_name.value as name,
    sp_cat.value as category,
    ibi.size_bytes,
    sp_archive.value as archive_rel_path,
    sp_infohash_v1.value as infohash_v1,
    sp_infohash_v2.value as infohash_v2,
    sp_tags.value as tags,
    sp_blob.value as torrent_blob_path,
    ibi.blob_status,
    ibi.blob_error,
    ibi.save_path,
    ibi.created_at
FROM instance_backup_items ibi
LEFT JOIN string_pool sp_hash ON ibi.torrent_hash_id = sp_hash.id
LEFT JOIN string_pool sp_name ON ibi.name_id = sp_name.id
LEFT JOIN string_pool sp_cat ON ibi.category_id = sp_cat.id
LEFT JOIN string_pool sp_archive ON ibi.archive_rel_path_id = sp_archive.id
LEFT JOIN string_pool sp_infohash_v1 ON ibi.infohash_v1_id = sp_infohash_v1.id
LEFT JOIN string_pool sp_infohash_v2 ON ibi.infohash_v2_id = sp_infohash_v2.id
LEFT JOIN string_pool sp_tags ON ibi.tags_id = sp_tags.id
LEFT JOIN string_pool sp_blob ON ibi.torrent_blob_path_id = sp_blob.id;
