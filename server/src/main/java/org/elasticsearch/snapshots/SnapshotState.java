/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.snapshots;

/**
 * Represents the state of a snapshot.
 * <p>
 * The state is determined during snapshot finalization and stored in the {@link SnapshotInfo} object in the repository, and this is also
 * duplicated in {@link org.elasticsearch.repositories.RepositoryData.SnapshotDetails}. These values can be read back by the get-snapshots
 * and restore-snapshots APIs.
 */
public enum SnapshotState {
    /**
     * Snapshot process has started
     */
    IN_PROGRESS((byte) 0, false, false),
    /**
     * Snapshot process completed successfully
     */
    SUCCESS((byte) 1, true, true),
    /**
     * Snapshots stored in the repository are only in state {@code FAILED} if they were aborted because deletion was requested while they
     * were running. The existence of such a snapshot is usually temporary since the deletion of the now-finalized snapshot is enqueued at
     * the same time, and (unless the deletion fails) will remove the {@code FAILED} snapshot from the repository once all running snapshots
     * have finished.
     * <p>
     * This enum value appears in {@link SnapshotInfo} only for snapshots and not for clones: aborted clones are removed from the cluster
     * state without being finalized, so they are never seen as {@code FAILED} in get-snapshot responses.
     * <p>
     * {@code FAILED} snapshots are not available for restore; see {@link #PARTIAL} for partial backups that are retained.
     */
    FAILED((byte) 2, true, false),
    /**
     * Snapshot completed with at least one shard failure (e.g. because a node left the cluster) but was not aborted. The snapshot remains
     * in the repository and successful shards may be restored.
     */
    PARTIAL((byte) 3, true, true),
    /**
     * Snapshot is incompatible with the current version of the cluster
     */
    INCOMPATIBLE((byte) 4, true, false);

    private final byte value;

    private final boolean completed;

    private final boolean restorable;

    SnapshotState(byte value, boolean completed, boolean restorable) {
        this.value = value;
        this.completed = completed;
        this.restorable = restorable;
    }

    /**
     * Returns code that represents the snapshot state
     *
     * @return code for the state
     */
    public byte value() {
        return value;
    }

    /**
     * Returns true if snapshot completed (successfully or not)
     *
     * @return true if snapshot completed, false otherwise
     */
    public boolean completed() {
        return completed;
    }

    /**
     * Returns true if snapshot can be restored (at least partially)
     *
     * @return true if snapshot can be restored, false otherwise
     */
    public boolean restorable() {
        return restorable;
    }

    /**
     * Generate snapshot state from code
     *
     * @param value the state code
     * @return state
     */
    public static SnapshotState fromValue(byte value) {
        return switch (value) {
            case 0 -> IN_PROGRESS;
            case 1 -> SUCCESS;
            case 2 -> FAILED;
            case 3 -> PARTIAL;
            case 4 -> INCOMPATIBLE;
            default -> throw new IllegalArgumentException("No snapshot state for value [" + value + "]");
        };
    }
}
