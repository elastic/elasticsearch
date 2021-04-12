/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.snapshots.status;

public enum SnapshotIndexShardStage {

    /**
     * Snapshot hasn't started yet
     */
    INIT((byte) 0, false),
    /**
     * Index files are being copied
     */
    STARTED((byte) 1, false),
    /**
     * Snapshot metadata is being written or this shard's status in the cluster state is being updated
     */
    FINALIZE((byte) 2, false),
    /**
     * Snapshot completed successfully
     */
    DONE((byte) 3, true),
    /**
     * Snapshot failed
     */
    FAILURE((byte) 4, true);

    private byte value;

    private boolean completed;

    SnapshotIndexShardStage(byte value, boolean completed) {
        this.value = value;
        this.completed = completed;
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
     * Generate snapshot state from code
     *
     * @param value the state code
     * @return state
     */
    public static SnapshotIndexShardStage fromValue(byte value) {
        switch (value) {
            case 0:
                return INIT;
            case 1:
                return STARTED;
            case 2:
                return FINALIZE;
            case 3:
                return DONE;
            case 4:
                return FAILURE;
            default:
                throw new IllegalArgumentException("No snapshot shard stage for value [" + value + "]");
        }
    }
}
