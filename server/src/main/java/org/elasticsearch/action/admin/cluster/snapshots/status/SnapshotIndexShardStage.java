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
    INIT((byte) 0),
    /**
     * Index files are being copied
     */
    STARTED((byte) 1),
    /**
     * Snapshot metadata is being written or this shard's status in the cluster state is being updated
     */
    FINALIZE((byte) 2),
    /**
     * Snapshot completed successfully
     */
    DONE((byte) 3),
    /**
     * Snapshot failed
     */
    FAILURE((byte) 4);

    private final byte value;

    SnapshotIndexShardStage(byte value) {
        this.value = value;
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
     * Generate snapshot state from code
     *
     * @param value the state code
     * @return state
     */
    public static SnapshotIndexShardStage fromValue(byte value) {
        return switch (value) {
            case 0 -> INIT;
            case 1 -> STARTED;
            case 2 -> FINALIZE;
            case 3 -> DONE;
            case 4 -> FAILURE;
            default -> throw new IllegalArgumentException("No snapshot shard stage for value [" + value + "]");
        };
    }
}
