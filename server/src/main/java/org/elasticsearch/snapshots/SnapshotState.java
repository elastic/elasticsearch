/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.snapshots;

/**
 * Represents the state that a snapshot can be in
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
     * Snapshot failed
     */
    FAILED((byte) 2, true, false),
    /**
     * Snapshot was partial successful
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
        switch (value) {
            case 0:
                return IN_PROGRESS;
            case 1:
                return SUCCESS;
            case 2:
                return FAILED;
            case 3:
                return PARTIAL;
            case 4:
                return INCOMPATIBLE;
            default:
                throw new IllegalArgumentException("No snapshot state for value [" + value + "]");
        }
    }
}
