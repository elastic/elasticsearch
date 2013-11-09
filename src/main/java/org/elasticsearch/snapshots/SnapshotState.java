package org.elasticsearch.snapshots;

import org.elasticsearch.ElasticSearchIllegalArgumentException;

/**
 * Represents the state that a snapshot can be in
 */
public enum SnapshotState {
    /**
     * Snapshot process has started
     */
    IN_PROGRESS((byte) 0),
    /**
     * Snapshot process completed successfully
     */
    SUCCESS((byte) 1),
    /**
     * Snapshot failed
     */
    FAILED((byte) 2);

    private byte value;

    private SnapshotState(byte value) {
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
     * Returns true if snapshot completed (successfully or not)
     *
     * @return true if snapshot completed, false otherwise
     */
    public boolean completed() {
        return this == SUCCESS || this == FAILED;
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
            default:
                throw new ElasticSearchIllegalArgumentException("No snapshot state for value [" + value + "]");
        }
    }
}

