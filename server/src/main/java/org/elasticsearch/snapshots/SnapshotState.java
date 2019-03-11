/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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

