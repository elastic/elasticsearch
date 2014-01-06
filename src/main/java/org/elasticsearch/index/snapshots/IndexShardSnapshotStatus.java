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

package org.elasticsearch.index.snapshots;

/**
 * Represent shard snapshot status
 */
public class IndexShardSnapshotStatus {

    /**
     * Snapshot stage
     */
    public static enum Stage {
        /**
         * Snapshot hasn't started yet
         */
        INIT,
        /**
         * Index files are being copied
         */
        STARTED,
        /**
         * Snapshot metadata is being written
         */
        FINALIZE,
        /**
         * Snapshot completed successfully
         */
        DONE,
        /**
         * Snapshot failed
         */
        FAILURE
    }

    private Stage stage = Stage.INIT;

    private long startTime;

    private long time;

    private int numberOfFiles;

    private long totalSize;

    private long indexVersion;

    private boolean aborted;

    /**
     * Returns current snapshot stage
     *
     * @return current snapshot stage
     */
    public Stage stage() {
        return this.stage;
    }

    /**
     * Sets new snapshot stage
     *
     * @param stage new snapshot stage
     */
    public void updateStage(Stage stage) {
        this.stage = stage;
    }

    /**
     * Returns snapshot start time
     *
     * @return snapshot start time
     */
    public long startTime() {
        return this.startTime;
    }

    /**
     * Sets snapshot start time
     *
     * @param startTime snapshot start time
     */
    public void startTime(long startTime) {
        this.startTime = startTime;
    }

    /**
     * Returns snapshot processing time
     *
     * @return processing time
     */
    public long time() {
        return this.time;
    }

    /**
     * Sets snapshot processing time
     *
     * @param time snapshot processing time
     */
    public void time(long time) {
        this.time = time;
    }

    /**
     * Returns true if snapshot process was aborted
     *
     * @return true if snapshot process was aborted
     */
    public boolean aborted() {
        return this.aborted;
    }

    /**
     * Marks snapshot as aborted
     */
    public void abort() {
        this.aborted = true;
    }

    /**
     * Sets files stats
     *
     * @param numberOfFiles number of files in this snapshot
     * @param totalSize     total size of files in this snapshot
     */
    public void files(int numberOfFiles, long totalSize) {
        this.numberOfFiles = numberOfFiles;
        this.totalSize = totalSize;
    }

    /**
     * Number of files
     *
     * @return number of files
     */
    public int numberOfFiles() {
        return numberOfFiles;
    }

    /**
     * Total snapshot size
     *
     * @return snapshot size
     */
    public long totalSize() {
        return totalSize;
    }

    /**
     * Sets index version
     *
     * @param indexVersion index version
     */
    public void indexVersion(long indexVersion) {
        this.indexVersion = indexVersion;
    }

    /**
     * Returns index version
     *
     * @return index version
     */
    public long indexVersion() {
        return indexVersion;
    }
}
