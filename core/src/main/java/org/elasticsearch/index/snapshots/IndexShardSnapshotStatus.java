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

import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Represent shard snapshot status
 */
public class IndexShardSnapshotStatus {

    /**
     * Snapshot stage
     */
    public enum Stage {
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
        FAILURE,
        /**
         * Snapshot aborted
         */
        ABORTED
    }

    private final AtomicReference<Stage> stage;
    private long startTime;
    private long totalTime;
    private int numberOfFiles;
    private int processedFiles;
    private long totalSize;
    private long processedSize;
    private long indexVersion;
    private String failure;

    private IndexShardSnapshotStatus(final Stage stage, final long startTime, final long totalTime,
                                     final int numberOfFiles, final int processedFiles, final long totalSize, final long processedSize,
                                     final long indexVersion, final String failure) {
        this.stage = new AtomicReference<>(Objects.requireNonNull(stage));
        this.startTime = startTime;
        this.totalTime = totalTime;
        this.numberOfFiles = numberOfFiles;
        this.processedFiles = processedFiles;
        this.totalSize = totalSize;
        this.processedSize = processedSize;
        this.indexVersion = indexVersion;
        this.failure = failure;
    }

    public synchronized Copy moveToStarted(final long startTime, final int numberOfFiles, final long totalSize) {
        if (stage.compareAndSet(Stage.INIT, Stage.STARTED)) {
            this.startTime = startTime;
            this.numberOfFiles = numberOfFiles;
            this.totalSize = totalSize;
        } else {
            throw new IllegalStateException("Unable to move the shard snapshot status to [STARTED]: " +
                "expecting [INIT] but got [" + stage.get() + "]");
        }
        return asCopy();
    }

    public synchronized Copy moveToFinalize(final long indexVersion) {
        if (stage.compareAndSet(Stage.STARTED, Stage.FINALIZE)) {
            this.indexVersion = indexVersion;
        } else {
            throw new IllegalStateException("Unable to move the shard snapshot status to [FINALIZE]: " +
                "expecting [STARTED] but got [" + stage.get() + "]");
        }
        return asCopy();
    }

    public synchronized Copy moveToDone(final long endTime) {
        if (stage.compareAndSet(Stage.FINALIZE, Stage.DONE)) {
            this.totalTime = Math.max(0L, endTime - startTime);
        } else {
            throw new IllegalStateException("Unable to move the shard snapshot status to [DONE]: " +
                "expecting [FINALIZE] but got [" + stage.get() + "]");
        }
        return asCopy();
    }

    public synchronized Copy abortIfNotCompleted(final String failure) {
        if (stage.compareAndSet(Stage.INIT, Stage.ABORTED) || stage.compareAndSet(Stage.STARTED, Stage.ABORTED)) {
            this.failure = failure;
        }
        return asCopy();
    }

    public synchronized void moveToFailed(final long endTime, final String failure) {
        if (stage.getAndSet(Stage.FAILURE) != Stage.FAILURE) {
            this.totalTime = Math.max(0L, endTime - startTime);
            this.failure = failure;
        }
    }

    public boolean isAborted() {
        return stage.get() == Stage.ABORTED;
    }

    /**
     * Increments number of processed files
     */
    public synchronized void addProcessedFile(long size) {
        processedFiles++;
        processedSize += size;
    }

    /**
     * Returns a copy of the current {@link IndexShardSnapshotStatus}. This method is
     * intended to be used when a coherent state of {@link IndexShardSnapshotStatus} is needed.
     *
     * @return a  {@link IndexShardSnapshotStatus.Copy}
     */
    public synchronized IndexShardSnapshotStatus.Copy asCopy() {
        return new IndexShardSnapshotStatus.Copy(stage.get(), startTime, totalTime, numberOfFiles, processedFiles, totalSize, processedSize,
                                                 indexVersion, failure);
    }

    public static IndexShardSnapshotStatus newInitializing() {
        return new IndexShardSnapshotStatus(Stage.INIT, 0L, 0L, 0, 0, 0, 0, 0, null);
    }

    public static IndexShardSnapshotStatus newFailed(final String failure) {
        assert failure != null : "expecting non null failure for a failed IndexShardSnapshotStatus";
        if (failure == null) {
            throw new IllegalArgumentException("A failure description is required for a failed IndexShardSnapshotStatus");
        }
        return new IndexShardSnapshotStatus(Stage.FAILURE, 0L, 0L, 0, 0, 0, 0, 0, failure);
    }

    public static IndexShardSnapshotStatus newDone(final long startTime, final long totalTime, final int files, final long size) {
        // The snapshot is done which means the number of processed files is the same as total
        return new IndexShardSnapshotStatus(Stage.DONE, startTime, totalTime, files, files, size, size, 0, null);
    }

    /**
     * Returns an immutable state of {@link IndexShardSnapshotStatus} at a given point in time.
     */
    public static class Copy {

        private final Stage stage;
        private final long startTime;
        private final long totalTime;
        private final int numberOfFiles;
        private final int processedFiles;
        private final long totalSize;
        private final long processedSize;
        private final long indexVersion;
        private final String failure;

        public Copy(final Stage stage, final long startTime, final long totalTime,
                    final int numberOfFiles, final int processedFiles, final long totalSize, final long processedSize,
                    final long indexVersion, final String failure) {
            this.stage = stage;
            this.startTime = startTime;
            this.totalTime = totalTime;
            this.numberOfFiles = numberOfFiles;
            this.processedFiles = processedFiles;
            this.totalSize = totalSize;
            this.processedSize = processedSize;
            this.indexVersion = indexVersion;
            this.failure = failure;
        }

        public Stage getStage() {
            return stage;
        }

        public long getStartTime() {
            return startTime;
        }

        public long getTotalTime() {
            return totalTime;
        }

        public int getNumberOfFiles() {
            return numberOfFiles;
        }

        public int getProcessedFiles() {
            return processedFiles;
        }

        public long getTotalSize() {
            return totalSize;
        }

        public long getProcessedSize() {
            return processedSize;
        }

        public long getIndexVersion() {
            return indexVersion;
        }

        public String getFailure() {
            return failure;
        }

        @Override
        public String toString() {
            return "index shard snapshot status (" +
                "stage=" + stage +
                ", startTime=" + startTime +
                ", totalTime=" + totalTime +
                ", numberOfFiles=" + numberOfFiles +
                ", processedFiles=" + processedFiles +
                ", totalSize=" + totalSize +
                ", processedSize=" + processedSize +
                ", indexVersion=" + indexVersion +
                ", failure='" + failure + '\'' +
                ')';
        }
    }
}
