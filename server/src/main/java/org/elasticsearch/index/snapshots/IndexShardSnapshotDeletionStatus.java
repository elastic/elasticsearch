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
 * Represent shard snapshot deletion status
 */
public class IndexShardSnapshotDeletionStatus {

    /**
     * Snapshot Deletion Stage
     */
    public enum Stage {
        /**
         * Shard snapshot deletion hasn't started yet
         */
        INIT,
        /**
         * Shard snapshot deletion in progress
         */
        STARTED,
        /**
         * Shard snapshot deletion completed successfully
         */
        DONE,
        /**
         * Shard snapshot deletion failed
         */
        FAILURE
    }

    private final AtomicReference<Stage> stage;
    private final int version;
    private long startTime;
    private long totalTime;
    private String failure;

    private IndexShardSnapshotDeletionStatus(final Stage stage, final int version, final long startTime,
                                             final long totalTime, final String failure) {
        this.stage = new AtomicReference<>(Objects.requireNonNull(stage));
        this.version = version;
        this.startTime = startTime;
        this.totalTime = totalTime;
        this.failure = failure;
    }

    public synchronized void moveToStarted(final long startTime) {
        if (stage.compareAndSet(Stage.INIT, Stage.STARTED)) {
            this.startTime = startTime;
        } else {
            throw new IllegalStateException("Unable to move the shard snapshot deletion status to [STARTED]: " +
                "expecting [INIT] but got [" + stage.get() + "]");
        }
    }

    public synchronized void moveToDone(final long endTime) {
        if (stage.compareAndSet(Stage.STARTED, Stage.DONE)) {
            this.totalTime = Math.max(0L, endTime - startTime);
        } else {
            throw new IllegalStateException("Unable to move the shard snapshot deletion status to [DONE]: " +
                "expecting [STARTED] but got [" + stage.get() + "]");
        }
    }

    public synchronized void moveToFailed(final long endTime, final String failure) {
        if (stage.getAndSet(Stage.FAILURE) != Stage.FAILURE) {
            this.totalTime = Math.max(0L, endTime - startTime);
            this.failure = failure;
        }
    }

    public boolean isFailed() {
        return stage.get() == Stage.FAILURE;
    }

    public int getVersion() {
        return version;
    }

    public synchronized IndexShardSnapshotDeletionStatus.Copy asCopy() {
        return new IndexShardSnapshotDeletionStatus.Copy(stage.get(), version, startTime, totalTime, failure);
    }

    public static IndexShardSnapshotDeletionStatus newInitializing(final int version) {
        return new IndexShardSnapshotDeletionStatus(Stage.INIT, version, 0L, 0L, null);
    }

    public static class Copy {

        private final Stage stage;
        private final int version;
        private final long startTime;
        private final long totalTime;
        private final String failure;

        public Copy(final Stage stage, final int version, final long startTime, final long totalTime,
                    final String failure) {
            this.stage = stage;
            this.version = version;
            this.startTime = startTime;
            this.totalTime = totalTime;
            this.failure = failure;
        }

        public Stage getStage() {
            return stage;
        }

        public int getVersion() {
            return version;
        }

        public long getStartTime() {
            return startTime;
        }

        public long getTotalTime() {
            return totalTime;
        }

        public String getFailure() {
            return failure;
        }

        @Override
        public String toString() {
            return "index shard snapshot deletion status (" +
                "stage=" + stage +
                ", version=" + version +
                ", startTime=" + startTime +
                ", totalTime=" + totalTime +
                ", failure='" + failure + '\'' +
                ')';
        }

    }

}
