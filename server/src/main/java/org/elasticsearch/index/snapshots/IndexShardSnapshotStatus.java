/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.snapshots;

import org.elasticsearch.common.Strings;
import org.elasticsearch.repositories.ShardGeneration;
import org.elasticsearch.repositories.ShardSnapshotResult;
import org.elasticsearch.snapshots.AbortedSnapshotException;

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
    private final AtomicReference<ShardGeneration> generation;
    private final AtomicReference<ShardSnapshotResult> shardSnapshotResult; // only set in stage DONE
    private long startTime;
    private long totalTime;
    private int incrementalFileCount;
    private int totalFileCount;
    private int processedFileCount;
    private long totalSize;
    private long incrementalSize;
    private long processedSize;
    private String failure;

    private IndexShardSnapshotStatus(
        final Stage stage,
        final long startTime,
        final long totalTime,
        final int incrementalFileCount,
        final int totalFileCount,
        final int processedFileCount,
        final long incrementalSize,
        final long totalSize,
        final long processedSize,
        final String failure,
        final ShardGeneration generation
    ) {
        this.stage = new AtomicReference<>(Objects.requireNonNull(stage));
        this.generation = new AtomicReference<>(generation);
        this.shardSnapshotResult = new AtomicReference<>();
        this.startTime = startTime;
        this.totalTime = totalTime;
        this.incrementalFileCount = incrementalFileCount;
        this.totalFileCount = totalFileCount;
        this.processedFileCount = processedFileCount;
        this.totalSize = totalSize;
        this.processedSize = processedSize;
        this.incrementalSize = incrementalSize;
        this.failure = failure;
    }

    public synchronized Copy moveToStarted(
        final long startTime,
        final int incrementalFileCount,
        final int totalFileCount,
        final long incrementalSize,
        final long totalSize
    ) {
        if (stage.compareAndSet(Stage.INIT, Stage.STARTED)) {
            this.startTime = startTime;
            this.incrementalFileCount = incrementalFileCount;
            this.totalFileCount = totalFileCount;
            this.incrementalSize = incrementalSize;
            this.totalSize = totalSize;
        } else if (isAborted()) {
            throw new AbortedSnapshotException();
        } else {
            assert false : "Should not try to move stage [" + stage.get() + "] to [STARTED]";
            throw new IllegalStateException(
                "Unable to move the shard snapshot status to [STARTED]: " + "expecting [INIT] but got [" + stage.get() + "]"
            );
        }
        return asCopy();
    }

    public synchronized Copy moveToFinalize() {
        final var prevStage = stage.compareAndExchange(Stage.STARTED, Stage.FINALIZE);
        return switch (prevStage) {
            case STARTED -> asCopy();
            case ABORTED -> throw new AbortedSnapshotException();
            default -> {
                final var message = Strings.format(
                    "Unable to move the shard snapshot status to [FINALIZE]: expecting [STARTED] but got [%s]",
                    prevStage
                );
                assert false : message;
                throw new IllegalStateException(message);
            }
        };
    }

    public synchronized void moveToDone(final long endTime, final ShardSnapshotResult shardSnapshotResult) {
        assert shardSnapshotResult != null;
        assert shardSnapshotResult.getGeneration() != null;
        if (stage.compareAndSet(Stage.FINALIZE, Stage.DONE)) {
            this.totalTime = Math.max(0L, endTime - startTime);
            this.shardSnapshotResult.set(shardSnapshotResult);
            this.generation.set(shardSnapshotResult.getGeneration());
        } else {
            assert false : "Should not try to move stage [" + stage.get() + "] to [DONE]";
            throw new IllegalStateException(
                "Unable to move the shard snapshot status to [DONE]: " + "expecting [FINALIZE] but got [" + stage.get() + "]"
            );
        }
    }

    public synchronized void abortIfNotCompleted(final String failure) {
        if (stage.compareAndSet(Stage.INIT, Stage.ABORTED) || stage.compareAndSet(Stage.STARTED, Stage.ABORTED)) {
            this.failure = failure;
        }
    }

    public synchronized void moveToFailed(final long endTime, final String failure) {
        if (stage.getAndSet(Stage.FAILURE) != Stage.FAILURE) {
            this.totalTime = Math.max(0L, endTime - startTime);
            this.failure = failure;
        }
    }

    public ShardGeneration generation() {
        return generation.get();
    }

    public ShardSnapshotResult getShardSnapshotResult() {
        assert stage.get() == Stage.DONE : stage.get();
        return shardSnapshotResult.get();
    }

    public boolean isAborted() {
        return stage.get() == Stage.ABORTED;
    }

    public void ensureNotAborted() {
        if (isAborted()) {
            throw new AbortedSnapshotException();
        }
    }

    /**
     * Increments number of processed files
     */
    public synchronized void addProcessedFile(long size) {
        processedFileCount++;
        processedSize += size;
    }

    public synchronized void addProcessedFiles(int count, long totalSize) {
        processedFileCount += count;
        processedSize += totalSize;
    }

    /**
     * Returns a copy of the current {@link IndexShardSnapshotStatus}. This method is
     * intended to be used when a coherent state of {@link IndexShardSnapshotStatus} is needed.
     *
     * @return a  {@link IndexShardSnapshotStatus.Copy}
     */
    public synchronized IndexShardSnapshotStatus.Copy asCopy() {
        return new IndexShardSnapshotStatus.Copy(
            stage.get(),
            startTime,
            totalTime,
            incrementalFileCount,
            totalFileCount,
            processedFileCount,
            incrementalSize,
            totalSize,
            processedSize,
            failure
        );
    }

    public static IndexShardSnapshotStatus newInitializing(ShardGeneration generation) {
        return new IndexShardSnapshotStatus(Stage.INIT, 0L, 0L, 0, 0, 0, 0, 0, 0, null, generation);
    }

    public static IndexShardSnapshotStatus newFailed(final String failure) {
        assert failure != null : "expecting non null failure for a failed IndexShardSnapshotStatus";
        if (failure == null) {
            throw new IllegalArgumentException("A failure description is required for a failed IndexShardSnapshotStatus");
        }
        return new IndexShardSnapshotStatus(Stage.FAILURE, 0L, 0L, 0, 0, 0, 0, 0, 0, failure, null);
    }

    public static IndexShardSnapshotStatus newDone(
        final long startTime,
        final long totalTime,
        final int incrementalFileCount,
        final int fileCount,
        final long incrementalSize,
        final long size,
        ShardGeneration generation
    ) {
        // The snapshot is done which means the number of processed files is the same as total
        return new IndexShardSnapshotStatus(
            Stage.DONE,
            startTime,
            totalTime,
            incrementalFileCount,
            fileCount,
            incrementalFileCount,
            incrementalSize,
            size,
            incrementalSize,
            null,
            generation
        );
    }

    /**
     * Returns an immutable state of {@link IndexShardSnapshotStatus} at a given point in time.
     */
    public static class Copy {

        private final Stage stage;
        private final long startTime;
        private final long totalTime;
        private final int incrementalFileCount;
        private final int totalFileCount;
        private final int processedFileCount;
        private final long totalSize;
        private final long processedSize;
        private final long incrementalSize;
        private final String failure;

        public Copy(
            final Stage stage,
            final long startTime,
            final long totalTime,
            final int incrementalFileCount,
            final int totalFileCount,
            final int processedFileCount,
            final long incrementalSize,
            final long totalSize,
            final long processedSize,
            final String failure
        ) {
            this.stage = stage;
            this.startTime = startTime;
            this.totalTime = totalTime;
            this.incrementalFileCount = incrementalFileCount;
            this.totalFileCount = totalFileCount;
            this.processedFileCount = processedFileCount;
            this.totalSize = totalSize;
            this.processedSize = processedSize;
            this.incrementalSize = incrementalSize;
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

        public int getIncrementalFileCount() {
            return incrementalFileCount;
        }

        public int getTotalFileCount() {
            return totalFileCount;
        }

        public int getProcessedFileCount() {
            return processedFileCount;
        }

        public long getIncrementalSize() {
            return incrementalSize;
        }

        public long getTotalSize() {
            return totalSize;
        }

        public long getProcessedSize() {
            return processedSize;
        }

        public String getFailure() {
            return failure;
        }

        @Override
        public String toString() {
            return "index shard snapshot status ("
                + "stage="
                + stage
                + ", startTime="
                + startTime
                + ", totalTime="
                + totalTime
                + ", incrementalFileCount="
                + incrementalFileCount
                + ", totalFileCount="
                + totalFileCount
                + ", processedFileCount="
                + processedFileCount
                + ", incrementalSize="
                + incrementalSize
                + ", totalSize="
                + totalSize
                + ", processedSize="
                + processedSize
                + ", failure='"
                + failure
                + '\''
                + ')';
        }
    }
}
