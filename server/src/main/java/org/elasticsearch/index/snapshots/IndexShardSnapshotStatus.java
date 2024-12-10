/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.snapshots;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.repositories.ShardGeneration;
import org.elasticsearch.repositories.ShardSnapshotResult;
import org.elasticsearch.snapshots.AbortedSnapshotException;
import org.elasticsearch.snapshots.PausedSnapshotException;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

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
         * Snapshot pausing because of node removal
         */
        PAUSING,
        /**
         * Snapshot paused because of node removal
         */
        PAUSED,
        /**
         * Snapshot aborted
         */
        ABORTED
    }

    /**
     * Used to complete listeners added via {@link #addAbortListener} when the shard snapshot is either aborted/paused or it gets past the
     * stages where an abort/pause could have occurred.
     */
    public enum AbortStatus {
        /**
         * The shard snapshot got past the stage where an abort or pause could have occurred, and is either complete or on its way to
         * completion.
         */
        NO_ABORT,

        /**
         * The shard snapshot stopped before completion, either because the whole snapshot was aborted or because this node is to be
         * removed.
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
    private final SubscribableListener<AbortStatus> abortListeners = new SubscribableListener<>();
    private volatile String statusDescription;

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
        final ShardGeneration generation,
        final String statusDescription
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
        updateStatusDescription(statusDescription);
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
        } else {
            ensureNotAborted();
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
            case STARTED -> {
                abortListeners.onResponse(AbortStatus.NO_ABORT);
                yield asCopy();
            }
            case ABORTED -> throw new AbortedSnapshotException();
            case PAUSING -> throw new PausedSnapshotException();
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

    public Stage getStage() {
        return stage.get();
    }

    public void addAbortListener(ActionListener<AbortStatus> listener) {
        abortListeners.addListener(listener);
    }

    public void abortIfNotCompleted(final String failure, Consumer<ActionListener<Releasable>> notifyRunner) {
        abortAndMoveToStageIfNotCompleted(Stage.ABORTED, failure, notifyRunner);
    }

    public void pauseIfNotCompleted(Consumer<ActionListener<Releasable>> notifyRunner) {
        abortAndMoveToStageIfNotCompleted(Stage.PAUSING, "paused for removal of node holding primary", notifyRunner);
    }

    private synchronized void abortAndMoveToStageIfNotCompleted(
        final Stage newStage,
        final String failure,
        final Consumer<ActionListener<Releasable>> notifyRunner
    ) {
        assert newStage == Stage.ABORTED || newStage == Stage.PAUSING : newStage;
        if (stage.compareAndSet(Stage.INIT, newStage) || stage.compareAndSet(Stage.STARTED, newStage)) {
            this.failure = failure;
            notifyRunner.accept(abortListeners.map(r -> {
                Releasables.closeExpectNoException(r);
                return AbortStatus.ABORTED;
            }));
        }
    }

    public synchronized SnapshotsInProgress.ShardState moveToUnsuccessful(final Stage newStage, final String failure, final long endTime) {
        assert newStage == Stage.PAUSED || newStage == Stage.FAILURE : newStage;
        if (newStage == Stage.PAUSED && stage.compareAndSet(Stage.PAUSING, Stage.PAUSED)) {
            this.totalTime = Math.max(0L, endTime - startTime);
            this.failure = failure;
            return SnapshotsInProgress.ShardState.PAUSED_FOR_NODE_REMOVAL;
        }

        moveToFailed(endTime, failure);
        return SnapshotsInProgress.ShardState.FAILED;
    }

    public synchronized void moveToFailed(final long endTime, final String failure) {
        if (stage.getAndSet(Stage.FAILURE) != Stage.FAILURE) {
            abortListeners.onResponse(AbortStatus.NO_ABORT);
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

    public void ensureNotAborted() {
        ensureNotAborted(stage.get());
    }

    public static void ensureNotAborted(Stage shardSnapshotStage) {
        switch (shardSnapshotStage) {
            case ABORTED -> throw new AbortedSnapshotException();
            case PAUSING -> throw new PausedSnapshotException();
        }
    }

    public boolean isPaused() {
        return stage.get() == Stage.PAUSED;
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
     * Updates the string explanation for what the snapshot is actively doing right now.
     */
    public void updateStatusDescription(String statusString) {
        assert statusString != null;
        assert statusString.isEmpty() == false;
        this.statusDescription = statusString;
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
            failure,
            statusDescription
        );
    }

    public static IndexShardSnapshotStatus newInitializing(ShardGeneration generation) {
        return new IndexShardSnapshotStatus(Stage.INIT, 0L, 0L, 0, 0, 0, 0, 0, 0, null, generation, "initializing");
    }

    public static IndexShardSnapshotStatus.Copy newFailed(final String failure) {
        assert failure != null : "expecting non null failure for a failed IndexShardSnapshotStatus";
        if (failure == null) {
            throw new IllegalArgumentException("A failure description is required for a failed IndexShardSnapshotStatus");
        }
        return new IndexShardSnapshotStatus(Stage.FAILURE, 0L, 0L, 0, 0, 0, 0, 0, 0, failure, null, "initialized as failed").asCopy();
    }

    public static IndexShardSnapshotStatus.Copy newDone(
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
            generation,
            "initialized as done"
        ).asCopy();
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
        private final String statusDescription;

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
            final String failure,
            final String statusDescription
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
            this.statusDescription = statusDescription;
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

        public String getStatusDescription() {
            return statusDescription;
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
                + "', statusDescription='"
                + statusDescription
                + '\''
                + ')';
        }
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
            + "', statusDescription='"
            + statusDescription
            + '\''
            + ')';
    }
}
