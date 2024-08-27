/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.repositories.blobstore;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.common.CheckedBiConsumer;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.PrioritizedThrottledTaskRunner;
import org.elasticsearch.core.Strings;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.repositories.SnapshotShardContext;

import java.io.IOException;
import java.util.Comparator;
import java.util.concurrent.Executor;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot.FileInfo;

/**
 * {@link ShardSnapshotTaskRunner} performs snapshotting tasks, prioritizing {@link ShardSnapshotTask}
 * over {@link FileSnapshotTask}. Each enqueued shard to snapshot results in one {@link ShardSnapshotTask}
 * and zero or more {@link FileSnapshotTask}s.
 */
public class ShardSnapshotTaskRunner {
    private static final Logger logger = LogManager.getLogger(ShardSnapshotTaskRunner.class);
    private final PrioritizedThrottledTaskRunner<SnapshotTask> taskRunner;
    private final Consumer<SnapshotShardContext> shardSnapshotter;
    private final CheckedBiConsumer<SnapshotShardContext, FileInfo, IOException> fileSnapshotter;

    abstract static class SnapshotTask extends AbstractRunnable implements Comparable<SnapshotTask> {
        protected final SnapshotShardContext context;

        SnapshotTask(SnapshotShardContext context) {
            this.context = context;
        }

        public final SnapshotShardContext context() {
            return context;
        }

        private long getSnapshotStartTime() {
            return context().snapshotStartTime();
        }

        private String snapshotUUID() {
            return context().snapshotId().getUUID();
        }

        public abstract int priority();

        @SuppressWarnings("resource")
        private ShardId shardId() {
            return context().store().shardId();
        }

        private static final Comparator<SnapshotTask> COMPARATOR = Comparator
            // Prefer work from the oldest running snapshot ...
            .comparingLong(SnapshotTask::getSnapshotStartTime)
            // ... tiebreaking on UUID just in case two of them start at the same millisecond ...
            .thenComparing(SnapshotTask::snapshotUUID)
            // ... then prefer starting new shard snapshots over uploading files for ongoing ones (for fast noop shard snapshots) ...
            .thenComparingInt(SnapshotTask::priority)
            // ... then break ties by shard ID to limit WIP and avoid PAUSED_FOR_NODE_REMOVAL discarding more work than it needs to
            .thenComparing(SnapshotTask::shardId);

        @Override
        public final int compareTo(SnapshotTask other) {
            return COMPARATOR.compare(this, other);
        }

        @Override
        public void onFailure(Exception e) {
            assert false : e;
            logger.error(Strings.format("snapshot task [%s] unexpectedly failed", this), e);
        }
    }

    class ShardSnapshotTask extends SnapshotTask {
        ShardSnapshotTask(SnapshotShardContext context) {
            super(context);
        }

        @Override
        public void doRun() {
            shardSnapshotter.accept(context);
        }

        @Override
        public int priority() {
            return 1;
        }

        @Override
        public String toString() {
            return getClass().getSimpleName() + "{snapshotID=[" + context.snapshotId() + "], indexID=[" + context.indexId() + "]}";
        }
    }

    class FileSnapshotTask extends SnapshotTask {
        private final Supplier<FileInfo> fileInfos;
        private final ActionListener<Void> fileSnapshotListener;

        FileSnapshotTask(SnapshotShardContext context, Supplier<FileInfo> fileInfos, ActionListener<Void> fileSnapshotListener) {
            super(context);
            this.fileInfos = fileInfos;
            this.fileSnapshotListener = fileSnapshotListener;
        }

        @Override
        public void doRun() {
            ActionRunnable.run(fileSnapshotListener, () -> {
                FileInfo fileInfo = fileInfos.get();
                if (fileInfo != null) {
                    fileSnapshotter.accept(context, fileInfo);
                }
            }).run();
        }

        @Override
        public int priority() {
            return 2;
        }

        @Override
        public String toString() {
            return getClass().getSimpleName() + "{snapshotID=[" + context.snapshotId() + "], indexID=[" + context.indexId() + "]}";
        }
    }

    public ShardSnapshotTaskRunner(
        final int maxRunningTasks,
        final Executor executor,
        final Consumer<SnapshotShardContext> shardSnapshotter,
        final CheckedBiConsumer<SnapshotShardContext, FileInfo, IOException> fileSnapshotter
    ) {
        this.taskRunner = new PrioritizedThrottledTaskRunner<>("ShardSnapshotTaskRunner", maxRunningTasks, executor);
        this.shardSnapshotter = shardSnapshotter;
        this.fileSnapshotter = fileSnapshotter;
    }

    public void enqueueShardSnapshot(final SnapshotShardContext context) {
        ShardSnapshotTask task = new ShardSnapshotTask(context);
        taskRunner.enqueueTask(task);
    }

    public void enqueueFileSnapshot(
        final SnapshotShardContext context,
        final Supplier<FileInfo> fileInfos,
        final ActionListener<Void> listener
    ) {
        final FileSnapshotTask task = new FileSnapshotTask(context, fileInfos, listener);
        taskRunner.enqueueTask(task);
    }

    // visible for testing
    int runningTasks() {
        return taskRunner.runningTasks();
    }

    // visible for testing
    int queueSize() {
        return taskRunner.queueSize();
    }
}
