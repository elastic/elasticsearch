/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.repositories.blobstore;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.common.CheckedBiConsumer;
import org.elasticsearch.common.util.concurrent.PrioritizedThrottledTaskRunner;
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
    private final PrioritizedThrottledTaskRunner<SnapshotTask> taskRunner;
    private final Consumer<SnapshotShardContext> shardSnapshotter;
    private final CheckedBiConsumer<SnapshotShardContext, FileInfo, IOException> fileSnapshotter;

    abstract static class SnapshotTask implements Comparable<SnapshotTask>, Runnable {

        private static final Comparator<SnapshotTask> COMPARATOR = Comparator.comparingLong(
            (SnapshotTask t) -> t.context().snapshotStartTime()
        ).thenComparing(t -> t.context().snapshotId().getUUID()).thenComparingInt(SnapshotTask::priority);

        protected final SnapshotShardContext context;

        SnapshotTask(SnapshotShardContext context) {
            this.context = context;
        }

        public abstract int priority();

        public SnapshotShardContext context() {
            return context;
        }

        @Override
        public final int compareTo(SnapshotTask other) {
            return COMPARATOR.compare(this, other);
        }
    }

    class ShardSnapshotTask extends SnapshotTask {
        ShardSnapshotTask(SnapshotShardContext context) {
            super(context);
        }

        @Override
        public void run() {
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
        public void run() {
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
