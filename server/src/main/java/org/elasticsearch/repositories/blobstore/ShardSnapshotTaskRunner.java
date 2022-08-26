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
import org.elasticsearch.common.util.concurrent.ThrottledTaskRunner;
import org.elasticsearch.repositories.SnapshotShardContext;

import java.io.IOException;
import java.util.concurrent.Executor;
import java.util.function.Consumer;

import static org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot.FileInfo;

/**
 * {@link ShardSnapshotTaskRunner} performs snapshotting tasks, prioritizing {@link ShardSnapshotTask}
 * over {@link FileSnapshotTask}. Each enqueued shard to snapshot results in one {@link ShardSnapshotTask}
 * and zero or more {@link FileSnapshotTask}s.
 */
public class ShardSnapshotTaskRunner {
    private final ThrottledTaskRunner<SnapshotTask> throttledTaskRunner;
    private final Consumer<SnapshotShardContext> shardSnapshotter;
    private final CheckedBiConsumer<SnapshotShardContext, FileInfo, IOException> fileSnapshotter;

    abstract static class SnapshotTask implements Comparable<SnapshotTask>, Runnable {
        protected final SnapshotShardContext context;

        SnapshotTask(SnapshotShardContext context) {
            this.context = context;
        }

        public abstract short priority();

        public SnapshotShardContext context() {
            return context;
        }

        @Override
        public final int compareTo(SnapshotTask other) {
            int res = Integer.compare(priority(), other.priority());
            if (res != 0) {
                return res;
            }
            return Long.compare(context.snapshotStartTime(), other.context.snapshotStartTime());
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
        public short priority() {
            return 1;
        }

        @Override
        public String toString() {
            return getClass().getSimpleName() + "{snapshotID=[" + context.snapshotId() + "], indexID=[" + context.indexId() + "]}";
        }
    }

    class FileSnapshotTask extends SnapshotTask {
        private final FileInfo fileInfo;
        private final ActionListener<Void> fileSnapshotListener;

        FileSnapshotTask(SnapshotShardContext context, FileInfo fileInfo, ActionListener<Void> fileSnapshotListener) {
            super(context);
            this.fileInfo = fileInfo;
            this.fileSnapshotListener = fileSnapshotListener;
        }

        @Override
        public void run() {
            ActionRunnable.run(fileSnapshotListener, () -> fileSnapshotter.accept(context, fileInfo)).run();
        }

        @Override
        public short priority() {
            return 2;
        }

        @Override
        public String toString() {
            return getClass().getSimpleName()
                + "{snapshotID=["
                + context.snapshotId()
                + "], indexID=["
                + context.indexId()
                + "], file=["
                + fileInfo.name()
                + "]}";
        }
    }

    public ShardSnapshotTaskRunner(
        final int maxRunningTasks,
        final Executor executor,
        final Consumer<SnapshotShardContext> shardSnapshotter,
        final CheckedBiConsumer<SnapshotShardContext, FileInfo, IOException> fileSnapshotter
    ) {
        this.throttledTaskRunner = new ThrottledTaskRunner<>(maxRunningTasks, executor);
        this.shardSnapshotter = shardSnapshotter;
        this.fileSnapshotter = fileSnapshotter;
    }

    public void enqueueShardSnapshot(final SnapshotShardContext context) {
        ShardSnapshotTask task = new ShardSnapshotTask(context);
        throttledTaskRunner.enqueueTask(task);
    }

    public void enqueueFileSnapshot(final SnapshotShardContext context, final FileInfo fileInfo, final ActionListener<Void> listener) {
        final FileSnapshotTask task = new FileSnapshotTask(context, fileInfo, listener);
        throttledTaskRunner.enqueueTask(task);
    }

    // visible for testing
    int runningTasks() {
        return throttledTaskRunner.runningTasks();
    }

    // visible for testing
    int queueSize() {
        return throttledTaskRunner.queueSize();
    }
}
