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
import org.elasticsearch.repositories.SnapshotShardContext;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot.FileInfo;

/**
 * ShardSnapshotWorkers performs snapshotting tasks in the order dictated by the PriorityQueue of snapshot tasks.
 * Each enqueued shard to snapshot results in one @{@link ShardSnapshotTask} and zero or more @{@link FileSnapshotTask}.
 */
public class ShardSnapshotWorkers {
    private static final Logger logger = LogManager.getLogger(ShardSnapshotWorkers.class);

    private final int maxRunningTasks;
    private final AtomicInteger runningTasks = new AtomicInteger();
    private final BlockingQueue<SnapshotTask> snapshotTasks = new PriorityBlockingQueue<>();
    private final Executor executor;
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
        private final ActionListener<Void> fileUploadListener;

        FileSnapshotTask(SnapshotShardContext context, FileInfo fileInfo, ActionListener<Void> fileUploadListener) {
            super(context);
            this.fileInfo = fileInfo;
            this.fileUploadListener = fileUploadListener;
        }

        @Override
        public void run() {
            ActionRunnable.run(fileUploadListener, () -> fileSnapshotter.accept(context, fileInfo)).run();
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

    public ShardSnapshotWorkers(
        final int maxRunningTasks,
        final Executor executor,
        final Consumer<SnapshotShardContext> shardSnapshotter,
        final CheckedBiConsumer<SnapshotShardContext, FileInfo, IOException> fileSnapshotter
    ) {
        assert maxRunningTasks > 0;
        logger.info("starting shard snapshot worker pool of max size {}", maxRunningTasks);
        this.maxRunningTasks = maxRunningTasks;
        this.executor = executor;
        this.shardSnapshotter = shardSnapshotter;
        this.fileSnapshotter = fileSnapshotter;
    }

    public void enqueueShardSnapshot(final SnapshotShardContext context) {
        ShardSnapshotTask task = new ShardSnapshotTask(context);
        logger.trace("enqueuing {}", task);
        snapshotTasks.add(task);
        pollAndSpawn();
    }

    public void enqueueFileSnapshot(
        final SnapshotShardContext context,
        final FileInfo fileInfo,
        final ActionListener<Void> fileUploadListener
    ) {
        final FileSnapshotTask task = new FileSnapshotTask(context, fileInfo, fileUploadListener);
        logger.trace("enqueuing {}", task);
        snapshotTasks.add(task);
        pollAndSpawn();
    }

    private void pollAndSpawn() {
        if (incrementRunningTasks()) {
            SnapshotTask task = snapshotTasks.poll();
            if (task == null) {
                logger.trace("snapshot task queue is empty");
                int decremented = runningTasks.decrementAndGet();
                assert decremented >= 0;
                return;
            }
            executor.execute(() -> runTask(task));
        }
    }

    private boolean incrementRunningTasks() {
        return runningTasks.getAndUpdate(v -> v < maxRunningTasks ? v + 1 : v) < maxRunningTasks;
    }

    // for testing
    int size() {
        return runningTasks.get();
    }

    private void runTask(final SnapshotTask task) {
        try {
            logger.trace("running snapshot task {}", task);
            task.run();
        } finally {
            int decremented = runningTasks.decrementAndGet();
            assert decremented >= 0;
            pollAndSpawn();
        }
    }
}
