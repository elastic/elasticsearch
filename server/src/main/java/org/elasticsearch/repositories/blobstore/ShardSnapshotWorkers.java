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
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.repositories.SnapshotShardContext;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.function.Consumer;

import static org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot.FileInfo;

/**
 * ShardSnapshotWorkers performs snapshotting tasks in the order dictated by the PriorityQueue of snapshot tasks.
 * Each enqueued shard to snapshot results in one @{@link ShardSnapshotTask} and zero or more @{@link FileSnapshotTask}.
 */
public class ShardSnapshotWorkers {
    private static final Logger logger = LogManager.getLogger(ShardSnapshotWorkers.class);

    private final int maxWorkers;
    private final Object mutex = new Object();
    private final BlockingQueue<SnapshotTask> snapshotTasks = new PriorityBlockingQueue<>();
    private final Executor executor;
    private final Consumer<SnapshotShardContext> shardSnapshotter;
    private final CheckedBiConsumer<SnapshotShardContext, FileInfo, IOException> fileSnapshotter;
    private int workerCount = 0;

    abstract class SnapshotTask implements Comparable<SnapshotTask>, Runnable {
        protected final SnapshotShardContext context;

        SnapshotTask(SnapshotShardContext context) {
            this.context = context;
        }

        public abstract short priority();

        public SnapshotShardContext context() {
            return context;
        }

        @Override
        public int compareTo(SnapshotTask other) {
            int res = context.snapshotId().compareTo(other.context.snapshotId());
            if (res != 0) {
                return res;
            }
            return Integer.compare(priority(), other.priority());
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
    }

    public ShardSnapshotWorkers(
        final int maxWorkers,
        final Executor executor,
        final Consumer<SnapshotShardContext> shardSnapshotter,
        final CheckedBiConsumer<SnapshotShardContext, FileInfo, IOException> fileSnapshotter
    ) {
        assert maxWorkers > 0;

        logger.info("starting shard snapshot worker pool of max size {}", maxWorkers);
        this.maxWorkers = maxWorkers;
        this.executor = executor;
        this.shardSnapshotter = shardSnapshotter;
        this.fileSnapshotter = fileSnapshotter;
    }

    public void enqueueShardSnapshot(final SnapshotShardContext context) {
        logger.trace("enqueuing shard snapshot task [snapshotID={}, indexID={}]", context.snapshotId(), context.indexId());
        snapshotTasks.add(new ShardSnapshotTask(context));
        synchronized (mutex) {
            ensureEnoughWorkers();
        }
    }

    public void enqueueFileSnapshot(
        final SnapshotShardContext context,
        final FileInfo fileInfo,
        final ActionListener<Void> fileUploadListener
    ) {
        logger.trace(
            "enqueuing shard snapshot file upload task [snapshotID={}, indexID={}, file={}]",
            context.snapshotId(),
            context.indexId(),
            fileInfo.name()
        );
        snapshotTasks.add(new FileSnapshotTask(context, fileInfo, fileUploadListener));
        synchronized (mutex) {
            ensureEnoughWorkers();
        }
    }

    private void ensureEnoughWorkers() {
        assert Thread.holdsLock(mutex);

        int workersToCreate = Math.min(maxWorkers - workerCount, snapshotTasks.size());
        if (workersToCreate > 0) {
            logger.debug("starting {} shard snapshot workers", workersToCreate);
        }
        for (int i = 0; i < workersToCreate; i++) {
            startWorker(UUIDs.base64UUID());
        }
        workerCount += workersToCreate;
        logger.debug("worker pool size is {}", workerCount);
    }

    private void workerDone() {
        synchronized (mutex) {
            assert workerCount > 0;

            workerCount--;
            ensureEnoughWorkers();
        }
    }

    // for testing
    int size() {
        synchronized (mutex) {
            return workerCount;
        }
    }

    private void startWorker(final String workerId) {
        logger.debug("starting snapshot pool worker {}", workerId);
        executor.execute(() -> {
            try {
                while (true) {
                    SnapshotTask task = snapshotTasks.poll();
                    if (task == null) {
                        logger.trace("[worker {}] snapshot task queue is empty", workerId);
                        break;
                    }
                    task.run();
                }
            } finally {
                logger.debug("[worker {}] worker is done", workerId);
                workerDone();
            }
        });
    }

}
