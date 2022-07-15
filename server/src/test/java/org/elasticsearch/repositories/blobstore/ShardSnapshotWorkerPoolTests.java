/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.repositories.blobstore;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.GroupedActionListener;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot;
import org.elasticsearch.index.store.StoreFileMetadata;
import org.elasticsearch.repositories.SnapshotShardContext;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;

@TestLogging(value = "org.elasticsearch.repositories.blobstore:TRACE", reason = "debugging")
public class ShardSnapshotWorkerPoolTests extends ESTestCase {

    private static ThreadPool threadPool;

    @Before
    public void setUpThreadPool() {
        threadPool = new TestThreadPool("test");
    }

    @After
    public void shutdownThreadPool() {
        TestThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
    }

    private class DummyRepo {
        private final AtomicInteger expectedUploads = new AtomicInteger();
        private final AtomicInteger finishedUploads = new AtomicInteger();
        private final AtomicInteger finishedSnapshots = new AtomicInteger();
        private final CountDownLatch uploadBlocker;
        private ShardSnapshotWorkerPool workers;

        DummyRepo() {
            this(new CountDownLatch(0));
        }

        DummyRepo(CountDownLatch uploadBlocker) {
            this.uploadBlocker = uploadBlocker;
        }

        public void setWorkers(ShardSnapshotWorkerPool workers) {
            this.workers = workers;
        }

        public void snapshotShard(SnapshotShardContext context) {
            int filesToUpload = randomIntBetween(1, 10);
            if (filesToUpload == 0) {
                finishedSnapshots.incrementAndGet();
                return;
            }
            expectedUploads.addAndGet(filesToUpload);
            ActionListener<Void> uploadListener = new GroupedActionListener<>(
                ActionListener.wrap(finishedSnapshots::incrementAndGet),
                filesToUpload
            );
            for (int i = 0; i < filesToUpload; i++) {
                workers.enqueueFileUpload(new ShardSnapshotWorkerPool.SnapshotFileUpload(context, createMockedFileInfo(), uploadListener));
            }
        }

        public void uploadFile(SnapshotShardContext context, BlobStoreIndexShardSnapshot.FileInfo fileInfo) {
            try {
                uploadBlocker.await();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            finishedUploads.incrementAndGet();
        }

        public int expectedUploads() {
            return expectedUploads.get();
        }

        public int finishedUploads() {
            return finishedUploads.get();
        }

        public int finishedSnapshots() {
            return finishedSnapshots.get();
        }
    }

    private BlobStoreIndexShardSnapshot.FileInfo createMockedFileInfo() {
        String filename = randomAlphaOfLength(10);
        return new BlobStoreIndexShardSnapshot.FileInfo(filename, mock(StoreFileMetadata.class), null);
    }

    public void testAllWorkersExitWhenBothQueuesAreExhausted() throws Exception {
        Executor executor = threadPool.executor(ThreadPool.Names.SNAPSHOT);
        int desiredSize = randomIntBetween(1, threadPool.info(ThreadPool.Names.SNAPSHOT).getMax());
        DummyRepo repo = new DummyRepo();
        ShardSnapshotWorkerPool workers = new ShardSnapshotWorkerPool(desiredSize, executor, repo::snapshotShard, repo::uploadFile);
        assertThat(workers.size(), equalTo(0));
        repo.setWorkers(workers);
        int shardsToSnapshot = randomIntBetween(1, 10);
        for (int i = 0; i < shardsToSnapshot; i++) {
            workers.enqueueShardSnapshot(mock(SnapshotShardContext.class));
        }
        assertBusy(() -> assertThat(repo.finishedSnapshots(), equalTo(shardsToSnapshot)));
        assertBusy(() -> assertThat(workers.size(), equalTo(0)));
        assertBusy(() -> assertThat(repo.finishedUploads(), equalTo(repo.expectedUploads())));
    }
}
