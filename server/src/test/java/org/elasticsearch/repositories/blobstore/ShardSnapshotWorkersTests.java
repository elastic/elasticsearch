/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.repositories.blobstore;

import org.apache.lucene.store.ByteBuffersDirectory;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.GroupedActionListener;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.snapshots.IndexShardSnapshotStatus;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.store.StoreFileMetadata;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.SnapshotShardContext;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.test.DummyShardLock;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThan;

public class ShardSnapshotWorkersTests extends ESTestCase {

    private static ThreadPool threadPool;
    private static Executor executor;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        threadPool = new TestThreadPool("test");
        executor = threadPool.executor(ThreadPool.Names.SNAPSHOT);
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        TestThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
    }

    private static class MockedRepo {
        private final AtomicInteger expectedFileSnapshotTasks = new AtomicInteger();
        private final AtomicInteger finishedFileSnapshotTasks = new AtomicInteger();
        private final AtomicInteger finishedShardSnapshotTasks = new AtomicInteger();
        private final AtomicInteger finishedShardSnapshots = new AtomicInteger();
        private final CountDownLatch snapshotShardBlocker;
        private ShardSnapshotWorkers workers;

        MockedRepo(CountDownLatch snapshotShardBlocker) {
            this.snapshotShardBlocker = snapshotShardBlocker;
        }

        public void setWorkers(ShardSnapshotWorkers workers) {
            this.workers = workers;
        }

        public void snapshotShard(SnapshotShardContext context) {
            try {
                snapshotShardBlocker.await();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            int filesToUpload = randomIntBetween(0, 10);
            if (filesToUpload == 0) {
                finishedShardSnapshots.incrementAndGet();
            } else {
                expectedFileSnapshotTasks.addAndGet(filesToUpload);
                ActionListener<Void> uploadListener = new GroupedActionListener<>(
                    ActionListener.wrap(finishedShardSnapshots::incrementAndGet),
                    filesToUpload
                );
                for (int i = 0; i < filesToUpload; i++) {
                    workers.enqueueFileSnapshot(context, dummyFileInfo(), uploadListener);
                }
            }
            finishedShardSnapshotTasks.incrementAndGet();
        }

        public void snapshotFile(SnapshotShardContext context, BlobStoreIndexShardSnapshot.FileInfo fileInfo) {
            finishedFileSnapshotTasks.incrementAndGet();
        }

        public int expectedFileSnapshotTasks() {
            return expectedFileSnapshotTasks.get();
        }

        public int finishedFileSnapshotTasks() {
            return finishedFileSnapshotTasks.get();
        }

        public int finishedShardSnapshots() {
            return finishedShardSnapshots.get();
        }

        public int finishedShardSnapshotTasks() {
            return finishedShardSnapshotTasks.get();
        }
    }

    private static BlobStoreIndexShardSnapshot.FileInfo dummyFileInfo() {
        String filename = randomAlphaOfLength(10);
        StoreFileMetadata metadata = new StoreFileMetadata(filename, 10, "CHECKSUM", Version.CURRENT.luceneVersion.toString());
        return new BlobStoreIndexShardSnapshot.FileInfo(filename, metadata, null);
    }

    private SnapshotShardContext dummyContext() {
        return dummyContext(new SnapshotId(randomAlphaOfLength(10), UUIDs.randomBase64UUID()), threadPool.absoluteTimeInMillis());
    }

    private SnapshotShardContext dummyContext(final SnapshotId snapshotId, final long startTime) {
        IndexId indexId = new IndexId(randomAlphaOfLength(10), UUIDs.randomBase64UUID());
        ShardId shardId = new ShardId(indexId.getName(), indexId.getId(), 1);
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .build();
        IndexSettings indexSettings = new IndexSettings(
            IndexMetadata.builder(indexId.getName()).settings(settings).build(),
            Settings.EMPTY
        );
        Store dummyStore = new Store(shardId, indexSettings, new ByteBuffersDirectory(), new DummyShardLock(shardId));
        return new SnapshotShardContext(
            dummyStore,
            null,
            snapshotId,
            indexId,
            new Engine.IndexCommitRef(null, () -> {}),
            null,
            IndexShardSnapshotStatus.newInitializing(null),
            Version.CURRENT,
            Collections.emptyMap(),
            startTime,
            ActionListener.noop()
        );
    }

    public void testEnqueueCreatesNewWorkersIfNecessary() throws Exception {
        int maxSize = randomIntBetween(1, threadPool.info(ThreadPool.Names.SNAPSHOT).getMax());
        CountDownLatch snapshotBlocker = new CountDownLatch(1);
        MockedRepo repo = new MockedRepo(snapshotBlocker);
        ShardSnapshotWorkers workers = new ShardSnapshotWorkers(maxSize, executor, repo::snapshotShard, repo::snapshotFile);
        repo.setWorkers(workers);
        int enqueuedSnapshots = maxSize - 1; // So that it is possible to create at least one more worker
        for (int i = 0; i < enqueuedSnapshots; i++) {
            workers.enqueueShardSnapshot(dummyContext());
        }
        assertBusy(() -> assertThat(workers.size(), equalTo(maxSize - 1)));
        // Adding at least one new shard snapshot would create a new worker
        int newTasks = randomIntBetween(1, 10);
        for (int i = 0; i < newTasks; i++) {
            workers.enqueueShardSnapshot(dummyContext());
        }
        enqueuedSnapshots += newTasks;
        assertThat(workers.size(), equalTo(maxSize));
        snapshotBlocker.countDown();
        // Eventually all workers exit
        assertBusy(() -> assertThat(workers.size(), equalTo(0)));
        assertThat(repo.finishedFileSnapshotTasks(), equalTo(repo.expectedFileSnapshotTasks()));
        assertThat(repo.finishedShardSnapshotTasks(), equalTo(enqueuedSnapshots));
        assertThat(repo.finishedShardSnapshots(), equalTo(enqueuedSnapshots));
    }

    public void testCompareToShardSnapshotTask() {
        ShardSnapshotWorkers workers = new ShardSnapshotWorkers(1, executor, context -> {}, (context, fileInfo) -> {});
        SnapshotId s1 = new SnapshotId("s1", UUIDs.randomBase64UUID());
        SnapshotId s2 = new SnapshotId("s2", UUIDs.randomBase64UUID());
        SnapshotId s3 = new SnapshotId("s3", UUIDs.randomBase64UUID());
        ActionListener<Void> listener = ActionListener.noop();
        final long s1StartTime = threadPool.absoluteTimeInMillis();
        final long s2StartTime = s1StartTime + randomLongBetween(1, 1000);
        SnapshotShardContext s1Context = dummyContext(s1, s1StartTime);
        SnapshotShardContext s2Context = dummyContext(s2, s2StartTime);
        SnapshotShardContext s3Context = dummyContext(s3, s2StartTime);
        // Two tasks with the same start time and of the same type have the same priority
        assertThat(workers.new ShardSnapshotTask(s2Context).compareTo(workers.new ShardSnapshotTask(s3Context)), equalTo(0));
        // Shard snapshot task always has a higher priority over file snapshot
        assertThat(
            workers.new ShardSnapshotTask(s1Context).compareTo(workers.new FileSnapshotTask(s1Context, dummyFileInfo(), listener)),
            lessThan(0)
        );
        assertThat(
            workers.new ShardSnapshotTask(s2Context).compareTo(workers.new FileSnapshotTask(s1Context, dummyFileInfo(), listener)),
            lessThan(0)
        );
        // File snapshots are prioritized by start time.
        assertThat(
            workers.new FileSnapshotTask(s1Context, dummyFileInfo(), listener).compareTo(
                workers.new FileSnapshotTask(s2Context, dummyFileInfo(), listener)
            ),
            lessThan(0)
        );
    }
}
