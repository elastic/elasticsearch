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
import org.elasticsearch.action.support.RefCountingRunnable;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.snapshots.IndexShardSnapshotStatus;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.store.StoreFileMetadata;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.SnapshotIndexCommit;
import org.elasticsearch.repositories.SnapshotShardContext;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.test.DummyShardLock;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThan;

public class ShardSnapshotTaskRunnerTests extends ESTestCase {

    private ThreadPool threadPool;
    private Executor executor;

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
        private ShardSnapshotTaskRunner taskRunner;

        public void setTaskRunner(ShardSnapshotTaskRunner taskRunner) {
            this.taskRunner = taskRunner;
        }

        public void snapshotShard(SnapshotShardContext context) {
            int filesToUpload = randomIntBetween(0, 10);
            expectedFileSnapshotTasks.addAndGet(filesToUpload);
            try (var refs = new RefCountingRunnable(finishedShardSnapshots::incrementAndGet)) {
                for (int i = 0; i < filesToUpload; i++) {
                    taskRunner.enqueueFileSnapshot(context, ShardSnapshotTaskRunnerTests::dummyFileInfo, refs.acquireListener());
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

    public static BlobStoreIndexShardSnapshot.FileInfo dummyFileInfo() {
        String filename = randomAlphaOfLength(10);
        StoreFileMetadata metadata = new StoreFileMetadata(filename, 10, "CHECKSUM", IndexVersion.current().luceneVersion().toString());
        return new BlobStoreIndexShardSnapshot.FileInfo(filename, metadata, null);
    }

    public static SnapshotShardContext dummyContext() {
        return dummyContext(new SnapshotId(randomAlphaOfLength(10), UUIDs.randomBase64UUID()), randomMillisUpToYear9999());
    }

    public static SnapshotShardContext dummyContext(final SnapshotId snapshotId, final long startTime) {
        IndexId indexId = new IndexId(randomAlphaOfLength(10), UUIDs.randomBase64UUID());
        ShardId shardId = new ShardId(indexId.getName(), indexId.getId(), 1);
        IndexSettings indexSettings = new IndexSettings(
            IndexMetadata.builder(indexId.getName()).settings(indexSettings(Version.CURRENT, 1, 0)).build(),
            Settings.EMPTY
        );
        Store dummyStore = new Store(shardId, indexSettings, new ByteBuffersDirectory(), new DummyShardLock(shardId));
        return new SnapshotShardContext(
            dummyStore,
            null,
            snapshotId,
            indexId,
            new SnapshotIndexCommit(new Engine.IndexCommitRef(null, () -> {})),
            null,
            IndexShardSnapshotStatus.newInitializing(null),
            Version.CURRENT,
            startTime,
            ActionListener.noop()
        );
    }

    public void testShardSnapshotTaskRunner() throws Exception {
        int maxTasks = randomIntBetween(1, threadPool.info(ThreadPool.Names.SNAPSHOT).getMax());
        MockedRepo repo = new MockedRepo();
        ShardSnapshotTaskRunner taskRunner = new ShardSnapshotTaskRunner(maxTasks, executor, repo::snapshotShard, repo::snapshotFile);
        repo.setTaskRunner(taskRunner);
        int enqueuedSnapshots = randomIntBetween(maxTasks * 2, maxTasks * 10);
        for (int i = 0; i < enqueuedSnapshots; i++) {
            threadPool.generic().execute(() -> taskRunner.enqueueShardSnapshot(dummyContext()));
        }
        // Eventually all snapshots are finished
        assertBusy(() -> {
            assertThat(repo.finishedShardSnapshots(), equalTo(enqueuedSnapshots));
            assertThat(taskRunner.runningTasks(), equalTo(0));
        });
        assertThat(taskRunner.queueSize(), equalTo(0));
        assertThat(repo.finishedFileSnapshotTasks(), equalTo(repo.expectedFileSnapshotTasks()));
        assertThat(repo.finishedShardSnapshotTasks(), equalTo(enqueuedSnapshots));
    }

    public void testCompareToShardSnapshotTask() {
        ShardSnapshotTaskRunner workers = new ShardSnapshotTaskRunner(1, executor, context -> {}, (context, fileInfo) -> {});
        SnapshotId s1 = new SnapshotId("s1", "s1-uuid");
        SnapshotId s2 = new SnapshotId("s2", "s2-uuid");
        SnapshotId s3 = new SnapshotId("s3", "s3-uuid");
        ActionListener<Void> listener = ActionListener.noop();
        final long s1StartTime = threadPool.absoluteTimeInMillis();
        final long s2StartTime = s1StartTime + randomLongBetween(1, 1000);
        SnapshotShardContext s1Context = dummyContext(s1, s1StartTime);
        SnapshotShardContext s2Context = dummyContext(s2, s2StartTime);
        SnapshotShardContext s3Context = dummyContext(s3, s2StartTime);
        // Shard snapshot and file snapshot tasks for earlier snapshots have higher priority
        assertThat(
            workers.new ShardSnapshotTask(s1Context).compareTo(
                workers.new FileSnapshotTask(s2Context, ShardSnapshotTaskRunnerTests::dummyFileInfo, listener)
            ),
            lessThan(0)
        );
        assertThat(
            workers.new FileSnapshotTask(s1Context, ShardSnapshotTaskRunnerTests::dummyFileInfo, listener).compareTo(
                workers.new ShardSnapshotTask(s2Context)
            ),
            lessThan(0)
        );
        // Two tasks with the same start time and of the same type are ordered by snapshot UUID
        assertThat(workers.new ShardSnapshotTask(s2Context).compareTo(workers.new ShardSnapshotTask(s3Context)), lessThan(0));
        assertThat(
            workers.new FileSnapshotTask(s2Context, ShardSnapshotTaskRunnerTests::dummyFileInfo, listener).compareTo(
                workers.new ShardSnapshotTask(s3Context)
            ),
            lessThan(0)
        );
        // Shard snapshot task has a higher priority over file snapshot within the same snapshot
        assertThat(
            workers.new ShardSnapshotTask(s1Context).compareTo(
                workers.new FileSnapshotTask(s1Context, ShardSnapshotTaskRunnerTests::dummyFileInfo, listener)
            ),
            lessThan(0)
        );
    }
}
