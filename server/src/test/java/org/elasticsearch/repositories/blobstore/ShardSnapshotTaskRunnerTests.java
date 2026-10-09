/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.repositories.blobstore;

import org.apache.lucene.store.ByteBuffersDirectory;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.RefCountingRunnable;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.PrioritizedThrottledTaskRunner;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.snapshots.IndexShardSnapshotStatus;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.store.StoreFileMetadata;
import org.elasticsearch.indices.recovery.BackgroundNetworkQos;
import org.elasticsearch.indices.recovery.RecoverySettings;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.LocalPrimarySnapshotShardContext;
import org.elasticsearch.repositories.SnapshotIndexCommit;
import org.elasticsearch.repositories.SnapshotShardContext;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.test.DummyShardLock;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.equalTo;

public class ShardSnapshotTaskRunnerTests extends ESTestCase {

    private ThreadPool threadPool;
    private Executor executor;

    @Before
    public void startThreadPool() throws Exception {
        threadPool = new TestThreadPool("test");
        executor = threadPool.executor(ThreadPool.Names.SNAPSHOT);
    }

    @After
    public void stopThreadPool() throws Exception {
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
        return dummyContext(snapshotId, startTime, randomIdentifier(), 1);
    }

    public static SnapshotShardContext dummyContext(final SnapshotId snapshotId, final long startTime, String indexName, int shardIndex) {
        final var indexId = new IndexId(indexName, UUIDs.randomBase64UUID());
        final var shardId = new ShardId(indexId.getName(), UUIDs.randomBase64UUID(), shardIndex);
        final var indexSettings = new IndexSettings(
            IndexMetadata.builder(indexId.getName()).settings(indexSettings(IndexVersion.current(), 1, 0)).build(),
            Settings.EMPTY
        );
        final var dummyStore = new Store(shardId, indexSettings, new ByteBuffersDirectory(), new DummyShardLock(shardId));
        return new LocalPrimarySnapshotShardContext(
            dummyStore,
            null,
            snapshotId,
            indexId,
            new SnapshotIndexCommit(new Engine.IndexCommitRef(null, () -> {})),
            null,
            IndexShardSnapshotStatus.newInitializing(null, randomLongBetween(1, Long.MAX_VALUE)),
            IndexVersion.current(),
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

        record CapturedTask(SnapshotShardContext context, @Nullable BlobStoreIndexShardSnapshot.FileInfo fileInfo) {
            CapturedTask(SnapshotShardContext context) {
                this(context, null);
            }
        }

        final List<CapturedTask> tasksInExpectedOrder = new ArrayList<>();

        // first snapshot, one shard, one file, but should execute the shard-level task before the file task
        final var earlyStartTime = randomLongBetween(1L, Long.MAX_VALUE - 1000);
        final var s1Context = dummyContext(new SnapshotId(randomIdentifier(), randomUUID()), earlyStartTime);
        tasksInExpectedOrder.add(new CapturedTask(s1Context));
        tasksInExpectedOrder.add(new CapturedTask(s1Context, dummyFileInfo()));

        // second snapshot, also one shard and one file, starts later than the first
        final var laterStartTime = randomLongBetween(earlyStartTime + 1, Long.MAX_VALUE);
        final var s2Context = dummyContext(new SnapshotId(randomIdentifier(), "early-uuid"), laterStartTime);
        tasksInExpectedOrder.add(new CapturedTask(s2Context));
        tasksInExpectedOrder.add(new CapturedTask(s2Context, dummyFileInfo()));

        // third snapshot, starts at the same time as the second but has a later UUID
        final var snapshotId3 = new SnapshotId(randomIdentifier(), "later-uuid");

        // the third snapshot has three shards, and their respective tasks should execute in shard-id then index-name order:
        final var s3ContextShard1 = dummyContext(snapshotId3, laterStartTime, "early-index-name", 0);
        final var s3ContextShard2 = dummyContext(snapshotId3, laterStartTime, "later-index-name", 0);
        final var s3ContextShard3 = dummyContext(snapshotId3, laterStartTime, randomIdentifier(), 1);

        tasksInExpectedOrder.add(new CapturedTask(s3ContextShard1));
        tasksInExpectedOrder.add(new CapturedTask(s3ContextShard2));
        tasksInExpectedOrder.add(new CapturedTask(s3ContextShard3));

        tasksInExpectedOrder.add(new CapturedTask(s3ContextShard1, dummyFileInfo()));
        tasksInExpectedOrder.add(new CapturedTask(s3ContextShard2, dummyFileInfo()));
        tasksInExpectedOrder.add(new CapturedTask(s3ContextShard3, dummyFileInfo()));

        final var readyLatch = new CountDownLatch(1);
        final var startLatch = new CountDownLatch(1);
        final var doneLatch = new CountDownLatch(tasksInExpectedOrder.size() + 1);

        final List<CapturedTask> tasksInExecutionOrder = new ArrayList<>();
        final var runner = new ShardSnapshotTaskRunner(1, executor, context -> {
            tasksInExecutionOrder.add(new CapturedTask(context, null));
            readyLatch.countDown();
            safeAwait(startLatch);
            doneLatch.countDown();
        }, (context, fileInfo) -> {
            tasksInExecutionOrder.add(new CapturedTask(context, fileInfo));
            doneLatch.countDown();
        });

        // prime the pipeline by executing a dummy task and waiting for it to block the executor, so that the rest of the tasks are sorted
        // by the underlying PriorityQueue before any of them start to execute
        runner.enqueueShardSnapshot(dummyContext(new SnapshotId(randomIdentifier(), UUIDs.randomBase64UUID()), randomNonNegativeLong()));
        safeAwait(readyLatch);
        tasksInExecutionOrder.clear(); // remove the dummy task

        // submit the tasks in random order
        for (final var task : shuffledList(tasksInExpectedOrder)) {
            if (task.fileInfo() == null) {
                runner.enqueueShardSnapshot(task.context());
            } else {
                runner.enqueueFileSnapshot(task.context(), task::fileInfo, ActionListener.noop());
            }
        }

        // allow the tasks to execute
        startLatch.countDown();
        safeAwait(doneLatch);

        // finally verify that they executed in the order we expected
        assertEquals(tasksInExpectedOrder, tasksInExecutionOrder);
    }

    /**
     * Two repositories, as they are on a node: with the node's adaptive upload concurrency off, each runs its shard snapshots in its own
     * runner as it always has, so that a long snapshot in one does not hold up a newer one in the other. With it on, they share one.
     */
    public void testRepositoriesDoNotBlockEachOtherUnlessUploadConcurrencyIsShared() throws Exception {
        final ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        final BackgroundNetworkQos qos = new BackgroundNetworkQos(
            clusterSettings,
            threadPool,
            new RecoverySettings(Settings.EMPTY, clusterSettings),
            false
        );
        // one task at a time in each runner, on an executor with threads to spare
        qos.getUploadTaskRunner().setMaxRunningTasks(1);
        final Executor plentyOfThreads = threadPool.generic();
        final Supplier<PrioritizedThrottledTaskRunner<ShardSnapshotTaskRunner.SnapshotTask>> ownRunner =
            () -> new PrioritizedThrottledTaskRunner<>(ShardSnapshotTaskRunner.TASK_RUNNER_NAME, 1, plentyOfThreads);
        final var ownRunnerA = ownRunner.get();
        final var ownRunnerB = ownRunner.get();

        for (boolean shared : new boolean[] { false, true }) {
            clusterSettings.applySettings(
                Settings.builder().put(BackgroundNetworkQos.ADAPTIVE_UPLOAD_CONCURRENCY_ENABLED_SETTING.getKey(), shared).build()
            );
            qos.getUploadTaskRunner().setMaxRunningTasks(1);

            final var longSnapshotStarted = new CountDownLatch(1);
            final var releaseLongSnapshot = new CountDownLatch(1);
            final var newerSnapshotRan = new CountDownLatch(1);
            final var repoA = new ShardSnapshotTaskRunner(() -> qos.selectUploadTaskRunner(ownRunnerA), context -> {
                longSnapshotStarted.countDown();
                safeAwait(releaseLongSnapshot);
            }, (context, fileInfo) -> {});
            final var repoB = new ShardSnapshotTaskRunner(
                () -> qos.selectUploadTaskRunner(ownRunnerB),
                context -> newerSnapshotRan.countDown(),
                (context, fileInfo) -> {}
            );

            repoA.enqueueShardSnapshot(dummyContext(new SnapshotId("older", UUIDs.randomBase64UUID()), 1L));
            safeAwait(longSnapshotStarted);
            repoB.enqueueShardSnapshot(dummyContext(new SnapshotId("newer", UUIDs.randomBase64UUID()), 2L));
            if (shared) {
                // one runner for both: the newer snapshot waits for the long one
                assertThat(qos.getUploadTaskRunner().queueSize(), equalTo(1));
                assertThat(newerSnapshotRan.getCount(), equalTo(1L));
                releaseLongSnapshot.countDown();
            }
            safeAwait(newerSnapshotRan);
            releaseLongSnapshot.countDown();
            assertBusy(() -> {
                assertThat(qos.getUploadTaskRunner().runningTasks(), equalTo(0));
                assertThat(ownRunnerA.runningTasks(), equalTo(0));
                assertThat(ownRunnerB.runningTasks(), equalTo(0));
            });
        }
    }
}
