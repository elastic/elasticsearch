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
import org.elasticsearch.common.CheckedBiConsumer;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.PrioritizedThrottledTaskRunner;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

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
     * A node's upload concurrency control with a thread pool that has room for it, and repositories that get their runner from it, as
     * {@link BlobStoreRepository} does.
     */
    private static class UploadNode implements Releasable {
        final ThreadPool threadPool;
        final ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        final BackgroundNetworkQos qos;
        final int floor;
        private final List<Releasable> registrations = new ArrayList<>();

        UploadNode(int floor, int ceiling) {
            this.floor = floor;
            threadPool = new TestThreadPool(
                "upload-node",
                Settings.builder()
                    .put("thread_pool.snapshot.core", 1)
                    .put("thread_pool.snapshot.max", floor)
                    .put("thread_pool.snapshot_upload.core", 1)
                    .put("thread_pool.snapshot_upload.max", ceiling)
                    .build()
            );
            qos = new BackgroundNetworkQos(clusterSettings, threadPool, new RecoverySettings(Settings.EMPTY, clusterSettings), false);
        }

        void switchAdaptive(boolean on) {
            clusterSettings.applySettings(
                Settings.builder().put(BackgroundNetworkQos.ADAPTIVE_UPLOAD_CONCURRENCY_ENABLED_SETTING.getKey(), on).build()
            );
        }

        ShardSnapshotTaskRunner newRepository(
            Consumer<SnapshotShardContext> shardSnapshotter,
            CheckedBiConsumer<SnapshotShardContext, BlobStoreIndexShardSnapshot.FileInfo, IOException> fileSnapshotter
        ) {
            final var taskRunner = new PrioritizedThrottledTaskRunner<ShardSnapshotTaskRunner.SnapshotTask>(
                ShardSnapshotTaskRunner.TASK_RUNNER_NAME,
                floor,
                qos.getUploadExecutor(),
                qos::tryAcquireUploadPermit
            );
            registrations.add(qos.registerUploadTaskRunner(taskRunner));
            return new ShardSnapshotTaskRunner(taskRunner, shardSnapshotter, fileSnapshotter);
        }

        @Override
        public void close() {
            Releasables.close(registrations);
            terminate(threadPool);
        }
    }

    /** Shard snapshots that start when they run and then wait for the test to let them finish, by the uuid of their snapshot. */
    private static class Gates {
        private final Map<String, CountDownLatch> started = new ConcurrentHashMap<>();
        private final Map<String, CountDownLatch> released = new ConcurrentHashMap<>();
        final AtomicInteger finished = new AtomicInteger();

        SnapshotShardContext newSnapshot(long startTime) {
            final var snapshotId = new SnapshotId(randomIdentifier(), UUIDs.randomBase64UUID());
            started.put(snapshotId.getUUID(), new CountDownLatch(1));
            released.put(snapshotId.getUUID(), new CountDownLatch(1));
            return dummyContext(snapshotId, startTime);
        }

        void run(SnapshotShardContext context) {
            final String uuid = context.snapshotId().getUUID();
            started.get(uuid).countDown();
            safeAwait(released.get(uuid));
            finished.incrementAndGet();
        }

        boolean hasStarted(SnapshotShardContext context) {
            return started.get(context.snapshotId().getUUID()).getCount() == 0;
        }

        void release(SnapshotShardContext context) {
            released.get(context.snapshotId().getUUID()).countDown();
        }

        void awaitStarted(SnapshotShardContext context) {
            safeAwait(started.get(context.snapshotId().getUUID()));
        }
    }

    /**
     * Two repositories, as they are on a node: each has its own runner and queue, so that a long snapshot in one does not hold up a newer
     * one in the other, as it has always been. Only while the node adapts its upload concurrency do they share a budget of how many
     * tasks run at once, which goes to whichever repository asks first.
     */
    public void testRepositoriesDoNotBlockEachOtherUnlessTheyShareTheUploadBudget() throws Exception {
        try (var node = new UploadNode(2, 6)) {
            for (boolean adaptive : new boolean[] { false, true }) {
                node.switchAdaptive(adaptive);
                final var gates = new Gates();
                final var repoA = node.newRepository(gates::run, (context, fileInfo) -> {});
                final var repoB = node.newRepository(gates::run, (context, fileInfo) -> {});

                // the older snapshot has as many tasks running as the node's budget allows, or while there is none, fewer than its
                // runner allows and than the pool has threads
                final var older = new ArrayList<SnapshotShardContext>();
                for (int i = 0; i < (adaptive ? node.floor : 1); i++) {
                    older.add(gates.newSnapshot(1L));
                    repoA.enqueueShardSnapshot(older.get(i));
                }
                older.forEach(gates::awaitStarted);

                final var newer = gates.newSnapshot(2L);
                repoB.enqueueShardSnapshot(newer);
                if (adaptive) {
                    // the budget is used up: the newer snapshot waits in its own repository's queue
                    assertThat(repoB.runningTasks(), equalTo(0));
                    assertThat(repoB.queueSize(), equalTo(1));
                    assertFalse(gates.hasStarted(newer));
                    // and gets it when the other repository gives back some
                    gates.release(older.get(0));
                }
                gates.awaitStarted(newer);
                gates.release(newer);
                older.forEach(gates::release);
                assertBusy(() -> {
                    assertThat(repoA.runningTasks(), equalTo(0));
                    assertThat(repoB.runningTasks(), equalTo(0));
                    assertThat(node.qos.getRunningUploadTasks(), equalTo(0));
                });
                assertThat(gates.finished.get(), equalTo(older.size() + 1));
            }
        }
    }

    /**
     * Switching adaptive upload concurrency on and off while a snapshot runs only changes whether the budget is consulted: nothing that
     * is queued gets lost, and nothing starts that the limit in effect does not allow.
     */
    public void testSwitchingUploadBudgetOnAndOffMidSnapshot() throws Exception {
        try (var node = new UploadNode(2, 6)) {
            final var gates = new Gates();
            final var repo = node.newRepository(gates::run, (context, fileInfo) -> {});
            final var contexts = new ArrayList<SnapshotShardContext>();
            for (int i = 0; i < 4; i++) {
                contexts.add(gates.newSnapshot(i));
                repo.enqueueShardSnapshot(contexts.get(i));
            }
            // not adaptive: the runner's own limit
            assertThat(repo.runningTasks(), equalTo(node.floor));
            assertThat(repo.queueSize(), equalTo(4 - node.floor));
            assertThat(node.qos.getRunningUploadTasks(), equalTo(node.floor));

            // on: what runs is counted against the budget, which starts at the floor, so nothing more starts although the runner could
            node.switchAdaptive(true);
            assertThat(repo.runningTasks(), equalTo(node.floor));
            assertThat(repo.queueSize(), equalTo(4 - node.floor));

            // a task that finishes gives its budget to the next one in the queue
            gates.release(contexts.get(0));
            gates.awaitStarted(contexts.get(2));
            assertThat(repo.queueSize(), equalTo(1));
            assertThat(repo.runningTasks(), equalTo(node.floor));
            assertThat(node.qos.getRunningUploadTasks(), equalTo(node.floor));

            // off: the runner's own limit again, which what is running already uses up
            node.switchAdaptive(false);
            assertThat(repo.runningTasks(), equalTo(node.floor));
            assertThat(repo.queueSize(), equalTo(1));
            gates.release(contexts.get(1));
            gates.awaitStarted(contexts.get(3));
            assertThat(repo.queueSize(), equalTo(0));

            // on again, with work that has to wait for the budget
            node.switchAdaptive(true);
            final var more = new ArrayList<SnapshotShardContext>();
            for (int i = 0; i < 2; i++) {
                more.add(gates.newSnapshot(10 + i));
                repo.enqueueShardSnapshot(more.get(i));
            }
            assertThat(repo.queueSize(), equalTo(2));
            assertThat(repo.runningTasks(), equalTo(node.floor));

            // nothing was lost
            contexts.forEach(gates::release);
            more.forEach(gates::release);
            assertBusy(() -> {
                assertThat(gates.finished.get(), equalTo(contexts.size() + more.size()));
                assertThat(repo.runningTasks(), equalTo(0));
                assertThat(repo.queueSize(), equalTo(0));
                assertThat(node.qos.getRunningUploadTasks(), equalTo(0));
            });
        }
    }

    public void testSwitchingUploadBudgetOnAndOffWhileTasksRun() throws Exception {
        try (var node = new UploadNode(randomIntBetween(1, 3), 6)) {
            final int tasks = 200;
            final var finished = new CountDownLatch(tasks);
            final var repoA = node.newRepository(context -> finished.countDown(), (context, fileInfo) -> {});
            final var repoB = node.newRepository(context -> finished.countDown(), (context, fileInfo) -> {});
            for (int i = 0; i < tasks; i++) {
                node.switchAdaptive(randomBoolean());
                (randomBoolean() ? repoA : repoB).enqueueShardSnapshot(dummyContext());
            }
            // whichever way it is left, everything runs, and every task has given its budget back
            node.switchAdaptive(randomBoolean());
            safeAwait(finished);
            assertBusy(() -> {
                assertThat(repoA.runningTasks(), equalTo(0));
                assertThat(repoB.runningTasks(), equalTo(0));
                assertThat(repoA.queueSize(), equalTo(0));
                assertThat(repoB.queueSize(), equalTo(0));
                assertThat(node.qos.getRunningUploadTasks(), equalTo(0));
            });
        }
    }
}
