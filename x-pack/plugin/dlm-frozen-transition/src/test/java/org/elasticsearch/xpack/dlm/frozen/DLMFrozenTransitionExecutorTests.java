/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.dlm.frozen;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.datastreams.lifecycle.ErrorEntry;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.datastreams.DataStreamsPlugin;
import org.elasticsearch.datastreams.lifecycle.DataStreamLifecycleService;
import org.elasticsearch.dlm.DataStreamLifecycleErrorStore;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.FixedExecutorBuilder;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.containsString;

public class DLMFrozenTransitionExecutorTests extends ESTestCase {
    private ThreadPool threadPool;
    private ClusterService clusterService;
    private DLMFrozenTransitionSettings transitionSettings;

    @Before
    public void setup() throws Exception {
        super.setUp();
        this.threadPool = new TestThreadPool("test-dlm-frozen-transition-executor");
        Set<Setting<?>> settingSet = new HashSet<>(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        settingSet.add(DLMFrozenTransitionSettings.TRANSITION_ENABLED_SETTING);
        this.clusterService = ClusterServiceUtils.createClusterService(
            threadPool,
            DiscoveryNodeUtils.create("node", "node"),
            Settings.EMPTY,
            new ClusterSettings(Settings.EMPTY, settingSet)
        );
        this.clusterService.getMasterService().setClusterStatePublisher((event, publishListener, ackListener) -> {
            ClusterServiceUtils.setAllElapsedMillis(event);
            ackListener.onCommit(TimeValue.ZERO);
            clusterService.getClusterApplierService()
                .onNewClusterState("mock_publish_to_self[" + event.getSummary() + "]", event::getNewState, ActionListener.wrap(ignored -> {
                    ackListener.onNodeAck(event.getNewState().nodes().getLocalNode(), null);
                    publishListener.onResponse(null);
                }, publishListener::onFailure));
        });
        this.transitionSettings = DLMFrozenTransitionSettings.create(clusterService);
    }

    @After
    public void tearDown() throws Exception {
        if (this.clusterService != null) {
            this.clusterService.close();
        }
        if (this.threadPool != null) {
            terminate(threadPool);
        }
        super.tearDown();
    }

    /**
     * Pairs a {@link TestThreadPool} carrying the DLM-frozen-transition executor with the
     * {@link DLMFrozenTransitionExecutor} that consumes it, so each test can own its own correctly-sized
     * executor and clean it up via try-with-resources.
     */
    private record TestExecutorHandle(TestThreadPool pool, DLMFrozenTransitionExecutor executor) implements Closeable {
        @Override
        public void close() {
            ThreadPool.terminate(pool, 10, TimeUnit.SECONDS);
        }
    }

    private TestExecutorHandle newExecutor(int maxConcurrency, int maxQueueSize) {
        return newExecutor(maxConcurrency, maxQueueSize, makeErrorStore());
    }

    private TestExecutorHandle newExecutor(int maxConcurrency, int maxQueueSize, DataStreamLifecycleErrorStore errorStore) {
        TestThreadPool pool = new TestThreadPool(
            "test-dlm-frozen-transition-pool",
            new FixedExecutorBuilder(
                Settings.EMPTY,
                DLMFrozenTransitionPlugin.EXECUTOR_NAME,
                maxConcurrency,
                maxQueueSize,
                "dlm.frozen.transition.thread_pool",
                EsExecutors.TaskTrackingConfig.DEFAULT
            )
        );
        DLMFrozenTransitionExecutor exec = new DLMFrozenTransitionExecutor(
            clusterService,
            maxConcurrency + maxQueueSize,
            transitionSettings,
            errorStore,
            pool.executor(DLMFrozenTransitionPlugin.EXECUTOR_NAME)
        );
        return new TestExecutorHandle(pool, exec);
    }

    public void testTransitionSubmitted() throws Exception {
        try (var handle = newExecutor(2, 10)) {
            var executor = handle.executor();
            var task = new TestDLMFrozenTransitionRunnable("running-index");
            task.blockUntil = new CountDownLatch(1);

            assertFalse(executor.transitionSubmitted("running-index"));

            Future<?> future = executor.submit(task);
            safeAwait(task.started);

            assertTrue(executor.transitionSubmitted("running-index"));
            assertFalse(executor.transitionSubmitted("other-index"));

            task.blockUntil.countDown();
            future.get(10, TimeUnit.SECONDS);
        }
    }

    public void testTransitionRemovedAfterCompletion() throws Exception {
        try (var handle = newExecutor(2, 100)) {
            var executor = handle.executor();
            var task = new TestDLMFrozenTransitionRunnable("done-index");

            executor.submit(task).get(10, TimeUnit.SECONDS);

            assertFalse(executor.transitionSubmitted("done-index"));
        }
    }

    public void testTransitionRemovedAfterFailure() throws Exception {
        var errorStore = makeErrorStore();
        try (var handle = newExecutor(2, 100, errorStore)) {
            var executor = handle.executor();
            var runtimeTask = new TestDLMFrozenTransitionRunnable("exception-index");
            runtimeTask.throwOnRun = new IllegalStateException("simulated failure");
            executor.submit(runtimeTask).get(10, TimeUnit.SECONDS);
            assertFalse(executor.transitionSubmitted("exception-index"));
            ErrorEntry err = errorStore.getError(ProjectId.DEFAULT, "exception-index");
            assertNotNull("expected an error to be recorded in the error store", err);
            assertThat(err.error(), containsString("simulated failure"));
        }
    }

    public void testIndexUnmarkedAfterUnrecoverableFailure() throws Exception {
        var errorStore = makeErrorStore();
        String indexName = "exception-index";
        try (var handle = newExecutor(2, 100, errorStore)) {
            var executor = handle.executor();
            ClusterServiceUtils.setState(
                clusterService,
                ClusterState.builder(ClusterState.EMPTY_STATE)
                    .nodes(
                        DiscoveryNodes.builder().add(DiscoveryNodeUtils.create("local")).localNodeId("local").masterNodeId("local").build()
                    )
                    .putProjectMetadata(
                        ProjectMetadata.builder(ProjectId.DEFAULT)
                            .put(
                                IndexMetadata.builder(indexName)
                                    .settings(
                                        Settings.builder()
                                            .put(IndexMetadata.SETTING_INDEX_UUID, randomAlphaOfLength(5))
                                            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                                            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                                            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                                            .build()
                                    )
                                    .putCustom(
                                        DataStreamsPlugin.LIFECYCLE_CUSTOM_INDEX_METADATA_KEY,
                                        Map.of(DataStreamLifecycleService.FROZEN_CANDIDATE_REPOSITORY_METADATA_KEY, "myrepo")
                                    )
                            )
                    )
            );
            var runtimeTask = new TestDLMFrozenTransitionRunnable(indexName);
            runtimeTask.throwOnRun = new DLMUnrecoverableException(indexName, "simulated unrecoverable failure");
            executor.submit(runtimeTask).get(10, TimeUnit.SECONDS);

            // Check that the cluster state has been updated with the index having its mark removed
            ClusterServiceUtils.awaitClusterState(cs -> {
                IndexMetadata index = clusterService.state().projectState(ProjectId.DEFAULT).metadata().index(indexName);
                if (index == null) {
                    fail("index should always exist");
                }
                Map<String, String> custom = index.getCustomData(DataStreamsPlugin.LIFECYCLE_CUSTOM_INDEX_METADATA_KEY);
                if (custom.containsKey(DataStreamLifecycleService.FROZEN_CANDIDATE_REPOSITORY_METADATA_KEY)) {
                    return false;
                }
                return custom.get(DataStreamLifecycleService.FROZEN_CANDIDATE_REPOSITORY_METADATA_KEY) == null;
            }, clusterService);

            // The removal happens asynchronously after all nodes ack the cluster state change
            assertBusy(() -> assertFalse(executor.transitionSubmitted(indexName)));
            assertNull(errorStore.getError(ProjectId.DEFAULT, indexName));
        }
    }

    public void testHasCapacity() throws Exception {
        int maxQueue = randomIntBetween(2, 50);
        try (var handle = newExecutor(1, maxQueue)) {
            var executor = handle.executor();
            CountDownLatch tasksStarted = new CountDownLatch(1);
            CountDownLatch firstTaskBlock = new CountDownLatch(1);
            CountDownLatch taskBlock = new CountDownLatch(1);

            assertTrue(executor.hasCapacity());

            var firstTask = new TestDLMFrozenTransitionRunnable("index-first");
            firstTask.started = tasksStarted;
            firstTask.blockUntil = firstTaskBlock;
            executor.submit(firstTask);

            // Fill remaining queue
            for (int i = 0; i < maxQueue; i++) {
                var task = new TestDLMFrozenTransitionRunnable("index-" + i);
                task.started = tasksStarted;
                task.blockUntil = taskBlock;
                executor.submit(task);
            }

            assertTrue(tasksStarted.await(10, TimeUnit.SECONDS));
            assertFalse(executor.hasCapacity());

            firstTaskBlock.countDown();
            assertBusy(() -> assertTrue(executor.hasCapacity()));
            taskBlock.countDown();
        }
    }

    public void testStop() throws Exception {
        try (var handle = newExecutor(1, 10)) {
            var executor = handle.executor();
            var task = new TestDLMFrozenTransitionRunnable("block-index");
            task.blockUntil = new CountDownLatch(1);

            executor.submit(task);
            safeAwait(task.started);

            executor.stop();
            assertFalse("executor must stop accepting work after stop()", executor.isAccepting());
            assertFalse(
                "submittedTransitions must be cleared so a re-mastered node can resubmit work",
                executor.transitionSubmitted("block-index")
            );
        }
    }

    /**
     * A task that is submitted to the executor but waiting in the queue (single thread occupied) must still
     * be reported as "submitted" by {@link DLMFrozenTransitionExecutor#transitionSubmitted}, because the entry
     * is added to {@code submittedTransitions} at submission time, not when the thread actually starts.
     * This is the invariant that {@code checkForFrozenIndices} relies on to prevent re-submission of queued tasks.
     */
    public void testTransitionSubmittedReturnsTrueForQueuedTask() throws Exception {
        try (var handle = newExecutor(1, 2)) {
            var executor = handle.executor();
            CountDownLatch firstStarted = new CountDownLatch(1);
            CountDownLatch block = new CountDownLatch(1);

            var runningTask = new TestDLMFrozenTransitionRunnable("running-index");
            runningTask.started = firstStarted;
            runningTask.blockUntil = block;
            executor.submit(runningTask);
            safeAwait(firstStarted); // single thread is now occupied

            var queuedTask = new TestDLMFrozenTransitionRunnable("queued-index");
            queuedTask.blockUntil = block;
            executor.submit(queuedTask); // sits in the queue; has not started

            assertEquals("Queued task should not have started yet", 1, queuedTask.started.getCount());
            assertTrue("transitionSubmitted must return true for a queued task", executor.transitionSubmitted("queued-index"));

            block.countDown();
        }
    }

    /**
     * When the underlying executor rejects a submission (queue full), {@link DLMFrozenTransitionExecutor#submit}
     * must remove the index from {@code submittedTransitions} before rethrowing, so that a future poll can retry.
     */
    public void testSubmitCleansUpEntryOnRejectedExecution() throws Exception {
        try (var handle = newExecutor(1, 1)) {
            var executor = handle.executor();
            CountDownLatch block = new CountDownLatch(1);
            CountDownLatch firstStarted = new CountDownLatch(1);

            var runningTask = new TestDLMFrozenTransitionRunnable("running-index");
            runningTask.started = firstStarted;
            runningTask.blockUntil = block;
            executor.submit(runningTask);
            safeAwait(firstStarted); // single thread occupied

            var queuedTask = new TestDLMFrozenTransitionRunnable("queued-index");
            queuedTask.blockUntil = block;
            executor.submit(queuedTask); // fills the one queue slot

            // Thread and queue are both full; next submit must be rejected
            var rejectedTask = new TestDLMFrozenTransitionRunnable("rejected-index");
            expectThrows(RejectedExecutionException.class, () -> executor.submit(rejectedTask));

            // The cleanup branch in submit() must have removed the entry so the index is no longer tracked
            assertFalse("Rejected index must be removed from submittedTransitions", executor.transitionSubmitted("rejected-index"));

            block.countDown();
        }
    }

    /**
     * {@link DLMFrozenTransitionExecutor#stop()} must cancel tasks that were waiting in the queue
     * and had not yet started, not only the currently-executing task.
     */
    public void testStopCancelsQueuedTasks() throws Exception {
        try (var handle = newExecutor(1, 5)) {
            var executor = handle.executor();
            CountDownLatch block = new CountDownLatch(1);
            CountDownLatch firstStarted = new CountDownLatch(1);

            var runningTask = new TestDLMFrozenTransitionRunnable("running-index");
            runningTask.started = firstStarted;
            runningTask.blockUntil = block;
            Future<?> runningFuture = executor.submit(runningTask);
            safeAwait(firstStarted); // single thread occupied

            List<Future<?>> queuedFutures = new ArrayList<>(3);
            List<String> queuedIndexNames = new ArrayList<>(3);
            for (int i = 0; i < 3; i++) {
                String name = "queued-index-" + i;
                var queuedTask = new TestDLMFrozenTransitionRunnable(name);
                queuedTask.blockUntil = block;
                queuedFutures.add(executor.submit(queuedTask));
                queuedIndexNames.add(name);
            }

            executor.stop();

            assertTrue("running task's future must be cancelled by stop()", runningFuture.isCancelled() || runningFuture.isDone());
            for (int i = 0; i < queuedFutures.size(); i++) {
                Future<?> f = queuedFutures.get(i);
                assertTrue("queued task's future must be cancelled by stop()", f.isCancelled() || f.isDone());
                assertFalse(
                    "submittedTransitions must no longer contain the queued index after stop()",
                    executor.transitionSubmitted(queuedIndexNames.get(i))
                );
            }
            assertFalse(
                "submittedTransitions must no longer contain the running index after stop()",
                executor.transitionSubmitted("running-index")
            );
        }
    }

    /**
     * Uses a {@link CyclicBarrier} to ensure all submitting threads call {@code submit()} at the same time,
     * verifying the executor accepts {@code maxConcurrency} simultaneous submissions without rejection.
     */
    public void testSimultaneousSubmissionsFromMultipleThreads() throws Exception {
        int maxConcurrency = between(2, 50);
        try (var handle = newExecutor(maxConcurrency, 1)) {
            var executor = handle.executor();
            CyclicBarrier barrier = new CyclicBarrier(maxConcurrency + 1);
            List<Future<?>> futures = new CopyOnWriteArrayList<>();
            List<Throwable> errors = new CopyOnWriteArrayList<>();
            List<Thread> submitters = new ArrayList<>(maxConcurrency + 1);

            for (int i = 0; i < maxConcurrency + 1; i++) {
                final String indexName = "simultaneous-" + i;
                Thread submitter = new Thread(() -> {
                    try {
                        barrier.await(10, TimeUnit.SECONDS);
                        futures.add(executor.submit(new TestDLMFrozenTransitionRunnable(indexName)));
                    } catch (Exception e) {
                        errors.add(e);
                    }
                }, "submitter-" + i);
                submitters.add(submitter);
                submitter.start();
            }

            for (Thread submitter : submitters) {
                submitter.join(10_000);
                assertFalse("Submitter thread should have finished", submitter.isAlive());
            }

            assertTrue("All submissions should succeed without error: " + errors, errors.isEmpty());
            for (Future<?> future : futures) {
                future.get(10, TimeUnit.SECONDS);
            }
        }
    }

    /**
     * Minimal test double implementing {@link DLMFrozenTransitionRunnable} with deterministic, test-controlled behavior.
     * The {@code started} latch always counts down when the task begins. Set {@code blockUntil} to a non-released latch
     * to hold the task, or leave it at the default (already released) for tasks that complete immediately.
     */
    static class TestDLMFrozenTransitionRunnable implements DLMFrozenTransitionRunnable {
        private final String indexName;
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch blockUntil = new CountDownLatch(0);
        Throwable throwOnRun;

        TestDLMFrozenTransitionRunnable(String indexName) {
            this.indexName = indexName;
        }

        @Override
        public String getIndexName() {
            return indexName;
        }

        @Override
        public ProjectId getProjectId() {
            return ProjectId.DEFAULT;
        }

        @Override
        public void run() {
            started.countDown();
            try {
                blockUntil.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
            if (throwOnRun instanceof RuntimeException rte) {
                throw rte;
            } else if (throwOnRun instanceof Error error) {
                throw error;
            }
        }
    }

    private DataStreamLifecycleErrorStore makeErrorStore() {
        return new DataStreamLifecycleErrorStore(System::currentTimeMillis);
    }
}
