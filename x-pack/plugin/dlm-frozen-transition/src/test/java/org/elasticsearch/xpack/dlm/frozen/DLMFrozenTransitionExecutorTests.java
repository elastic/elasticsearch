/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.dlm.frozen;

import org.elasticsearch.Version;
import org.elasticsearch.action.datastreams.lifecycle.ErrorEntry;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.datastreams.DataStreamsPlugin;
import org.elasticsearch.datastreams.lifecycle.DataStreamLifecycleService;
import org.elasticsearch.datastreams.lifecycle.FrozenTransitionInfoProvider;
import org.elasticsearch.test.ClusterServiceUtils;
import org.junit.After;
import org.junit.Before;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.containsString;

public class DLMFrozenTransitionExecutorTests extends DLMFrozenTransitionExecutorTestCase {

    private ProjectId projectId = randomProjectIdOrDefault();

    @Before
    public void setUpExecutor() throws Exception {
        setupExecutorTestCase();
    }

    @After
    public void tearDownExecutor() throws Exception {
        tearDownExecutorTestCase();
    }

    public void testTransitionSubmitted() throws Exception {
        try (var handle = newExecutor(2, 10)) {
            var executor = handle.executor();
            var task = new TestDLMFrozenTransitionRunnable("running-index", projectId);
            task.blockUntil = new CountDownLatch(1);

            assertFalse(executor.transitionSubmitted(projectId, "running-index"));

            Future<?> future = executor.submit(task);
            safeAwait(task.started);

            assertTrue(executor.transitionSubmitted(projectId, "running-index"));
            assertFalse(executor.transitionSubmitted(projectId, "other-index"));

            task.blockUntil.countDown();
            future.get(10, TimeUnit.SECONDS);
        }
    }

    public void testTransitionRemovedAfterCompletion() throws Exception {
        try (var handle = newExecutor(2, 100)) {
            var executor = handle.executor();
            var task = new TestDLMFrozenTransitionRunnable("done-index", projectId);

            executor.submit(task).get(10, TimeUnit.SECONDS);

            assertFalse(executor.transitionSubmitted(projectId, "done-index"));
        }
    }

    public void testTransitionRemovedAfterFailure() throws Exception {
        var errorStore = makeErrorStore();
        try (var handle = newExecutor(2, 100, errorStore)) {
            var executor = handle.executor();
            var runtimeTask = new TestDLMFrozenTransitionRunnable("exception-index", projectId);
            runtimeTask.throwOnRun = new IllegalStateException("simulated failure");
            executor.submit(runtimeTask).get(10, TimeUnit.SECONDS);
            assertFalse(executor.transitionSubmitted(projectId, "exception-index"));
            ErrorEntry err = errorStore.getError(projectId, "exception-index");
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
                        ProjectMetadata.builder(projectId)
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
            var runtimeTask = new TestDLMFrozenTransitionRunnable(indexName, projectId);
            runtimeTask.throwOnRun = new DLMUnrecoverableException(indexName, "simulated unrecoverable failure");
            executor.submit(runtimeTask).get(10, TimeUnit.SECONDS);

            // Check that the cluster state has been updated with the index having its mark removed
            ClusterServiceUtils.awaitClusterState(cs -> {
                IndexMetadata index = clusterService.state().projectState(projectId).metadata().index(indexName);
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
            assertBusy(() -> assertFalse(executor.transitionSubmitted(projectId, indexName)));
            assertNull(errorStore.getError(projectId, indexName));
        }
    }

    public void testGetTransitionStatusNotStartedForUnknownIndex() throws Exception {
        try (var handle = newExecutor(2, 10)) {
            assertEquals(
                FrozenTransitionInfoProvider.Status.NOT_STARTED,
                handle.executor().getTransitionStatus(projectId, "never-submitted")
            );
        }
    }

    /**
     * Verifies the full status lifecycle a submitted transition goes through: {@code QUEUED} while sitting behind
     * another task occupying the single thread, {@code RUNNING} once it starts executing, and back to
     * {@code NOT_STARTED} once it completes and is removed from {@code submittedTransitions}.
     */
    public void testGetTransitionStatusTracksQueuedRunningAndCompletion() throws Exception {
        try (var handle = newExecutor(1, 2)) {
            var executor = handle.executor();
            CountDownLatch firstStarted = new CountDownLatch(1);
            CountDownLatch block = new CountDownLatch(1);

            var runningTask = new TestDLMFrozenTransitionRunnable("running-index", projectId);
            runningTask.started = firstStarted;
            runningTask.blockUntil = block;
            executor.submit(runningTask);
            safeAwait(firstStarted); // single thread is now occupied

            var queuedTask = new TestDLMFrozenTransitionRunnable("queued-index", projectId);
            queuedTask.blockUntil = block;
            Future<?> queuedFuture = executor.submit(queuedTask); // sits in the queue; has not started

            assertEquals(FrozenTransitionInfoProvider.Status.RUNNING, executor.getTransitionStatus(projectId, "running-index"));
            assertEquals(FrozenTransitionInfoProvider.Status.QUEUED, executor.getTransitionStatus(projectId, "queued-index"));

            block.countDown();
            queuedFuture.get(10, TimeUnit.SECONDS);

            assertEquals(FrozenTransitionInfoProvider.Status.NOT_STARTED, executor.getTransitionStatus(projectId, "running-index"));
            assertEquals(FrozenTransitionInfoProvider.Status.NOT_STARTED, executor.getTransitionStatus(projectId, "queued-index"));
        }
    }

    public void testGetTransitionStatusNotStartedAfterStop() throws Exception {
        try (var handle = newExecutor(1, 10)) {
            var executor = handle.executor();
            var task = new TestDLMFrozenTransitionRunnable("block-index", projectId);
            task.blockUntil = new CountDownLatch(1);

            executor.submit(task);
            safeAwait(task.started);
            assertEquals(FrozenTransitionInfoProvider.Status.RUNNING, executor.getTransitionStatus(projectId, "block-index"));

            executor.stop();
            assertEquals(FrozenTransitionInfoProvider.Status.NOT_STARTED, executor.getTransitionStatus(projectId, "block-index"));
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

            var firstTask = new TestDLMFrozenTransitionRunnable("index-first", projectId);
            firstTask.started = tasksStarted;
            firstTask.blockUntil = firstTaskBlock;
            executor.submit(firstTask);

            // Fill remaining queue
            for (int i = 0; i < maxQueue; i++) {
                var task = new TestDLMFrozenTransitionRunnable("index-" + i, projectId);
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
            var task = new TestDLMFrozenTransitionRunnable("block-index", projectId);
            task.blockUntil = new CountDownLatch(1);

            executor.submit(task);
            safeAwait(task.started);

            executor.stop();
            assertFalse("executor must stop accepting work after stop()", executor.isAccepting());
            assertFalse(
                "submittedTransitions must be cleared so a re-mastered node can resubmit work",
                executor.transitionSubmitted(projectId, "block-index")
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

            var runningTask = new TestDLMFrozenTransitionRunnable("running-index", projectId);
            runningTask.started = firstStarted;
            runningTask.blockUntil = block;
            executor.submit(runningTask);
            safeAwait(firstStarted); // single thread is now occupied

            var queuedTask = new TestDLMFrozenTransitionRunnable("queued-index", projectId);
            queuedTask.blockUntil = block;
            executor.submit(queuedTask); // sits in the queue; has not started

            assertEquals("Queued task should not have started yet", 1, queuedTask.started.getCount());
            assertTrue("transitionSubmitted must return true for a queued task", executor.transitionSubmitted(projectId, "queued-index"));

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

            var runningTask = new TestDLMFrozenTransitionRunnable("running-index", projectId);
            runningTask.started = firstStarted;
            runningTask.blockUntil = block;
            executor.submit(runningTask);
            safeAwait(firstStarted); // single thread occupied

            var queuedTask = new TestDLMFrozenTransitionRunnable("queued-index", projectId);
            queuedTask.blockUntil = block;
            executor.submit(queuedTask); // fills the one queue slot

            // Thread and queue are both full; next submit must be rejected
            var rejectedTask = new TestDLMFrozenTransitionRunnable("rejected-index", projectId);
            expectThrows(RejectedExecutionException.class, () -> executor.submit(rejectedTask));

            // The cleanup branch in submit() must have removed the entry so the index is no longer tracked
            assertFalse(
                "Rejected index must be removed from submittedTransitions",
                executor.transitionSubmitted(projectId, "rejected-index")
            );

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

            var runningTask = new TestDLMFrozenTransitionRunnable("running-index", projectId);
            runningTask.started = firstStarted;
            runningTask.blockUntil = block;
            Future<?> runningFuture = executor.submit(runningTask);
            safeAwait(firstStarted); // single thread occupied

            List<Future<?>> queuedFutures = new ArrayList<>(3);
            List<String> queuedIndexNames = new ArrayList<>(3);
            for (int i = 0; i < 3; i++) {
                String name = "queued-index-" + i;
                var queuedTask = new TestDLMFrozenTransitionRunnable(name, projectId);
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
                    executor.transitionSubmitted(projectId, queuedIndexNames.get(i))
                );
            }
            assertFalse(
                "submittedTransitions must no longer contain the running index after stop()",
                executor.transitionSubmitted(projectId, "running-index")
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
                        futures.add(executor.submit(new TestDLMFrozenTransitionRunnable(indexName, projectId)));
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
}
