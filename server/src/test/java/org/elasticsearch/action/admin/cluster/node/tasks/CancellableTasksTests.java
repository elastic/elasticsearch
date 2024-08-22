/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.action.admin.cluster.node.tasks;

import com.carrotsearch.randomizedtesting.RandomizedContext;
import com.carrotsearch.randomizedtesting.generators.RandomNumbers;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.node.tasks.cancel.CancelTasksRequest;
import org.elasticsearch.action.admin.cluster.node.tasks.cancel.TransportCancelTasksAction;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksRequest;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksResponse;
import org.elasticsearch.action.support.ActionTestUtils;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.nodes.BaseNodesRequest;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskCancelHelper;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskInfo;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.test.ReachabilityChecker;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.FakeTcpChannel;
import org.elasticsearch.transport.TestTransportChannels;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Phaser;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.Collections.emptyMap;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class CancellableTasksTests extends TaskManagerTestCase {

    public static class CancellableNodeRequest extends TransportRequest {
        protected String requestName;

        public CancellableNodeRequest() {
            super();
        }

        public CancellableNodeRequest(StreamInput in) throws IOException {
            super(in);
            requestName = in.readString();
        }

        public CancellableNodeRequest(CancellableNodesRequest request) {
            requestName = request.requestName;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(requestName);
        }

        @Override
        public String getDescription() {
            return "CancellableNodeRequest[" + requestName + "]";
        }

        @Override
        public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
            return new CancellableTask(id, type, action, getDescription(), parentTaskId, headers) {
                @Override
                public boolean shouldCancelChildrenOnCancellation() {
                    return false;
                }
            };
        }
    }

    public static class CancellableNodesRequest extends BaseNodesRequest<CancellableNodesRequest> {
        private final String requestName;

        public CancellableNodesRequest(String requestName, String... nodesIds) {
            super(nodesIds);
            this.requestName = requestName;
        }

        @Override
        public String getDescription() {
            return "CancellableNodesRequest[" + requestName + "]";
        }

        @Override
        public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
            return new CancellableTask(id, type, action, getDescription(), parentTaskId, headers);
        }
    }

    /**
     * Simulates a cancellable node-based task that can be used to block node tasks so they are guaranteed to be registered by task manager
     */
    class CancellableTestNodesAction extends AbstractTestNodesAction<CancellableNodesRequest, CancellableNodeRequest> {

        // True if the node operation should get stuck until its cancelled
        final boolean shouldBlock;

        final CountDownLatch actionStartedLatch;

        CancellableTestNodesAction(
            String actionName,
            ThreadPool threadPool,
            ClusterService clusterService,
            TransportService transportService,
            boolean shouldBlock,
            CountDownLatch actionStartedLatch
        ) {
            super(actionName, threadPool, clusterService, transportService, CancellableNodeRequest::new);
            this.shouldBlock = shouldBlock;
            this.actionStartedLatch = actionStartedLatch;
        }

        @Override
        protected CancellableNodeRequest newNodeRequest(CancellableNodesRequest request) {
            return new CancellableNodeRequest(request);
        }

        @Override
        protected NodeResponse nodeOperation(CancellableNodeRequest request, Task task) {
            assert task instanceof CancellableTask;
            debugDelay("op1");
            if (actionStartedLatch != null) {
                actionStartedLatch.countDown();
            }

            debugDelay("op2");
            if (shouldBlock) {
                // Simulate a job that takes forever to finish
                // Using periodic checks method to identify that the task was cancelled
                waitUntil(() -> {
                    ((CancellableTask) task).ensureNotCancelled();
                    return false;
                });
                fail("It should have thrown an exception");
            }
            debugDelay("op4");

            return new NodeResponse(clusterService.localNode());
        }
    }

    /**
     * Simulates a cancellation listener and sets a flag to true if the task was cancelled
     */
    static class CancellableTestCancellationListener implements CancellableTask.CancellationListener {

        final AtomicBoolean calledUponCancellation = new AtomicBoolean(false);

        @Override
        public void onCancelled() {
            calledUponCancellation.set(true);
        }
    }

    private Task startCancellableTestNodesAction(
        boolean waitForActionToStart,
        int runNodesCount,
        int blockedNodesCount,
        ActionListener<NodesResponse> listener
    ) throws InterruptedException {
        List<TestNode> runOnNodes = randomSubsetOf(runNodesCount, testNodes);

        return startCancellableTestNodesAction(
            waitForActionToStart,
            runOnNodes,
            randomSubsetOf(blockedNodesCount, runOnNodes),
            new CancellableNodesRequest("Test Request", runOnNodes.stream().map(TestNode::getNodeId).toArray(String[]::new)),
            listener
        );
    }

    private Task startCancellableTestNodesAction(
        boolean waitForActionToStart,
        List<TestNode> runOnNodes,
        Collection<TestNode> blockOnNodes,
        CancellableNodesRequest request,
        ActionListener<NodesResponse> listener
    ) throws InterruptedException {
        CountDownLatch actionLatch = waitForActionToStart ? new CountDownLatch(runOnNodes.size()) : null;
        CancellableTestNodesAction[] actions = new CancellableTestNodesAction[nodesCount];
        for (int i = 0; i < testNodes.length; i++) {
            boolean shouldBlock = blockOnNodes.contains(testNodes[i]);
            boolean shouldRun = runOnNodes.contains(testNodes[i]);
            logger.info(
                "The action on the node [{}] should run: [{}] should block: [{}]",
                testNodes[i].getNodeId(),
                shouldRun,
                shouldBlock
            );
            actions[i] = new CancellableTestNodesAction(
                "internal:testAction",
                threadPool,
                testNodes[i].clusterService,
                testNodes[i].transportService,
                shouldBlock,
                actionLatch
            );
        }
        Task task = testNodes[0].transportService.getTaskManager()
            .registerAndExecute("transport", actions[0], request, testNodes[0].transportService.getLocalNodeConnection(), listener);
        if (waitForActionToStart) {
            logger.info("Awaiting for all actions to start");
            actionLatch.await();
            logger.info("Done waiting for all actions to start");
        }
        return task;
    }

    public void testBasicTaskCancellation() throws Exception {
        setupTestNodes(Settings.EMPTY);
        connectNodes(testNodes);
        CountDownLatch responseLatch = new CountDownLatch(1);
        AtomicBoolean listenerCalledUponCancellation = new AtomicBoolean(false);
        boolean waitForActionToStart = randomBoolean();
        logger.info("waitForActionToStart is set to {}", waitForActionToStart);
        final AtomicReference<NodesResponse> responseReference = new AtomicReference<>();
        final AtomicReference<Throwable> throwableReference = new AtomicReference<>();
        int runNodesCount = randomIntBetween(1, nodesCount);
        // Block at least 1 node, otherwise it's quite easy to end up in a race condition where the node tasks
        // have finished before the cancel request has arrived
        int blockedNodesCount = randomIntBetween(1, runNodesCount);
        Task mainTask = startCancellableTestNodesAction(waitForActionToStart, runNodesCount, blockedNodesCount, new ActionListener<>() {
            @Override
            public void onResponse(NodesResponse listTasksResponse) {
                responseReference.set(listTasksResponse);
                responseLatch.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                throwableReference.set(e);
                responseLatch.countDown();
            }
        });

        assert mainTask instanceof CancellableTask;
        CancellableTestCancellationListener listenerAddedBeforeCancellation = new CancellableTestCancellationListener();
        ((CancellableTask) mainTask).addListener(listenerAddedBeforeCancellation);

        // Cancel main task
        CancelTasksRequest request = new CancelTasksRequest();
        request.setReason("Testing Cancellation");
        request.setTargetTaskId(new TaskId(testNodes[0].getNodeId(), mainTask.getId()));
        // And send the cancellation request to a random node
        ListTasksResponse response = ActionTestUtils.executeBlocking(
            testNodes[randomIntBetween(0, testNodes.length - 1)].transportCancelTasksAction,
            request
        );

        // Awaiting for the main task to finish
        responseLatch.await();

        if (response.getTasks().size() == 0) {
            // We didn't cancel the request and it finished successfully
            // That should be rare and can be only in case we didn't block on a single node
            assertEquals(0, blockedNodesCount);
            // Make sure that the request was successful
            assertNull(throwableReference.get());
            assertNotNull(responseReference.get());
            assertEquals(runNodesCount, responseReference.get().getNodes().size());
            assertEquals(0, responseReference.get().failureCount());
        } else {
            assertThat(throwableReference.get(), instanceOf(TaskCancelledException.class));

            // We should have the information about the cancelled task in the cancel operation response
            assertEquals(1, response.getTasks().size());
            assertEquals(mainTask.getId(), response.getTasks().get(0).id());
            // Verify that all cancelled tasks reported that they support cancellation
            for (TaskInfo taskInfo : response.getTasks()) {
                assertTrue(taskInfo.cancellable());
            }

            CancellableTestCancellationListener listenerAddedAfterCancellation = new CancellableTestCancellationListener();
            ((CancellableTask) mainTask).addListener(listenerAddedAfterCancellation);

            // Verify both cancellation listeners have been notified
            assertTrue(listenerAddedBeforeCancellation.calledUponCancellation.get());
            assertTrue(listenerAddedAfterCancellation.calledUponCancellation.get());
        }

        // Make sure that tasks are no longer running
        ListTasksResponse listTasksResponse = ActionTestUtils.executeBlocking(
            testNodes[randomIntBetween(0, testNodes.length - 1)].transportListTasksAction,
            new ListTasksRequest().setTargetTaskId(new TaskId(testNodes[0].getNodeId(), mainTask.getId()))
        );
        assertEquals(0, listTasksResponse.getTasks().size());

        // Make sure that there are no leftover bans, the ban removal is async, so we might return from the cancellation
        // while the ban is still there, but it should disappear shortly
        assertBusy(() -> {
            for (int i = 0; i < testNodes.length; i++) {
                assertThat("No bans on the node " + i, testNodes[i].transportService.getTaskManager().getBannedTaskIds(), empty());
            }
        });
    }

    public void testChildTasksCancellation() throws Exception {
        setupTestNodes(Settings.EMPTY);
        connectNodes(testNodes);
        CountDownLatch responseLatch = new CountDownLatch(1);
        final AtomicReference<NodesResponse> responseReference = new AtomicReference<>();
        final AtomicReference<Throwable> throwableReference = new AtomicReference<>();
        int runNodesCount = randomIntBetween(1, nodesCount);
        int blockedNodesCount = randomIntBetween(0, runNodesCount);
        Task mainTask = startCancellableTestNodesAction(true, runNodesCount, blockedNodesCount, new ActionListener<>() {
            @Override
            public void onResponse(NodesResponse listTasksResponse) {
                responseReference.set(listTasksResponse);
                responseLatch.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                throwableReference.set(e);
                responseLatch.countDown();
            }
        });

        // Cancel all child tasks without cancelling the main task, which should quit on its own
        CancelTasksRequest request = new CancelTasksRequest();
        request.setReason("Testing Cancellation");
        request.setTargetParentTaskId(new TaskId(testNodes[0].getNodeId(), mainTask.getId()));
        // And send the cancellation request to a random node
        ListTasksResponse response = ActionTestUtils.executeBlocking(
            testNodes[randomIntBetween(1, testNodes.length - 1)].transportCancelTasksAction,
            request
        );

        // Awaiting for the main task to finish
        responseLatch.await();

        // Should have cancelled tasks at least on all nodes where it was blocked
        assertThat(response.getTasks().size(), lessThanOrEqualTo(runNodesCount));
        // but may also encounter some nodes where it was still running
        assertThat(response.getTasks().size(), greaterThanOrEqualTo(blockedNodesCount));

        assertBusy(() -> {
            // Make sure that main task is no longer running
            ListTasksResponse listTasksResponse = ActionTestUtils.executeBlocking(
                testNodes[randomIntBetween(0, testNodes.length - 1)].transportListTasksAction,
                new ListTasksRequest().setTargetTaskId(new TaskId(testNodes[0].getNodeId(), mainTask.getId()))
            );
            assertEquals(0, listTasksResponse.getTasks().size());
        });
    }

    public void testRegisterAndExecuteChildTaskWhileParentTaskIsBeingCanceled() throws Exception {
        setupTestNodes(Settings.EMPTY);
        connectNodes(testNodes);
        final TaskManager taskManager = testNodes[0].transportService.getTaskManager();
        CancellableNodesRequest parentRequest = new CancellableNodesRequest("parent");
        final Task parentTask = taskManager.register("test", "test", parentRequest);
        final TaskId parentTaskId = parentTask.taskInfo(testNodes[0].getNodeId(), false).taskId();
        taskManager.setBan(
            new TaskId(testNodes[0].getNodeId(), parentTask.getId()),
            "test",
            TestTransportChannels.newFakeTcpTransportChannel(
                testNodes[0].getNodeId(),
                new FakeTcpChannel(),
                threadPool,
                "test",
                randomNonNegativeLong(),
                TransportVersion.current()
            )
        );
        CancellableNodesRequest childRequest = new CancellableNodesRequest("child");
        childRequest.setParentTask(parentTaskId);
        CancellableTestNodesAction testAction = new CancellableTestNodesAction(
            "internal:testAction",
            threadPool,
            testNodes[1].clusterService,
            testNodes[0].transportService,
            false,
            new CountDownLatch(1)
        );
        TaskCancelledException cancelledException = expectThrows(
            TaskCancelledException.class,
            () -> taskManager.registerAndExecute(
                "test",
                testAction,
                childRequest,
                testNodes[0].transportService.getLocalNodeConnection(),
                ActionListener.noop()
            )
        );
        assertThat(cancelledException.getMessage(), equalTo("task cancelled before starting [test]"));
        CountDownLatch latch = new CountDownLatch(1);
        taskManager.startBanOnChildTasks(parentTaskId.getId(), "reason", latch::countDown);
        assertTrue("onChildTasksCompleted() is not invoked", latch.await(1, TimeUnit.SECONDS));
    }

    public void testTaskCancelledWhenConnectionClose() throws Exception {
        setupTestNodes(Settings.EMPTY);
        connectNodes(testNodes);
        CountDownLatch responseLatch = new CountDownLatch(1);
        final AtomicReference<NodesResponse> responseReference = new AtomicReference<>();
        final AtomicReference<Throwable> throwableReference = new AtomicReference<>();
        int blockedNodesCount = randomIntBetween(0, nodesCount - 1);

        // We shouldn't block on the first node since it's leaving the cluster anyway so it doesn't matter
        List<TestNode> blockOnNodes = randomSubsetOf(blockedNodesCount, Arrays.copyOfRange(testNodes, 1, nodesCount));
        Task mainTask = startCancellableTestNodesAction(
            true,
            Arrays.asList(testNodes),
            blockOnNodes,
            new CancellableNodesRequest("Test Request"),
            new ActionListener<NodesResponse>() {
                @Override
                public void onResponse(NodesResponse listTasksResponse) {
                    responseReference.set(listTasksResponse);
                    responseLatch.countDown();
                }

                @Override
                public void onFailure(Exception e) {
                    throwableReference.set(e);
                    responseLatch.countDown();
                }
            }
        );

        String mainNode = testNodes[0].getNodeId();

        // Make sure that tasks are running
        ListTasksResponse listTasksResponse = ActionTestUtils.executeBlocking(
            testNodes[randomIntBetween(0, testNodes.length - 1)].transportListTasksAction,
            new ListTasksRequest().setTargetParentTaskId(new TaskId(mainNode, mainTask.getId()))
        );
        assertThat(listTasksResponse.getTasks().size(), greaterThanOrEqualTo(blockOnNodes.size()));

        // Simulate connections close
        if (randomBoolean()) {
            for (int i = 1; i < testNodes.length; i++) {
                testNodes[i].transportService.disconnectFromNode(testNodes[0].discoveryNode());
            }
            if (randomBoolean()) {
                logger.info("--> Simulate issuing cancel request on the node that is about to leave the cluster");
                // Simulate issuing cancel request on the node that is about to leave the cluster
                CancelTasksRequest request = new CancelTasksRequest();
                request.setReason("Testing Cancellation");
                request.setTargetTaskId(new TaskId(testNodes[0].getNodeId(), mainTask.getId()));
                // And send the cancellation request to a random node
                ListTasksResponse response = ActionTestUtils.executeBlocking(testNodes[0].transportCancelTasksAction, request);
                logger.info("--> Done simulating issuing cancel request on the node that is about to leave the cluster");
                // This node still thinks that's part of the cluster, so cancelling should look successful
                assertThat(response.getTasks().size(), lessThanOrEqualTo(1));
                assertThat(response.getTaskFailures().size(), lessThanOrEqualTo(1));
                assertThat(response.getTaskFailures().size() + response.getTasks().size(), lessThanOrEqualTo(1));
            }
        }

        boolean mainNodeClosed = randomBoolean();
        if (mainNodeClosed) {
            testNodes[0].close();
        } else {
            for (TestNode blockOnNode : blockOnNodes) {
                if (randomBoolean()) {
                    testNodes[0].transportService.disconnectFromNode(blockOnNode.discoveryNode());
                } else {
                    testNodes[0].transportService.getConnection(blockOnNode.discoveryNode()).close();
                }
            }
        }

        assertBusy(() -> {
            // Make sure that tasks are no longer running
            ListTasksResponse listTasksResponse1 = ActionTestUtils.executeBlocking(
                testNodes[randomIntBetween(1, testNodes.length - 1)].transportListTasksAction,
                new ListTasksRequest().setTargetTaskId(new TaskId(mainNode, mainTask.getId()))
            );
            assertEquals(0, listTasksResponse1.getTasks().size());
        });

        // Wait for clean up
        responseLatch.await();
        assertBusy(() -> {
            // If the main node is closed, then we won't able to send unban requests to remove bans, but a direct channel never closes.
            // Hence, we can't verify if all bans are removed.
            if (mainNodeClosed == false) {
                assertThat("No bans on the node " + 0, testNodes[0].transportService.getTaskManager().getBannedTaskIds(), empty());
            }
            for (int i = 1; i < testNodes.length; i++) {
                assertThat("No bans on the node " + i, testNodes[i].transportService.getTaskManager().getBannedTaskIds(), empty());
            }
        });
    }

    public void testNonExistingTaskCancellation() throws Exception {
        setupTestNodes(Settings.EMPTY);
        connectNodes(testNodes);

        // Cancel a task that doesn't exist
        CancelTasksRequest request = new CancelTasksRequest();
        request.setReason("Testing Cancellation");
        request.setActions("do-not-match-anything");
        request.setNodes(
            randomSubsetOf(randomIntBetween(1, testNodes.length - 1), testNodes).stream().map(TestNode::getNodeId).toArray(String[]::new)
        );
        // And send the cancellation request to a random node
        ListTasksResponse response = ActionTestUtils.executeBlocking(
            testNodes[randomIntBetween(1, testNodes.length - 1)].transportCancelTasksAction,
            request
        );

        // Shouldn't have cancelled anything
        assertThat(response.getTasks().size(), equalTo(0));

        assertBusy(() -> {
            // Make sure that main task is no longer running
            ListTasksResponse listTasksResponse = ActionTestUtils.executeBlocking(
                testNodes[randomIntBetween(0, testNodes.length - 1)].transportListTasksAction,
                new ListTasksRequest().setActions(TransportCancelTasksAction.NAME + "*")
            );
            assertEquals(0, listTasksResponse.getTasks().size());
        });
    }

    public void testCancelConcurrently() throws Exception {
        setupTestNodes(Settings.EMPTY);
        final TaskManager taskManager = testNodes[0].transportService.getTaskManager();
        int numTasks = randomIntBetween(1, 10);
        List<CancellableTask> tasks = new ArrayList<>(numTasks);
        for (int i = 0; i < numTasks; i++) {
            tasks.add((CancellableTask) taskManager.register("type-" + i, "action-" + i, new CancellableNodeRequest()));
        }
        Thread[] threads = new Thread[randomIntBetween(1, 8)];
        AtomicIntegerArray notified = new AtomicIntegerArray(threads.length);
        Phaser phaser = new Phaser(threads.length + 1);
        final CancellableTask cancellingTask = randomFrom(tasks);
        for (int i = 0; i < threads.length; i++) {
            int idx = i;
            threads[i] = new Thread(() -> {
                phaser.arriveAndAwaitAdvance();
                taskManager.cancel(cancellingTask, "test", () -> assertTrue(notified.compareAndSet(idx, 0, 1)));
            });
            threads[i].start();
        }
        phaser.arriveAndAwaitAdvance();
        taskManager.unregister(cancellingTask);
        for (int i = 0; i < threads.length; i++) {
            threads[i].join();
            assertThat(notified.get(i), equalTo(1));
        }
        AtomicBoolean called = new AtomicBoolean();
        taskManager.cancel(cancellingTask, "test", () -> assertTrue(called.compareAndSet(false, true)));
        assertTrue(called.get());
    }

    private static void debugDelay(String name) {
        // Introduce an additional pseudo random repeatable race conditions
        String delayName = RandomizedContext.current().getRunnerSeedAsString() + ":" + name;
        Random random = new Random(delayName.hashCode());
        if (RandomNumbers.randomIntBetween(random, 0, 10) < 1) {
            try {
                Thread.sleep(RandomNumbers.randomIntBetween(random, 20, 50));
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
            }
        }
    }

    public void testEnsureNotCancelled() throws Exception {
        final CancellableTask task = new CancellableTask(randomLong(), "transport", "action", "", TaskId.EMPTY_TASK_ID, emptyMap());
        task.ensureNotCancelled(); // does not throw
        TaskCancelHelper.cancel(task, "simulated");
        assertThat(
            expectThrows(TaskCancelledException.class, task::ensureNotCancelled).getMessage(),
            equalTo("task cancelled [simulated]")
        );
    }

    public void testNotifyIfCancelled() throws Exception {
        final CancellableTask task = new CancellableTask(randomLong(), "transport", "action", "", TaskId.EMPTY_TASK_ID, emptyMap());

        final PlainActionFuture<Void> future = new PlainActionFuture<>();
        task.notifyIfCancelled(future);
        assertFalse(future.isDone());

        TaskCancelHelper.cancel(task, "simulated");

        final CyclicBarrier barrier = new CyclicBarrier(2);

        final Thread concurrentNotify = new Thread(() -> task.notifyIfCancelled(new ActionListener<Void>() {
            @Override
            public void onResponse(Void unused) {
                fail("onResponse");
            }

            @Override
            public void onFailure(Exception e) {
                safeAwait(barrier);
                // main thread calls notifyIfCancelled again between these two blocks
                safeAwait(barrier);
            }
        }), "concurrent notify");
        concurrentNotify.start();

        safeAwait(barrier);
        task.notifyIfCancelled(future);
        assertTrue(future.isDone());
        assertThat(expectThrows(TaskCancelledException.class, future::actionGet).getMessage(), equalTo("task cancelled [simulated]"));
        safeAwait(barrier);
        concurrentNotify.join();
    }

    public void testReleaseListenersOnCancellation() {
        final CancellableTask task = new CancellableTask(randomLong(), "transport", "action", "", TaskId.EMPTY_TASK_ID, emptyMap());
        final AtomicBoolean cancelNotified = new AtomicBoolean();
        final ReachabilityChecker reachabilityChecker = new ReachabilityChecker();
        task.addListener(reachabilityChecker.register(() -> assertTrue(cancelNotified.compareAndSet(false, true))));

        reachabilityChecker.checkReachable();
        TaskCancelHelper.cancel(task, "simulated");
        reachabilityChecker.ensureUnreachable();
        assertTrue(cancelNotified.get());
    }

}
