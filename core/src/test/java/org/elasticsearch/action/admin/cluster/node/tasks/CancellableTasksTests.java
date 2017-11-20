/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.action.admin.cluster.node.tasks;

import com.carrotsearch.randomizedtesting.RandomizedContext;
import com.carrotsearch.randomizedtesting.generators.RandomNumbers;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.node.tasks.cancel.CancelTasksRequest;
import org.elasticsearch.action.admin.cluster.node.tasks.cancel.CancelTasksResponse;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksRequest;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksResponse;
import org.elasticsearch.action.support.nodes.BaseNodeRequest;
import org.elasticsearch.action.support.nodes.BaseNodesRequest;
import org.elasticsearch.action.support.replication.ClusterStateCreationUtils;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskInfo;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.test.ClusterServiceUtils.setState;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class CancellableTasksTests extends TaskManagerTestCase {

    public static class CancellableNodeRequest extends BaseNodeRequest {
        protected String requestName;
        protected String nodeId;

        public CancellableNodeRequest() {
            super();
        }

        public CancellableNodeRequest(CancellableNodesRequest request, String nodeId) {
            super(nodeId);
            requestName = request.requestName;
            this.nodeId = nodeId;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            requestName = in.readString();
            nodeId = in.readString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(requestName);
            out.writeString(nodeId);
        }

        @Override
        public String getDescription() {
            return "CancellableNodeRequest[" + requestName + ", " + nodeId + "]";
        }

        @Override
        public Task createTask(long id, String type, String action, TaskId parentTaskId) {
            return new CancellableTask(id, type, action, getDescription(), parentTaskId) {
                @Override
                public boolean shouldCancelChildrenOnCancellation() {
                    return false;
                }
            };
        }
    }

    public static class CancellableNodesRequest extends BaseNodesRequest<CancellableNodesRequest> {
        private String requestName;

        private CancellableNodesRequest() {
            super();
        }

        public CancellableNodesRequest(String requestName, String... nodesIds) {
            super(nodesIds);
            this.requestName = requestName;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            requestName = in.readString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(requestName);
        }

        @Override
        public String getDescription() {
            return "CancellableNodesRequest[" + requestName + "]";
        }

        @Override
        public Task createTask(long id, String type, String action, TaskId parentTaskId) {
            return new CancellableTask(id, type, action, getDescription(), parentTaskId) {
                @Override
                public boolean shouldCancelChildrenOnCancellation() {
                    return true;
                }
            };
        }
    }

    /**
     * Simulates a cancellable node-based task that can be used to block node tasks so they are guaranteed to be registered by task manager
     */
    class CancellableTestNodesAction extends AbstractTestNodesAction<CancellableNodesRequest, CancellableNodeRequest> {

        // True if the node operation should get stuck until its cancelled
        final boolean shouldBlock;

        final CountDownLatch actionStartedLatch;

        CancellableTestNodesAction(Settings settings, String actionName, ThreadPool threadPool,
                                   ClusterService clusterService, TransportService transportService, boolean shouldBlock, CountDownLatch
                                       actionStartedLatch) {
            super(settings, actionName, threadPool, clusterService, transportService, CancellableNodesRequest::new,
                CancellableNodeRequest::new);
            this.shouldBlock = shouldBlock;
            this.actionStartedLatch = actionStartedLatch;
        }

        @Override
        protected CancellableNodeRequest newNodeRequest(String nodeId, CancellableNodesRequest request) {
            return new CancellableNodeRequest(request, nodeId);
        }

        @Override
        protected NodeResponse nodeOperation(CancellableNodeRequest request, Task task) {
            assert task instanceof CancellableTask;
            debugDelay(request.nodeId, "op1");
            if (actionStartedLatch != null) {
                actionStartedLatch.countDown();
            }

            debugDelay(request.nodeId, "op2");
            if (shouldBlock) {
                // Simulate a job that takes forever to finish
                // Using periodic checks method to identify that the task was cancelled
                try {
                    awaitBusy(() -> {
                        if (((CancellableTask) task).isCancelled()) {
                            throw new TaskCancelledException("Cancelled");
                        }
                        return false;
                    });
                    fail("It should have thrown an exception");
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                }
            }
            debugDelay(request.nodeId, "op4");

            return new NodeResponse(clusterService.localNode());
        }

        @Override
        protected NodeResponse nodeOperation(CancellableNodeRequest request) {
            throw new UnsupportedOperationException("the task parameter is required");
        }
    }

    private Task startCancellableTestNodesAction(boolean waitForActionToStart, int blockedNodesCount, ActionListener<NodesResponse>
        listener) throws InterruptedException {
        return startCancellableTestNodesAction(waitForActionToStart, randomSubsetOf(blockedNodesCount, testNodes), new
            CancellableNodesRequest("Test Request"), listener);
    }

    private Task startCancellableTestNodesAction(boolean waitForActionToStart, Collection<TestNode> blockOnNodes, CancellableNodesRequest
        request, ActionListener<NodesResponse> listener) throws InterruptedException {
        CountDownLatch actionLatch = waitForActionToStart ? new CountDownLatch(nodesCount) : null;
        CancellableTestNodesAction[] actions = new CancellableTestNodesAction[nodesCount];
        for (int i = 0; i < testNodes.length; i++) {
            boolean shouldBlock = blockOnNodes.contains(testNodes[i]);
            logger.info("The action in the node [{}] should block: [{}]", testNodes[i].getNodeId(), shouldBlock);
            actions[i] = new CancellableTestNodesAction(CLUSTER_SETTINGS, "testAction", threadPool, testNodes[i]
                .clusterService, testNodes[i].transportService, shouldBlock, actionLatch);
        }
        Task task = actions[0].execute(request, listener);
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
        boolean waitForActionToStart = randomBoolean();
        logger.info("waitForActionToStart is set to {}", waitForActionToStart);
        final AtomicReference<NodesResponse> responseReference = new AtomicReference<>();
        final AtomicReference<Throwable> throwableReference = new AtomicReference<>();
        int blockedNodesCount = randomIntBetween(0, nodesCount);
        Task mainTask = startCancellableTestNodesAction(waitForActionToStart, blockedNodesCount, new ActionListener<NodesResponse>() {
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

        // Cancel main task
        CancelTasksRequest request = new CancelTasksRequest();
        request.setReason("Testing Cancellation");
        request.setTaskId(new TaskId(testNodes[0].getNodeId(), mainTask.getId()));
        // And send the cancellation request to a random node
        CancelTasksResponse response = testNodes[randomIntBetween(0, testNodes.length - 1)].transportCancelTasksAction.execute(request)
            .get();

        // Awaiting for the main task to finish
        responseLatch.await();

        if (response.getTasks().size() == 0) {
            // We didn't cancel the request and it finished successfully
            // That should be rare and can be only in case we didn't block on a single node
            assertEquals(0, blockedNodesCount);
            // Make sure that the request was successful
            assertNull(throwableReference.get());
            assertNotNull(responseReference.get());
            assertEquals(nodesCount, responseReference.get().getNodes().size());
            assertEquals(0, responseReference.get().failureCount());
        } else {
            // We canceled the request, in this case it should have fail, but we should get partial response
            assertNull(throwableReference.get());
            assertEquals(nodesCount, responseReference.get().failureCount() + responseReference.get().getNodes().size());
            // and we should have at least as many failures as the number of blocked operations
            // (we might have cancelled some non-blocked operations before they even started and that's ok)
            assertThat(responseReference.get().failureCount(), greaterThanOrEqualTo(blockedNodesCount));

            // We should have the information about the cancelled task in the cancel operation response
            assertEquals(1, response.getTasks().size());
            assertEquals(mainTask.getId(), response.getTasks().get(0).getId());
            // Verify that all cancelled tasks reported that they support cancellation
            for(TaskInfo taskInfo : response.getTasks()) {
                assertTrue(taskInfo.isCancellable());
            }
        }

        // Make sure that tasks are no longer running
        ListTasksResponse listTasksResponse = testNodes[randomIntBetween(0, testNodes.length - 1)]
            .transportListTasksAction.execute(new ListTasksRequest().setTaskId(
                new TaskId(testNodes[0].getNodeId(), mainTask.getId()))).get();
        assertEquals(0, listTasksResponse.getTasks().size());

        // Make sure that there are no leftover bans, the ban removal is async, so we might return from the cancellation
        // while the ban is still there, but it should disappear shortly
        assertBusy(() -> {
            for (int i = 0; i < testNodes.length; i++) {
                assertEquals("No bans on the node " + i, 0, testNodes[i].transportService.getTaskManager().getBanCount());
            }
        });
    }

    public void testChildTasksCancellation() throws Exception {
        setupTestNodes(Settings.EMPTY);
        connectNodes(testNodes);
        CountDownLatch responseLatch = new CountDownLatch(1);
        final AtomicReference<NodesResponse> responseReference = new AtomicReference<>();
        final AtomicReference<Throwable> throwableReference = new AtomicReference<>();
        Task mainTask = startCancellableTestNodesAction(true, nodesCount, new ActionListener<NodesResponse>() {
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
        request.setParentTaskId(new TaskId(testNodes[0].getNodeId(), mainTask.getId()));
        // And send the cancellation request to a random node
        CancelTasksResponse response = testNodes[randomIntBetween(1, testNodes.length - 1)].transportCancelTasksAction.execute(request)
            .get();

        // Awaiting for the main task to finish
        responseLatch.await();

        // Should have cancelled tasks on all nodes
        assertThat(response.getTasks().size(), equalTo(testNodes.length));

        assertBusy(() -> {
            try {
            // Make sure that main task is no longer running
                ListTasksResponse listTasksResponse = testNodes[randomIntBetween(0, testNodes.length - 1)]
                    .transportListTasksAction.execute(new ListTasksRequest().setTaskId(
                        new TaskId(testNodes[0].getNodeId(), mainTask.getId()))).get();
                assertEquals(0, listTasksResponse.getTasks().size());

            } catch (ExecutionException | InterruptedException ex) {
                throw new RuntimeException(ex);
            }
        });
    }

    public void testTaskCancellationOnCoordinatingNodeLeavingTheCluster() throws Exception {
        setupTestNodes(Settings.EMPTY);
        connectNodes(testNodes);
        CountDownLatch responseLatch = new CountDownLatch(1);
        boolean simulateBanBeforeLeaving = randomBoolean();
        final AtomicReference<NodesResponse> responseReference = new AtomicReference<>();
        final AtomicReference<Throwable> throwableReference = new AtomicReference<>();
        int blockedNodesCount = randomIntBetween(0, nodesCount - 1);

        // We shouldn't block on the first node since it's leaving the cluster anyway so it doesn't matter
        List<TestNode> blockOnNodes = randomSubsetOf(blockedNodesCount, Arrays.copyOfRange(testNodes, 1, nodesCount));
        Task mainTask = startCancellableTestNodesAction(true, blockOnNodes, new CancellableNodesRequest("Test Request"), new
            ActionListener<NodesResponse>() {
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

        String mainNode = testNodes[0].getNodeId();

        // Make sure that tasks are running
        ListTasksResponse listTasksResponse = testNodes[randomIntBetween(0, testNodes.length - 1)]
            .transportListTasksAction.execute(new ListTasksRequest().setParentTaskId(new TaskId(mainNode, mainTask.getId()))).get();
        assertThat(listTasksResponse.getTasks().size(), greaterThanOrEqualTo(blockOnNodes.size()));

        // Simulate the coordinating node leaving the cluster
        DiscoveryNode[] discoveryNodes = new DiscoveryNode[testNodes.length - 1];
        for (int i = 1; i < testNodes.length; i++) {
            discoveryNodes[i - 1] = testNodes[i].discoveryNode();
        }
        DiscoveryNode master = discoveryNodes[0];
        for (int i = 1; i < testNodes.length; i++) {
            // Notify only nodes that should remain in the cluster
            setState(testNodes[i].clusterService, ClusterStateCreationUtils.state(testNodes[i].discoveryNode(), master, discoveryNodes));
        }

        if (simulateBanBeforeLeaving) {
            logger.info("--> Simulate issuing cancel request on the node that is about to leave the cluster");
            // Simulate issuing cancel request on the node that is about to leave the cluster
            CancelTasksRequest request = new CancelTasksRequest();
            request.setReason("Testing Cancellation");
            request.setTaskId(new TaskId(testNodes[0].getNodeId(), mainTask.getId()));
            // And send the cancellation request to a random node
            CancelTasksResponse response = testNodes[0].transportCancelTasksAction.execute(request).get();
            logger.info("--> Done simulating issuing cancel request on the node that is about to leave the cluster");
            // This node still thinks that's part of the cluster, so cancelling should look successful
            if (response.getTasks().size() == 0) {
                logger.error("!!!!");
            }
            assertThat(response.getTasks().size(), lessThanOrEqualTo(1));
            assertThat(response.getTaskFailures().size(), lessThanOrEqualTo(1));
            assertThat(response.getTaskFailures().size() + response.getTasks().size(), lessThanOrEqualTo(1));
        }

        for (int i = 1; i < testNodes.length; i++) {
            assertEquals("No bans on the node " + i, 0, testNodes[i].transportService.getTaskManager().getBanCount());
        }

        // Close the first node
        testNodes[0].close();

        assertBusy(() -> {
            // Make sure that tasks are no longer running
            try {
                ListTasksResponse listTasksResponse1 = testNodes[randomIntBetween(1, testNodes.length - 1)]
                    .transportListTasksAction.execute(new ListTasksRequest().setTaskId(new TaskId(mainNode, mainTask.getId()))).get();
                assertEquals(0, listTasksResponse1.getTasks().size());
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
            } catch (ExecutionException ex2) {
                fail("shouldn't be here");
            }
        });

        // Wait for clean up
        responseLatch.await();

    }

    private static void debugDelay(String nodeId, String name) {
        // Introduce an additional pseudo random repeatable race conditions
        String delayName = RandomizedContext.current().getRunnerSeedAsString() + ":" + nodeId + ":" + name;
        Random random = new Random(delayName.hashCode());
        if (RandomNumbers.randomIntBetween(random, 0, 10) < 1) {
            try {
                Thread.sleep(RandomNumbers.randomIntBetween(random, 20, 50));
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
            }
        }
    }

}
