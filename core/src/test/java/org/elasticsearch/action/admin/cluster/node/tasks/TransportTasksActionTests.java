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

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.TaskOperationFailure;
import org.elasticsearch.action.admin.cluster.node.tasks.cancel.CancelTasksRequest;
import org.elasticsearch.action.admin.cluster.node.tasks.cancel.CancelTasksResponse;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksRequest;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksResponse;
import org.elasticsearch.action.admin.cluster.node.tasks.list.TaskInfo;
import org.elasticsearch.action.support.ActionFilter;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.nodes.BaseNodeRequest;
import org.elasticsearch.action.support.nodes.BaseNodesRequest;
import org.elasticsearch.action.support.tasks.BaseTasksRequest;
import org.elasticsearch.action.support.tasks.BaseTasksResponse;
import org.elasticsearch.action.support.tasks.TransportTasksAction;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.tasks.MockTaskManager;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;


import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.action.support.PlainActionFuture.newFuture;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.not;

public class TransportTasksActionTests extends TaskManagerTestCase {

    public static class NodeRequest extends BaseNodeRequest {
        protected String requestName;
        private boolean enableTaskManager;

        public NodeRequest() {
            super();
        }

        public NodeRequest(NodesRequest request, String nodeId) {
            super(request, nodeId);
            requestName = request.requestName;
            enableTaskManager = request.enableTaskManager;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            requestName = in.readString();
            enableTaskManager = in.readBoolean();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(requestName);
            out.writeBoolean(enableTaskManager);
        }

        @Override
        public String getDescription() {
            return "CancellableNodeRequest[" + requestName + ", " + enableTaskManager + "]";
        }

        @Override
        public Task createTask(long id, String type, String action, TaskId parentTaskId) {
            if (enableTaskManager) {
                return super.createTask(id, type, action, parentTaskId);
            } else {
                return null;
            }
        }
    }

    public static class NodesRequest extends BaseNodesRequest<NodesRequest> {
        private String requestName;
        private boolean enableTaskManager;

        public NodesRequest() {
            super();
        }

        public NodesRequest(String requestName, String... nodesIds) {
            this(requestName, true, nodesIds);
        }

        public NodesRequest(String requestName, boolean enableTaskManager, String... nodesIds) {
            super(nodesIds);
            this.requestName = requestName;
            this.enableTaskManager = enableTaskManager;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            requestName = in.readString();
            enableTaskManager = in.readBoolean();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(requestName);
            out.writeBoolean(enableTaskManager);
        }

        @Override
        public String getDescription() {
            return "CancellableNodesRequest[" + requestName + ", " + enableTaskManager + "]";
        }

        @Override
        public Task createTask(long id, String type, String action) {
            if (enableTaskManager) {
                return super.createTask(id, type, action);
            } else {
                return null;
            }
        }
    }

    /**
     * Simulates node-based task that can be used to block node tasks so they are guaranteed to be registered by task manager
     */
    abstract class TestNodesAction extends AbstractTestNodesAction<NodesRequest, NodeRequest> {

        TestNodesAction(Settings settings, String actionName, ClusterName clusterName, ThreadPool threadPool,
                        ClusterService clusterService, TransportService transportService) {
            super(settings, actionName, clusterName, threadPool, clusterService, transportService, NodesRequest.class, NodeRequest.class);
        }

        @Override
        protected NodeRequest newNodeRequest(String nodeId, NodesRequest request) {
            return new NodeRequest(request, nodeId);
        }

        @Override
        protected NodeResponse newNodeResponse() {
            return new NodeResponse();
        }
    }

    static class TestTaskResponse implements Writeable<TestTaskResponse> {

        private final String status;

        public TestTaskResponse(StreamInput in) throws IOException {
            status = in.readString();
        }

        public TestTaskResponse(String status) {
            this.status = status;
        }

        public String getStatus() {
            return status;
        }

        @Override
        public TestTaskResponse readFrom(StreamInput in) throws IOException {
            return new TestTaskResponse(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(status);
        }
    }


    static class TestTasksRequest extends BaseTasksRequest<TestTasksRequest> {

    }

    static class TestTasksResponse extends BaseTasksResponse {

        private List<TestTaskResponse> tasks;

        public TestTasksResponse() {

        }

        public TestTasksResponse(List<TestTaskResponse> tasks, List<TaskOperationFailure> taskFailures, List<? extends FailedNodeException> nodeFailures) {
            super(taskFailures, nodeFailures);
            if (tasks == null) {
                this.tasks = Collections.emptyList();
            } else {
                this.tasks = Collections.unmodifiableList(new ArrayList<>(tasks));
            }
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            int taskCount = in.readVInt();
            List<TestTaskResponse> builder = new ArrayList<>();
            for (int i = 0; i < taskCount; i++) {
                builder.add(new TestTaskResponse(in));
            }
            tasks = Collections.unmodifiableList(builder);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeVInt(tasks.size());
            for (TestTaskResponse task : tasks) {
                task.writeTo(out);
            }
        }
    }

    /**
     * Test class for testing task operations
     */
    static abstract class TestTasksAction extends TransportTasksAction<Task, TestTasksRequest, TestTasksResponse, TestTaskResponse> {

        protected TestTasksAction(Settings settings, String actionName, ClusterName clusterName, ThreadPool threadPool, ClusterService clusterService,
                                  TransportService transportService) {
            super(settings, actionName, clusterName, threadPool, clusterService, transportService, new ActionFilters(new HashSet<ActionFilter>()),
                new IndexNameExpressionResolver(Settings.EMPTY),
                new Callable<TestTasksRequest>() {
                    @Override
                    public TestTasksRequest call() throws Exception {
                        return new TestTasksRequest();
                    }
                }, ThreadPool.Names.MANAGEMENT);
        }

        @Override
        protected TestTasksResponse newResponse(TestTasksRequest request, List<TestTaskResponse> tasks, List<TaskOperationFailure> taskOperationFailures, List<FailedNodeException> failedNodeExceptions) {
            return new TestTasksResponse(tasks, taskOperationFailures, failedNodeExceptions);
        }

        @Override
        protected TestTaskResponse readTaskResponse(StreamInput in) throws IOException {
            return new TestTaskResponse(in);
        }

        @Override
        protected boolean accumulateExceptions() {
            return true;
        }
    }

    private ActionFuture<NodesResponse> startBlockingTestNodesAction(CountDownLatch checkLatch) throws InterruptedException {
        return startBlockingTestNodesAction(checkLatch, new NodesRequest("Test Request"));
    }

    private ActionFuture<NodesResponse> startBlockingTestNodesAction(CountDownLatch checkLatch,  NodesRequest request) throws InterruptedException {
        PlainActionFuture<NodesResponse> future = newFuture();
        startBlockingTestNodesAction(checkLatch, request, future);
        return future;
    }

    private Task startBlockingTestNodesAction(CountDownLatch checkLatch, ActionListener<NodesResponse> listener) throws InterruptedException {
        return startBlockingTestNodesAction(checkLatch, new NodesRequest("Test Request"), listener);
    }

    private Task startBlockingTestNodesAction(final CountDownLatch checkLatch, NodesRequest request,  ActionListener<NodesResponse>
        listener) throws InterruptedException {
        final CountDownLatch actionLatch = new CountDownLatch(nodesCount);
        TestNodesAction[] actions = new TestNodesAction[nodesCount];
        for (int i = 0; i < testNodes.length; i++) {
            final int node = i;
            actions[i] = new TestNodesAction(Settings.EMPTY, "testAction", clusterName, threadPool, testNodes[i].clusterService, testNodes[i].transportService) {
                @Override
                protected NodeResponse nodeOperation(NodeRequest request) {
                    logger.info("Action on node " + node);
                    actionLatch.countDown();
                    try {
                        checkLatch.await();
                    } catch (InterruptedException ex) {
                        Thread.currentThread().interrupt();
                    }
                    logger.info("Action on node " + node + " finished");
                    return new NodeResponse(testNodes[node].discoveryNode);
                }
            };
        }
        // Make sure no tasks are running
        for (TestNode node : testNodes) {
            assertEquals(0, node.transportService.getTaskManager().getTasks().size());
        }
        Task task = actions[0].execute(request, listener);
        logger.info("Awaiting for all actions to start");
        assertTrue(actionLatch.await(10, TimeUnit.SECONDS));
        logger.info("Done waiting for all actions to start");
        return task;
    }

    public void testRunningTasksCount() throws Exception {
        setupTestNodes(Settings.EMPTY);
        connectNodes(testNodes);
        CountDownLatch checkLatch = new CountDownLatch(1);
        final CountDownLatch responseLatch = new CountDownLatch(1);
        final AtomicReference<NodesResponse> responseReference = new AtomicReference<>();
        Task mainTask = startBlockingTestNodesAction(checkLatch, new ActionListener<NodesResponse>() {
            @Override
            public void onResponse(NodesResponse listTasksResponse) {
                responseReference.set(listTasksResponse);
                responseLatch.countDown();
            }

            @Override
            public void onFailure(Throwable e) {
                logger.warn("Couldn't get list of tasks", e);
                responseLatch.countDown();
            }
        });

        // Check task counts using taskManager
        Map<Long, Task> localTasks = testNodes[0].transportService.getTaskManager().getTasks();
        assertEquals(2, localTasks.size()); // all node tasks + 1 coordinating task
        Task coordinatingTask = localTasks.get(Collections.min(localTasks.keySet()));
        Task subTask = localTasks.get(Collections.max(localTasks.keySet()));
        assertThat(subTask.getAction(), endsWith("[n]"));
        assertThat(coordinatingTask.getAction(), not(endsWith("[n]")));
        for (int i = 1; i < testNodes.length; i++) {
            Map<Long, Task> remoteTasks = testNodes[i].transportService.getTaskManager().getTasks();
            assertEquals(1, remoteTasks.size());
            Task remoteTask = remoteTasks.values().iterator().next();
            assertThat(remoteTask.getAction(), endsWith("[n]"));
        }

        // Check task counts using transport
        int testNodeNum = randomIntBetween(0, testNodes.length - 1);
        TestNode testNode = testNodes[testNodeNum];
        ListTasksRequest listTasksRequest = new ListTasksRequest();
        listTasksRequest.setActions("testAction*"); // pick all test actions
        logger.info("Listing currently running tasks using node [{}]", testNodeNum);
        ListTasksResponse response = testNode.transportListTasksAction.execute(listTasksRequest).get();
        logger.info("Checking currently running tasks");
        assertEquals(testNodes.length, response.getPerNodeTasks().size());

        // Coordinating node
        assertEquals(2, response.getPerNodeTasks().get(testNodes[0].discoveryNode).size());
        // Other nodes node
        for (int i = 1; i < testNodes.length; i++) {
            assertEquals(1, response.getPerNodeTasks().get(testNodes[i].discoveryNode).size());
        }

        // Check task counts using transport with filtering
        testNode = testNodes[randomIntBetween(0, testNodes.length - 1)];
        listTasksRequest = new ListTasksRequest();
        listTasksRequest.setActions("testAction[n]"); // only pick node actions
        response = testNode.transportListTasksAction.execute(listTasksRequest).get();
        assertEquals(testNodes.length, response.getPerNodeTasks().size());
        for (Map.Entry<DiscoveryNode, List<TaskInfo>> entry : response.getPerNodeTasks().entrySet()) {
            assertEquals(1, entry.getValue().size());
            assertNull(entry.getValue().get(0).getDescription());
        }

        // Check task counts using transport with detailed description
        listTasksRequest.setDetailed(true); // same request only with detailed description
        response = testNode.transportListTasksAction.execute(listTasksRequest).get();
        assertEquals(testNodes.length, response.getPerNodeTasks().size());
        for (Map.Entry<DiscoveryNode, List<TaskInfo>> entry : response.getPerNodeTasks().entrySet()) {
            assertEquals(1, entry.getValue().size());
            assertEquals("CancellableNodeRequest[Test Request, true]", entry.getValue().get(0).getDescription());
        }

        // Make sure that the main task on coordinating node is the task that was returned to us by execute()
        listTasksRequest.setActions("testAction"); // only pick the main task
        response = testNode.transportListTasksAction.execute(listTasksRequest).get();
        assertEquals(1, response.getTasks().size());
        assertEquals(mainTask.getId(), response.getTasks().get(0).getId());

        // Release all tasks and wait for response
        checkLatch.countDown();
        assertTrue(responseLatch.await(10, TimeUnit.SECONDS));

        NodesResponse responses = responseReference.get();
        assertEquals(0, responses.failureCount());

        // Make sure that we don't have any lingering tasks
        for (TestNode node : testNodes) {
            assertEquals(0, node.transportService.getTaskManager().getTasks().size());
        }
    }

    public void testFindChildTasks() throws Exception {
        setupTestNodes(Settings.EMPTY);
        connectNodes(testNodes);
        CountDownLatch checkLatch = new CountDownLatch(1);
        ActionFuture<NodesResponse> future = startBlockingTestNodesAction(checkLatch);

        TestNode testNode = testNodes[randomIntBetween(0, testNodes.length - 1)];

        // Get the parent task
        ListTasksRequest listTasksRequest = new ListTasksRequest();
        listTasksRequest.setActions("testAction");
        ListTasksResponse response = testNode.transportListTasksAction.execute(listTasksRequest).get();
        assertEquals(1, response.getTasks().size());
        String parentNode = response.getTasks().get(0).getNode().getId();
        long parentTaskId = response.getTasks().get(0).getId();

        // Find tasks with common parent
        listTasksRequest = new ListTasksRequest();
        listTasksRequest.setParentTaskId(new TaskId(parentNode, parentTaskId));
        response = testNode.transportListTasksAction.execute(listTasksRequest).get();
        assertEquals(testNodes.length, response.getTasks().size());
        for (TaskInfo task : response.getTasks()) {
            assertEquals("testAction[n]", task.getAction());
            assertEquals(parentNode, task.getParentTaskId().getNodeId());
            assertEquals(parentTaskId, task.getParentTaskId().getId());
        }

        // Release all tasks and wait for response
        checkLatch.countDown();
        NodesResponse responses = future.get();
        assertEquals(0, responses.failureCount());
    }

    public void testTaskManagementOptOut() throws Exception {
        setupTestNodes(Settings.EMPTY);
        connectNodes(testNodes);
        CountDownLatch checkLatch = new CountDownLatch(1);
        // Starting actions that disable task manager
        ActionFuture<NodesResponse> future = startBlockingTestNodesAction(checkLatch, new NodesRequest("Test Request", false));

        TestNode testNode = testNodes[randomIntBetween(0, testNodes.length - 1)];

        // Get the parent task
        ListTasksRequest listTasksRequest = new ListTasksRequest();
        listTasksRequest.setActions("testAction*");
        ListTasksResponse response = testNode.transportListTasksAction.execute(listTasksRequest).get();
        assertEquals(0, response.getTasks().size());

        // Release all tasks and wait for response
        checkLatch.countDown();
        NodesResponse responses = future.get();
        assertEquals(0, responses.failureCount());
    }

    public void testTasksDescriptions() throws Exception {
        long minimalStartTime = System.currentTimeMillis();
        setupTestNodes(Settings.EMPTY);
        connectNodes(testNodes);
        CountDownLatch checkLatch = new CountDownLatch(1);
        ActionFuture<NodesResponse> future = startBlockingTestNodesAction(checkLatch);
        long maximumStartTimeNanos = System.nanoTime();

        // Check task counts using transport with filtering
        TestNode testNode = testNodes[randomIntBetween(0, testNodes.length - 1)];
        ListTasksRequest listTasksRequest = new ListTasksRequest();
        listTasksRequest.setActions("testAction[n]"); // only pick node actions
        ListTasksResponse response = testNode.transportListTasksAction.execute(listTasksRequest).get();
        assertEquals(testNodes.length, response.getPerNodeTasks().size());
        for (Map.Entry<DiscoveryNode, List<TaskInfo>> entry : response.getPerNodeTasks().entrySet()) {
            assertEquals(1, entry.getValue().size());
            assertNull(entry.getValue().get(0).getDescription());
        }

        // Check task counts using transport with detailed description
        long minimalDurationNanos = System.nanoTime() - maximumStartTimeNanos;
        listTasksRequest.setDetailed(true); // same request only with detailed description
        response = testNode.transportListTasksAction.execute(listTasksRequest).get();
        assertEquals(testNodes.length, response.getPerNodeTasks().size());
        for (Map.Entry<DiscoveryNode, List<TaskInfo>> entry : response.getPerNodeTasks().entrySet()) {
            assertEquals(1, entry.getValue().size());
            assertEquals("CancellableNodeRequest[Test Request, true]", entry.getValue().get(0).getDescription());
            assertThat(entry.getValue().get(0).getStartTime(), greaterThanOrEqualTo(minimalStartTime));
            assertThat(entry.getValue().get(0).getRunningTimeNanos(), greaterThanOrEqualTo(minimalDurationNanos));
        }

        // Release all tasks and wait for response
        checkLatch.countDown();
        NodesResponse responses = future.get();
        assertEquals(0, responses.failureCount());
    }

    public void testCancellingTasksThatDontSupportCancellation() throws Exception {
        setupTestNodes(Settings.EMPTY);
        connectNodes(testNodes);
        CountDownLatch checkLatch = new CountDownLatch(1);
        final CountDownLatch responseLatch = new CountDownLatch(1);
        Task task = startBlockingTestNodesAction(checkLatch, new ActionListener<NodesResponse>() {
            @Override
            public void onResponse(NodesResponse nodeResponses) {
                responseLatch.countDown();
            }

            @Override
            public void onFailure(Throwable e) {
                responseLatch.countDown();
            }
        });
        String actionName = "testAction"; // only pick the main action

        // Try to cancel main task using action name
        CancelTasksRequest request = new CancelTasksRequest();
        request.setNodesIds(testNodes[0].discoveryNode.getId());
        request.setReason("Testing Cancellation");
        request.setActions(actionName);
        CancelTasksResponse response = testNodes[randomIntBetween(0, testNodes.length - 1)].transportCancelTasksAction.execute(request)
            .get();

        // Shouldn't match any tasks since testAction doesn't support cancellation
        assertEquals(0, response.getTasks().size());
        assertEquals(0, response.getTaskFailures().size());
        assertEquals(0, response.getNodeFailures().size());


        // Try to cancel main task using id
        request = new CancelTasksRequest();
        request.setReason("Testing Cancellation");
        request.setTaskId(new TaskId(testNodes[0].discoveryNode.getId(), task.getId()));
        response = testNodes[randomIntBetween(0, testNodes.length - 1)].transportCancelTasksAction.execute(request).get();

        // Shouldn't match any tasks since testAction doesn't support cancellation
        assertEquals(0, response.getTasks().size());
        assertEquals(0, response.getTaskFailures().size());
        assertEquals(1, response.getNodeFailures().size());
        assertThat(response.getNodeFailures().get(0).getDetailedMessage(), containsString("doesn't support cancellation"));

        // Make sure that task is still running
        ListTasksRequest listTasksRequest = new ListTasksRequest();
        listTasksRequest.setActions(actionName);
        ListTasksResponse listResponse = testNodes[randomIntBetween(0, testNodes.length - 1)].transportListTasksAction.execute
            (listTasksRequest).get();
        assertEquals(1, listResponse.getPerNodeTasks().size());

        // Release all tasks and wait for response
        checkLatch.countDown();
        responseLatch.await(10, TimeUnit.SECONDS);
    }

    public void testFailedTasksCount() throws ExecutionException, InterruptedException, IOException {
        Settings settings = Settings.builder().put(MockTaskManager.USE_MOCK_TASK_MANAGER, true).build();
        setupTestNodes(settings);
        connectNodes(testNodes);
        TestNodesAction[] actions = new TestNodesAction[nodesCount];
        RecordingTaskManagerListener[] listeners = setupListeners(testNodes, "testAction*");
        for (int i = 0; i < testNodes.length; i++) {
            final int node = i;
            actions[i] = new TestNodesAction(Settings.EMPTY, "testAction", clusterName, threadPool, testNodes[i].clusterService, testNodes[i].transportService) {
                @Override
                protected NodeResponse nodeOperation(NodeRequest request) {
                    logger.info("Action on node " + node);
                    throw new RuntimeException("Test exception");
                }
            };
        }

        for (TestNode testNode : testNodes) {
            assertEquals(0, testNode.transportService.getTaskManager().getTasks().size());
        }
        NodesRequest request = new NodesRequest("Test Request");
        NodesResponse responses = actions[0].execute(request).get();
        assertEquals(nodesCount, responses.failureCount());

        // Make sure that actions are still registered in the task manager on all nodes
        // Twice on the coordinating node and once on all other nodes.
        assertEquals(4, listeners[0].getEvents().size());
        assertEquals(2, listeners[0].getRegistrationEvents().size());
        assertEquals(2, listeners[0].getUnregistrationEvents().size());
        for (int i = 1; i < listeners.length; i++) {
            assertEquals(2, listeners[i].getEvents().size());
            assertEquals(1, listeners[i].getRegistrationEvents().size());
            assertEquals(1, listeners[i].getUnregistrationEvents().size());
        }
    }

    public void testTaskLevelActionFailures() throws ExecutionException, InterruptedException, IOException {
        setupTestNodes(Settings.EMPTY);
        connectNodes(testNodes);
        CountDownLatch checkLatch = new CountDownLatch(1);
        ActionFuture<NodesResponse> future = startBlockingTestNodesAction(checkLatch);

        TestTasksAction[] tasksActions = new TestTasksAction[nodesCount];
        final int failTaskOnNode = randomIntBetween(1, nodesCount - 1);
        for (int i = 0; i < testNodes.length; i++) {
            final int node = i;
            // Simulate task action that fails on one of the tasks on one of the nodes
            tasksActions[i] = new TestTasksAction(Settings.EMPTY, "testTasksAction", clusterName, threadPool, testNodes[i].clusterService, testNodes[i].transportService) {
                @Override
                protected TestTaskResponse taskOperation(TestTasksRequest request, Task task) {
                    logger.info("Task action on node {}", node);
                    if (failTaskOnNode == node && task.getParentTaskId().isSet()) {
                        logger.info("Failing on node {}", node);
                        throw new RuntimeException("Task level failure");
                    }
                    return new TestTaskResponse("Success on node " + node);
                }
            };
        }

        // Run task action on node tasks that are currently running
        // should be successful on all nodes except one
        TestTasksRequest testTasksRequest = new TestTasksRequest();
        testTasksRequest.setActions("testAction[n]"); // pick all test actions
        TestTasksResponse response = tasksActions[0].execute(testTasksRequest).get();
        // Get successful responses from all nodes except one
        assertEquals(testNodes.length - 1, response.tasks.size());
        assertEquals(1, response.getTaskFailures().size()); // one task failed
        assertThat(response.getTaskFailures().get(0).getReason(), containsString("Task level failure"));
        assertEquals(0, response.getNodeFailures().size()); // no nodes failed

        // Release all node tasks and wait for response
        checkLatch.countDown();
        NodesResponse responses = future.get();
        assertEquals(0, responses.failureCount());
    }
}
