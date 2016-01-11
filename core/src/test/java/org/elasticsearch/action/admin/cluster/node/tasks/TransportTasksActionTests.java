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

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.TaskOperationFailure;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksRequest;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksResponse;
import org.elasticsearch.action.admin.cluster.node.tasks.list.TaskInfo;
import org.elasticsearch.action.admin.cluster.node.tasks.list.TransportListTasksAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.nodes.BaseNodeRequest;
import org.elasticsearch.action.support.nodes.BaseNodeResponse;
import org.elasticsearch.action.support.nodes.BaseNodesRequest;
import org.elasticsearch.action.support.nodes.BaseNodesResponse;
import org.elasticsearch.action.support.nodes.TransportNodesAction;
import org.elasticsearch.action.support.replication.ClusterStateCreationUtils;
import org.elasticsearch.action.support.tasks.BaseTasksRequest;
import org.elasticsearch.action.support.tasks.BaseTasksResponse;
import org.elasticsearch.action.support.tasks.TransportTasksAction;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.cluster.TestClusterService;
import org.elasticsearch.test.tasks.MockTaskManager;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.transport.local.LocalTransport;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceArray;

import static org.elasticsearch.action.support.PlainActionFuture.newFuture;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.not;

public class TransportTasksActionTests extends ESTestCase {

    private static ThreadPool threadPool;
    private static final ClusterName clusterName = new ClusterName("test-cluster");
    private TestNode[] testNodes;
    private int nodesCount;

    @BeforeClass
    public static void beforeClass() {
        threadPool = new ThreadPool(TransportTasksActionTests.class.getSimpleName());
    }

    @AfterClass
    public static void afterClass() {
        ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
        threadPool = null;
    }

    public void setupTestNodes(Settings settings) {
        nodesCount = randomIntBetween(2, 10);
        testNodes = new TestNode[nodesCount];
        for (int i = 0; i < testNodes.length; i++) {
            testNodes[i] = new TestNode("node" + i, threadPool, settings);
        }
    }

    @After
    public final void shutdownTestNodes() throws Exception {
        for (TestNode testNode : testNodes) {
            testNode.close();
        }
    }

    private static class TestNode implements Releasable {
        public TestNode(String name, ThreadPool threadPool, Settings settings) {
            transportService = new TransportService(settings,
                new LocalTransport(settings, threadPool, Version.CURRENT, new NamedWriteableRegistry()),
                threadPool){
                @Override
                protected TaskManager createTaskManager() {
                    if (settings.getAsBoolean(MockTaskManager.USE_MOCK_TASK_MANAGER, false)) {
                        return new MockTaskManager(settings);
                    } else {
                        return super.createTaskManager();
                    }
                }
            };
            transportService.start();
            clusterService = new TestClusterService(threadPool, transportService);
            discoveryNode = new DiscoveryNode(name, transportService.boundAddress().publishAddress(), Version.CURRENT);
            transportListTasksAction = new TransportListTasksAction(settings, clusterName, threadPool, clusterService, transportService,
                new ActionFilters(Collections.emptySet()), new IndexNameExpressionResolver(settings));
        }

        public final TestClusterService clusterService;
        public final TransportService transportService;
        public final DiscoveryNode discoveryNode;
        public final TransportListTasksAction transportListTasksAction;

        @Override
        public void close() {
            transportService.close();
        }
    }

    public static void connectNodes(TestNode... nodes) {
        DiscoveryNode[] discoveryNodes = new DiscoveryNode[nodes.length];
        for (int i = 0; i < nodes.length; i++) {
            discoveryNodes[i] = nodes[i].discoveryNode;
        }
        DiscoveryNode master = discoveryNodes[0];
        for (TestNode node : nodes) {
            node.clusterService.setState(ClusterStateCreationUtils.state(node.discoveryNode, master, discoveryNodes));
        }
        for (TestNode nodeA : nodes) {
            for (TestNode nodeB : nodes) {
                nodeA.transportService.connectToNode(nodeB.discoveryNode);
            }
        }
    }

    public static RecordingTaskManagerListener[] setupListeners(TestNode[] nodes, String... actionMasks) {
        RecordingTaskManagerListener[] listeners = new RecordingTaskManagerListener[nodes.length];
        for (int i = 0; i < nodes.length; i++) {
            listeners[i] = new RecordingTaskManagerListener(nodes[i].discoveryNode, actionMasks);
            ((MockTaskManager)(nodes[i].clusterService.getTaskManager())).addListener(listeners[i]);
        }
        return listeners;
    }

    public static class NodeRequest extends BaseNodeRequest {
        protected String requestName;
        private boolean enableTaskManager;

        public NodeRequest() {
            super();
        }

        public NodeRequest(NodesRequest request, String nodeId) {
            super(nodeId);
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
            return "NodeRequest[" + requestName + ", " + enableTaskManager + "]";
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

    public static class NodesRequest extends BaseNodesRequest<NodesRequest> {
        private String requestName;
        private boolean enableTaskManager;

        private NodesRequest() {
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
            return "NodesRequest[" + requestName + ", " + enableTaskManager + "]";
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

    static class NodeResponse extends BaseNodeResponse {

        protected NodeResponse() {
            super();
        }

        protected NodeResponse(DiscoveryNode node) {
            super(node);
        }
    }

    static class NodesResponse extends BaseNodesResponse<NodeResponse> {

        private int failureCount;

        protected NodesResponse(ClusterName clusterName, NodeResponse[] nodes, int failureCount) {
            super(clusterName, nodes);
            this.failureCount = failureCount;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            failureCount = in.readVInt();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeVInt(failureCount);
        }

        public int failureCount() {
            return failureCount;
        }
    }

    /**
     * Simulates node-based task that can be used to block node tasks so they are guaranteed to be registered by task manager
     */
    abstract class TestNodesAction extends TransportNodesAction<NodesRequest, NodesResponse, NodeRequest, NodeResponse> {

        TestNodesAction(Settings settings, String actionName, ClusterName clusterName, ThreadPool threadPool,
                        ClusterService clusterService, TransportService transportService) {
            super(settings, actionName, clusterName, threadPool, clusterService, transportService,
                new ActionFilters(new HashSet<>()), new IndexNameExpressionResolver(Settings.EMPTY),
                NodesRequest::new, NodeRequest::new, ThreadPool.Names.GENERIC);
        }

        @Override
        protected NodesResponse newResponse(NodesRequest request, AtomicReferenceArray responses) {
            final List<NodeResponse> nodesList = new ArrayList<>();
            int failureCount = 0;
            for (int i = 0; i < responses.length(); i++) {
                Object resp = responses.get(i);
                if (resp instanceof NodeResponse) { // will also filter out null response for unallocated ones
                    nodesList.add((NodeResponse) resp);
                } else if (resp instanceof FailedNodeException) {
                    failureCount++;
                } else {
                    logger.warn("unknown response type [{}], expected NodeLocalGatewayMetaState or FailedNodeException", resp);
                }
            }
            return new NodesResponse(clusterName, nodesList.toArray(new NodeResponse[nodesList.size()]), failureCount);
        }

        @Override
        protected NodeRequest newNodeRequest(String nodeId, NodesRequest request) {
            return new NodeRequest(request, nodeId);
        }

        @Override
        protected NodeResponse newNodeResponse() {
            return new NodeResponse();
        }

        @Override
        protected abstract NodeResponse nodeOperation(NodeRequest request);

        @Override
        protected boolean accumulateExceptions() {
            return true;
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
            this.tasks = tasks == null ? Collections.emptyList() : Collections.unmodifiableList(new ArrayList<>(tasks));
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
    static abstract class TestTasksAction extends TransportTasksAction<TestTasksRequest, TestTasksResponse, TestTaskResponse> {

        protected TestTasksAction(Settings settings, String actionName, ClusterName clusterName, ThreadPool threadPool, ClusterService clusterService,
                                  TransportService transportService) {
            super(settings, actionName, clusterName, threadPool, clusterService, transportService, new ActionFilters(new HashSet<>()), new IndexNameExpressionResolver(Settings.EMPTY),
                TestTasksRequest::new, TestTasksResponse::new, ThreadPool.Names.MANAGEMENT);
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

    private Task startBlockingTestNodesAction(CountDownLatch checkLatch, NodesRequest request,  ActionListener<NodesResponse> listener) throws InterruptedException {
        CountDownLatch actionLatch = new CountDownLatch(nodesCount);
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
        actionLatch.await();
        logger.info("Done waiting for all actions to start");
        return task;
    }

    public void testRunningTasksCount() throws Exception {
        setupTestNodes(Settings.EMPTY);
        connectNodes(testNodes);
        CountDownLatch checkLatch = new CountDownLatch(1);
        CountDownLatch responseLatch = new CountDownLatch(1);
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
        listTasksRequest.actions("testAction*"); // pick all test actions
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
        listTasksRequest.actions("testAction[n]"); // only pick node actions
        response = testNode.transportListTasksAction.execute(listTasksRequest).get();
        assertEquals(testNodes.length, response.getPerNodeTasks().size());
        for (Map.Entry<DiscoveryNode, List<TaskInfo>> entry : response.getPerNodeTasks().entrySet()) {
            assertEquals(1, entry.getValue().size());
            assertNull(entry.getValue().get(0).getDescription());
        }

        // Check task counts using transport with detailed description
        listTasksRequest.detailed(true); // same request only with detailed description
        response = testNode.transportListTasksAction.execute(listTasksRequest).get();
        assertEquals(testNodes.length, response.getPerNodeTasks().size());
        for (Map.Entry<DiscoveryNode, List<TaskInfo>> entry : response.getPerNodeTasks().entrySet()) {
            assertEquals(1, entry.getValue().size());
            assertEquals("NodeRequest[Test Request, true]", entry.getValue().get(0).getDescription());
        }

        // Make sure that the main task on coordinating node is the task that was returned to us by execute()
        listTasksRequest.actions("testAction"); // only pick the main task
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
        listTasksRequest.actions("testAction");
        ListTasksResponse response = testNode.transportListTasksAction.execute(listTasksRequest).get();
        assertEquals(1, response.getTasks().size());
        String parentNode = response.getTasks().get(0).getNode().getId();
        long parentTaskId = response.getTasks().get(0).getId();

        // Find tasks with common parent
        listTasksRequest = new ListTasksRequest();
        listTasksRequest.parentNode(parentNode);
        listTasksRequest.parentTaskId(parentTaskId);
        response = testNode.transportListTasksAction.execute(listTasksRequest).get();
        assertEquals(testNodes.length, response.getTasks().size());
        for (TaskInfo task : response.getTasks()) {
            assertEquals("testAction[n]", task.getAction());
            assertEquals(parentNode, task.getParentNode());
            assertEquals(parentTaskId, task.getParentId());
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
        listTasksRequest.actions("testAction*");
        ListTasksResponse response = testNode.transportListTasksAction.execute(listTasksRequest).get();
        assertEquals(0, response.getTasks().size());

        // Release all tasks and wait for response
        checkLatch.countDown();
        NodesResponse responses = future.get();
        assertEquals(0, responses.failureCount());
    }

    public void testTasksDescriptions() throws Exception {
        setupTestNodes(Settings.EMPTY);
        connectNodes(testNodes);
        CountDownLatch checkLatch = new CountDownLatch(1);
        ActionFuture<NodesResponse> future = startBlockingTestNodesAction(checkLatch);

        // Check task counts using transport with filtering
        TestNode testNode = testNodes[randomIntBetween(0, testNodes.length - 1)];
        ListTasksRequest listTasksRequest = new ListTasksRequest();
        listTasksRequest.actions("testAction[n]"); // only pick node actions
        ListTasksResponse response = testNode.transportListTasksAction.execute(listTasksRequest).get();
        assertEquals(testNodes.length, response.getPerNodeTasks().size());
        for (Map.Entry<DiscoveryNode, List<TaskInfo>> entry : response.getPerNodeTasks().entrySet()) {
            assertEquals(1, entry.getValue().size());
            assertNull(entry.getValue().get(0).getDescription());
        }

        // Check task counts using transport with detailed description
        listTasksRequest.detailed(true); // same request only with detailed description
        response = testNode.transportListTasksAction.execute(listTasksRequest).get();
        assertEquals(testNodes.length, response.getPerNodeTasks().size());
        for (Map.Entry<DiscoveryNode, List<TaskInfo>> entry : response.getPerNodeTasks().entrySet()) {
            assertEquals(1, entry.getValue().size());
            assertEquals("NodeRequest[Test Request, true]", entry.getValue().get(0).getDescription());
        }

        // Release all tasks and wait for response
        checkLatch.countDown();
        NodesResponse responses = future.get();
        assertEquals(0, responses.failureCount());
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
                    logger.info("Task action on node " + node);
                    if (failTaskOnNode == node && task.getParentNode() != null) {
                        logger.info("Failing on node " + node);
                        throw new RuntimeException("Task level failure");
                    }
                    return new TestTaskResponse("Success on node " + node);
                }
            };
        }

        // Run task action on node tasks that are currently running
        // should be successful on all nodes except one
        TestTasksRequest testTasksRequest = new TestTasksRequest();
        testTasksRequest.actions("testAction[n]"); // pick all test actions
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
