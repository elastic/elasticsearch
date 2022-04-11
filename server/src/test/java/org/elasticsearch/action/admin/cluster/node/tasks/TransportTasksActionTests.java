/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.action.admin.cluster.node.tasks;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.TaskOperationFailure;
import org.elasticsearch.action.admin.cluster.node.tasks.cancel.CancelTasksRequest;
import org.elasticsearch.action.admin.cluster.node.tasks.cancel.CancelTasksResponse;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksAction;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksRequest;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksResponse;
import org.elasticsearch.action.admin.cluster.node.tasks.list.TaskGroup;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.ActionTestUtils;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.nodes.BaseNodesRequest;
import org.elasticsearch.action.support.tasks.BaseTasksRequest;
import org.elasticsearch.action.support.tasks.BaseTasksResponse;
import org.elasticsearch.action.support.tasks.TransportTasksAction;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskInfo;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.test.tasks.MockTaskManager;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.elasticsearch.action.support.PlainActionFuture.newFuture;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;

public class TransportTasksActionTests extends TaskManagerTestCase {

    public static class NodeRequest extends TransportRequest {
        protected String requestName;

        public NodeRequest(StreamInput in) throws IOException {
            super(in);
            requestName = in.readString();
        }

        public NodeRequest(NodesRequest request) {
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
            return super.createTask(id, type, action, parentTaskId, headers);
        }
    }

    public static class NodesRequest extends BaseNodesRequest<NodesRequest> {
        private String requestName;

        NodesRequest(StreamInput in) throws IOException {
            super(in);
            requestName = in.readString();
        }

        public NodesRequest(String requestName, String... nodesIds) {
            super(nodesIds);
            this.requestName = requestName;
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
        public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
            return super.createTask(id, type, action, parentTaskId, headers);
        }
    }

    /**
     * Simulates node-based task that can be used to block node tasks so they are guaranteed to be registered by task manager
     */
    abstract class TestNodesAction extends AbstractTestNodesAction<NodesRequest, NodeRequest> {

        TestNodesAction(String actionName, ThreadPool threadPool, ClusterService clusterService, TransportService transportService) {
            super(actionName, threadPool, clusterService, transportService, NodesRequest::new, NodeRequest::new);
        }

        @Override
        protected NodeRequest newNodeRequest(NodesRequest request) {
            return new NodeRequest(request);
        }

        @Override
        protected NodeResponse newNodeResponse(StreamInput in, DiscoveryNode node) throws IOException {
            return new NodeResponse(in);
        }
    }

    static class TestTaskResponse implements Writeable {

        private final String status;

        TestTaskResponse(StreamInput in) throws IOException {
            status = in.readString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(status);
        }

        TestTaskResponse(String status) {
            this.status = status;
        }

        public String getStatus() {
            return status;
        }
    }

    static class TestTasksRequest extends BaseTasksRequest<TestTasksRequest> {

        TestTasksRequest(StreamInput in) throws IOException {
            super(in);
        }

        TestTasksRequest() {}
    }

    static class TestTasksResponse extends BaseTasksResponse {

        private List<TestTaskResponse> tasks;

        TestTasksResponse(
            List<TestTaskResponse> tasks,
            List<TaskOperationFailure> taskFailures,
            List<? extends FailedNodeException> nodeFailures
        ) {
            super(taskFailures, nodeFailures);
            this.tasks = tasks == null ? Collections.emptyList() : List.copyOf(tasks);
        }

        TestTasksResponse(StreamInput in) throws IOException {
            super(in);
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
    abstract static class TestTasksAction extends TransportTasksAction<Task, TestTasksRequest, TestTasksResponse, TestTaskResponse> {

        protected TestTasksAction(String actionName, ClusterService clusterService, TransportService transportService) {
            super(
                actionName,
                clusterService,
                transportService,
                new ActionFilters(new HashSet<>()),
                TestTasksRequest::new,
                TestTasksResponse::new,
                TestTaskResponse::new,
                ThreadPool.Names.MANAGEMENT
            );
        }

        @Override
        protected TestTasksResponse newResponse(
            TestTasksRequest request,
            List<TestTaskResponse> tasks,
            List<TaskOperationFailure> taskOperationFailures,
            List<FailedNodeException> failedNodeExceptions
        ) {
            return new TestTasksResponse(tasks, taskOperationFailures, failedNodeExceptions);
        }

    }

    private ActionFuture<NodesResponse> startBlockingTestNodesAction(CountDownLatch checkLatch) throws InterruptedException {
        return startBlockingTestNodesAction(checkLatch, new NodesRequest("Test Request"));
    }

    private ActionFuture<NodesResponse> startBlockingTestNodesAction(CountDownLatch checkLatch, NodesRequest request)
        throws InterruptedException {
        PlainActionFuture<NodesResponse> future = newFuture();
        startBlockingTestNodesAction(checkLatch, request, future);
        return future;
    }

    private Task startBlockingTestNodesAction(CountDownLatch checkLatch, ActionListener<NodesResponse> listener)
        throws InterruptedException {
        return startBlockingTestNodesAction(checkLatch, new NodesRequest("Test Request"), listener);
    }

    private Task startBlockingTestNodesAction(CountDownLatch checkLatch, NodesRequest request, ActionListener<NodesResponse> listener)
        throws InterruptedException {
        CountDownLatch actionLatch = new CountDownLatch(nodesCount);
        TestNodesAction[] actions = new TestNodesAction[nodesCount];
        for (int i = 0; i < testNodes.length; i++) {
            final int node = i;
            actions[i] = new TestNodesAction(
                "internal:testAction",
                threadPool,
                testNodes[i].clusterService,
                testNodes[i].transportService
            ) {
                @Override
                protected NodeResponse nodeOperation(NodeRequest request, Task task) {
                    logger.info("Action on node {}", node);
                    actionLatch.countDown();
                    try {
                        checkLatch.await();
                    } catch (InterruptedException ex) {
                        Thread.currentThread().interrupt();
                    }
                    logger.info("Action on node {} finished", node);
                    return new NodeResponse(testNodes[node].discoveryNode());
                }
            };
        }
        // Make sure no tasks are running
        for (TestNode node : testNodes) {
            assertEquals(0, node.transportService.getTaskManager().getTasks().size());
        }
        Task task = testNodes[0].transportService.getTaskManager()
            .registerAndExecute(
                "transport",
                actions[0],
                request,
                testNodes[0].transportService.getLocalNodeConnection(),
                ActionTestUtils.wrapAsTaskListener(listener)
            );
        logger.info("Awaiting for all actions to start");
        assertTrue(actionLatch.await(10, TimeUnit.SECONDS));
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
            public void onFailure(Exception e) {
                logger.warn("Couldn't get list of tasks", e);
                responseLatch.countDown();
            }
        });

        // Check task counts using taskManager
        Map<Long, Task> localTasks = testNodes[0].transportService.getTaskManager().getTasks();
        logger.info(
            "local tasks [{}]",
            localTasks.values()
                .stream()
                .map(t -> Strings.toString(t.taskInfo(testNodes[0].getNodeId(), true)))
                .collect(Collectors.joining(","))
        );
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
        listTasksRequest.setActions("internal:testAction*"); // pick all test actions
        logger.info("Listing currently running tasks using node [{}]", testNodeNum);
        ListTasksResponse response = ActionTestUtils.executeBlocking(testNode.transportListTasksAction, listTasksRequest);
        logger.info("Checking currently running tasks");
        assertEquals(testNodes.length, response.getPerNodeTasks().size());

        // Coordinating node
        assertEquals(2, response.getPerNodeTasks().get(testNodes[0].getNodeId()).size());
        // Other nodes node
        for (int i = 1; i < testNodes.length; i++) {
            assertEquals(1, response.getPerNodeTasks().get(testNodes[i].getNodeId()).size());
        }
        // There should be a single main task when grouped by tasks
        assertEquals(1, response.getTaskGroups().size());
        // And as many child tasks as we have nodes
        assertEquals(testNodes.length, response.getTaskGroups().get(0).childTasks().size());

        // Check task counts using transport with filtering
        testNode = testNodes[randomIntBetween(0, testNodes.length - 1)];
        listTasksRequest = new ListTasksRequest();
        listTasksRequest.setActions("internal:testAction[n]"); // only pick node actions
        response = ActionTestUtils.executeBlocking(testNode.transportListTasksAction, listTasksRequest);
        assertEquals(testNodes.length, response.getPerNodeTasks().size());
        for (Map.Entry<String, List<TaskInfo>> entry : response.getPerNodeTasks().entrySet()) {
            assertEquals(1, entry.getValue().size());
            assertNull(entry.getValue().get(0).description());
        }
        // Since the main task is not in the list - all tasks should be by themselves
        assertEquals(testNodes.length, response.getTaskGroups().size());
        for (TaskGroup taskGroup : response.getTaskGroups()) {
            assertEquals(0, taskGroup.childTasks().size());
        }

        // Check task counts using transport with detailed description
        listTasksRequest.setDetailed(true); // same request only with detailed description
        response = ActionTestUtils.executeBlocking(testNode.transportListTasksAction, listTasksRequest);
        assertEquals(testNodes.length, response.getPerNodeTasks().size());
        for (Map.Entry<String, List<TaskInfo>> entry : response.getPerNodeTasks().entrySet()) {
            assertEquals(1, entry.getValue().size());
            assertEquals("CancellableNodeRequest[Test Request]", entry.getValue().get(0).description());
        }

        // Make sure that the main task on coordinating node is the task that was returned to us by execute()
        listTasksRequest.setActions("internal:testAction"); // only pick the main task
        response = ActionTestUtils.executeBlocking(testNode.transportListTasksAction, listTasksRequest);
        assertEquals(1, response.getTasks().size());
        assertEquals(mainTask.getId(), response.getTasks().get(0).id());

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
        listTasksRequest.setActions("internal:testAction");
        ListTasksResponse response = ActionTestUtils.executeBlocking(testNode.transportListTasksAction, listTasksRequest);
        assertEquals(1, response.getTasks().size());
        String parentNode = response.getTasks().get(0).taskId().getNodeId();
        long parentTaskId = response.getTasks().get(0).id();

        // Find tasks with common parent
        listTasksRequest = new ListTasksRequest();
        listTasksRequest.setTargetParentTaskId(new TaskId(parentNode, parentTaskId));
        response = ActionTestUtils.executeBlocking(testNode.transportListTasksAction, listTasksRequest);
        assertEquals(testNodes.length, response.getTasks().size());
        for (TaskInfo task : response.getTasks()) {
            assertEquals("internal:testAction[n]", task.action());
            assertEquals(parentNode, task.parentTaskId().getNodeId());
            assertEquals(parentTaskId, task.parentTaskId().getId());
        }

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
        listTasksRequest.setActions("internal:testAction[n]"); // only pick node actions
        ListTasksResponse response = ActionTestUtils.executeBlocking(testNode.transportListTasksAction, listTasksRequest);
        assertEquals(testNodes.length, response.getPerNodeTasks().size());
        for (Map.Entry<String, List<TaskInfo>> entry : response.getPerNodeTasks().entrySet()) {
            assertEquals(1, entry.getValue().size());
            assertNull(entry.getValue().get(0).description());
        }

        // Check task counts using transport with detailed description
        long minimalDurationNanos = System.nanoTime() - maximumStartTimeNanos;
        listTasksRequest.setDetailed(true); // same request only with detailed description
        response = ActionTestUtils.executeBlocking(testNode.transportListTasksAction, listTasksRequest);
        assertEquals(testNodes.length, response.getPerNodeTasks().size());
        for (Map.Entry<String, List<TaskInfo>> entry : response.getPerNodeTasks().entrySet()) {
            assertEquals(1, entry.getValue().size());
            assertEquals("CancellableNodeRequest[Test Request]", entry.getValue().get(0).description());
            assertThat(entry.getValue().get(0).startTime(), greaterThanOrEqualTo(minimalStartTime));
            assertThat(entry.getValue().get(0).runningTimeNanos(), greaterThanOrEqualTo(minimalDurationNanos));
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
        CountDownLatch responseLatch = new CountDownLatch(1);
        Task task = startBlockingTestNodesAction(checkLatch, ActionListener.wrap(responseLatch::countDown));
        String actionName = "internal:testAction"; // only pick the main action

        // Try to cancel main task using action name
        CancelTasksRequest request = new CancelTasksRequest();
        request.setNodes(testNodes[0].getNodeId());
        request.setReason("Testing Cancellation");
        request.setActions(actionName);
        CancelTasksResponse response = ActionTestUtils.executeBlocking(
            testNodes[randomIntBetween(0, testNodes.length - 1)].transportCancelTasksAction,
            request
        );

        // Shouldn't match any tasks since testAction doesn't support cancellation
        assertEquals(0, response.getTasks().size());
        assertEquals(0, response.getTaskFailures().size());
        assertEquals(0, response.getNodeFailures().size());

        // Try to cancel main task using id
        request = new CancelTasksRequest();
        request.setReason("Testing Cancellation");
        request.setTargetTaskId(new TaskId(testNodes[0].getNodeId(), task.getId()));
        response = ActionTestUtils.executeBlocking(
            testNodes[randomIntBetween(0, testNodes.length - 1)].transportCancelTasksAction,
            request
        );

        // Shouldn't match any tasks since testAction doesn't support cancellation
        assertEquals(0, response.getTasks().size());
        assertEquals(0, response.getTaskFailures().size());
        assertEquals(1, response.getNodeFailures().size());
        assertThat(response.getNodeFailures().get(0).getDetailedMessage(), containsString("doesn't support cancellation"));

        // Make sure that task is still running
        ListTasksRequest listTasksRequest = new ListTasksRequest();
        listTasksRequest.setActions(actionName);
        ListTasksResponse listResponse = ActionTestUtils.executeBlocking(
            testNodes[randomIntBetween(0, testNodes.length - 1)].transportListTasksAction,
            listTasksRequest
        );
        assertEquals(1, listResponse.getPerNodeTasks().size());
        // Verify that tasks are marked as non-cancellable
        for (TaskInfo taskInfo : listResponse.getTasks()) {
            assertFalse(taskInfo.cancellable());
        }

        // Release all tasks and wait for response
        checkLatch.countDown();
        responseLatch.await(10, TimeUnit.SECONDS);
    }

    @TestLogging(
        reason = "debugging for https://github.com/elastic/elasticsearch/issues/69731",
        value = "org.elasticsearch.transport.TcpTransport:TRACE,"
            + "org.elasticsearch.transport.TransportService.tracer:TRACE,"
            + "org.elasticsearch.tasks.TaskManager:TRACE"
    )
    public void testFailedTasksCount() throws Exception {
        Settings settings = Settings.builder().put(MockTaskManager.USE_MOCK_TASK_MANAGER_SETTING.getKey(), true).build();
        setupTestNodes(settings);
        connectNodes(testNodes);
        TestNodesAction[] actions = new TestNodesAction[nodesCount];
        RecordingTaskManagerListener[] listeners = setupListeners(testNodes, "internal:testAction*");
        for (int i = 0; i < testNodes.length; i++) {
            final int node = i;
            actions[i] = new TestNodesAction(
                "internal:testAction",
                threadPool,
                testNodes[i].clusterService,
                testNodes[i].transportService
            ) {
                @Override
                protected NodeResponse nodeOperation(NodeRequest request, Task task) {
                    TransportTasksActionTests.this.logger.info("Action on node {}", node);
                    throw new RuntimeException("Test exception");
                }
            };
        }

        logger.info("--> checking for ongoing tasks before starting test actions");
        final String immediateTaskDescriptions = getAllTaskDescriptions();

        // Hunting for cause of https://github.com/elastic/elasticsearch/issues/69731: if there's an unexpected task then we check whether
        // it goes away if we wait for long enough first.
        assertBusy(() -> {
            final String ongoingTaskDescriptions = getAllTaskDescriptions();
            assertThat(
                "initially:\n" + immediateTaskDescriptions + "\nongoing:\n" + ongoingTaskDescriptions,
                ongoingTaskDescriptions.length(),
                equalTo(0)
            );
        });

        assertThat(
            "eventually completed, but still unexpected:\n" + immediateTaskDescriptions,
            immediateTaskDescriptions.length(),
            equalTo(0)
        );

        NodesRequest request = new NodesRequest("Test Request");
        NodesResponse responses = ActionTestUtils.executeBlockingWithTask(
            testNodes[0].transportService.getTaskManager(),
            testNodes[0].transportService.getLocalNodeConnection(),
            actions[0],
            request
        );
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

    private String getAllTaskDescriptions() {
        final StringBuilder taskDescriptions = new StringBuilder();
        for (TestNode testNode : testNodes) {
            final Map<Long, Task> tasks = testNode.transportService.getTaskManager().getTasks();
            if (tasks.isEmpty() == false) {
                taskDescriptions.append("still running tasks on node [").append(testNode.getNodeId()).append("]\n");
                for (Map.Entry<Long, Task> entry : tasks.entrySet()) {
                    final Task task = entry.getValue();
                    taskDescriptions.append(entry.getKey())
                        .append(": [")
                        .append(task.getId())
                        .append("][")
                        .append(task.getAction())
                        .append("] started at ")
                        .append(task.getStartTime())
                        .append('\n');
                }
            }
        }
        return taskDescriptions.toString();
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
            tasksActions[i] = new TestTasksAction("internal:testTasksAction", testNodes[i].clusterService, testNodes[i].transportService) {
                @Override
                protected void taskOperation(TestTasksRequest request, Task task, ActionListener<TestTaskResponse> listener) {
                    logger.info("Task action on node {}", node);
                    if (failTaskOnNode == node && task.getParentTaskId().isSet()) {
                        logger.info("Failing on node {}", node);
                        // Fail in a random way to make sure we can handle all these ways
                        Runnable failureMode = randomFrom(() -> {
                            logger.info("Throwing exception from taskOperation");
                            throw new RuntimeException("Task level failure (direct)");
                        }, () -> {
                            logger.info("Calling listener synchronously with exception from taskOperation");
                            listener.onFailure(new RuntimeException("Task level failure (sync listener)"));
                        }, () -> {
                            logger.info("Calling listener asynchronously with exception from taskOperation");
                            threadPool.generic()
                                .execute(() -> listener.onFailure(new RuntimeException("Task level failure (async listener)")));
                        });
                        failureMode.run();
                    } else {
                        if (randomBoolean()) {
                            listener.onResponse(new TestTaskResponse("Success on node (sync)" + node));
                        } else {
                            threadPool.generic().execute(() -> listener.onResponse(new TestTaskResponse("Success on node (async)" + node)));
                        }
                    }
                }
            };
        }

        // Run task action on node tasks that are currently running
        // should be successful on all nodes except one
        TestTasksRequest testTasksRequest = new TestTasksRequest();
        testTasksRequest.setActions("internal:testAction[n]"); // pick all test actions
        TestTasksResponse response = ActionTestUtils.executeBlocking(tasksActions[0], testTasksRequest);
        assertThat(response.getTaskFailures(), hasSize(1)); // one task failed
        assertThat(response.getTaskFailures().get(0).getCause().getMessage(), containsString("Task level failure"));
        // Get successful responses from all nodes except one
        assertEquals(testNodes.length - 1, response.tasks.size());
        assertEquals(0, response.getNodeFailures().size()); // no nodes failed

        // Release all node tasks and wait for response
        checkLatch.countDown();
        NodesResponse responses = future.get();
        assertEquals(0, responses.failureCount());
    }

    /**
     * This test starts nodes actions that blocks on all nodes. While node actions are blocked in the middle of execution
     * it executes a tasks action that targets these blocked node actions. The test verifies that task actions are only
     * getting executed on nodes that are not listed in the node filter.
     */
    public void testTaskNodeFiltering() throws ExecutionException, InterruptedException, IOException {
        setupTestNodes(Settings.EMPTY);
        connectNodes(testNodes);
        CountDownLatch checkLatch = new CountDownLatch(1);
        // Start some test nodes action so we could have something to run tasks actions on
        ActionFuture<NodesResponse> future = startBlockingTestNodesAction(checkLatch);

        String[] allNodes = new String[testNodes.length];
        for (int i = 0; i < testNodes.length; i++) {
            allNodes[i] = testNodes[i].getNodeId();
        }

        int filterNodesSize = randomInt(allNodes.length);
        Set<String> filterNodes = new HashSet<>(randomSubsetOf(filterNodesSize, allNodes));
        logger.info("Filtering out nodes {} size: {}", filterNodes, filterNodesSize);

        TestTasksAction[] tasksActions = new TestTasksAction[nodesCount];
        for (int i = 0; i < testNodes.length; i++) {
            final int node = i;
            // Simulate a task action that works on all nodes except nodes listed in filterNodes.
            // We are testing that it works.
            tasksActions[i] = new TestTasksAction("internal:testTasksAction", testNodes[i].clusterService, testNodes[i].transportService) {

                @Override
                protected String[] filterNodeIds(DiscoveryNodes nodes, String[] nodesIds) {
                    String[] superNodes = super.filterNodeIds(nodes, nodesIds);
                    List<String> filteredNodes = new ArrayList<>();
                    for (String node : superNodes) {
                        if (filterNodes.contains(node) == false) {
                            filteredNodes.add(node);
                        }
                    }
                    return filteredNodes.toArray(new String[filteredNodes.size()]);
                }

                @Override
                protected void taskOperation(TestTasksRequest request, Task task, ActionListener<TestTaskResponse> listener) {
                    if (randomBoolean()) {
                        listener.onResponse(new TestTaskResponse(testNodes[node].getNodeId()));
                    } else {
                        threadPool.generic().execute(() -> listener.onResponse(new TestTaskResponse(testNodes[node].getNodeId())));
                    }
                }
            };
        }

        // Run task action on node tasks that are currently running
        // should be successful on all nodes except nodes that we filtered out
        TestTasksRequest testTasksRequest = new TestTasksRequest();
        testTasksRequest.setActions("internal:testAction[n]"); // pick all test actions
        TestTasksResponse response = ActionTestUtils.executeBlocking(tasksActions[randomIntBetween(0, nodesCount - 1)], testTasksRequest);

        // Get successful responses from all nodes except nodes that we filtered out
        assertEquals(testNodes.length - filterNodes.size(), response.tasks.size());
        assertEquals(0, response.getTaskFailures().size()); // no task failed
        assertEquals(0, response.getNodeFailures().size()); // no nodes failed

        // Make sure that filtered nodes didn't send any responses
        for (TestTaskResponse taskResponse : response.tasks) {
            String nodeId = taskResponse.getStatus();
            assertFalse("Found response from filtered node " + nodeId, filterNodes.contains(nodeId));
        }

        // Release all node tasks and wait for response
        checkLatch.countDown();
        NodesResponse responses = future.get();
        assertEquals(0, responses.failureCount());
    }

    @SuppressWarnings("unchecked")
    public void testTasksToXContentGrouping() throws Exception {
        setupTestNodes(Settings.EMPTY);
        connectNodes(testNodes);

        // Get the parent task
        ListTasksRequest listTasksRequest = new ListTasksRequest();
        listTasksRequest.setActions(ListTasksAction.NAME + "*");
        ListTasksResponse response = ActionTestUtils.executeBlockingWithTask(
            testNodes[0].transportService.getTaskManager(),
            testNodes[0].transportService.getLocalNodeConnection(),
            testNodes[0].transportListTasksAction,
            listTasksRequest
        );
        assertEquals(testNodes.length + 1, response.getTasks().size());

        Map<String, Object> byNodes = serialize(response, true);
        byNodes = (Map<String, Object>) byNodes.get("nodes");
        // One element on the top level
        assertEquals(testNodes.length, byNodes.size());
        Map<String, Object> firstNode = (Map<String, Object>) byNodes.get(testNodes[0].getNodeId());
        firstNode = (Map<String, Object>) firstNode.get("tasks");
        assertEquals(2, firstNode.size()); // two tasks for the first node
        for (int i = 1; i < testNodes.length; i++) {
            Map<String, Object> otherNode = (Map<String, Object>) byNodes.get(testNodes[i].getNodeId());
            otherNode = (Map<String, Object>) otherNode.get("tasks");
            assertEquals(1, otherNode.size()); // one tasks for the all other nodes
        }

        // Group by parents
        Map<String, Object> byParent = serialize(response, false);
        byParent = (Map<String, Object>) byParent.get("tasks");
        // One element on the top level
        assertEquals(1, byParent.size()); // Only one top level task
        Map<String, Object> topTask = (Map<String, Object>) byParent.values().iterator().next();
        List<Object> children = (List<Object>) topTask.get("children");
        assertEquals(testNodes.length, children.size()); // two tasks for the first node
        for (int i = 0; i < testNodes.length; i++) {
            Map<String, Object> child = (Map<String, Object>) children.get(i);
            assertNull(child.get("children"));
        }
    }

    private Map<String, Object> serialize(ListTasksResponse response, boolean byParents) throws IOException {
        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        builder.startObject();
        if (byParents) {
            DiscoveryNodes nodes = testNodes[0].clusterService.state().nodes();
            response.toXContentGroupedByNode(builder, ToXContent.EMPTY_PARAMS, nodes);
        } else {
            response.toXContentGroupedByParents(builder, ToXContent.EMPTY_PARAMS);
        }
        builder.endObject();
        builder.flush();
        logger.info(Strings.toString(builder));
        return XContentHelper.convertToMap(BytesReference.bytes(builder), false, builder.contentType()).v2();
    }
}
