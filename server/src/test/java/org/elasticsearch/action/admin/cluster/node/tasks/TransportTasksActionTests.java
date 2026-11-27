/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.action.admin.cluster.node.tasks;

import org.apache.logging.log4j.Level;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.TaskOperationFailure;
import org.elasticsearch.action.admin.cluster.node.tasks.cancel.CancelTasksRequest;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksRequest;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksResponse;
import org.elasticsearch.action.admin.cluster.node.tasks.list.TaskGroup;
import org.elasticsearch.action.admin.cluster.node.tasks.list.TransportListTasksAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.ActionTestUtils;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.action.support.nodes.BaseNodesRequest;
import org.elasticsearch.action.support.tasks.BaseTasksRequest;
import org.elasticsearch.action.support.tasks.BaseTasksResponse;
import org.elasticsearch.action.support.tasks.TransportTasksAction;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.ChunkedToXContent;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.AbstractRefCounted;
import org.elasticsearch.core.RefCounted;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.indices.breaker.AllCircuitBreakerStats;
import org.elasticsearch.indices.breaker.CircuitBreakerMetrics;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.indices.breaker.CircuitBreakerStats;
import org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskAwareRequest;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskInfo;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.test.ReachabilityChecker;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.test.tasks.MockTaskManager;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.AbstractTransportRequest;
import org.elasticsearch.transport.InboundHandler;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.junit.After;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

public class TransportTasksActionTests extends TaskManagerTestCase {

    public static class NodeRequest extends AbstractTransportRequest {
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

    public static class NodesRequest extends BaseNodesRequest {
        private final String requestName;

        public NodesRequest(String requestName, String... nodesIds) {
            super(nodesIds);
            this.requestName = requestName;
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
            super(actionName, threadPool, clusterService, transportService, NodeRequest::new);
        }

        @Override
        protected NodeRequest newNodeRequest(NodesRequest request) {
            return new NodeRequest(request);
        }

    }

    public static class TestTaskResponse implements Writeable {

        private final String status;

        public TestTaskResponse(StreamInput in) throws IOException {
            status = in.readString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(status);
        }

        public TestTaskResponse(String status) {
            this.status = status;
        }

        public String getStatus() {
            return status;
        }
    }

    public static class TestTasksRequest extends BaseTasksRequest<TestTasksRequest> {
        private final RefCounted refCounted;

        public TestTasksRequest(StreamInput in) throws IOException {
            super(in);
            refCounted = AbstractRefCounted.of(() -> {});
        }

        public TestTasksRequest() {
            this(() -> {});
        }

        public TestTasksRequest(Runnable onClose) {
            refCounted = AbstractRefCounted.of(onClose);
        }

        @Override
        public CancellableTask createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
            return new CancellableTask(id, type, action, "testTasksRequest", parentTaskId, headers);
        }

        @Override
        public void incRef() {
            refCounted.incRef();
        }

        @Override
        public boolean tryIncRef() {
            return refCounted.tryIncRef();
        }

        @Override
        public boolean decRef() {
            return refCounted.decRef();
        }

        @Override
        public boolean hasReferences() {
            return refCounted.hasReferences();
        }
    }

    public static class TestTasksResponse extends BaseTasksResponse {

        private final List<TestTaskResponse> tasks;

        public TestTasksResponse(
            List<TestTaskResponse> tasks,
            List<TaskOperationFailure> taskFailures,
            List<? extends FailedNodeException> nodeFailures
        ) {
            super(taskFailures, nodeFailures);
            this.tasks = tasks == null ? Collections.emptyList() : List.copyOf(tasks);
        }

        public TestTasksResponse(StreamInput in) throws IOException {
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
                ActionFilters.EMPTY,
                TestTasksRequest::new,
                TestTaskResponse::new,
                transportService.getThreadPool().executor(ThreadPool.Names.MANAGEMENT)
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

    private final List<CountDownLatch> latchesToRelease = new ArrayList<>();

    @After
    public void releaseBlockedTasks() {
        latchesToRelease.forEach(CountDownLatch::countDown);
    }

    private ActionFuture<NodesResponse> startBlockingTestNodesAction(CountDownLatch checkLatch) throws Exception {
        return startBlockingTestNodesAction(checkLatch, new NodesRequest("Test Request"));
    }

    private ActionFuture<NodesResponse> startBlockingTestNodesAction(CountDownLatch checkLatch, NodesRequest request) throws Exception {
        PlainActionFuture<NodesResponse> future = new PlainActionFuture<>();
        startBlockingTestNodesAction(checkLatch, request, future);
        return future;
    }

    private Task startBlockingTestNodesAction(CountDownLatch checkLatch, ActionListener<NodesResponse> listener) throws Exception {
        return startBlockingTestNodesAction(checkLatch, new NodesRequest("Test Request"), listener);
    }

    private Task startBlockingTestNodesAction(CountDownLatch checkLatch, NodesRequest request, ActionListener<NodesResponse> listener)
        throws Exception {
        latchesToRelease.add(checkLatch);
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
            assertBusy(() -> assertEquals(0, node.transportService.getTaskManager().getTasks().size()));
        }
        Task task = testNodes[0].transportService.getTaskManager()
            .registerAndExecute("transport", actions[0], request, testNodes[0].transportService.getLocalNodeConnection(), listener);
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
        Task mainTask = startBlockingTestNodesAction(checkLatch, new ActionListener<>() {
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
            assertBusy(() -> assertEquals(0, node.transportService.getTaskManager().getTasks().size()));
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
        Task task = startBlockingTestNodesAction(checkLatch, ActionListener.running(responseLatch::countDown));
        String actionName = "internal:testAction"; // only pick the main action

        // Try to cancel main task using action name
        CancelTasksRequest request = new CancelTasksRequest();
        request.setNodes(testNodes[0].getNodeId());
        request.setReason("Testing Cancellation");
        request.setActions(actionName);
        ListTasksResponse response = ActionTestUtils.executeBlocking(
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

        // Since https://github.com/elastic/elasticsearch/pull/94865 task unregistration is not guaranteed to have happened upon
        // receiving the response, e.g. for a `internal:transport/handshake` when connecting the test nodes. Therefore, wait
        // for ongoing tasks to finish.
        assertBusy(() -> {
            final List<String> ongoingTaskDescriptions = getAllTaskDescriptions();
            assertThat("Ongoing tasks:" + ongoingTaskDescriptions, ongoingTaskDescriptions.size(), equalTo(0));
        });

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
        assertBusy(() -> {
            assertEquals(2, listeners[0].getRegistrationEvents().size());
            assertEquals(2, listeners[0].getUnregistrationEvents().size());
            for (int i = 1; i < listeners.length; i++) {
                assertEquals(1, listeners[i].getRegistrationEvents().size());
                assertEquals(1, listeners[i].getUnregistrationEvents().size());
            }
        });
    }

    private List<String> getAllTaskDescriptions() {
        List<String> taskDescriptions = new ArrayList<>();
        for (TestNode testNode : testNodes) {
            for (Task task : testNode.transportService.getTaskManager().getTasks().values()) {
                taskDescriptions.add(
                    Strings.format(
                        "node [%s]: task [id:%d][%s] started at %d",
                        testNode.getNodeId(),
                        task.getId(),
                        task.getAction(),
                        task.getStartTime()
                    )
                );
            }
        }
        return taskDescriptions;
    }

    public void testActionParentCancellationPropagates() throws Exception {
        setupTestNodes(Settings.EMPTY);
        connectNodes(testNodes);
        CountDownLatch checkLatch = new CountDownLatch(1);
        CountDownLatch taskLatch = new CountDownLatch(1);
        ActionFuture<NodesResponse> future = startBlockingTestNodesAction(checkLatch);
        int numNodes = 2;

        CountDownLatch taskExecutesLatch = new CountDownLatch(numNodes);
        TestTasksAction[] tasksActions = new TestTasksAction[numNodes];
        for (int j = 0; j < numNodes; j++) {
            final int nodeId = j;
            tasksActions[j] = new TestTasksAction(
                "internal:testTasksAction",
                testNodes[nodeId].clusterService,
                testNodes[nodeId].transportService
            ) {
                @Override
                protected void taskOperation(
                    CancellableTask actionTask,
                    TestTasksRequest request,
                    Task task,
                    ActionListener<TestTaskResponse> listener
                ) {
                    try {
                        taskExecutesLatch.countDown();
                        logger.info("Task handled on node {} {}", nodeId, actionTask);
                        taskLatch.await();
                        assertThat(actionTask, instanceOf(CancellableTask.class));
                        logger.info("Task is now proceeding with cancellation check {}", nodeId);
                        assertBusy(() -> assertTrue(actionTask.isCancelled()));
                        listener.onResponse(new TestTaskResponse("CANCELLED"));
                    } catch (Exception e) {
                        listener.onFailure(e);
                        fail(e.getMessage());
                    }
                }
            };
        }

        TestTasksRequest testTasksRequest = new TestTasksRequest();
        testTasksRequest.setActions("internal:testAction[n]"); // pick all test actions
        testTasksRequest.setNodes(testNodes[0].getNodeId(), testNodes[1].getNodeId()); // only first two nodes
        PlainActionFuture<TestTasksResponse> taskFuture = new PlainActionFuture<>();
        CancellableTask task = (CancellableTask) testNodes[0].transportService.getTaskManager()
            .registerAndExecute(
                "direct",
                tasksActions[0],
                testTasksRequest,
                testNodes[0].transportService.getLocalNodeConnection(),
                taskFuture
            );
        logger.info("Executing test task request and awaiting their execution");
        taskExecutesLatch.await();
        logger.info("All test tasks are now executing");

        PlainActionFuture<Void> cancellationFuture = new PlainActionFuture<>();
        logger.info("Cancelling tasks");

        testNodes[0].transportService.getTaskManager().cancelTaskAndDescendants(task, "test case", false, cancellationFuture);
        logger.info("Awaiting task cancellation");
        cancellationFuture.actionGet();
        logger.info("Parent task is now cancelled counting down task latch");
        taskLatch.countDown();
        expectThrows(TaskCancelledException.class, taskFuture);

        // Release all node tasks and wait for response
        checkLatch.countDown();
        NodesResponse responses = future.get();
        assertEquals(0, responses.failureCount());
    }

    public void testTaskResponsesDiscardedOnCancellation() throws Exception {
        setupTestNodes(Settings.EMPTY);
        connectNodes(testNodes);
        CountDownLatch blockedActionLatch = new CountDownLatch(1);
        ActionFuture<NodesResponse> future = startBlockingTestNodesAction(blockedActionLatch);

        final var taskResponseListeners = new LinkedBlockingQueue<ActionListener<TestTaskResponse>>();
        final var taskResponseListenersCountDown = new CountDownLatch(2); // test action plus the list[n] action

        final TestTasksAction tasksAction = new TestTasksAction(
            "internal:testTasksAction",
            testNodes[0].clusterService,
            testNodes[0].transportService
        ) {
            @Override
            protected void taskOperation(
                CancellableTask actionTask,
                TestTasksRequest request,
                Task task,
                ActionListener<TestTaskResponse> listener
            ) {
                taskResponseListeners.add(listener);
                taskResponseListenersCountDown.countDown();
            }
        };

        TestTasksRequest testTasksRequest = new TestTasksRequest();
        testTasksRequest.setNodes(testNodes[0].getNodeId()); // only local node
        PlainActionFuture<TestTasksResponse> taskFuture = new PlainActionFuture<>();
        CancellableTask task = (CancellableTask) testNodes[0].transportService.getTaskManager()
            .registerAndExecute(
                "direct",
                tasksAction,
                testTasksRequest,
                testNodes[0].transportService.getLocalNodeConnection(),
                taskFuture
            );
        safeAwait(taskResponseListenersCountDown);

        final var reachabilityChecker = new ReachabilityChecker();

        final var listener0 = Objects.requireNonNull(taskResponseListeners.poll());
        if (randomBoolean()) {
            listener0.onResponse(reachabilityChecker.register(new TestTaskResponse("status")));
        } else {
            listener0.onFailure(reachabilityChecker.register(new ElasticsearchException("simulated")));
        }
        reachabilityChecker.checkReachable();

        safeAwait(
            (ActionListener<Void> l) -> testNodes[0].transportService.getTaskManager().cancelTaskAndDescendants(task, "test", false, l)
        );

        reachabilityChecker.ensureUnreachable();

        while (true) {
            final var listener = taskResponseListeners.poll();
            if (listener == null) {
                break;
            }
            if (randomBoolean()) {
                listener.onResponse(reachabilityChecker.register(new TestTaskResponse("status")));
            } else {
                listener.onFailure(reachabilityChecker.register(new ElasticsearchException("simulated")));
            }
            reachabilityChecker.ensureUnreachable();
        }

        expectThrows(TaskCancelledException.class, taskFuture);

        blockedActionLatch.countDown();
        NodesResponse responses = future.get(10, TimeUnit.SECONDS);
        assertEquals(0, responses.failureCount());
    }

    public void testNodeResponsesDiscardedOnCancellation() {
        setupTestNodes(Settings.EMPTY);
        connectNodes(testNodes);

        final var taskResponseListeners = new AtomicReferenceArray<ActionListener<TestTaskResponse>>(testNodes.length);
        final var taskResponseListenersCountDown = new CountDownLatch(testNodes.length); // one list[n] action per node
        final var tasksActions = new TestTasksAction[testNodes.length];
        for (int i = 0; i < testNodes.length; i++) {
            final var nodeIndex = i;
            tasksActions[i] = new TestTasksAction("internal:testTasksAction", testNodes[i].clusterService, testNodes[i].transportService) {
                @Override
                protected void taskOperation(
                    CancellableTask actionTask,
                    TestTasksRequest request,
                    Task task,
                    ActionListener<TestTaskResponse> listener
                ) {
                    assertThat(taskResponseListeners.getAndSet(nodeIndex, ActionListener.notifyOnce(listener)), nullValue());
                    taskResponseListenersCountDown.countDown();
                }
            };
        }

        TestTasksRequest testTasksRequest = new TestTasksRequest();
        testTasksRequest.setActions("internal:testTasksAction[n]");
        PlainActionFuture<TestTasksResponse> taskFuture = new PlainActionFuture<>();
        CancellableTask task = (CancellableTask) testNodes[0].transportService.getTaskManager()
            .registerAndExecute(
                "direct",
                tasksActions[0],
                testTasksRequest,
                testNodes[0].transportService.getLocalNodeConnection(),
                taskFuture
            );
        safeAwait(taskResponseListenersCountDown);

        final var reachabilityChecker = new ReachabilityChecker();

        if (randomBoolean()) {
            // local node does not de/serialize node-level response so retains references to the task-level response
            if (randomBoolean()) {
                taskResponseListeners.get(0).onResponse(reachabilityChecker.register(new TestTaskResponse("status")));
            } else {
                taskResponseListeners.get(0).onFailure(reachabilityChecker.register(new ElasticsearchException("simulated")));
            }
            reachabilityChecker.checkReachable();
        }

        safeAwait(
            (ActionListener<Void> l) -> testNodes[0].transportService.getTaskManager().cancelTaskAndDescendants(task, "test", false, l)
        );

        reachabilityChecker.ensureUnreachable();
        assertFalse(taskFuture.isDone());

        for (int i = 0; i < testNodes.length; i++) {
            if (randomBoolean()) {
                taskResponseListeners.get(i).onResponse(reachabilityChecker.register(new TestTaskResponse("status")));
            } else {
                taskResponseListeners.get(i).onFailure(reachabilityChecker.register(new ElasticsearchException("simulated")));
            }
            reachabilityChecker.ensureUnreachable();
        }

        expectThrows(TaskCancelledException.class, taskFuture);
    }

    public void testTaskLevelActionFailures() throws Exception {
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
                protected void taskOperation(
                    CancellableTask actionTask,
                    TestTasksRequest request,
                    Task task,
                    ActionListener<TestTaskResponse> listener
                ) {
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

    private static final ByteSizeValue IN_FLIGHT_LIMIT = ByteSizeValue.ofKb(64);

    private HierarchyCircuitBreakerService[] setupNodesWithInFlightLimit() {
        final Settings settings = Settings.builder()
            .put(HierarchyCircuitBreakerService.USE_REAL_MEMORY_USAGE_SETTING.getKey(), false)
            .put(HierarchyCircuitBreakerService.IN_FLIGHT_REQUESTS_CIRCUIT_BREAKER_LIMIT_SETTING.getKey(), IN_FLIGHT_LIMIT)
            .build();
        final HierarchyCircuitBreakerService[] breakerServices = new HierarchyCircuitBreakerService[3];
        setupTestNodes(3, Settings.EMPTY, i -> {
            breakerServices[i] = new HierarchyCircuitBreakerService(
                CircuitBreakerMetrics.NOOP,
                settings,
                List.of(),
                new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
            );
            return breakerServices[i];
        });
        connectNodes(testNodes);
        return breakerServices;
    }

    private TestTasksAction[] newTasksActions(int bigReplyNode, int smallReplyLength) {
        return newTasksActions(bigReplyNode, Math.toIntExact(IN_FLIGHT_LIMIT.getBytes()) * 4, smallReplyLength);
    }

    private TestTasksAction[] newTasksActions(int bigReplyNode, int bigReplyLength, int smallReplyLength) {
        final TestTasksAction[] tasksActions = new TestTasksAction[nodesCount];
        for (int i = 0; i < testNodes.length; i++) {
            final int node = i;
            tasksActions[i] = new TestTasksAction("internal:testTasksAction", testNodes[i].clusterService, testNodes[i].transportService) {
                @Override
                protected boolean nodeRepliesCanTripCircuitBreaker() {
                    return true;
                }

                @Override
                protected void taskOperation(
                    CancellableTask actionTask,
                    TestTasksRequest request,
                    Task task,
                    ActionListener<TestTaskResponse> listener
                ) {
                    final int length = node == bigReplyNode ? bigReplyLength : smallReplyLength;
                    listener.onResponse(new TestTaskResponse("x".repeat(length)));
                }
            };
        }
        return tasksActions;
    }

    public void testNodeReplyTrippingInFlightBreakerFailsOnlyThatNode() throws Exception {
        final HierarchyCircuitBreakerService[] breakerServices = setupNodesWithInFlightLimit();
        final CountDownLatch checkLatch = new CountDownLatch(1);
        final ActionFuture<NodesResponse> future = startBlockingTestNodesAction(checkLatch);
        final int bigReplyNode = 2;
        final TestTasksAction[] tasksActions = newTasksActions(bigReplyNode, 16);
        final CircuitBreaker breaker = breakerServices[0].getBreaker(CircuitBreaker.IN_FLIGHT_REQUESTS);
        final long usedBefore = breaker.getUsed();
        final long trippedBefore = breaker.getTrippedCount();

        final TestTasksRequest request = new TestTasksRequest();
        request.setActions("internal:testAction[n]");
        final TestTasksResponse response = ActionTestUtils.executeBlocking(tasksActions[0], request);

        assertThat(response.tasks, hasSize(nodesCount - 1));
        assertThat(response.getNodeFailures(), hasSize(1));
        final FailedNodeException nodeFailure = (FailedNodeException) response.getNodeFailures().get(0);
        assertThat(nodeFailure.nodeId(), equalTo(testNodes[bigReplyNode].getNodeId()));
        assertNotNull(ExceptionsHelper.unwrap(nodeFailure, CircuitBreakingException.class));
        assertThat(breaker.getTrippedCount(), equalTo(trippedBefore + 1));
        assertBusy(() -> assertThat(breaker.getUsed(), equalTo(usedBefore)));

        checkLatch.countDown();
        assertEquals(0, future.get().failureCount());
    }

    public void testNodeRepliesAreNotChargedUnlessTheActionOptsIn() throws Exception {
        final HierarchyCircuitBreakerService[] breakerServices = setupNodesWithInFlightLimit();
        final CountDownLatch checkLatch = new CountDownLatch(1);
        final ActionFuture<NodesResponse> future = startBlockingTestNodesAction(checkLatch);
        final TestTasksAction[] tasksActions = new TestTasksAction[nodesCount];
        for (int i = 0; i < testNodes.length; i++) {
            tasksActions[i] = new TestTasksAction("internal:testTasksAction", testNodes[i].clusterService, testNodes[i].transportService) {
                @Override
                protected void taskOperation(
                    CancellableTask actionTask,
                    TestTasksRequest request,
                    Task task,
                    ActionListener<TestTaskResponse> listener
                ) {
                    listener.onResponse(new TestTaskResponse("x".repeat(Math.toIntExact(IN_FLIGHT_LIMIT.getBytes()) * 4)));
                }
            };
        }
        final CircuitBreaker breaker = breakerServices[0].getBreaker(CircuitBreaker.IN_FLIGHT_REQUESTS);

        final TestTasksRequest request = new TestTasksRequest();
        request.setActions("internal:testAction[n]");
        final TestTasksResponse response = ActionTestUtils.executeBlocking(tasksActions[0], request);

        assertThat(response.tasks, hasSize(nodesCount));
        assertThat(response.getNodeFailures(), hasSize(0));
        assertThat(breaker.getTrippedCount(), equalTo(0L));

        checkLatch.countDown();
        assertEquals(0, future.get().failureCount());
    }

    private Task registerTaskWithDescription(TestNode node, int descriptionLength) {
        return registerTaskWithDescription(node, descriptionLength, () -> {});
    }

    private static long highestNodeListingTaskId(TestNode node) {
        return node.transportService.getTaskManager()
            .getTasks()
            .values()
            .stream()
            .filter(t -> t.getAction().equals(TransportListTasksAction.TYPE.name() + "[n]"))
            .mapToLong(Task::getId)
            .max()
            .orElse(-1);
    }

    private Task registerTaskWithDescription(TestNode node, int descriptionLength, Runnable onTaskInfo) {
        final String description = "x".repeat(descriptionLength);
        return node.transportService.getTaskManager().register("test", "internal:test/described", new TaskAwareRequest() {
            @Override
            public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
                return new Task(id, type, action, description, parentTaskId, headers) {
                    @Override
                    public String getDescription() {
                        onTaskInfo.run();
                        return super.getDescription();
                    }
                };
            }

            private TaskId parentTask = TaskId.EMPTY_TASK_ID;

            @Override
            public void setParentTask(TaskId taskId) {
                parentTask = taskId;
            }

            @Override
            public void setRequestId(long requestId) {}

            @Override
            public TaskId getParentTask() {
                return parentTask;
            }

            @Override
            public String getDescription() {
                return description;
            }
        });
    }

    private static ListTasksRequest doubleListRequest() {
        final ListTasksRequest request = new ListTasksRequest();
        request.setDetailed(true);
        request.setActions("internal:test/described", TransportListTasksAction.RELOCATABLE_ACTIONS.iterator().next());
        return request;
    }

    public void testSecondListingPassIsCheckedAgainstTheFirstPass() throws Exception {
        final HierarchyCircuitBreakerService[] breakerServices = setupNodesWithInFlightLimit();
        final CircuitBreaker breaker = breakerServices[0].getBreaker(CircuitBreaker.IN_FLIGHT_REQUESTS);
        final long usedBefore = breaker.getUsed();
        final Task task1 = registerTaskWithDescription(testNodes[1], 12_000);
        final Task task2 = registerTaskWithDescription(testNodes[2], 12_000);
        try {
            final ListTasksResponse response = ActionTestUtils.executeBlockingWithTask(
                testNodes[0].transportService.getTaskManager(),
                testNodes[0].transportService.getLocalNodeConnection(),
                testNodes[0].transportListTasksAction,
                doubleListRequest()
            );

            assertThat(response.getNodeFailures().size(), greaterThan(0));
            assertNotNull(ExceptionsHelper.unwrap(response.getNodeFailures().get(0), CircuitBreakingException.class));
            assertBusy(() -> assertThat(breaker.getUsed(), equalTo(usedBefore)));
        } finally {
            testNodes[1].transportService.getTaskManager().unregister(task1);
            testNodes[2].transportService.getTaskManager().unregister(task2);
        }
    }

    public void testFirstListingPassStaysChargedWhileTheSecondOneWaits() throws Exception {
        final HierarchyCircuitBreakerService[] breakerServices = setupNodesWithInFlightLimit();
        final CircuitBreaker breaker = breakerServices[0].getBreaker(CircuitBreaker.IN_FLIGHT_REQUESTS);
        final long usedBefore = breaker.getUsed();
        final int descriptionLength = 5_000;
        final AtomicLong firstPassTaskIdOnNode1 = new AtomicLong(-1);
        final AtomicLong firstPassTaskIdOnNode2 = new AtomicLong(-1);
        final Task task1 = registerTaskWithDescription(
            testNodes[1],
            descriptionLength,
            () -> firstPassTaskIdOnNode1.compareAndSet(-1, highestNodeListingTaskId(testNodes[1]))
        );
        final Task task2 = registerTaskWithDescription(
            testNodes[2],
            descriptionLength,
            () -> firstPassTaskIdOnNode2.compareAndSet(-1, highestNodeListingTaskId(testNodes[2]))
        );
        final AtomicBoolean unregistered = new AtomicBoolean();
        try {
            final ListTasksRequest request = doubleListRequest();
            request.setWaitForCompletion(true);
            request.setTimeout(TimeValue.timeValueSeconds(30));
            final PlainActionFuture<ListTasksResponse> future = new PlainActionFuture<>();
            testNodes[0].transportService.getTaskManager()
                .registerAndExecute(
                    "direct",
                    testNodes[0].transportListTasksAction,
                    request,
                    testNodes[0].transportService.getLocalNodeConnection(),
                    future
                );

            assertBusy(() -> {
                assertThat(firstPassTaskIdOnNode1.get(), greaterThanOrEqualTo(0L));
                assertThat(firstPassTaskIdOnNode2.get(), greaterThanOrEqualTo(0L));
                assertThat(highestNodeListingTaskId(testNodes[1]), greaterThan(firstPassTaskIdOnNode1.get()));
                assertThat(highestNodeListingTaskId(testNodes[2]), greaterThan(firstPassTaskIdOnNode2.get()));
            });
            assertThat(breaker.getUsed(), greaterThanOrEqualTo(usedBefore + 2L * descriptionLength));
            assertFalse(future.isDone());

            unregistered.set(true);
            testNodes[1].transportService.getTaskManager().unregister(task1);
            testNodes[2].transportService.getTaskManager().unregister(task2);
            safeGet(future);
            assertBusy(() -> assertThat(breaker.getUsed(), equalTo(usedBefore)));
        } finally {
            if (unregistered.get() == false) {
                testNodes[1].transportService.getTaskManager().unregister(task1);
                testNodes[2].transportService.getTaskManager().unregister(task2);
            }
        }
    }

    public void testCancellingAListingWhoseSecondPassWaitsReleasesBothPasses() throws Exception {
        final HierarchyCircuitBreakerService[] breakerServices = setupNodesWithInFlightLimit();
        final CircuitBreaker breaker = breakerServices[0].getBreaker(CircuitBreaker.IN_FLIGHT_REQUESTS);
        final long usedBefore = breaker.getUsed();
        final int descriptionLength = 5_000;
        final Task task1 = registerTaskWithDescription(testNodes[1], descriptionLength);
        final Task task2 = registerTaskWithDescription(testNodes[2], descriptionLength);
        try {
            final ListTasksRequest request = doubleListRequest();
            request.setWaitForCompletion(true);
            request.setTimeout(TimeValue.timeValueSeconds(30));
            final PlainActionFuture<ListTasksResponse> future = new PlainActionFuture<>();
            final CancellableTask listTask = (CancellableTask) testNodes[0].transportService.getTaskManager()
                .registerAndExecute(
                    "direct",
                    testNodes[0].transportListTasksAction,
                    request,
                    testNodes[0].transportService.getLocalNodeConnection(),
                    future
                );
            assertBusy(() -> assertThat(breaker.getUsed(), greaterThanOrEqualTo(usedBefore + 2L * descriptionLength)));

            safeAwait(
                (ActionListener<Void> l) -> testNodes[0].transportService.getTaskManager()
                    .cancelTaskAndDescendants(listTask, "test", false, l)
            );

            assertBusy(() -> assertThat(breaker.getUsed(), equalTo(usedBefore)));
            expectThrows(TaskCancelledException.class, future);
        } finally {
            testNodes[1].transportService.getTaskManager().unregister(task1);
            testNodes[2].transportService.getTaskManager().unregister(task2);
        }
    }

    public void testOnlyTheSecondListingPassStaysChargedWhenTheMergedResponseIsDelivered() throws Exception {
        final HierarchyCircuitBreakerService[] breakerServices = setupNodesWithInFlightLimit();
        final CircuitBreaker breaker = breakerServices[0].getBreaker(CircuitBreaker.IN_FLIGHT_REQUESTS);
        final long usedBefore = breaker.getUsed();
        final int descriptionLength = 5_000;
        final Task task1 = registerTaskWithDescription(testNodes[1], descriptionLength);
        final Task task2 = registerTaskWithDescription(testNodes[2], descriptionLength);
        try {
            final PlainActionFuture<Long> usedWhileDelivering = new PlainActionFuture<>();
            testNodes[0].transportService.getTaskManager()
                .registerAndExecute(
                    "direct",
                    testNodes[0].transportListTasksAction,
                    doubleListRequest(),
                    testNodes[0].transportService.getLocalNodeConnection(),
                    ActionListener.wrap(r -> usedWhileDelivering.onResponse(breaker.getUsed()), usedWhileDelivering::onFailure)
                );

            final long usedAtDelivery = safeGet(usedWhileDelivering);
            assertThat(usedAtDelivery, greaterThanOrEqualTo(usedBefore + 2L * descriptionLength));
            assertThat(usedAtDelivery, lessThan(usedBefore + 3L * descriptionLength));
            assertBusy(() -> assertThat(breaker.getUsed(), equalTo(usedBefore)));
        } finally {
            testNodes[1].transportService.getTaskManager().unregister(task1);
            testNodes[2].transportService.getTaskManager().unregister(task2);
        }
    }

    public void testListResponseKeepsTheChargeWhileTheRestLayerRetainsIt() throws Exception {
        final HierarchyCircuitBreakerService[] breakerServices = setupNodesWithInFlightLimit();
        final CircuitBreaker breaker = breakerServices[0].getBreaker(CircuitBreaker.IN_FLIGHT_REQUESTS);
        final long usedBefore = breaker.getUsed();
        final int descriptionLength = 5_000;
        final Task task1 = registerTaskWithDescription(testNodes[1], descriptionLength);
        final Task task2 = registerTaskWithDescription(testNodes[2], descriptionLength);
        try {
            final PlainActionFuture<Releasable> retained = new PlainActionFuture<>();
            testNodes[0].transportService.getTaskManager()
                .registerAndExecute(
                    "direct",
                    testNodes[0].transportListTasksAction,
                    doubleListRequest(),
                    testNodes[0].transportService.getLocalNodeConnection(),
                    ActionListener.wrap(r -> retained.onResponse(r.retainNodeReplies()), retained::onFailure)
                );
            final Releasable retention = safeGet(retained);
            try {
                assertThat(breaker.getUsed(), greaterThanOrEqualTo(usedBefore + 2L * descriptionLength));
                assertThat(breaker.getUsed(), lessThan(usedBefore + 3L * descriptionLength));
            } finally {
                retention.close();
            }
            assertBusy(() -> assertThat(breaker.getUsed(), equalTo(usedBefore)));
        } finally {
            testNodes[1].transportService.getTaskManager().unregister(task1);
            testNodes[2].transportService.getTaskManager().unregister(task2);
        }
    }

    public void testNodeReplyReservationReleasedAfterSuccessfulReply() throws Exception {
        final HierarchyCircuitBreakerService[] breakerServices = setupNodesWithInFlightLimit();
        final CountDownLatch checkLatch = new CountDownLatch(1);
        final ActionFuture<NodesResponse> future = startBlockingTestNodesAction(checkLatch);
        final TestTasksAction[] tasksActions = newTasksActions(-1, 16);
        final CircuitBreaker breaker = breakerServices[0].getBreaker(CircuitBreaker.IN_FLIGHT_REQUESTS);
        final long usedBefore = breaker.getUsed();

        final TestTasksRequest request = new TestTasksRequest();
        request.setActions("internal:testAction[n]");
        final TestTasksResponse response = ActionTestUtils.executeBlocking(tasksActions[0], request);

        assertThat(response.tasks, hasSize(nodesCount));
        assertThat(response.getNodeFailures(), hasSize(0));
        assertThat(breaker.getTrippedCount(), equalTo(0L));
        assertBusy(() -> assertThat(breaker.getUsed(), equalTo(usedBefore)));

        checkLatch.countDown();
        assertEquals(0, future.get().failureCount());
    }

    public void testNodeReplyBytesStayChargedUntilResponseIsDelivered() throws Exception {
        final HierarchyCircuitBreakerService[] breakerServices = setupNodesWithInFlightLimit();
        final CountDownLatch checkLatch = new CountDownLatch(1);
        final ActionFuture<NodesResponse> future = startBlockingTestNodesAction(checkLatch);
        final TestTasksAction[] tasksActions = newTasksActions(-1, Math.toIntExact(IN_FLIGHT_LIMIT.getBytes()) / 8);
        final CircuitBreaker breaker = breakerServices[0].getBreaker(CircuitBreaker.IN_FLIGHT_REQUESTS);
        final long usedBefore = breaker.getUsed();

        final TestTasksRequest request = new TestTasksRequest();
        request.setActions("internal:testAction[n]");
        final PlainActionFuture<Long> usedWhileDelivering = new PlainActionFuture<>();
        ActionTestUtils.execute(
            tasksActions[0],
            request,
            ActionListener.wrap(r -> usedWhileDelivering.onResponse(breaker.getUsed()), usedWhileDelivering::onFailure)
        );

        assertThat(safeGet(usedWhileDelivering), greaterThanOrEqualTo(usedBefore + 2L * (IN_FLIGHT_LIMIT.getBytes() / 8)));
        assertBusy(() -> assertThat(breaker.getUsed(), equalTo(usedBefore)));

        checkLatch.countDown();
        assertEquals(0, future.get().failureCount());
    }

    private TestTasksAction[] newTasksActionsBlockingOnNode(
        int blockedNode,
        SubscribableListener<ActionListener<TestTaskResponse>> blockedListener,
        int replyLength
    ) {
        final TestTasksAction[] tasksActions = new TestTasksAction[nodesCount];
        for (int i = 0; i < testNodes.length; i++) {
            final int node = i;
            tasksActions[i] = new TestTasksAction("internal:testTasksAction", testNodes[i].clusterService, testNodes[i].transportService) {
                @Override
                protected boolean nodeRepliesCanTripCircuitBreaker() {
                    return true;
                }

                @Override
                protected void taskOperation(
                    CancellableTask actionTask,
                    TestTasksRequest request,
                    Task task,
                    ActionListener<TestTaskResponse> listener
                ) {
                    if (node == blockedNode) {
                        blockedListener.onResponse(listener);
                    } else {
                        listener.onResponse(new TestTaskResponse("x".repeat(replyLength)));
                    }
                }
            };
        }
        return tasksActions;
    }

    public void testRetainedNodeReplyBytesReleasedOnCancellation() throws Exception {
        final HierarchyCircuitBreakerService[] breakerServices = setupNodesWithInFlightLimit();
        final CountDownLatch blockedActionLatch = new CountDownLatch(1);
        final ActionFuture<NodesResponse> future = startBlockingTestNodesAction(blockedActionLatch);
        final int blockedNode = 2;
        final SubscribableListener<ActionListener<TestTaskResponse>> blockedListener = new SubscribableListener<>();
        final int replyLength = Math.toIntExact(IN_FLIGHT_LIMIT.getBytes()) / 8;
        final TestTasksAction[] tasksActions = newTasksActionsBlockingOnNode(blockedNode, blockedListener, replyLength);
        final CircuitBreaker breaker = breakerServices[0].getBreaker(CircuitBreaker.IN_FLIGHT_REQUESTS);
        final long usedBefore = breaker.getUsed();

        final TestTasksRequest request = new TestTasksRequest();
        request.setActions("internal:testAction[n]");
        final PlainActionFuture<TestTasksResponse> taskFuture = new PlainActionFuture<>();
        final CancellableTask task = (CancellableTask) testNodes[0].transportService.getTaskManager()
            .registerAndExecute("direct", tasksActions[0], request, testNodes[0].transportService.getLocalNodeConnection(), taskFuture);
        final ActionListener<TestTaskResponse> blocked = safeAwait(blockedListener);
        assertBusy(() -> assertThat(breaker.getUsed(), greaterThan(usedBefore)));

        safeAwait(
            (ActionListener<Void> l) -> testNodes[0].transportService.getTaskManager().cancelTaskAndDescendants(task, "test", false, l)
        );

        assertBusy(() -> assertThat(breaker.getUsed(), equalTo(usedBefore)));
        assertFalse(taskFuture.isDone());

        blocked.onResponse(new TestTaskResponse("x".repeat(replyLength)));
        expectThrows(TaskCancelledException.class, taskFuture);
        assertBusy(() -> assertThat(breaker.getUsed(), equalTo(usedBefore)));

        blockedActionLatch.countDown();
        assertEquals(0, future.get(10, TimeUnit.SECONDS).failureCount());
    }

    @TestLogging(reason = "asserts a debug message", value = "org.elasticsearch.transport.InboundHandler:DEBUG")
    public void testNodeReplyArrivingAfterCancellationIsRejected() throws Exception {
        final HierarchyCircuitBreakerService[] breakerServices = setupNodesWithInFlightLimit();
        final CountDownLatch blockedActionLatch = new CountDownLatch(1);
        final ActionFuture<NodesResponse> future = startBlockingTestNodesAction(blockedActionLatch);
        final SubscribableListener<ActionListener<TestTaskResponse>> blockedListener = new SubscribableListener<>();
        final TestTasksAction[] tasksActions = newTasksActionsBlockingOnNode(2, blockedListener, 16);
        final CircuitBreaker breaker = breakerServices[0].getBreaker(CircuitBreaker.IN_FLIGHT_REQUESTS);
        final long usedBefore = breaker.getUsed();

        final TestTasksRequest request = new TestTasksRequest();
        request.setActions("internal:testAction[n]");
        final PlainActionFuture<TestTasksResponse> taskFuture = new PlainActionFuture<>();
        final CancellableTask task = (CancellableTask) testNodes[0].transportService.getTaskManager()
            .registerAndExecute("direct", tasksActions[0], request, testNodes[0].transportService.getLocalNodeConnection(), taskFuture);
        final ActionListener<TestTaskResponse> blocked = safeAwait(blockedListener);

        // Only the coordinating task is cancelled, so the remote node does not know and still sends a successful reply
        testNodes[0].transportService.getTaskManager().cancel(task, "test", () -> {});
        assertTrue(task.isCancelled());
        assertFalse(taskFuture.isDone());

        MockLog.awaitLogger(
            () -> blocked.onResponse(new TestTaskResponse("x")),
            InboundHandler.class,
            new MockLog.SeenEventExpectation(
                "reply rejected as it arrives",
                InboundHandler.class.getCanonicalName(),
                Level.DEBUG,
                "Task cancelled while response from * arrived"
            )
        );
        expectThrows(TaskCancelledException.class, taskFuture);
        assertBusy(() -> assertThat(breaker.getUsed(), equalTo(usedBefore)));
        assertThat(breaker.getTrippedCount(), equalTo(0L));

        blockedActionLatch.countDown();
        assertEquals(0, future.get(10, TimeUnit.SECONDS).failureCount());
    }

    private static final class CancellingBreakerService extends CircuitBreakerService {
        private final CircuitBreakerService delegate;
        private final CircuitBreaker inFlightBreaker;

        CancellingBreakerService(CircuitBreakerService delegate, long cancelAfterBytes, Runnable cancel, LongAdder reservedBytes) {
            this.delegate = delegate;
            final CircuitBreaker breaker = delegate.getBreaker(CircuitBreaker.IN_FLIGHT_REQUESTS);
            this.inFlightBreaker = new CircuitBreaker() {
                private final AtomicBoolean cancelled = new AtomicBoolean();

                @Override
                public void circuitBreak(String fieldName, long bytesNeeded) {
                    breaker.circuitBreak(fieldName, bytesNeeded);
                }

                @Override
                public void addEstimateBytesAndMaybeBreak(long bytes, String label) throws CircuitBreakingException {
                    breaker.addEstimateBytesAndMaybeBreak(bytes, label);
                    if (reservedBytes.sum() + bytes >= cancelAfterBytes && cancelled.compareAndSet(false, true)) {
                        reservedBytes.add(bytes);
                        cancel.run();
                    } else {
                        reservedBytes.add(bytes);
                    }
                }

                @Override
                public void addWithoutBreaking(long bytes) {
                    breaker.addWithoutBreaking(bytes);
                }

                @Override
                public long getUsed() {
                    return breaker.getUsed();
                }

                @Override
                public long getLimit() {
                    return breaker.getLimit();
                }

                @Override
                public double getOverhead() {
                    return breaker.getOverhead();
                }

                @Override
                public long getTrippedCount() {
                    return breaker.getTrippedCount();
                }

                @Override
                public String getName() {
                    return breaker.getName();
                }

                @Override
                public Durability getDurability() {
                    return breaker.getDurability();
                }

                @Override
                public void setLimitAndOverhead(long limit, double overhead) {
                    breaker.setLimitAndOverhead(limit, overhead);
                }
            };
        }

        @Override
        public CircuitBreaker getBreaker(String name) {
            return CircuitBreaker.IN_FLIGHT_REQUESTS.equals(name) ? inFlightBreaker : delegate.getBreaker(name);
        }

        @Override
        public AllCircuitBreakerStats stats() {
            return delegate.stats();
        }

        @Override
        public CircuitBreakerStats stats(String name) {
            return delegate.stats(name);
        }
    }

    public void testCancellationWhileReplyIsArrivingDropsTheRestOfIt() throws Exception {
        final int bigReplyLength = Math.toIntExact(ByteSizeValue.ofMb(4).getBytes());
        final long cancelAfterBytes = ByteSizeValue.ofKb(128).getBytes();
        final Settings settings = Settings.builder()
            .put(HierarchyCircuitBreakerService.USE_REAL_MEMORY_USAGE_SETTING.getKey(), false)
            .put(HierarchyCircuitBreakerService.IN_FLIGHT_REQUESTS_CIRCUIT_BREAKER_LIMIT_SETTING.getKey(), ByteSizeValue.ofMb(64))
            .build();
        final HierarchyCircuitBreakerService[] breakerServices = new HierarchyCircuitBreakerService[3];
        final SubscribableListener<CancellableTask> taskListener = new SubscribableListener<>();
        final LongAdder reservedBytes = new LongAdder();
        final AtomicBoolean cancelled = new AtomicBoolean();
        setupTestNodes(3, Settings.EMPTY, i -> {
            breakerServices[i] = new HierarchyCircuitBreakerService(
                CircuitBreakerMetrics.NOOP,
                settings,
                List.of(),
                new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
            );
            if (i != 0) {
                return breakerServices[i];
            }
            return new CancellingBreakerService(
                breakerServices[i],
                cancelAfterBytes,
                () -> taskListener.addListener(ActionTestUtils.assertNoFailureListener(task -> {
                    testNodes[0].transportService.getTaskManager().cancel(task, "test", () -> {});
                    cancelled.set(task.isCancelled());
                })),
                reservedBytes
            );
        });
        connectNodes(testNodes);
        final CountDownLatch blockedActionLatch = new CountDownLatch(1);
        final ActionFuture<NodesResponse> future = startBlockingTestNodesAction(blockedActionLatch);
        final TestTasksAction[] tasksActions = newTasksActions(2, bigReplyLength, 16);
        final CircuitBreaker breaker = breakerServices[0].getBreaker(CircuitBreaker.IN_FLIGHT_REQUESTS);
        final long usedBefore = breaker.getUsed();

        final TestTasksRequest request = new TestTasksRequest();
        request.setActions("internal:testAction[n]");
        final PlainActionFuture<TestTasksResponse> taskFuture = new PlainActionFuture<>();
        final CancellableTask task = (CancellableTask) testNodes[0].transportService.getTaskManager()
            .registerAndExecute("direct", tasksActions[0], request, testNodes[0].transportService.getLocalNodeConnection(), taskFuture);
        taskListener.onResponse(task);

        expectThrows(TaskCancelledException.class, taskFuture);
        assertTrue(cancelled.get());
        assertThat(reservedBytes.sum(), greaterThanOrEqualTo(cancelAfterBytes));
        assertThat(reservedBytes.sum(), lessThan((long) bigReplyLength));
        assertBusy(() -> assertThat(breaker.getUsed(), equalTo(usedBefore)));
        assertThat(breaker.getTrippedCount(), equalTo(0L));

        blockedActionLatch.countDown();
        assertEquals(0, future.get(10, TimeUnit.SECONDS).failureCount());
    }

    @SuppressWarnings("unchecked")
    public void testTasksToXContentGrouping() throws Exception {
        setupTestNodes(Settings.EMPTY);
        connectNodes(testNodes);

        // Get the parent task
        ListTasksRequest listTasksRequest = new ListTasksRequest();
        listTasksRequest.setActions(TransportListTasksAction.TYPE.name() + "*");
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
        if (byParents) {
            DiscoveryNodes nodes = testNodes[0].clusterService.state().nodes();
            ChunkedToXContent.wrapAsToXContent(response.groupedByNode(() -> nodes)).toXContent(builder, ToXContent.EMPTY_PARAMS);
        } else {
            ChunkedToXContent.wrapAsToXContent(response.groupedByParent()).toXContent(builder, ToXContent.EMPTY_PARAMS);
        }
        builder.flush();
        logger.info(Strings.toString(builder));
        return XContentHelper.convertToMap(BytesReference.bytes(builder), false, builder.contentType()).v2();
    }

    public void testRefCounting() throws Exception {
        setupTestNodes(Settings.EMPTY);
        connectNodes(testNodes);

        CountDownLatch checkLatch = new CountDownLatch(1);
        ActionFuture<NodesResponse> future = startBlockingTestNodesAction(checkLatch);

        final var firstNodeListeners = new SubscribableListener<TestTaskResponse>();
        final var otherNodeListeners = new SubscribableListener<TestTaskResponse>();
        TestTasksAction[] tasksActions = new TestTasksAction[nodesCount];
        for (int nodeId = 0; nodeId < nodesCount; nodeId++) {
            final var listeners = nodeId == 0 ? firstNodeListeners : otherNodeListeners;
            tasksActions[nodeId] = new TestTasksAction(
                "internal:testTasksAction",
                testNodes[nodeId].clusterService,
                testNodes[nodeId].transportService
            ) {
                @Override
                protected void taskOperation(
                    CancellableTask actionTask,
                    TestTasksRequest request,
                    Task task,
                    ActionListener<TestTaskResponse> listener
                ) {
                    request.incRef();
                    listeners.addListener(ActionListener.runBefore(listener, request::decRef));
                }
            };
        }

        final var requestReleaseFuture = new PlainActionFuture<Void>();
        TestTasksRequest testTasksRequest = new TestTasksRequest(() -> requestReleaseFuture.onResponse(null));
        testTasksRequest.setActions("internal:testAction[n]"); // pick all test actions
        testTasksRequest.setNodes(testNodes[0].getNodeId(), testNodes[1].getNodeId()); // only first two nodes
        final var taskFuture = new PlainActionFuture<TestTasksResponse>();
        testNodes[0].transportService.getTaskManager()
            .registerAndExecute(
                "direct",
                tasksActions[0],
                testTasksRequest,
                testNodes[0].transportService.getLocalNodeConnection(),
                taskFuture
            );
        testTasksRequest.decRef();

        assertTrue(testTasksRequest.hasReferences());
        firstNodeListeners.onResponse(new TestTaskResponse("done"));
        assertNull(requestReleaseFuture.get(10, TimeUnit.SECONDS));

        assertFalse(testTasksRequest.hasReferences());
        assertFalse(taskFuture.isDone());

        otherNodeListeners.onResponse(new TestTaskResponse("done"));
        taskFuture.get(10, TimeUnit.SECONDS);

        // Release all node tasks and wait for response
        checkLatch.countDown();
        NodesResponse responses = future.get();
        assertEquals(0, responses.failureCount());
    }
}
