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

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksRequest;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cockroach.CockroachActionExecutor;
import org.elasticsearch.cockroach.CockroachActionResponseProcessor;
import org.elasticsearch.cockroach.CockroachRequest;
import org.elasticsearch.cockroach.CockroachResponse;
import org.elasticsearch.cockroach.CockroachService;
import org.elasticsearch.cockroach.CockroachTasksInProgress;
import org.elasticsearch.cockroach.TransportCockroachAction;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskInfo;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Function;

import static org.elasticsearch.common.xcontent.ToXContent.EMPTY_PARAMS;
import static org.hamcrest.CoreMatchers.anyOf;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.startsWith;

public class CockroachActionTests extends TaskManagerTestCase {

    public static class TestRequest extends CockroachRequest<TestRequest> {

        private String testParam;

        private String preferNode;

        public TestRequest() {

        }

        public TestRequest(String testParam) {
            this.testParam = testParam;
        }

        public TestRequest(String testParam, String preferNode) {
            this.testParam = testParam;
            this.preferNode = preferNode;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public String getWriteableName() {
            return TransportTestCockroachAction.NAME;
        }

        public void setTestParam(String testParam) {
            this.testParam = testParam;
        }

        public String getTestParam() {
            return testParam;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeOptionalString(testParam);
            out.writeOptionalString(preferNode);
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            testParam = in.readOptionalString();
            preferNode = in.readOptionalString();
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("param", testParam);
            builder.endObject();
            return builder;
        }

    }

    public static class TestResponse extends CockroachResponse {

        private String response;

        public TestResponse() {

        }

        public TestResponse(String response) {
            this.response = response;
        }

        @Override
        public String getWriteableName() {
            return TransportTestCockroachAction.NAME;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            response = in.readOptionalString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeOptionalString(response);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("text", response);
            builder.endObject();
            return builder;
        }


    }

    public static class TransportTestCockroachAction extends TransportCockroachAction<TestRequest, TestResponse> {

        public static final String NAME = "cluster:admin/cockroach/test";

        private final BiFunction<TransportTestCockroachAction, TestRequest, TestResponse> nodeOperation;

        private final BiFunction<TransportTestCockroachAction, TestResponse, TestResponse> callerOperation;

        private final ClusterService clusterService;

        @Inject
        public TransportTestCockroachAction(Settings settings, ThreadPool threadPool, TransportService transportService,
                                            ClusterService clusterService,
                                            CockroachService cockroachService, ActionFilters actionFilters,
                                            IndexNameExpressionResolver indexNameExpressionResolver,
                                            BiFunction<TransportTestCockroachAction, TestRequest, TestResponse> nodeOperation,
                                            BiFunction<TransportTestCockroachAction, TestResponse, TestResponse> callerOperation) {
            super(settings, NAME, false, threadPool, transportService, cockroachService, actionFilters,
                indexNameExpressionResolver, TestRequest::new, TestResponse::new, ThreadPool.Names.GENERIC);
            this.clusterService = clusterService;
            this.nodeOperation = nodeOperation;
            this.callerOperation = callerOperation;
        }

        @Override
        public DiscoveryNode executorNode(TestRequest request, ClusterState clusterState) {
            if (request.preferNode != null) {
                DiscoveryNode executorNode = clusterState.getNodes().get(request.preferNode);
                if (executorNode != null) {
                    return executorNode;
                }
            }
            return clusterState.getNodes().getMasterNode();
        }

        @Override
        public DiscoveryNode responseNode(TestRequest request, ClusterState clusterState) {
            return clusterState.getNodes().getMasterNode();
        }

        @Override
        public void executorNodeOperation(Task task, TestRequest request, ActionListener<TestResponse> listener) {
            try {
                listener.onResponse(nodeOperation.apply(this, request));
            } catch (Exception e) {
                listener.onFailure(e);
            }
        }

        @Override
        public void onResponse(Task task, TestRequest request, TestResponse response, ActionListener<TestResponse> listener) {
            if (callerOperation == null) {
                super.onResponse(task, request, response, listener);
            } else {
                listener.onResponse(callerOperation.apply(this, response));
            }
        }
    }

    private void assertCleanClusterState(int node) {
        CockroachTasksInProgress cockroachTasksInProgresses = testNodes[node].clusterService.state().custom(CockroachTasksInProgress.TYPE);
        assertTrue(cockroachTasksInProgresses.entries().isEmpty());
    }

    private void assertNoLeftOverTasks() {
        boolean found = false;
        for (int i = 0; i < testNodes.length; i++) {
            if (testNodes[i].isClosed.get() == false) {
                TaskManager taskManager = testNodes[i].transportService.getTaskManager();
                for (Task task : taskManager.getTasks().values()) {
                    if (task.getAction().startsWith(TransportTestCockroachAction.NAME)) {
                        found = true;
                        logger.warn("Found task with id {} for action {} on node {}", task.getId(), task.getAction(), i);
                    }
                }
            }
        }
        assertFalse(found);
    }

    public void testBasicCockroachAction() throws Exception {
        setupTestNodes(Settings.EMPTY);
        connectNodes(testNodes);
        startClusterStatePublishing(0, testNodes);
        int numberOfRequests = randomIntBetween(1, 100);
        CountDownLatch responseLatch = new CountDownLatch(numberOfRequests);

        final AtomicArray<TestResponse> responseReference = new AtomicArray<>(numberOfRequests);
        final AtomicArray<Exception> exceptionReference = new AtomicArray<>(numberOfRequests);
        boolean[] shouldFail = new boolean[numberOfRequests];
        for (int i = 0; i < numberOfRequests; i++) {
            shouldFail[i] = randomBoolean();
        }
        TransportTestCockroachAction[] actions = registerTestCockroachAction((transport, request) -> {
            if ("fail".equals(request.getTestParam())) {
                throw new RuntimeException("Should fail");
            } else {
                return new TestResponse("processed:" + request.getTestParam());
            }
        }, null);

        for (int i = 0; i < numberOfRequests; i++) {
            int coordinatingNode = randomInt(testNodes.length - 1);
            int executingNode = randomInt(testNodes.length - 1);
            final int current = i;
            logger.trace("Coordinating node: {}, executing node: {}, should fail: {}", coordinatingNode, executingNode, shouldFail[i]);
            TestRequest testRequest = new TestRequest(shouldFail[i] ? "fail" : "work", testNodes[executingNode].getNodeId());
            actions[coordinatingNode].execute(testRequest,
                new ActionListener<TestResponse>() {
                    @Override
                    public void onResponse(TestResponse listTasksResponse) {
                        responseReference.set(current, listTasksResponse);
                        responseLatch.countDown();
                    }

                    @Override
                    public void onFailure(Exception e) {
                        exceptionReference.set(current, e);
                        responseLatch.countDown();
                    }
                });
        }

        // Awaiting for the main task to finish
        responseLatch.await();
        logger.info("Done with {} requests", numberOfRequests);

        for (int i = 0; i < numberOfRequests; i++) {

            if (shouldFail[i]) {
                assertNull(responseReference.get(i));
                assertNotNull(exceptionReference.get(i));
            } else {
                assertNotNull(responseReference.get(i));
                assertNull(exceptionReference.get(i));
            }
        }
        // Make sure there is no leftover junk left in the cluster state and task manager
        assertCleanClusterState(0);
        assertNoLeftOverTasks();
    }

    public void testDeathOfCoordinatingNode() throws Exception {
        setupTestNodes(Settings.EMPTY);
        connectNodes(testNodes);
        startClusterStatePublishing(0, testNodes);
        CountDownLatch actionStartedLatch = new CountDownLatch(1);

        int node = randomIntBetween(1, testNodes.length - 1);
        logger.info("Coordinating node: {}", node);

        AtomicBoolean jobIsDone = new AtomicBoolean();
        AtomicBoolean responseProcessed = new AtomicBoolean();
        AtomicBoolean shouldNotBeCalled = new AtomicBoolean();

        CountDownLatch blockAction = new CountDownLatch(1);

        Task mainTask = startTestCockroachAction(node, new TestRequest("test"), (transport, request) -> {
                logger.info("action started");
                actionStartedLatch.countDown();
                try {
                    blockAction.await();
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                }
                assertFalse(jobIsDone.getAndSet(true));
                logger.info("processed exec");
                return new TestResponse("processed:" + request.getTestParam());
            },
            (transport, testResponse) -> {
                logger.info("processed response");
                assertFalse(responseProcessed.getAndSet(true));
                return testResponse;
            },
            new ActionListener<TestResponse>() {
                @Override
                public void onResponse(TestResponse listTasksResponse) {
                    shouldNotBeCalled.set(true);
                }

                @Override
                public void onFailure(Exception e) {

                }
            });

        // Wait for the action to start
        actionStartedLatch.await();

        // Shut down coordinating node
        removeNode(0, node, testNodes);

        // Perform action
        blockAction.countDown();

        // Awaiting for the cockroach task to disappear from the cluster state
        // There is no other way to wait for it since our coordinating node died
        assertBusy(() -> assertCleanClusterState(0));

        assertTrue(jobIsDone.get());
        assertTrue(responseProcessed.get());
        assertFalse(shouldNotBeCalled.get());

        // Make sure there is no leftover junk left in the task manager
        // Because our coordinating node died, we have to wait for the tasks to disappear since we have no other way to detect that
        // all tasks are finished
        assertBusy(this::assertNoLeftOverTasks);
    }

    public void testDeathOfExecutorNode() throws Exception {
        setupTestNodes(Settings.EMPTY);
        connectNodes(testNodes);
        startClusterStatePublishing(0, testNodes);
        CountDownLatch actionStartedLatch = new CountDownLatch(1);

        int executingNode = randomIntBetween(1, testNodes.length - 1);
        int coordinatingNode = randomIntBetween(0, testNodes.length - 2);
        if (coordinatingNode >= executingNode) {
            // the coordinating node and executing nodes should't be the same
            coordinatingNode++;
        }
        boolean shouldFail = randomBoolean();

        logger.info("Executing node: {}, coordinating node:{}, should fail: {}", executingNode, coordinatingNode, shouldFail);

        CountDownLatch responseLatch = new CountDownLatch(1);
        final AtomicReference<TestResponse> responseReference = new AtomicReference<>();
        final AtomicReference<Exception> exceptionReference = new AtomicReference<>();
        final AtomicBoolean triedToExecuteOnNonMaster = new AtomicBoolean();

        CountDownLatch blockAction = new CountDownLatch(1);

        Task mainTask = startTestCockroachAction(coordinatingNode,
            new TestRequest(shouldFail ? "fail" : "work", testNodes[executingNode].getNodeId()),
            (transport, request) -> {
                String nodeId = transport.clusterService.localNode().getId();
                logger.info("action started on {}", nodeId);
                // Only block on the first executing node
                if (nodeId.equals(testNodes[executingNode].getNodeId())) {
                    triedToExecuteOnNonMaster.set(true);
                    actionStartedLatch.countDown();
                    try {
                        blockAction.await();
                    } catch (InterruptedException ex) {
                        Thread.currentThread().interrupt();
                    }
                }
                if ("fail".equals(request.getTestParam())) {
                    throw new RuntimeException("Should fail");
                } else {
                    return new TestResponse("processed:" + request.getTestParam() + " on " + nodeId);
                }

            },
            null,
            new ActionListener<TestResponse>() {
                @Override
                public void onResponse(TestResponse listTasksResponse) {
                    responseReference.set(listTasksResponse);
                    responseLatch.countDown();
                }

                @Override
                public void onFailure(Exception e) {
                    exceptionReference.set(e);
                    responseLatch.countDown();
                }
            });

        // Wait for the action to start
        actionStartedLatch.await();

        // Shut down coordinating node
        removeNode(0, executingNode, testNodes);

        // Perform action
        blockAction.countDown();

        responseLatch.await();

        if (shouldFail) {
            assertNull(responseReference.get());
            assertNotNull(exceptionReference.get());
        } else {
            assertNotNull(responseReference.get());
            // Make sure that the work was processed on the master node
            assertEquals("processed:work on " + testNodes[0].getNodeId(), responseReference.get().response);
            assertNull(exceptionReference.get());
        }

        assertTrue(triedToExecuteOnNonMaster.get());

        // Make sure there is no leftover junk left in the cluster state and task manager
        assertCleanClusterState(0);
        assertNoLeftOverTasks();
    }

    public void testMasterRestartDuringNotification() throws Exception {
        setupTestNodes(Settings.EMPTY);
        connectNodes(testNodes);
        startClusterStatePublishing(0, testNodes);

        int executingNode = randomIntBetween(1, testNodes.length - 1);
        int coordinatingNode = randomIntBetween(1, testNodes.length - 1);
        int newMaster = randomIntBetween(1, testNodes.length - 1);
        boolean shouldFail = randomBoolean();

        logger.info("Executing node: {}, coordinating node: {}, new master: {}, should fail: {}", executingNode, coordinatingNode,
            newMaster, shouldFail);

        CountDownLatch responseLatch = new CountDownLatch(1);
        final AtomicReference<TestResponse> responseReference = new AtomicReference<>();
        final AtomicReference<Exception> exceptionReference = new AtomicReference<>();

        CountDownLatch blockAction = new CountDownLatch(1);

        Task callerTask = startTestCockroachAction(coordinatingNode,
            new TestRequest(shouldFail ? "fail" : "work", testNodes[executingNode].getNodeId()),
            (transport, request) -> {
                String nodeId = transport.clusterService.localNode().getId();
                logger.info("action started on {}", nodeId);
                // Block on execution so we can restart master before notification is sent
                try {
                    assertTrue(blockAction.await(10, TimeUnit.SECONDS));
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                }
                if ("fail".equals(request.getTestParam())) {
                    throw new RuntimeException("Should fail");
                } else {
                    return new TestResponse("processed:" + request.getTestParam() + " on " + nodeId);
                }
            },
            null,
            new ActionListener<TestResponse>() {
                @Override
                public void onResponse(TestResponse listTasksResponse) {
                    responseReference.set(listTasksResponse);
                    responseLatch.countDown();
                }

                @Override
                public void onFailure(Exception e) {
                    exceptionReference.set(e);
                    responseLatch.countDown();
                }
            });

        // Wait for the action to be acknowledged on coordinating node
        assertBusy(() -> assertTrue(((CockroachActionResponseProcessor.Status) callerTask.getStatus()).isInitialized()));

        // Shut down master
        removeNode(0, 0, testNodes);

        // Perform action and try to send notification - that should fail because we don't have a master
        blockAction.countDown();

        // Find the task id that runs on executing node
        long taskId = findTask(executingNode, task -> task.getAction().equals(TransportTestCockroachAction.NAME + "[c]"));
        assertNotEquals(-1L, taskId);

        // wait for the notification to fail
        assertBusy(() -> {
            TaskManager taskManager = testNodes[executingNode].transportService.getTaskManager();
            Task task = taskManager.getTask(taskId);
            assertNotNull(task);
            assertNotNull(task.getStatus());
            assertEquals(CockroachActionExecutor.State.FAILED_NOTIFICATION, ((CockroachActionExecutor.Status) task.getStatus()).getState());
        });

        electMaster(newMaster, testNodes);

        assertTrue(responseLatch.await(10, TimeUnit.SECONDS));

        if (shouldFail) {
            assertNull(responseReference.get());
            assertNotNull(exceptionReference.get());
        } else {
            assertNotNull(responseReference.get());
            // Make sure that the work was processed on the original executing node
            assertEquals("processed:work on " + testNodes[executingNode].getNodeId(), responseReference.get().response);
            assertNull(exceptionReference.get());
        }

        // Make sure there is no leftover junk left in the cluster state and task manager
        assertCleanClusterState(newMaster);
        assertNoLeftOverTasks();
    }


    public void testClusterStateSerialization() throws Exception {
        setupTestNodes(Settings.EMPTY);
        connectNodes(testNodes);
        startClusterStatePublishing(0, testNodes);
        int numberOfRequests = randomIntBetween(1, 4); // Cannot have more than 4 requests since they are blocking GENERIC thread pool
        CountDownLatch actionStartedLatch = new CountDownLatch(numberOfRequests);
        CountDownLatch responseLatch = new CountDownLatch(numberOfRequests);
        CountDownLatch acknowledgementLatch = new CountDownLatch(1);
        CountDownLatch finishResponseLatch = new CountDownLatch(numberOfRequests);

        CountDownLatch blockAction = new CountDownLatch(1);

        TransportTestCockroachAction[] actions = registerTestCockroachAction(
            (transport, request) -> {
                actionStartedLatch.countDown();
                try {
                    assertTrue(blockAction.await(10, TimeUnit.SECONDS));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                if ("fail".equals(request.getTestParam())) {
                    throw new RuntimeException("Should fail");
                } else {
                    return new TestResponse("processed:" + request.getTestParam());
                }
            },
            (transport, response) -> {
                responseLatch.countDown();
                try {
                    assertTrue(acknowledgementLatch.await(10, TimeUnit.SECONDS));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                return response;
            });

        for (int i = 0; i < numberOfRequests; i++) {
            int coordinatingNode = randomInt(testNodes.length - 1);
            int executingNode = randomInt(testNodes.length - 1);
            logger.trace("Coordinating node: {}, executing node: {}", coordinatingNode, executingNode);
            TestRequest testRequest = new TestRequest(randomBoolean() || true ? "work" : "fail", testNodes[executingNode].getNodeId());
            actions[coordinatingNode].execute(testRequest,
                new ActionListener<TestResponse>() {
                    @Override
                    public void onResponse(TestResponse listTasksResponse) {
                        finishResponseLatch.countDown();
                    }

                    @Override
                    public void onFailure(Exception e) {
                        finishResponseLatch.countDown();
                    }
                });
        }


        // Wait for the action to start
        assertTrue(actionStartedLatch.await(10, TimeUnit.SECONDS));

        // Check the cluster state is correctly serialized
        Map<String, Object> stateMap = xContentToMap(testNodes[0].clusterService.state());
        Map<String, Object> tasksMap = (Map<String, Object>) stateMap.get("cockroach_tasks");
        List<Object> tasksList = (List<Object>) tasksMap.get("running_tasks");
        assertEquals(numberOfRequests, tasksList.size());
        for (Object task : tasksList) {
            Map<String, Object> taskMap = (Map<String, Object>) task;
            assertEquals(TransportTestCockroachAction.NAME, taskMap.get("action"));
            Map<String, Object> requestMap = (Map<String, Object>) taskMap.get("request");
            assertThat(requestMap.get("param").toString(), anyOf(equalTo("work"), equalTo("fail")));
            assertNull(taskMap.get("response"));
            assertNull(taskMap.get("failure"));
            assertThat(taskMap.get("caller_task_id").toString(), startsWith(taskMap.get("caller_node") + ":"));
            assertThat(taskMap.get("executor_node").toString(), startsWith("node"));
        }


        // Perform action and wait for action to finish but block in response so we would have response in the cluster state
        blockAction.countDown();
        assertTrue(responseLatch.await(10, TimeUnit.SECONDS));

        stateMap = xContentToMap(testNodes[0].clusterService.state());
        tasksMap = (Map<String, Object>) stateMap.get("cockroach_tasks");
        tasksList = (List<Object>) tasksMap.get("running_tasks");
        assertEquals(numberOfRequests, tasksList.size());
        for (Object task : tasksList) {
            Map<String, Object> taskMap = (Map<String, Object>) task;
            Map<String, Object> requestMap = (Map<String, Object>) taskMap.get("request");
            if ("work".equals(requestMap.get("param"))) {
                // Should have response
                Map<String, Object> responseMap = (Map<String, Object>) taskMap.get("response");
                assertThat(responseMap.get("text").toString(), startsWith("processed:work"));
                assertNull(taskMap.get("failure"));
            } else {
                // Should have failure
                assertNull(taskMap.get("response"));
                assertNotNull(taskMap.get("failure"));
            }
        }

        // Verify that we can list cockroach tasks
        ListTasksRequest listTasksRequest = new ListTasksRequest();
        listTasksRequest.setActions(TransportTestCockroachAction.NAME + "*");
        listTasksRequest.setDetailed(true);
        int randomNode = randomIntBetween(0, testNodes.length - 1);
        ListTasksResponse response = testNodes[randomNode].transportListTasksAction.execute(listTasksRequest).get();
        response.setDiscoveryNodes(testNodes[randomNode].clusterService.state().getNodes());
        // Make sure we can serialize it
        xContentToMap(response);
        assertEquals(numberOfRequests * 2, response.getTasks().size());
        for (TaskInfo taskInfo : response.getTasks()) {
            if (taskInfo.getAction().equals(TransportTestCockroachAction.NAME)) {
                CockroachActionResponseProcessor.Status status = (CockroachActionResponseProcessor.Status) taskInfo.getStatus();
                assertTrue(status.isInitialized());
            } else if (taskInfo.getAction().equals(TransportTestCockroachAction.NAME + "[c]")) {
                CockroachActionExecutor.Status status = (CockroachActionExecutor.Status) taskInfo.getStatus();
                // The status is typically NOTIFIED, but it can be "DONE" as well if a node didn't process the latest
                // cluster state update yet
                assertThat(status.getState().toString(), anyOf(equalTo("NOTIFIED"), equalTo("DONE")));
            } else {
                fail("Unexpected action " + taskInfo.getAction());
            }
        }

        acknowledgementLatch.countDown();
        assertTrue(finishResponseLatch.await(10, TimeUnit.SECONDS));

        // Make sure there is no leftover junk left in the cluster state and task manager
        assertCleanClusterState(0);
        assertNoLeftOverTasks();
    }

    private static Map<String, Object> xContentToMap(ToXContent content) throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder();
        if (randomBoolean()) {
            builder.humanReadable(true);
        }
        builder.startObject();
        content.toXContent(builder, EMPTY_PARAMS);
        builder.endObject();
        return XContentHelper.convertToMap(builder.bytes(), true).v2();
    }

    private long findTask(int node, Function<Task, Boolean> op) {
        TaskManager taskManager = testNodes[node].transportService.getTaskManager();
        for (Map.Entry<Long, Task> entry : taskManager.getTasks().entrySet()) {
            if (op.apply(entry.getValue())) {
                return entry.getKey();
            }
        }
        return -1L;
    }

    private TransportTestCockroachAction[] registerTestCockroachAction(
        BiFunction<TransportTestCockroachAction, TestRequest, TestResponse> nodeOperation,
        BiFunction<TransportTestCockroachAction, TestResponse, TestResponse> callerOperation) throws InterruptedException {
        TransportTestCockroachAction[] actions = new TransportTestCockroachAction[testNodes.length];
        for (int i = 0; i < testNodes.length; i++) {
            actions[i] = new TransportTestCockroachAction(Settings.EMPTY, threadPool, testNodes[i].transportService,
                testNodes[i].clusterService, testNodes[i].cockroachService, testNodes[i].actionFilters,
                testNodes[i].indexNameExpressionResolver, nodeOperation, callerOperation);
        }
        return actions;
    }


    private Task startTestCockroachAction(int coordinatorNode, TestRequest request,
                                          BiFunction<TransportTestCockroachAction, TestRequest, TestResponse> nodeOperation,
                                          BiFunction<TransportTestCockroachAction, TestResponse, TestResponse> callerOperation,
                                          ActionListener<TestResponse> listener) throws InterruptedException {
        TransportTestCockroachAction[] actions = registerTestCockroachAction(nodeOperation, callerOperation);
        return actions[coordinatorNode].execute(request, listener);
    }

}
