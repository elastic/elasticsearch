/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.action.admin.cluster.node.tasks;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.DelegatingActionListener;
import org.elasticsearch.action.LatchedActionListener;
import org.elasticsearch.action.LegacyActionRequest;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.GroupedActionListener;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskInfo;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.junit.annotations.TestIssueLogging;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.ReceiveTimeoutTransportException;
import org.elasticsearch.transport.SendRequestTransportException;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.transport.TransportService;
import org.junit.After;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.containsStringIgnoringCase;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST)
public class CancellableTasksIT extends ESIntegTestCase {

    // Temporary addition for investigation into https://github.com/elastic/elasticsearch/issues/123568
    private static final Logger logger = LogManager.getLogger(CancellableTasksIT.class);

    static int idGenerator = 0;
    static final Map<TestRequest, CountDownLatch> beforeSendLatches = ConcurrentCollections.newConcurrentMap();
    static final Map<TestRequest, CountDownLatch> arrivedLatches = ConcurrentCollections.newConcurrentMap();
    static final Map<TestRequest, CountDownLatch> beforeExecuteLatches = ConcurrentCollections.newConcurrentMap();
    static final Map<TestRequest, CountDownLatch> completedLatches = ConcurrentCollections.newConcurrentMap();

    @After
    public void ensureBansAndCancellationsConsistency() throws Exception {
        assertBusy(() -> {
            for (String node : internalCluster().getNodeNames()) {
                TaskManager taskManager = internalCluster().getInstance(TransportService.class, node).getTaskManager();
                assertThat("node " + node, taskManager.getBannedTaskIds(), empty());
                assertThat("node " + node, taskManager.assertCancellableTaskConsistency(), equalTo(true));
            }
        }, 30, TimeUnit.SECONDS);
    }

    static TestRequest generateTestRequest(Set<DiscoveryNode> nodes, int level, int maxLevel, boolean timeout) {
        List<TestRequest> subRequests = new ArrayList<>();
        int lower = level == 0 ? 1 : 0;
        int upper = 10 / (level + 1);
        int numOfSubRequests = randomIntBetween(lower, upper);
        for (int i = 0; i < numOfSubRequests && level <= maxLevel; i++) {
            subRequests.add(generateTestRequest(nodes, level + 1, maxLevel, timeout));
        }
        final TestRequest request = new TestRequest(idGenerator++, randomFrom(nodes), subRequests, level == 0 ? false : timeout);
        beforeSendLatches.put(request, new CountDownLatch(1));
        arrivedLatches.put(request, new CountDownLatch(1));
        beforeExecuteLatches.put(request, new CountDownLatch(1));
        completedLatches.put(request, new CountDownLatch(1));
        return request;
    }

    static void randomDescendants(TestRequest request, Set<TestRequest> result) {
        if (randomBoolean()) {
            result.add(request);
            for (TestRequest subReq : request.subRequests) {
                randomDescendants(subReq, result);
            }
        }
    }

    /**
     * Allow some parts of the request to be completed
     *
     * @return a pending child requests
     */
    static Set<TestRequest> allowPartialRequest(TestRequest request) throws Exception {
        final Set<TestRequest> sentRequests = new HashSet<>();
        while (sentRequests.isEmpty()) {
            for (TestRequest subRequest : request.subRequests) {
                randomDescendants(subRequest, sentRequests);
            }
        }
        for (TestRequest req : sentRequests) {
            beforeSendLatches.get(req).countDown();
        }
        for (TestRequest req : sentRequests) {
            assertTrue(arrivedLatches.get(req).await(60, TimeUnit.SECONDS));
        }
        Set<TestRequest> completedRequests = new HashSet<>();
        for (TestRequest req : randomSubsetOf(sentRequests)) {
            if (sentRequests.containsAll(req.descendants())) {
                completedRequests.add(req);
                completedRequests.addAll(req.descendants());
            }
        }
        for (TestRequest req : completedRequests) {
            beforeExecuteLatches.get(req).countDown();
        }
        for (TestRequest req : completedRequests) {
            assertTrue(completedLatches.get(req).await(60, TimeUnit.SECONDS));
        }
        return Sets.difference(sentRequests, completedRequests);
    }

    static void allowEntireRequest(TestRequest request) {
        beforeSendLatches.get(request).countDown();
        beforeExecuteLatches.get(request).countDown();
        for (TestRequest subReq : request.subRequests) {
            allowEntireRequest(subReq);
        }
    }

    public void testBanOnlyNodesWithOutstandingDescendantTasks() throws Exception {
        if (randomBoolean()) {
            internalCluster().startNodes(randomIntBetween(1, 3));
        }
        Set<DiscoveryNode> nodes = clusterService().state().nodes().stream().collect(Collectors.toSet());
        final TestRequest rootRequest = generateTestRequest(nodes, 0, between(1, 4), false);
        ActionFuture<TestResponse> rootTaskFuture = client().execute(TransportTestAction.ACTION, rootRequest);
        Set<TestRequest> pendingRequests = allowPartialRequest(rootRequest);
        TaskId rootTaskId = getRootTaskId(rootRequest);
        ActionFuture<ListTasksResponse> cancelFuture = clusterAdmin().prepareCancelTasks()
            .setTargetTaskId(rootTaskId)
            .waitForCompletion(true)
            .execute();
        if (randomBoolean()) {
            List<TaskInfo> runningTasks = clusterAdmin().prepareListTasks()
                .setActions(TransportTestAction.ACTION.name())
                .setDetailed(true)
                .get()
                .getTasks();
            for (TaskInfo subTask : randomSubsetOf(runningTasks)) {
                clusterAdmin().prepareCancelTasks().setTargetTaskId(subTask.taskId()).waitForCompletion(false).get();
            }
        }
        try {
            assertBusy(() -> {
                for (DiscoveryNode node : nodes) {
                    TaskManager taskManager = internalCluster().getInstance(TransportService.class, node.getName()).getTaskManager();
                    Set<TaskId> expectedBans = new HashSet<>();
                    for (TestRequest req : pendingRequests) {
                        if (req.node.equals(node)) {
                            List<Task> childTasks = taskManager.getTasks()
                                .values()
                                .stream()
                                .filter(t -> t.getParentTaskId() != null && t.getDescription().equals(req.taskDescription()))
                                .toList();
                            assertThat(childTasks, hasSize(1));
                            CancellableTask childTask = (CancellableTask) childTasks.get(0);
                            assertTrue(childTask.isCancelled());
                            expectedBans.add(childTask.getParentTaskId());
                        }
                    }
                    assertThat(taskManager.getBannedTaskIds(), equalTo(expectedBans));
                }
            }, 30, TimeUnit.SECONDS);
        } finally {
            allowEntireRequest(rootRequest);
            cancelFuture.actionGet();
            waitForRootTask(rootTaskFuture, false);
            ensureBansAndCancellationsConsistency();
        }
    }

    public void testCancelTaskMultipleTimes() throws Exception {
        Set<DiscoveryNode> nodes = clusterService().state().nodes().stream().collect(Collectors.toSet());
        TestRequest rootRequest = generateTestRequest(nodes, 0, randomIntBetween(1, 3), false);
        ActionFuture<TestResponse> mainTaskFuture = client().execute(TransportTestAction.ACTION, rootRequest);
        TaskId taskId = getRootTaskId(rootRequest);
        allowPartialRequest(rootRequest);
        ListTasksResponse resp = clusterAdmin().prepareCancelTasks().setTargetTaskId(taskId).waitForCompletion(false).get();
        assertThat(resp.getTaskFailures(), empty());
        assertThat(resp.getNodeFailures(), empty());
        ActionFuture<ListTasksResponse> cancelFuture = clusterAdmin().prepareCancelTasks()
            .setTargetTaskId(taskId)
            .waitForCompletion(true)
            .execute();
        assertFalse(cancelFuture.isDone());
        allowEntireRequest(rootRequest);
        assertThat(cancelFuture.actionGet().getTaskFailures(), empty());
        waitForRootTask(mainTaskFuture, false);
        ListTasksResponse cancelError = clusterAdmin().prepareCancelTasks()
            .setTargetTaskId(taskId)
            .waitForCompletion(randomBoolean())
            .get();
        assertThat(cancelError.getNodeFailures(), hasSize(1));
        final Throwable notFound = ExceptionsHelper.unwrap(cancelError.getNodeFailures().get(0), ResourceNotFoundException.class);
        assertThat(notFound.getMessage(), equalTo("task [" + taskId + "] is not found"));
        ensureBansAndCancellationsConsistency();
    }

    public void testDoNotWaitForCompletion() throws Exception {
        Set<DiscoveryNode> nodes = clusterService().state().nodes().stream().collect(Collectors.toSet());
        TestRequest rootRequest = generateTestRequest(nodes, 0, between(1, 3), false);
        ActionFuture<TestResponse> mainTaskFuture = client().execute(TransportTestAction.ACTION, rootRequest);
        TaskId taskId = getRootTaskId(rootRequest);
        if (randomBoolean()) {
            allowPartialRequest(rootRequest);
        }
        boolean waitForCompletion = randomBoolean();
        ActionFuture<ListTasksResponse> cancelFuture = clusterAdmin().prepareCancelTasks()
            .setTargetTaskId(taskId)
            .waitForCompletion(waitForCompletion)
            .execute();
        if (waitForCompletion) {
            assertFalse(cancelFuture.isDone());
        } else {
            cancelFuture.get();
        }
        allowEntireRequest(rootRequest);
        waitForRootTask(mainTaskFuture, false);
        if (waitForCompletion) {
            cancelFuture.actionGet();
        }
        ensureBansAndCancellationsConsistency();
    }

    public void testFailedToStartChildTaskAfterCancelled() throws Exception {
        Set<DiscoveryNode> nodes = clusterService().state().nodes().stream().collect(Collectors.toSet());
        TestRequest rootRequest = generateTestRequest(nodes, 0, between(1, 3), false);
        ActionFuture<TestResponse> rootTaskFuture = client().execute(TransportTestAction.ACTION, rootRequest);
        TaskId taskId = getRootTaskId(rootRequest);
        clusterAdmin().prepareCancelTasks().setTargetTaskId(taskId).waitForCompletion(false).get();
        DiscoveryNode nodeWithParentTask = nodes.stream().filter(n -> n.getId().equals(taskId.getNodeId())).findFirst().get();
        TransportTestAction mainAction = internalCluster().getInstance(TransportTestAction.class, nodeWithParentTask.getName());
        PlainActionFuture<TestResponse> future = new PlainActionFuture<>();
        TestRequest subRequest = generateTestRequest(nodes, 0, between(0, 1), false);
        beforeSendLatches.get(subRequest).countDown();
        mainAction.startSubTask(taskId, subRequest, future);
        TaskCancelledException te = expectThrows(TaskCancelledException.class, future);
        assertThat(te.getMessage(), equalTo("parent task was cancelled [by user request]"));
        allowEntireRequest(rootRequest);
        waitForRootTask(rootTaskFuture, false);
        ensureBansAndCancellationsConsistency();
    }

    public void testCancelOrphanedTasks() throws Exception {
        final String nodeWithRootTask = internalCluster().startDataOnlyNode();
        Set<DiscoveryNode> nodes = clusterService().state().nodes().stream().collect(Collectors.toSet());
        TestRequest rootRequest = generateTestRequest(nodes, 0, between(1, 3), false);
        client(nodeWithRootTask).execute(TransportTestAction.ACTION, rootRequest);
        allowPartialRequest(rootRequest);
        try {
            internalCluster().stopNode(nodeWithRootTask);
            assertBusy(() -> {
                for (TransportService transportService : internalCluster().getInstances(TransportService.class)) {
                    for (CancellableTask task : transportService.getTaskManager().getCancellableTasks().values()) {
                        if (task.getAction().equals(TransportTestAction.ACTION.name())) {
                            final TaskInfo taskInfo = task.taskInfo(transportService.getLocalNode().getId(), false);
                            assertTrue(taskInfo.toString(), task.isCancelled());
                            assertNotNull(taskInfo.toString(), task.getReasonCancelled());
                            assertThat(taskInfo.toString(), task.getReasonCancelled(), equalTo("channel was closed"));
                        }
                    }
                }
            }, 30, TimeUnit.SECONDS);
        } finally {
            allowEntireRequest(rootRequest);
            ensureBansAndCancellationsConsistency();
        }
    }

    public void testRemoveBanParentsOnDisconnect() throws Exception {
        Set<DiscoveryNode> nodes = clusterService().state().nodes().stream().collect(Collectors.toSet());
        final TestRequest rootRequest = generateTestRequest(nodes, 0, between(1, 4), false);
        client().execute(TransportTestAction.ACTION, rootRequest);
        Set<TestRequest> pendingRequests = allowPartialRequest(rootRequest);
        TaskId rootTaskId = getRootTaskId(rootRequest);
        ActionFuture<ListTasksResponse> cancelFuture = clusterAdmin().prepareCancelTasks()
            .setTargetTaskId(rootTaskId)
            .waitForCompletion(true)
            .execute();
        try {
            assertBusy(() -> {
                for (DiscoveryNode node : nodes) {
                    TaskManager taskManager = internalCluster().getInstance(TransportService.class, node.getName()).getTaskManager();
                    Set<TaskId> expectedBans = new HashSet<>();
                    for (TestRequest req : pendingRequests) {
                        if (req.node.equals(node)) {
                            List<Task> childTasks = taskManager.getTasks()
                                .values()
                                .stream()
                                .filter(t -> t.getParentTaskId() != null && t.getDescription().equals(req.taskDescription()))
                                .toList();
                            assertThat(childTasks, hasSize(1));
                            CancellableTask childTask = (CancellableTask) childTasks.get(0);
                            assertTrue(childTask.isCancelled());
                            expectedBans.add(childTask.getParentTaskId());
                        }
                    }
                    assertThat(taskManager.getBannedTaskIds(), equalTo(expectedBans));
                }
            }, 30, TimeUnit.SECONDS);

            final Set<TaskId> bannedParents = new HashSet<>();
            for (DiscoveryNode node : nodes) {
                TaskManager taskManager = internalCluster().getInstance(TransportService.class, node.getName()).getTaskManager();
                bannedParents.addAll(taskManager.getBannedTaskIds());
            }
            // Disconnect some outstanding child connections
            for (DiscoveryNode node : nodes) {
                TaskManager taskManager = internalCluster().getInstance(TransportService.class, node.getName()).getTaskManager();
                for (TaskId bannedParent : bannedParents) {
                    if (bannedParent.getNodeId().equals(node.getId()) && randomBoolean()) {
                        Collection<Transport.Connection> childConns = taskManager.startBanOnChildTasks(bannedParent.getId(), "", () -> {});
                        for (Transport.Connection connection : randomSubsetOf(childConns)) {
                            if (connection.getNode().equals(node) == false) {
                                connection.close();
                            }
                        }
                    }
                }
            }
        } finally {
            allowEntireRequest(rootRequest);
            cancelFuture.actionGet();
            ensureBansAndCancellationsConsistency();
        }
    }

    @TestIssueLogging(
        issueUrl = "https://github.com/elastic/elasticsearch/issues/123568",
        value = "org.elasticsearch.transport.TransportService.tracer:TRACE"
            + ",org.elasticsearch.tasks.TaskManager:TRACE"
            + ",org.elasticsearch.action.admin.cluster.node.tasks.CancellableTasksIT:DEBUG"
    )
    public void testChildrenTasksCancelledOnTimeout() throws Exception {
        Set<DiscoveryNode> nodes = clusterService().state().nodes().stream().collect(Collectors.toSet());
        final TestRequest rootRequest = generateTestRequest(nodes, 0, between(1, 4), true);
        logger.info("generated request\n{}", buildTestRequestDescription(rootRequest, "", new StringBuilder()).toString());
        ActionFuture<TestResponse> rootTaskFuture = client().execute(TransportTestAction.ACTION, rootRequest);
        logger.info("action executed");
        allowEntireRequest(rootRequest);
        logger.info("execution released");
        waitForRootTask(rootTaskFuture, true);
        logger.info("root task completed");
        ensureBansAndCancellationsConsistency();
        logger.info("ensureBansAndCancellationsConsistency completed");

        // Make sure all descendent requests have completed
        for (TestRequest subRequest : rootRequest.descendants()) {
            logger.info("awaiting completion of request {}", subRequest.getDescription());
            safeAwait(completedLatches.get(subRequest));
        }
        logger.info("all requests completed");
    }

    // Temporary addition for investigation into https://github.com/elastic/elasticsearch/issues/123568
    static StringBuilder buildTestRequestDescription(TestRequest request, String prefix, StringBuilder stringBuilder) {
        stringBuilder.append(prefix)
            .append(Strings.format("id=%d [timeout=%s] %s", request.id, request.timeout, request.node.descriptionWithoutAttributes()))
            .append('\n');
        for (TestRequest subRequest : request.subRequests) {
            buildTestRequestDescription(subRequest, prefix + "  ", stringBuilder);
        }
        return stringBuilder;
    }

    static TaskId getRootTaskId(TestRequest request) throws Exception {
        SetOnce<TaskId> taskId = new SetOnce<>();
        assertBusy(() -> {
            ListTasksResponse listTasksResponse = clusterAdmin().prepareListTasks()
                .setActions(TransportTestAction.ACTION.name())
                .setDetailed(true)
                .get();
            List<TaskInfo> tasks = listTasksResponse.getTasks()
                .stream()
                .filter(t -> t.description().equals(request.taskDescription()))
                .toList();
            assertThat(tasks, hasSize(1));
            taskId.set(tasks.get(0).taskId());
        });
        return taskId.get();
    }

    static void waitForRootTask(ActionFuture<TestResponse> rootTask, boolean expectToTimeout) {
        try {
            rootTask.actionGet();
        } catch (Exception e) {
            final Throwable cause = ExceptionsHelper.unwrap(
                e,
                expectToTimeout ? ReceiveTimeoutTransportException.class : TaskCancelledException.class
            );
            assertNotNull(cause);
            assertThat(
                cause.getMessage(),
                expectToTimeout
                    ? containsStringIgnoringCase("timed out after")
                    : anyOf(
                        equalTo("parent task was cancelled [by user request]"),
                        equalTo("task cancelled before starting [by user request]"),
                        equalTo("task cancelled [by user request]")
                    )
            );
        }
    }

    static class TestRequest extends LegacyActionRequest {
        final int id;
        final DiscoveryNode node;
        final List<TestRequest> subRequests;
        final boolean timeout;

        TestRequest(int id, DiscoveryNode node, List<TestRequest> subRequests, boolean timeout) {
            this.id = id;
            this.node = node;
            this.subRequests = subRequests;
            this.timeout = timeout;
        }

        TestRequest(StreamInput in) throws IOException {
            super(in);
            this.id = in.readInt();
            this.node = new DiscoveryNode(in);
            this.subRequests = in.readCollectionAsList(TestRequest::new);
            this.timeout = in.readBoolean();
        }

        List<TestRequest> descendants() {
            List<TestRequest> descendants = new ArrayList<>();
            for (TestRequest subRequest : subRequests) {
                descendants.add(subRequest);
                descendants.addAll(subRequest.descendants());
            }
            return descendants;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeInt(id);
            node.writeTo(out);
            out.writeCollection(subRequests);
            out.writeBoolean(timeout);
        }

        @Override
        public String getDescription() {
            return taskDescription();
        }

        String taskDescription() {
            return "id=" + id;
        }

        @Override
        public Task createTask(long someId, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
            return new CancellableTask(someId, type, action, taskDescription(), parentTaskId, headers);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            TestRequest that = (TestRequest) o;
            return id == that.id;
        }

        @Override
        public int hashCode() {
            return Objects.hash(id);
        }
    }

    public static class TestResponse extends ActionResponse {
        public TestResponse() {

        }

        public TestResponse(StreamInput in) {}

        @Override
        public void writeTo(StreamOutput out) throws IOException {

        }
    }

    public static class TransportTestAction extends HandledTransportAction<TestRequest, TestResponse> {

        private static final Logger logger = CancellableTasksIT.logger;

        public static ActionType<TestResponse> ACTION = new ActionType<>("internal::test_action");
        private final TransportService transportService;
        private final NodeClient client;

        @Inject
        public TransportTestAction(TransportService transportService, NodeClient client, ActionFilters actionFilters) {
            super(
                ACTION.name(),
                transportService,
                actionFilters,
                TestRequest::new,
                transportService.getThreadPool().executor(ThreadPool.Names.GENERIC)
            );
            this.transportService = transportService;
            this.client = client;
        }

        private void schedule(Task task, TestRequest request, TimeValue delay, ActionListener<TestResponse> listener) {
            transportService.getThreadPool().schedule(new AbstractRunnable() {
                @Override
                public void onFailure(Exception e) {
                    listener.onFailure(e);
                }

                @Override
                protected void doRun() throws Exception {
                    assertTrue(beforeExecuteLatches.get(request).await(60, TimeUnit.SECONDS));
                    if (request.timeout) {
                        // Repeat work until cancelled
                        if (((CancellableTask) task).isCancelled() == false) {
                            schedule(task, request, TimeValue.timeValueMillis(10), listener);
                            return;
                        }
                    } else {
                        ((CancellableTask) task).ensureNotCancelled();
                    }
                    listener.onResponse(new TestResponse());
                }
            }, delay, transportService.getThreadPool().generic());
        }

        @Override
        protected void doExecute(Task task, TestRequest request, ActionListener<TestResponse> listener) {
            arrivedLatches.get(request).countDown();
            List<TestRequest> subRequests = request.subRequests;
            GroupedActionListener<TestResponse> groupedListener = new GroupedActionListener<>(
                subRequests.size() + 1,
                listener.map(r -> new TestResponse())
            );
            schedule(task, request, TimeValue.ZERO, groupedListener);
            for (TestRequest subRequest : subRequests) {
                TaskId parentTaskId = new TaskId(client.getLocalNodeId(), task.getId());
                startSubTask(parentTaskId, subRequest, groupedListener);
            }
        }

        protected void startSubTask(TaskId parentTaskId, TestRequest subRequest, ActionListener<TestResponse> listener) {
            subRequest.setParentTask(parentTaskId);
            CountDownLatch completeLatch = completedLatches.get(subRequest);
            ActionListener<TestResponse> latchedListener = new DelegatingActionListener<>(
                new LatchedActionListener<>(listener, completeLatch)
            ) {
                // Temporary logging addition for investigation into https://github.com/elastic/elasticsearch/issues/123568
                @Override
                public void onResponse(TestResponse testResponse) {
                    logger.debug("processing successful response to request [{}]", subRequest.getDescription());
                    delegate.onResponse(testResponse);
                }

                @Override
                public void onFailure(Exception e) {
                    logger.debug("processing exceptional response to request [{}]: {}", subRequest.getDescription(), e.getMessage());
                    super.onFailure(e);
                }
            };
            transportService.getThreadPool().generic().execute(new AbstractRunnable() {
                @Override
                public void onFailure(Exception e) {
                    latchedListener.onFailure(e);
                }

                @Override
                protected void doRun() throws Exception {
                    assertTrue(beforeSendLatches.get(subRequest).await(60, TimeUnit.SECONDS));
                    if (client.getLocalNodeId().equals(subRequest.node.getId()) && subRequest.timeout == false && randomBoolean()) {
                        try {
                            client.executeLocally(TransportTestAction.ACTION, subRequest, latchedListener);
                        } catch (TaskCancelledException e) {
                            latchedListener.onFailure(new SendRequestTransportException(subRequest.node, ACTION.name(), e));
                        }
                    } else {
                        final TransportRequestOptions transportRequestOptions = subRequest.timeout
                            ? TransportRequestOptions.timeout(TimeValue.timeValueMillis(400))
                            : TransportRequestOptions.EMPTY;
                        transportService.sendRequest(
                            subRequest.node,
                            ACTION.name(),
                            subRequest,
                            transportRequestOptions,
                            new ActionListenerResponseHandler<TestResponse>(
                                latchedListener,
                                TestResponse::new,
                                TransportResponseHandler.TRANSPORT_WORKER
                            )
                        );
                        // Temporary addition for investigation into https://github.com/elastic/elasticsearch/issues/123568
                        logger.debug(
                            "sent test request [{}] from [{}] to [{}]",
                            subRequest.getDescription(),
                            client.getLocalNodeId(),
                            subRequest.node.descriptionWithoutAttributes()
                        );
                    }
                }
            });
        }
    }

    public static class TaskPlugin extends Plugin implements ActionPlugin {
        @Override
        public List<ActionHandler> getActions() {
            return Collections.singletonList(new ActionHandler(TransportTestAction.ACTION, TransportTestAction.class));
        }
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), TaskPlugin.class);
    }
}
