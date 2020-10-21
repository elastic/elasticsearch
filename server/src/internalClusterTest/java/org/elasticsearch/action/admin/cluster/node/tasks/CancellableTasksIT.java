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

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.LatchedActionListener;
import org.elasticsearch.action.admin.cluster.node.tasks.cancel.CancelTasksResponse;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.GroupedActionListener;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskInfo;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.transport.TransportService;
import org.junit.Before;

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
import java.util.stream.StreamSupport;

import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;

public class CancellableTasksIT extends ESIntegTestCase {

    static int idGenerator = 0;
    static final Map<TestRequest, CountDownLatch> beforeSendLatches = ConcurrentCollections.newConcurrentMap();
    static final Map<TestRequest, CountDownLatch> arrivedLatches = ConcurrentCollections.newConcurrentMap();
    static final Map<TestRequest, CountDownLatch> beforeExecuteLatches = ConcurrentCollections.newConcurrentMap();
    static final Map<TestRequest, CountDownLatch> completedLatches = ConcurrentCollections.newConcurrentMap();

    @Before
    public void resetTestStates() {
        idGenerator = 0;
        beforeSendLatches.clear();
        arrivedLatches.clear();
        beforeExecuteLatches.clear();
        completedLatches.clear();
    }

    static TestRequest generateTestRequest(Set<DiscoveryNode> nodes, int level, int maxLevel) {
        List<TestRequest> subRequests = new ArrayList<>();
        int lower = level == 0 ? 1 : 0;
        int upper = 10 / (level + 1);
        int numOfSubRequests = randomIntBetween(lower, upper);
        for (int i = 0; i < numOfSubRequests && level <= maxLevel; i++) {
            subRequests.add(generateTestRequest(nodes, level + 1, maxLevel));
        }
        final TestRequest request = new TestRequest(idGenerator++, randomFrom(nodes), subRequests);
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
            arrivedLatches.get(req).await();
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
            completedLatches.get(req).await();
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

    void ensureAllBansRemoved() throws Exception {
        assertBusy(() -> {
            for (String node : internalCluster().getNodeNames()) {
                TaskManager taskManager = internalCluster().getInstance(TransportService.class, node).getTaskManager();
                assertThat("node " + node, taskManager.getBannedTaskIds(), empty());
            }
        }, 30, TimeUnit.SECONDS);
    }

    public void testBanOnlyNodesWithOutstandingDescendantTasks() throws Exception {
        if (randomBoolean()) {
            internalCluster().startNodes(randomIntBetween(1, 3));
        }
        Set<DiscoveryNode> nodes = StreamSupport.stream(clusterService().state().nodes().spliterator(), false).collect(Collectors.toSet());
        final TestRequest rootRequest = generateTestRequest(nodes, 0, between(1, 4));
        ActionFuture<TestResponse> rootTaskFuture = client().execute(TransportTestAction.ACTION, rootRequest);
        Set<TestRequest> pendingRequests = allowPartialRequest(rootRequest);
        TaskId rootTaskId = getRootTaskId(rootRequest);
        ActionFuture<CancelTasksResponse> cancelFuture = client().admin().cluster().prepareCancelTasks()
            .setTaskId(rootTaskId).waitForCompletion(true).execute();
        if (randomBoolean()) {
            List<TaskInfo> runningTasks = client().admin().cluster().prepareListTasks()
                .setActions(TransportTestAction.ACTION.name()).setDetailed(true).get().getTasks();
            for (TaskInfo subTask : randomSubsetOf(runningTasks)) {
                client().admin().cluster().prepareCancelTasks().setTaskId(subTask.getTaskId()).waitForCompletion(false).get();
            }
        }
        assertBusy(() -> {
            for (DiscoveryNode node : nodes) {
                TaskManager taskManager = internalCluster().getInstance(TransportService.class, node.getName()).getTaskManager();
                Set<TaskId> expectedBans = new HashSet<>();
                for (TestRequest req : pendingRequests) {
                    if (req.node.equals(node)) {
                        List<Task> childTasks = taskManager.getTasks().values().stream()
                            .filter(t -> t.getParentTaskId() != null && t.getDescription().equals(req.taskDescription()))
                            .collect(Collectors.toList());
                        assertThat(childTasks, hasSize(1));
                        CancellableTask childTask = (CancellableTask) childTasks.get(0);
                        assertTrue(childTask.isCancelled());
                        expectedBans.add(childTask.getParentTaskId());
                    }
                }
                assertThat(taskManager.getBannedTaskIds(), equalTo(expectedBans));
            }
        }, 30, TimeUnit.SECONDS);
        allowEntireRequest(rootRequest);
        cancelFuture.actionGet();
        waitForRootTask(rootTaskFuture);
        ensureAllBansRemoved();
    }

    public void testCancelTaskMultipleTimes() throws Exception {
        Set<DiscoveryNode> nodes = StreamSupport.stream(clusterService().state().nodes().spliterator(), false).collect(Collectors.toSet());
        TestRequest rootRequest = generateTestRequest(nodes, 0, randomIntBetween(1, 3));
        ActionFuture<TestResponse> mainTaskFuture = client().execute(TransportTestAction.ACTION, rootRequest);
        TaskId taskId = getRootTaskId(rootRequest);
        allowPartialRequest(rootRequest);
        CancelTasksResponse resp = client().admin().cluster().prepareCancelTasks().setTaskId(taskId).waitForCompletion(false).get();
        assertThat(resp.getTaskFailures(), empty());
        assertThat(resp.getNodeFailures(), empty());
        ActionFuture<CancelTasksResponse> cancelFuture = client().admin().cluster().prepareCancelTasks().setTaskId(taskId)
            .waitForCompletion(true).execute();
        assertFalse(cancelFuture.isDone());
        allowEntireRequest(rootRequest);
        assertThat(cancelFuture.actionGet().getTaskFailures(), empty());
        assertThat(cancelFuture.actionGet().getTaskFailures(), empty());
        waitForRootTask(mainTaskFuture);
        CancelTasksResponse cancelError = client().admin().cluster().prepareCancelTasks()
            .setTaskId(taskId).waitForCompletion(randomBoolean()).get();
        assertThat(cancelError.getNodeFailures(), hasSize(1));
        final Throwable notFound = ExceptionsHelper.unwrap(cancelError.getNodeFailures().get(0), ResourceNotFoundException.class);
        assertThat(notFound.getMessage(), equalTo("task [" + taskId + "] is not found"));
        ensureAllBansRemoved();
    }

    public void testDoNotWaitForCompletion() throws Exception {
        Set<DiscoveryNode> nodes = StreamSupport.stream(clusterService().state().nodes().spliterator(), false).collect(Collectors.toSet());
        TestRequest rootRequest = generateTestRequest(nodes, 0, between(1, 3));
        ActionFuture<TestResponse> mainTaskFuture = client().execute(TransportTestAction.ACTION, rootRequest);
        TaskId taskId = getRootTaskId(rootRequest);
        if (randomBoolean()) {
            allowPartialRequest(rootRequest);
        }
        boolean waitForCompletion = randomBoolean();
        ActionFuture<CancelTasksResponse> cancelFuture = client().admin().cluster().prepareCancelTasks().setTaskId(taskId)
            .waitForCompletion(waitForCompletion).execute();
        if (waitForCompletion) {
            assertFalse(cancelFuture.isDone());
        } else {
            assertBusy(() -> assertTrue(cancelFuture.isDone()));
        }
        allowEntireRequest(rootRequest);
        waitForRootTask(mainTaskFuture);
        cancelFuture.actionGet();
        ensureAllBansRemoved();
    }

    public void testFailedToStartChildTaskAfterCancelled() throws Exception {
        Set<DiscoveryNode> nodes = StreamSupport.stream(clusterService().state().nodes().spliterator(), false).collect(Collectors.toSet());
        TestRequest rootRequest = generateTestRequest(nodes, 0, between(1, 3));
        ActionFuture<TestResponse> rootTaskFuture = client().execute(TransportTestAction.ACTION, rootRequest);
        TaskId taskId = getRootTaskId(rootRequest);
        client().admin().cluster().prepareCancelTasks().setTaskId(taskId).waitForCompletion(false).get();
        DiscoveryNode nodeWithParentTask = nodes.stream().filter(n -> n.getId().equals(taskId.getNodeId())).findFirst().get();
        TransportTestAction mainAction = internalCluster().getInstance(TransportTestAction.class, nodeWithParentTask.getName());
        PlainActionFuture<TestResponse> future = new PlainActionFuture<>();
        TestRequest subRequest = generateTestRequest(nodes, 0, between(0, 1));
        beforeSendLatches.get(subRequest).countDown();
        mainAction.startSubTask(taskId, subRequest, future);
        TransportException te = expectThrows(TransportException.class, future::actionGet);
        assertThat(te.getCause(), instanceOf(TaskCancelledException.class));
        assertThat(te.getCause().getMessage(), equalTo("The parent task was cancelled, shouldn't start any child tasks"));
        allowEntireRequest(rootRequest);
        waitForRootTask(rootTaskFuture);
        ensureAllBansRemoved();
    }

    public void testCancelOrphanedTasks() throws Exception {
        final String nodeWithRootTask = internalCluster().startDataOnlyNode();
        Set<DiscoveryNode> nodes = StreamSupport.stream(clusterService().state().nodes().spliterator(), false).collect(Collectors.toSet());
        TestRequest rootRequest = generateTestRequest(nodes, 0, between(1, 3));
        client(nodeWithRootTask).execute(TransportTestAction.ACTION, rootRequest);
        allowPartialRequest(rootRequest);
        try {
            internalCluster().stopRandomNode(InternalTestCluster.nameFilter(nodeWithRootTask));
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
            ensureAllBansRemoved();
        }
    }

    static TaskId getRootTaskId(TestRequest request) throws Exception {
        SetOnce<TaskId> taskId = new SetOnce<>();
        assertBusy(() -> {
            ListTasksResponse listTasksResponse = client().admin().cluster().prepareListTasks()
                .setActions(TransportTestAction.ACTION.name()).setDetailed(true).get();
            List<TaskInfo> tasks = listTasksResponse.getTasks().stream()
                .filter(t -> t.getDescription().equals(request.taskDescription()))
                .collect(Collectors.toList());
            assertThat(tasks, hasSize(1));
            taskId.set(tasks.get(0).getTaskId());
        });
        return taskId.get();
    }

    static void waitForRootTask(ActionFuture<TestResponse> rootTask) {
        try {
            rootTask.actionGet();
        } catch (Exception e) {
            final Throwable cause = ExceptionsHelper.unwrap(e, TaskCancelledException.class);
            assertThat(cause.getMessage(), anyOf(
                equalTo("The parent task was cancelled, shouldn't start any child tasks"),
                containsString("Task cancelled before it started:"),
                equalTo("Task was cancelled while executing")));
        }
    }

    static class TestRequest extends ActionRequest {
        final int id;
        final DiscoveryNode node;
        final List<TestRequest> subRequests;

        TestRequest(int id, DiscoveryNode node, List<TestRequest> subRequests) {
            this.id = id;
            this.node = node;
            this.subRequests = subRequests;
        }

        TestRequest(StreamInput in) throws IOException {
            super(in);
            this.id = in.readInt();
            this.node = new DiscoveryNode(in);
            this.subRequests = in.readList(TestRequest::new);
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
            out.writeList(subRequests);
        }

        @Override
        public String getDescription() {
            return taskDescription();
        }

        String taskDescription() {
            return "id=" + id;
        }

        @Override
        public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
            return new CancellableTask(id, type, action, taskDescription(), parentTaskId, headers) {
                @Override
                public boolean shouldCancelChildrenOnCancellation() {
                    return true;
                }
            };
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

        public TestResponse(StreamInput in) throws IOException {
            super(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {

        }
    }

    public static class TransportTestAction extends HandledTransportAction<TestRequest, TestResponse> {

        public static ActionType<TestResponse> ACTION = new ActionType<>("internal::test_action", TestResponse::new);
        private final TransportService transportService;
        private final NodeClient client;

        @Inject
        public TransportTestAction(TransportService transportService, NodeClient client, ActionFilters actionFilters) {
            super(ACTION.name(), transportService, actionFilters, TestRequest::new, ThreadPool.Names.GENERIC);
            this.transportService = transportService;
            this.client = client;
        }

        @Override
        protected void doExecute(Task task, TestRequest request, ActionListener<TestResponse> listener) {
            arrivedLatches.get(request).countDown();
            List<TestRequest> subRequests = request.subRequests;
            GroupedActionListener<TestResponse> groupedListener =
                new GroupedActionListener<>(ActionListener.map(listener, r -> new TestResponse()), subRequests.size() + 1);
            transportService.getThreadPool().generic().execute(ActionRunnable.supply(groupedListener, () -> {
                beforeExecuteLatches.get(request).await();
                if (((CancellableTask) task).isCancelled()) {
                    throw new TaskCancelledException("Task was cancelled while executing");
                }
                return new TestResponse();
            }));
            for (TestRequest subRequest : subRequests) {
                TaskId parentTaskId = new TaskId(client.getLocalNodeId(), task.getId());
                startSubTask(parentTaskId, subRequest, groupedListener);
            }
        }

        protected void startSubTask(TaskId parentTaskId, TestRequest subRequest, ActionListener<TestResponse> listener) {
            subRequest.setParentTask(parentTaskId);
            CountDownLatch completeLatch = completedLatches.get(subRequest);
            LatchedActionListener<TestResponse> latchedListener = new LatchedActionListener<>(listener, completeLatch);
            transportService.getThreadPool().generic().execute(new AbstractRunnable() {
                @Override
                public void onFailure(Exception e) {
                    latchedListener.onFailure(e);
                }

                @Override
                protected void doRun() throws Exception {
                    beforeSendLatches.get(subRequest).await();
                    if (client.getLocalNodeId().equals(subRequest.node.getId()) && randomBoolean()) {
                        try {
                            client.executeLocally(TransportTestAction.ACTION, subRequest, latchedListener);
                        } catch (TaskCancelledException e) {
                            latchedListener.onFailure(new TransportException(e));
                        }
                    } else {
                        transportService.sendRequest(subRequest.node, ACTION.name(), subRequest,
                            new TransportResponseHandler<TestResponse>() {
                                @Override
                                public void handleResponse(TestResponse response) {
                                    latchedListener.onResponse(response);
                                }

                                @Override
                                public void handleException(TransportException exp) {
                                    latchedListener.onFailure(exp);
                                }

                                @Override
                                public String executor() {
                                    return ThreadPool.Names.SAME;
                                }

                                @Override
                                public TestResponse read(StreamInput in) throws IOException {
                                    return new TestResponse(in);
                                }
                            });
                    }
                }
            });
        }
    }


    public static class TaskPlugin extends Plugin implements ActionPlugin {
        @Override
        public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
            return Collections.singletonList(new ActionHandler<>(TransportTestAction.ACTION, TransportTestAction.class));
        }
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        final List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(TaskPlugin.class);
        return plugins;
    }
}
