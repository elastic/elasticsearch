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
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.transport.TransportService;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.either;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;

public class CancellableTasksIT extends ESIntegTestCase {
    static final Map<ChildRequest, CountDownLatch> arrivedLatches = ConcurrentCollections.newConcurrentMap();
    static final Map<ChildRequest, CountDownLatch> beforeExecuteLatches = ConcurrentCollections.newConcurrentMap();
    static final Map<ChildRequest, CountDownLatch> completedLatches = ConcurrentCollections.newConcurrentMap();

    @Before
    public void resetTestStates() {
        arrivedLatches.clear();
        beforeExecuteLatches.clear();
        completedLatches.clear();
    }

    List<ChildRequest> setupChildRequests(Set<DiscoveryNode> nodes) {
        int numRequests = randomIntBetween(1, 30);
        List<ChildRequest> childRequests = new ArrayList<>();
        for (int i = 0; i < numRequests; i++) {
            ChildRequest req = new ChildRequest(i, randomFrom(nodes));
            childRequests.add(req);
            arrivedLatches.put(req, new CountDownLatch(1));
            beforeExecuteLatches.put(req, new CountDownLatch(1));
            completedLatches.put(req, new CountDownLatch(1));
        }
        return childRequests;
    }

    public void testBanOnlyNodesWithOutstandingChildTasks() throws Exception {
        if (randomBoolean()) {
            internalCluster().startNodes(randomIntBetween(1, 3));
        }
        Set<DiscoveryNode> nodes = StreamSupport.stream(clusterService().state().nodes().spliterator(), false).collect(Collectors.toSet());
        List<ChildRequest> childRequests = setupChildRequests(nodes);
        ActionFuture<MainResponse> mainTaskFuture = client().execute(TransportMainAction.ACTION, new MainRequest(childRequests));
        List<ChildRequest> completedRequests = randomSubsetOf(between(0, childRequests.size() - 1), childRequests);
        for (ChildRequest req : completedRequests) {
            beforeExecuteLatches.get(req).countDown();
            completedLatches.get(req).await();
        }
        List<ChildRequest> outstandingRequests = childRequests.stream().
            filter(r -> completedRequests.contains(r) == false)
            .collect(Collectors.toList());
        for (ChildRequest req : outstandingRequests) {
            arrivedLatches.get(req).await();
        }
        TaskId taskId = getMainTaskId();
        ActionFuture<CancelTasksResponse> cancelFuture = client().admin().cluster().prepareCancelTasks().setTaskId(taskId)
            .waitForCompletion(true).execute();
        Set<DiscoveryNode> nodesWithOutstandingChildTask = outstandingRequests.stream().map(r -> r.targetNode).collect(Collectors.toSet());
        assertBusy(() -> {
            for (DiscoveryNode node : nodes) {
                TaskManager taskManager = internalCluster().getInstance(TransportService.class, node.getName()).getTaskManager();
                if (nodesWithOutstandingChildTask.contains(node)) {
                    assertThat(taskManager.getBanCount(), equalTo(1));
                } else {
                    assertThat(taskManager.getBanCount(), equalTo(0));
                }
            }
        });
        // failed to spawn child tasks after cancelling
        if (randomBoolean()) {
            DiscoveryNode nodeWithParentTask = nodes.stream().filter(n -> n.getId().equals(taskId.getNodeId())).findFirst().get();
            TransportMainAction mainAction = internalCluster().getInstance(TransportMainAction.class, nodeWithParentTask.getName());
            PlainActionFuture<ChildResponse> future = new PlainActionFuture<>();
            ChildRequest req = new ChildRequest(-1, randomFrom(nodes));
            completedLatches.put(req, new CountDownLatch(1));
            mainAction.startChildTask(taskId, req, future);
            TransportException te = expectThrows(TransportException.class, future::actionGet);
            assertThat(te.getCause(), instanceOf(TaskCancelledException.class));
            assertThat(te.getCause().getMessage(), equalTo("The parent task was cancelled, shouldn't start any child tasks"));
        }
        for (ChildRequest req : outstandingRequests) {
            beforeExecuteLatches.get(req).countDown();
        }
        cancelFuture.actionGet();
        waitForMainTask(mainTaskFuture);
        assertBusy(() -> {
            for (DiscoveryNode node : nodes) {
                TaskManager taskManager = internalCluster().getInstance(TransportService.class, node.getName()).getTaskManager();
                assertThat(taskManager.getBanCount(), equalTo(0));
            }
        });
    }

    public void testCancelTaskMultipleTimes() throws Exception {
        Set<DiscoveryNode> nodes = StreamSupport.stream(clusterService().state().nodes().spliterator(), false).collect(Collectors.toSet());
        List<ChildRequest> childRequests = setupChildRequests(nodes);
        ActionFuture<MainResponse> mainTaskFuture = client().execute(TransportMainAction.ACTION, new MainRequest(childRequests));
        for (ChildRequest r : randomSubsetOf(between(1, childRequests.size()), childRequests)) {
            arrivedLatches.get(r).await();
        }
        TaskId taskId = getMainTaskId();
        ActionFuture<CancelTasksResponse> cancelFuture = client().admin().cluster().prepareCancelTasks().setTaskId(taskId)
            .waitForCompletion(true).execute();
        ensureChildTasksCancelledOrBanned(taskId);
        if (randomBoolean()) {
            CancelTasksResponse resp = client().admin().cluster().prepareCancelTasks().setTaskId(taskId).waitForCompletion(false).get();
            assertThat(resp.getTaskFailures(), empty());
            assertThat(resp.getNodeFailures(), empty());
        }
        assertFalse(cancelFuture.isDone());
        for (ChildRequest r : childRequests) {
            beforeExecuteLatches.get(r).countDown();
        }
        assertThat(cancelFuture.actionGet().getTaskFailures(), empty());
        assertThat(cancelFuture.actionGet().getTaskFailures(), empty());
        waitForMainTask(mainTaskFuture);
        CancelTasksResponse cancelError = client().admin().cluster().prepareCancelTasks()
            .setTaskId(taskId).waitForCompletion(randomBoolean()).get();
        assertThat(cancelError.getNodeFailures(), hasSize(1));
        final Throwable notFound = ExceptionsHelper.unwrap(cancelError.getNodeFailures().get(0), ResourceNotFoundException.class);
        assertThat(notFound.getMessage(), equalTo("task [" + taskId + "] is not found"));
    }

    public void testDoNotWaitForCompletion() throws Exception {
        Set<DiscoveryNode> nodes = StreamSupport.stream(clusterService().state().nodes().spliterator(), false).collect(Collectors.toSet());
        List<ChildRequest> childRequests = setupChildRequests(nodes);
        ActionFuture<MainResponse> mainTaskFuture = client().execute(TransportMainAction.ACTION, new MainRequest(childRequests));
        for (ChildRequest r : randomSubsetOf(between(1, childRequests.size()), childRequests)) {
            arrivedLatches.get(r).await();
        }
        TaskId taskId = getMainTaskId();
        boolean waitForCompletion = randomBoolean();
        ActionFuture<CancelTasksResponse> cancelFuture = client().admin().cluster().prepareCancelTasks().setTaskId(taskId)
            .waitForCompletion(waitForCompletion).execute();
        if (waitForCompletion) {
            assertFalse(cancelFuture.isDone());
        } else {
            assertBusy(() -> assertTrue(cancelFuture.isDone()));
        }
        for (ChildRequest r : childRequests) {
            beforeExecuteLatches.get(r).countDown();
        }
        waitForMainTask(mainTaskFuture);
    }

    TaskId getMainTaskId() {
        ListTasksResponse listTasksResponse = client().admin().cluster().prepareListTasks()
            .setActions(TransportMainAction.ACTION.name()).setDetailed(true).get();
        assertThat(listTasksResponse.getTasks(), hasSize(1));
        return listTasksResponse.getTasks().get(0).getTaskId();
    }

    void waitForMainTask(ActionFuture<MainResponse> mainTask) {
        try {
            mainTask.actionGet();
        } catch (Exception e) {
            final Throwable cause = ExceptionsHelper.unwrap(e, TaskCancelledException.class);
            assertThat(cause.getMessage(),
                either(equalTo("The parent task was cancelled, shouldn't start any child tasks"))
                    .or(containsString("Task cancelled before it started:")));
        }
    }

    public static class MainRequest extends ActionRequest {
        final List<ChildRequest> childRequests;

        public MainRequest(List<ChildRequest> childRequests) {
            this.childRequests = childRequests;
        }

        public MainRequest(StreamInput in) throws IOException {
            super(in);
            this.childRequests = in.readList(ChildRequest::new);
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeList(childRequests);
        }

        @Override
        public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
            return new CancellableTask(id, type, action, getDescription(), parentTaskId, headers) {
                @Override
                public boolean shouldCancelChildrenOnCancellation() {
                    return true;
                }
            };
        }
    }

    public static class MainResponse extends ActionResponse {
        public MainResponse() {
        }

        public MainResponse(StreamInput in) throws IOException {
            super(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {

        }
    }

    public static class ChildRequest extends ActionRequest {
        final int id;
        final DiscoveryNode targetNode;

        public ChildRequest(int id, DiscoveryNode targetNode) {
            this.id = id;
            this.targetNode = targetNode;
        }


        public ChildRequest(StreamInput in) throws IOException {
            super(in);
            this.id = in.readInt();
            this.targetNode = new DiscoveryNode(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeInt(id);
            targetNode.writeTo(out);
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public String getDescription() {
            return "childTask[" + id + "]";
        }

        @Override
        public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
            if (randomBoolean()) {
                boolean shouldCancelChildrenOnCancellation = randomBoolean();
                return new CancellableTask(id, type, action, getDescription(), parentTaskId, headers) {
                    @Override
                    public boolean shouldCancelChildrenOnCancellation() {
                        return shouldCancelChildrenOnCancellation;
                    }
                };
            } else {
                return super.createTask(id, type, action, parentTaskId, headers);
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ChildRequest that = (ChildRequest) o;
            return id == that.id && targetNode.equals(that.targetNode);
        }

        @Override
        public int hashCode() {
            return Objects.hash(id, targetNode);
        }
    }

    public static class ChildResponse extends ActionResponse {
        public ChildResponse() {
        }

        public ChildResponse(StreamInput in) throws IOException {
            super(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {

        }
    }

    public static class TransportMainAction extends HandledTransportAction<MainRequest, MainResponse> {

        public static ActionType<MainResponse> ACTION = new ActionType<>("internal::main_action", MainResponse::new);
        private final TransportService transportService;
        private final NodeClient client;

        @Inject
        public TransportMainAction(TransportService transportService, NodeClient client, ActionFilters actionFilters) {
            super(ACTION.name(), transportService, actionFilters, MainRequest::new, ThreadPool.Names.GENERIC);
            this.transportService = transportService;
            this.client = client;
        }

        @Override
        protected void doExecute(Task task, MainRequest request, ActionListener<MainResponse> listener) {
            GroupedActionListener<ChildResponse> groupedListener =
                new GroupedActionListener<>(ActionListener.map(listener, r -> new MainResponse()), request.childRequests.size());
            for (ChildRequest childRequest : request.childRequests) {
                TaskId parentTaskId = new TaskId(client.getLocalNodeId(), task.getId());
                startChildTask(parentTaskId, childRequest, groupedListener);
            }
        }

        protected void startChildTask(TaskId parentTaskId, ChildRequest childRequest, ActionListener<ChildResponse> listener) {
            childRequest.setParentTask(parentTaskId);
            final CountDownLatch completeLatch = completedLatches.get(childRequest);
            LatchedActionListener<ChildResponse> latchedListener = new LatchedActionListener<>(listener, completeLatch);
            transportService.getThreadPool().generic().execute(new AbstractRunnable() {
                @Override
                public void onFailure(Exception e) {
                    listener.onFailure(e);
                }

                @Override
                protected void doRun() {
                    if (client.getLocalNodeId().equals(childRequest.targetNode.getId()) && randomBoolean()) {
                        try {
                            client.executeLocally(TransportChildAction.ACTION, childRequest, latchedListener);
                        } catch (TaskCancelledException e) {
                            latchedListener.onFailure(new TransportException(e));
                        }
                    } else {
                        transportService.sendRequest(childRequest.targetNode, TransportChildAction.ACTION.name(), childRequest,
                            new TransportResponseHandler<>() {

                                @Override
                                public void handleResponse(TransportResponse response) {
                                    latchedListener.onResponse(new ChildResponse());
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
                                public TransportResponse read(StreamInput in) {
                                    return TransportResponse.Empty.INSTANCE;
                                }
                            });
                    }
                }
            });
        }
    }

    public static class TransportChildAction extends HandledTransportAction<ChildRequest, ChildResponse> {
        public static ActionType<ChildResponse> ACTION = new ActionType<>("internal:child_action", ChildResponse::new);
        private final TransportService transportService;


        @Inject
        public TransportChildAction(TransportService transportService, ActionFilters actionFilters) {
            super(ACTION.name(), transportService, actionFilters, ChildRequest::new, ThreadPool.Names.GENERIC);
            this.transportService = transportService;
        }

        @Override
        protected void doExecute(Task task, ChildRequest request, ActionListener<ChildResponse> listener) {
            assertThat(request.targetNode, equalTo(transportService.getLocalNode()));
            arrivedLatches.get(request).countDown();
            transportService.getThreadPool().executor(ThreadPool.Names.GENERIC).execute(ActionRunnable.supply(listener, () -> {
                beforeExecuteLatches.get(request).await();
                return new ChildResponse();
            }));
        }
    }

    public static class TaskPlugin extends Plugin implements ActionPlugin {
        @Override
        public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
            return Arrays.asList(
                new ActionHandler<>(TransportMainAction.ACTION, TransportMainAction.class),
                new ActionHandler<>(TransportChildAction.ACTION, TransportChildAction.class)
            );
        }
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        final List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(TaskPlugin.class);
        return plugins;
    }

    /**
     * Ensures that all outstanding child tasks of the given parent task are banned or being cancelled.
     */
    protected static void ensureChildTasksCancelledOrBanned(TaskId taskId) throws Exception {
        assertBusy(() -> {
            for (String nodeName : internalCluster().getNodeNames()) {
                final TaskManager taskManager = internalCluster().getInstance(TransportService.class, nodeName).getTaskManager();
                assertTrue(taskManager.childTasksCancelledOrBanned(taskId));
            }
        });
    }
}
