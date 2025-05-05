/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.action.admin.cluster.node.tasks;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.TaskOperationFailure;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.MasterNodeRequestHelper;
import org.elasticsearch.action.support.nodes.BaseNodeResponse;
import org.elasticsearch.action.support.nodes.BaseNodesRequest;
import org.elasticsearch.action.support.nodes.BaseNodesResponse;
import org.elasticsearch.action.support.nodes.TransportNodesAction;
import org.elasticsearch.action.support.tasks.BaseTasksRequest;
import org.elasticsearch.action.support.tasks.BaseTasksResponse;
import org.elasticsearch.action.support.tasks.TransportTasksAction;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.NetworkPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.AbstractTransportRequest;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportInterceptor;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.action.admin.cluster.node.tasks.get.TransportGetTaskAction.TASKS_ORIGIN;
import static org.elasticsearch.test.ESTestCase.waitUntil;

/**
 * A plugin that adds a cancellable blocking test task of integration testing of the task manager.
 */
public class TestTaskPlugin extends Plugin implements ActionPlugin, NetworkPlugin {

    private static final Logger logger = LogManager.getLogger(TestTaskPlugin.class);

    public static final ActionType<NodesResponse> TEST_TASK_ACTION = new ActionType<>("cluster:admin/tasks/test");
    public static final ActionType<UnblockTestTasksResponse> UNBLOCK_TASK_ACTION = new ActionType<>("cluster:admin/tasks/testunblock");

    @Override
    public List<ActionHandler> getActions() {
        return Arrays.asList(
            new ActionHandler(TEST_TASK_ACTION, TransportTestTaskAction.class),
            new ActionHandler(UNBLOCK_TASK_ACTION, TransportUnblockTestTasksAction.class)
        );
    }

    @Override
    public Collection<String> getTaskHeaders() {
        return Collections.singleton("Custom-Task-Header");
    }

    /**
     * Intercept transport requests to verify that all of the ones that should
     * have the origin set <strong>do</strong> have the origin set and the ones
     * that should not have the origin set <strong>do not</strong> have it set.
     */
    @Override
    public List<TransportInterceptor> getTransportInterceptors(NamedWriteableRegistry namedWriteableRegistry, ThreadContext threadContext) {
        return Collections.singletonList(new OriginAssertingInterceptor(threadContext));
    }

    static class TestTask extends CancellableTask {

        private volatile boolean blocked = true;

        TestTask(long id, String type, String action, String description, TaskId parentTaskId, Map<String, String> headers) {
            super(id, type, action, description, parentTaskId, headers);
        }

        @Override
        public boolean shouldCancelChildrenOnCancellation() {
            return false;
        }

        public boolean isBlocked() {
            return blocked;
        }

        public void unblock() {
            blocked = false;
        }
    }

    public static class NodeResponse extends BaseNodeResponse {

        public NodeResponse(StreamInput in) throws IOException {
            super(in);
        }

        public NodeResponse(DiscoveryNode node) {
            super(node);
        }
    }

    public static class NodesResponse extends BaseNodesResponse<NodeResponse> implements ToXContentFragment {

        NodesResponse(StreamInput in) throws IOException {
            super(in);
        }

        NodesResponse(ClusterName clusterName, List<NodeResponse> nodes, List<FailedNodeException> failures) {
            super(clusterName, nodes, failures);
        }

        @Override
        protected List<NodeResponse> readNodesFrom(StreamInput in) throws IOException {
            return in.readCollectionAsList(NodeResponse::new);
        }

        @Override
        protected void writeNodesTo(StreamOutput out, List<NodeResponse> nodes) throws IOException {
            out.writeCollection(nodes);
        }

        public int getFailureCount() {
            return failures().size();
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.field("failure_count", getFailureCount());
            return builder;
        }
    }

    public static class NodeRequest extends AbstractTransportRequest {
        protected final String requestName;
        protected final boolean shouldBlock;

        public NodeRequest(StreamInput in) throws IOException {
            super(in);
            requestName = in.readString();
            shouldBlock = in.readBoolean();
        }

        public NodeRequest(NodesRequest request, boolean shouldBlock) {
            requestName = request.requestName;
            this.shouldBlock = shouldBlock;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(requestName);
            out.writeBoolean(shouldBlock);
        }

        @Override
        public String getDescription() {
            return "NodeRequest[" + requestName + "]";
        }

        @Override
        public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
            return new TestTask(id, type, action, this.getDescription(), parentTaskId, headers);
        }
    }

    public static class NodesRequest extends BaseNodesRequest {
        private final String requestName;
        private boolean shouldStoreResult = false;
        private boolean shouldBlock = true;
        private boolean shouldFail = false;

        NodesRequest(String requestName, String... nodesIds) {
            super(nodesIds);
            this.requestName = requestName;
        }

        public void setShouldStoreResult(boolean shouldStoreResult) {
            this.shouldStoreResult = shouldStoreResult;
        }

        @Override
        public boolean getShouldStoreResult() {
            return shouldStoreResult;
        }

        public void setShouldBlock(boolean shouldBlock) {
            this.shouldBlock = shouldBlock;
        }

        public boolean getShouldBlock() {
            return shouldBlock;
        }

        public void setShouldFail(boolean shouldFail) {
            this.shouldFail = shouldFail;
        }

        public boolean getShouldFail() {
            return shouldFail;
        }

        @Override
        public String getDescription() {
            return "NodesRequest[" + requestName + "]";
        }

        @Override
        public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
            return new CancellableTask(id, type, action, getDescription(), parentTaskId, headers);
        }
    }

    public static class TransportTestTaskAction extends TransportNodesAction<NodesRequest, NodesResponse, NodeRequest, NodeResponse, Void> {

        @Inject
        public TransportTestTaskAction(ThreadPool threadPool, ClusterService clusterService, TransportService transportService) {
            super(
                TEST_TASK_ACTION.name(),
                clusterService,
                transportService,
                new ActionFilters(new HashSet<>()),
                NodeRequest::new,
                threadPool.executor(ThreadPool.Names.GENERIC)
            );
        }

        @Override
        protected NodesResponse newResponse(NodesRequest request, List<NodeResponse> responses, List<FailedNodeException> failures) {
            if (request.getShouldFail()) {
                throw new IllegalStateException("Simulating operation failure");
            }
            return new NodesResponse(clusterService.getClusterName(), responses, failures);
        }

        @Override
        protected NodeRequest newNodeRequest(NodesRequest request) {
            return new NodeRequest(request, request.getShouldBlock());
        }

        @Override
        protected NodeResponse newNodeResponse(StreamInput in, DiscoveryNode node) throws IOException {
            return new NodeResponse(in);
        }

        @Override
        protected void doExecute(Task task, NodesRequest request, ActionListener<NodesResponse> listener) {
            super.doExecute(task, request, listener);
        }

        @Override
        protected NodeResponse nodeOperation(NodeRequest request, Task task) {
            logger.info("Test task started on the node {}", clusterService.localNode());
            if (request.shouldBlock) {
                waitUntil(() -> {
                    if (((CancellableTask) task).isCancelled()) {
                        throw new RuntimeException("Cancelled!");
                    }
                    return ((TestTask) task).isBlocked() == false;
                });
            }
            logger.info("Test task finished on the node {}", clusterService.localNode());
            return new NodeResponse(clusterService.localNode());
        }
    }

    public static class UnblockTestTaskResponse implements Writeable {

        UnblockTestTaskResponse() {}

        UnblockTestTaskResponse(StreamInput in) {}

        @Override
        public void writeTo(StreamOutput out) throws IOException {}
    }

    public static class UnblockTestTasksRequest extends BaseTasksRequest<UnblockTestTasksRequest> {

        UnblockTestTasksRequest() {}

        UnblockTestTasksRequest(StreamInput in) throws IOException {
            super(in);
        }

        @Override
        public boolean match(Task task) {
            return task instanceof TestTask && super.match(task);
        }
    }

    public static class UnblockTestTasksResponse extends BaseTasksResponse {

        private List<UnblockTestTaskResponse> tasks;

        UnblockTestTasksResponse(
            List<UnblockTestTaskResponse> tasks,
            List<TaskOperationFailure> taskFailures,
            List<? extends FailedNodeException> nodeFailures
        ) {
            super(taskFailures, nodeFailures);
            this.tasks = tasks == null ? Collections.emptyList() : List.copyOf(tasks);
        }

        UnblockTestTasksResponse(StreamInput in) throws IOException {
            super(in);
            int taskCount = in.readVInt();
            List<UnblockTestTaskResponse> builder = new ArrayList<>();
            for (int i = 0; i < taskCount; i++) {
                builder.add(new UnblockTestTaskResponse(in));
            }
            tasks = Collections.unmodifiableList(builder);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeVInt(tasks.size());
            for (UnblockTestTaskResponse task : tasks) {
                task.writeTo(out);
            }
        }
    }

    /**
     * Test class for testing task operations
     */
    public static class TransportUnblockTestTasksAction extends TransportTasksAction<
        Task,
        UnblockTestTasksRequest,
        UnblockTestTasksResponse,
        UnblockTestTaskResponse> {

        @Inject
        public TransportUnblockTestTasksAction(ClusterService clusterService, TransportService transportService) {
            super(
                UNBLOCK_TASK_ACTION.name(),
                clusterService,
                transportService,
                new ActionFilters(new HashSet<>()),
                UnblockTestTasksRequest::new,
                UnblockTestTaskResponse::new,
                transportService.getThreadPool().executor(ThreadPool.Names.MANAGEMENT)
            );
        }

        @Override
        protected UnblockTestTasksResponse newResponse(
            UnblockTestTasksRequest request,
            List<UnblockTestTaskResponse> tasks,
            List<TaskOperationFailure> taskOperationFailures,
            List<FailedNodeException> failedNodeExceptions
        ) {
            return new UnblockTestTasksResponse(tasks, taskOperationFailures, failedNodeExceptions);
        }

        @Override
        protected void taskOperation(
            CancellableTask actionTask,
            UnblockTestTasksRequest request,
            Task task,
            ActionListener<UnblockTestTaskResponse> listener
        ) {
            ((TestTask) task).unblock();
            listener.onResponse(new UnblockTestTaskResponse());
        }

    }

    private static class OriginAssertingInterceptor implements TransportInterceptor {
        private final ThreadContext threadContext;

        private OriginAssertingInterceptor(ThreadContext threadContext) {
            this.threadContext = threadContext;
        }

        @Override
        public AsyncSender interceptSender(AsyncSender sender) {
            return new AsyncSender() {
                @Override
                public <T extends TransportResponse> void sendRequest(
                    Transport.Connection connection,
                    String action,
                    TransportRequest request,
                    TransportRequestOptions options,
                    TransportResponseHandler<T> handler
                ) {
                    if (action.startsWith("indices:data/write/bulk[s]")) {
                        /*
                         * We can't reason about these requests because
                         * *sometimes* they should have the origin, if they are
                         * running on the node that stores the task. But
                         * sometimes they won't be and in that case they don't
                         * need the origin. Either way, the interesting work is
                         * done by checking that the main bulk request
                         * (without the [s] part) has the origin.
                         */
                        sender.sendRequest(connection, action, request, options, handler);
                        return;
                    }
                    String expectedOrigin = shouldHaveOrigin(action, request) ? TASKS_ORIGIN : null;
                    String actualOrigin = threadContext.getTransient(ThreadContext.ACTION_ORIGIN_TRANSIENT_NAME);
                    if (Objects.equals(expectedOrigin, actualOrigin)) {
                        sender.sendRequest(connection, action, request, options, handler);
                        return;
                    }
                    handler.handleException(
                        new TransportException(
                            "should have origin of ["
                                + expectedOrigin
                                + "] but was ["
                                + actualOrigin
                                + "] action was ["
                                + action
                                + "]["
                                + request
                                + "]"
                        )
                    );
                }
            };
        }

        private boolean shouldHaveOrigin(String action, TransportRequest request) {
            if (false == action.startsWith("indices:")) {
                /*
                 * The Tasks API never uses origin with non-indices actions.
                 */
                return false;
            }
            if (action.startsWith("indices:admin/refresh") || action.startsWith("indices:data/read/search")) {
                /*
                 * The test refreshes and searches to count the number of tasks
                 * in the index and the Tasks API never does either.
                 */
                return false;
            }

            if (MasterNodeRequestHelper.unwrapTermOverride(request) instanceof IndicesRequest indicesRequest) {
                /*
                 * When the API Tasks API makes an indices request it only every
                 * targets the .tasks index. Other requests come from the tests.
                 */
                return Arrays.equals(new String[] { ".tasks" }, indicesRequest.indices());
            } else {
                return false;
            }
        }
    }
}
