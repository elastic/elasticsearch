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

import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.TaskOperationFailure;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.nodes.BaseNodeRequest;
import org.elasticsearch.action.support.nodes.BaseNodeResponse;
import org.elasticsearch.action.support.nodes.BaseNodesRequest;
import org.elasticsearch.action.support.nodes.BaseNodesResponse;
import org.elasticsearch.action.support.nodes.NodesOperationRequestBuilder;
import org.elasticsearch.action.support.nodes.TransportNodesAction;
import org.elasticsearch.action.support.tasks.BaseTasksRequest;
import org.elasticsearch.action.support.tasks.BaseTasksResponse;
import org.elasticsearch.action.support.tasks.TransportTasksAction;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent.Params;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

import static org.elasticsearch.test.ESTestCase.awaitBusy;

/**
 * A plugin that adds a cancellable blocking test task of integration testing of the task manager.
 */
public class TestTaskPlugin extends Plugin implements ActionPlugin {

    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        return Arrays.asList(new ActionHandler<>(TestTaskAction.INSTANCE, TransportTestTaskAction.class),
                new ActionHandler<>(UnblockTestTasksAction.INSTANCE, TransportUnblockTestTasksAction.class));
    }

    static class TestTask extends CancellableTask {

        private volatile boolean blocked = true;

        TestTask(long id, String type, String action, String description, TaskId parentTaskId) {
            super(id, type, action, description, parentTaskId);
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

        protected NodeResponse() {
            super();
        }

        public NodeResponse(DiscoveryNode node) {
            super(node);
        }
    }

    public static class NodesResponse extends BaseNodesResponse<NodeResponse> implements ToXContentFragment {

        NodesResponse() {

        }

        public NodesResponse(ClusterName clusterName, List<NodeResponse> nodes, List<FailedNodeException> failures) {
            super(clusterName, nodes, failures);
        }

        @Override
        protected List<NodeResponse> readNodesFrom(StreamInput in) throws IOException {
            return in.readStreamableList(NodeResponse::new);
        }

        @Override
        protected void writeNodesTo(StreamOutput out, List<NodeResponse> nodes) throws IOException {
            out.writeStreamableList(nodes);
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

    public static class NodeRequest extends BaseNodeRequest {
        protected String requestName;
        protected String nodeId;
        protected boolean shouldBlock;

        public NodeRequest() {
            super();
        }

        public NodeRequest(NodesRequest request, String nodeId, boolean shouldBlock) {
            super(nodeId);
            requestName = request.requestName;
            this.nodeId = nodeId;
            this.shouldBlock = shouldBlock;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            requestName = in.readString();
            nodeId = in.readString();
            shouldBlock = in.readBoolean();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(requestName);
            out.writeString(nodeId);
            out.writeBoolean(shouldBlock);
        }

        @Override
        public String getDescription() {
            return "NodeRequest[" + requestName + ", " + nodeId + "]";
        }

        @Override
        public Task createTask(long id, String type, String action, TaskId parentTaskId) {
            return new TestTask(id, type, action, this.getDescription(), parentTaskId);
        }
    }

    public static class NodesRequest extends BaseNodesRequest<NodesRequest> {
        private String requestName;
        private boolean shouldStoreResult = false;
        private boolean shouldBlock = true;
        private boolean shouldFail = false;

        NodesRequest() {
            super();
        }

        public NodesRequest(String requestName, String... nodesIds) {
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
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            requestName = in.readString();
            shouldStoreResult = in.readBoolean();
            shouldBlock = in.readBoolean();
            shouldFail = in.readBoolean();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(requestName);
            out.writeBoolean(shouldStoreResult);
            out.writeBoolean(shouldBlock);
            out.writeBoolean(shouldFail);
        }

        @Override
        public String getDescription() {
            return "NodesRequest[" + requestName + "]";
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

    public static class TransportTestTaskAction extends TransportNodesAction<NodesRequest, NodesResponse, NodeRequest, NodeResponse> {

        @Inject
        public TransportTestTaskAction(Settings settings, ThreadPool threadPool,
                                       ClusterService clusterService, TransportService transportService) {
            super(settings, TestTaskAction.NAME, threadPool, clusterService, transportService,
                  new ActionFilters(new HashSet<>()), new IndexNameExpressionResolver(Settings.EMPTY),
                  NodesRequest::new, NodeRequest::new, ThreadPool.Names.GENERIC, NodeResponse.class);
        }

        @Override
        protected NodesResponse newResponse(NodesRequest request, List<NodeResponse> responses, List<FailedNodeException> failures) {
            if (request.getShouldFail()) {
                throw new IllegalStateException("Simulating operation failure");
            }
            return new NodesResponse(clusterService.getClusterName(), responses, failures);
        }

        @Override
        protected NodeRequest newNodeRequest(String nodeId, NodesRequest request) {
            return new NodeRequest(request, nodeId, request.getShouldBlock());
        }

        @Override
        protected NodeResponse newNodeResponse() {
            return new NodeResponse();
        }

        @Override
        protected void doExecute(Task task, NodesRequest request, ActionListener<NodesResponse> listener) {
            super.doExecute(task, request, listener);
        }

        @Override
        protected NodeResponse nodeOperation(NodeRequest request, Task task) {
            logger.info("Test task started on the node {}", clusterService.localNode());
            if (request.shouldBlock) {
                try {
                    awaitBusy(() -> {
                        if (((CancellableTask) task).isCancelled()) {
                            throw new RuntimeException("Cancelled!");
                        }
                        return ((TestTask) task).isBlocked() == false;
                    });
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                }
            }
            logger.info("Test task finished on the node {}", clusterService.localNode());
            return new NodeResponse(clusterService.localNode());
        }

        @Override
        protected NodeResponse nodeOperation(NodeRequest request) {
            throw new UnsupportedOperationException("the task parameter is required");
        }

    }

    public static class TestTaskAction extends Action<NodesRequest, NodesResponse, NodesRequestBuilder> {

        public static final TestTaskAction INSTANCE = new TestTaskAction();
        public static final String NAME = "cluster:admin/tasks/test";

        private TestTaskAction() {
            super(NAME);
        }

        @Override
        public NodesResponse newResponse() {
            return new NodesResponse();
        }

        @Override
        public NodesRequestBuilder newRequestBuilder(ElasticsearchClient client) {
            return new NodesRequestBuilder(client, this);
        }
    }

    public static class NodesRequestBuilder extends NodesOperationRequestBuilder<NodesRequest, NodesResponse, NodesRequestBuilder> {

        protected NodesRequestBuilder(ElasticsearchClient client, Action<NodesRequest, NodesResponse, NodesRequestBuilder> action) {
            super(client, action, new NodesRequest("test"));
        }


        public NodesRequestBuilder setShouldStoreResult(boolean shouldStoreResult) {
            request().setShouldStoreResult(shouldStoreResult);
            return this;
        }

        public NodesRequestBuilder setShouldBlock(boolean shouldBlock) {
            request().setShouldBlock(shouldBlock);
            return this;
        }

        public NodesRequestBuilder setShouldFail(boolean shouldFail) {
            request().setShouldFail(shouldFail);
            return this;
        }
    }


    public static class UnblockTestTaskResponse implements Writeable {

        public UnblockTestTaskResponse() {

        }

        public UnblockTestTaskResponse(StreamInput in) {
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
        }
    }


    public static class UnblockTestTasksRequest extends BaseTasksRequest<UnblockTestTasksRequest> {
        @Override
        public boolean match(Task task) {
            return task instanceof TestTask && super.match(task);
        }
    }

    public static class UnblockTestTasksResponse extends BaseTasksResponse {

        private List<UnblockTestTaskResponse> tasks;

        public UnblockTestTasksResponse() {
            super(null, null);
        }

        public UnblockTestTasksResponse(List<UnblockTestTaskResponse> tasks, List<TaskOperationFailure> taskFailures, List<? extends
            FailedNodeException> nodeFailures) {
            super(taskFailures, nodeFailures);
            this.tasks = tasks == null ? Collections.emptyList() : Collections.unmodifiableList(new ArrayList<>(tasks));
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
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
    public static class TransportUnblockTestTasksAction extends TransportTasksAction<Task, UnblockTestTasksRequest,
        UnblockTestTasksResponse, UnblockTestTaskResponse> {

        @Inject
        public TransportUnblockTestTasksAction(Settings settings,ThreadPool threadPool, ClusterService
            clusterService,
                                               TransportService transportService) {
            super(settings, UnblockTestTasksAction.NAME, threadPool, clusterService, transportService, new ActionFilters(new
                HashSet<>()), new IndexNameExpressionResolver(Settings.EMPTY),
                UnblockTestTasksRequest::new, UnblockTestTasksResponse::new, ThreadPool.Names.MANAGEMENT);
        }

        @Override
        protected UnblockTestTasksResponse newResponse(UnblockTestTasksRequest request, List<UnblockTestTaskResponse> tasks,
                                                       List<TaskOperationFailure> taskOperationFailures, List<FailedNodeException>
                                                                   failedNodeExceptions) {
            return new UnblockTestTasksResponse(tasks, taskOperationFailures, failedNodeExceptions);
        }

        @Override
        protected UnblockTestTaskResponse readTaskResponse(StreamInput in) throws IOException {
            return new UnblockTestTaskResponse(in);
        }

        @Override
        protected void taskOperation(UnblockTestTasksRequest request, Task task, ActionListener<UnblockTestTaskResponse> listener) {
            ((TestTask) task).unblock();
            listener.onResponse(new UnblockTestTaskResponse());
        }

    }

    public static class UnblockTestTasksAction extends Action<UnblockTestTasksRequest, UnblockTestTasksResponse,
        UnblockTestTasksRequestBuilder> {

        public static final UnblockTestTasksAction INSTANCE = new UnblockTestTasksAction();
        public static final String NAME = "cluster:admin/tasks/testunblock";

        private UnblockTestTasksAction() {
            super(NAME);
        }

        @Override
        public UnblockTestTasksResponse newResponse() {
            return new UnblockTestTasksResponse();
        }

        @Override
        public UnblockTestTasksRequestBuilder newRequestBuilder(ElasticsearchClient client) {
            return new UnblockTestTasksRequestBuilder(client, this);
        }
    }

    public static class UnblockTestTasksRequestBuilder extends ActionRequestBuilder<UnblockTestTasksRequest, UnblockTestTasksResponse,
        UnblockTestTasksRequestBuilder> {

        protected UnblockTestTasksRequestBuilder(ElasticsearchClient client, Action<UnblockTestTasksRequest, UnblockTestTasksResponse,
            UnblockTestTasksRequestBuilder> action) {
            super(client, action, new UnblockTestTasksRequest());
        }
    }

}
