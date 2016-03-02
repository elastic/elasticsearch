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

import com.google.common.base.Predicate;
import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionModule;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.TaskOperationFailure;
import org.elasticsearch.action.support.ActionFilter;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.nodes.BaseNodeRequest;
import org.elasticsearch.action.support.nodes.BaseNodeResponse;
import org.elasticsearch.action.support.nodes.BaseNodesRequest;
import org.elasticsearch.action.support.nodes.BaseNodesResponse;
import org.elasticsearch.action.support.nodes.TransportNodesAction;
import org.elasticsearch.action.support.tasks.BaseTasksRequest;
import org.elasticsearch.action.support.tasks.BaseTasksResponse;
import org.elasticsearch.action.support.tasks.TransportTasksAction;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicReferenceArray;

import static org.elasticsearch.test.ESTestCase.awaitBusy;

/**
 * A plugin that adds a cancellable blocking test task of integration testing of the task manager.
 */
public class TestTaskPlugin extends Plugin {


    @Override
    public String name() {
        return "test-task-plugin";
    }

    @Override
    public String description() {
        return "Test plugin for testing task management";
    }

    public void onModule(ActionModule module) {
        module.registerAction(TestTaskAction.INSTANCE, TransportTestTaskAction.class);
        module.registerAction(UnblockTestTasksAction.INSTANCE, TransportUnblockTestTasksAction.class);
    }

    static class TestTask extends CancellableTask {

        private volatile boolean blocked = true;

        public TestTask(long id, String type, String action, String description, TaskId parentTaskId) {
            super(id, type, action, description, parentTaskId);
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

    public static class NodesResponse extends BaseNodesResponse<NodeResponse> {

        private int failureCount;

        NodesResponse() {

        }

        public NodesResponse(ClusterName clusterName, NodeResponse[] nodes, int failureCount) {
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

    public static class NodeRequest extends BaseNodeRequest {
        protected String requestName;
        protected String nodeId;

        public NodeRequest() {
            super();
        }

        public NodeRequest(NodesRequest request, String nodeId) {
            super(request, nodeId);
            requestName = request.requestName;
            this.nodeId = nodeId;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            requestName = in.readString();
            nodeId = in.readString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(requestName);
            out.writeString(nodeId);
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

        public NodesRequest() {
            super();
        }

        public NodesRequest(String requestName, String... nodesIds) {
            super(nodesIds);
            this.requestName = requestName;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            requestName = in.readString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(requestName);
        }

        @Override
        public String getDescription() {
            return "NodesRequest[" + requestName + "]";
        }

        @Override
        public Task createTask(long id, String type, String action) {
            return new CancellableTask(id, type, action, getDescription());
        }
    }

    public static class TransportTestTaskAction extends TransportNodesAction<NodesRequest, NodesResponse, NodeRequest, NodeResponse> {

        @Inject
        public TransportTestTaskAction(Settings settings, ClusterName clusterName, ThreadPool threadPool,
                                       ClusterService clusterService, TransportService transportService) {
            super(settings, TestTaskAction.NAME, clusterName, threadPool, clusterService, transportService,
                new ActionFilters(new HashSet<ActionFilter>()), new IndexNameExpressionResolver(Settings.EMPTY),
                NodesRequest.class, NodeRequest.class, ThreadPool.Names.GENERIC);
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
        protected String[] filterNodeIds(DiscoveryNodes nodes, String[] nodesIds) {
            List<String> list = new ArrayList<>();
            for (String node : nodesIds) {
                if (nodes.getDataNodes().containsKey(node)) {
                    list.add(node);
                }
            }
            return list.toArray(new String[list.size()]);
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
        protected void doExecute(Task task, NodesRequest request, ActionListener<NodesResponse> listener) {
            super.doExecute(task, request, listener);
        }

        @Override
        protected NodeResponse nodeOperation(NodeRequest request, final Task task) {
            logger.info("Test task started on the node {}", clusterService.localNode());
            try {
                awaitBusy(new Predicate<Object>() {
                    @Override
                    public boolean apply(Object obj) {
                        if (((CancellableTask) task).isCancelled()) {
                            throw new RuntimeException("Cancelled!");
                        }
                        return ((TestTask) task).isBlocked() == false;
                    }
                });
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
            }
            logger.info("Test task finished on the node {}", clusterService.localNode());
            return new NodeResponse(clusterService.localNode());
        }

        @Override
        protected NodeResponse nodeOperation(NodeRequest request) {
            throw new UnsupportedOperationException("the task parameter is required");
        }

        @Override
        protected boolean accumulateExceptions() {
            return true;
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

    public static class NodesRequestBuilder extends ActionRequestBuilder<NodesRequest, NodesResponse, NodesRequestBuilder> {

        protected NodesRequestBuilder(ElasticsearchClient client, Action<NodesRequest, NodesResponse, NodesRequestBuilder> action) {
            super(client, action, new NodesRequest("test"));
        }
    }


    public static class UnblockTestTaskResponse implements Writeable<UnblockTestTaskResponse> {

        public UnblockTestTaskResponse() {

        }

        public UnblockTestTaskResponse(StreamInput in) {

        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {

        }

        @Override
        public UnblockTestTaskResponse readFrom(StreamInput in) throws IOException {
            return new UnblockTestTaskResponse(in);
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

        }

        public UnblockTestTasksResponse(List<UnblockTestTaskResponse> tasks, List<TaskOperationFailure> taskFailures, List<? extends
            FailedNodeException> nodeFailures) {
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
        public TransportUnblockTestTasksAction(Settings settings, ClusterName clusterName, ThreadPool threadPool, ClusterService
            clusterService,
                                               TransportService transportService) {
            super(settings, UnblockTestTasksAction.NAME, clusterName, threadPool, clusterService, transportService, new ActionFilters(new
                HashSet<ActionFilter>()), new IndexNameExpressionResolver(Settings.EMPTY),
                new Callable<UnblockTestTasksRequest>() {
                    @Override
                    public UnblockTestTasksRequest call() throws Exception {
                        return new UnblockTestTasksRequest();
                    }
                }, ThreadPool.Names.MANAGEMENT);
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
        protected UnblockTestTaskResponse taskOperation(UnblockTestTasksRequest request, Task task) {
            ((TestTask) task).unblock();
            return new UnblockTestTaskResponse();
        }

        @Override
        protected boolean accumulateExceptions() {
            return true;
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
