/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.TaskOperationFailure;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.tasks.BaseTasksRequest;
import org.elasticsearch.action.support.tasks.BaseTasksResponse;
import org.elasticsearch.action.support.tasks.TransportTasksAction;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContent.Params;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.MlMetadata;
import org.elasticsearch.xpack.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.persistent.PersistentTasksCustomMetaData;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * An internal action that isolates a datafeed.
 * Datafeed isolation is effectively disconnecting a running datafeed task
 * from its job, i.e. even though the datafeed performs a search, the retrieved
 * data is not sent to the job, etc. As stopping a datafeed cannot always happen
 * instantaneously (e.g. cannot cancel an ongoing search), isolating a datafeed
 * task ensures the current datafeed task can complete inconsequentially while
 * the datafeed persistent task may be stopped or reassigned on another node.
 */
public class IsolateDatafeedAction
        extends Action<IsolateDatafeedAction.Request, IsolateDatafeedAction.Response, IsolateDatafeedAction.RequestBuilder> {

    public static final IsolateDatafeedAction INSTANCE = new IsolateDatafeedAction();
    public static final String NAME = "cluster:internal/xpack/ml/datafeed/isolate";

    private IsolateDatafeedAction() {
        super(NAME);
    }

    @Override
    public RequestBuilder newRequestBuilder(ElasticsearchClient client) {
        return new RequestBuilder(client, this);
    }

    @Override
    public Response newResponse() {
        return new Response();
    }

    public static class Request extends BaseTasksRequest<Request> implements ToXContentObject {

        public static ObjectParser<Request, Void> PARSER = new ObjectParser<>(NAME, Request::new);

        static {
            PARSER.declareString((request, datafeedId) -> request.datafeedId = datafeedId, DatafeedConfig.ID);
        }

        public static Request fromXContent(XContentParser parser) {
            return parseRequest(null, parser);
        }

        public static Request parseRequest(String datafeedId, XContentParser parser) {
            Request request = PARSER.apply(parser, null);
            if (datafeedId != null) {
                request.datafeedId = datafeedId;
            }
            return request;
        }

        private String datafeedId;

        public Request(String datafeedId) {
            this.datafeedId = ExceptionsHelper.requireNonNull(datafeedId, DatafeedConfig.ID.getPreferredName());
        }

        Request() {
        }

        private String getDatafeedId() {
            return datafeedId;
        }

        @Override
        public boolean match(Task task) {
            String expectedDescription = MlMetadata.datafeedTaskId(datafeedId);
            if (task instanceof StartDatafeedAction.DatafeedTask && expectedDescription.equals(task.getDescription())){
                return true;
            }
            return false;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            datafeedId = in.readString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(datafeedId);
        }

        @Override
        public int hashCode() {
            return Objects.hash(datafeedId);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(DatafeedConfig.ID.getPreferredName(), datafeedId);
            builder.endObject();
            return builder;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            Request other = (Request) obj;
            return Objects.equals(datafeedId, other.datafeedId);
        }
    }

    public static class Response extends BaseTasksResponse implements Writeable {

        private boolean isolated;

        public Response(boolean isolated) {
            super(null, null);
            this.isolated = isolated;
        }

        public Response(StreamInput in) throws IOException {
            super(null, null);
            readFrom(in);
        }

        Response() {
            super(null, null);
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            isolated = in.readBoolean();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeBoolean(isolated);
        }
    }

    static class RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder> {

        RequestBuilder(ElasticsearchClient client, IsolateDatafeedAction action) {
            super(client, action, new Request());
        }
    }

    public static class TransportAction extends TransportTasksAction<StartDatafeedAction.DatafeedTask, Request, Response, Response> {

        @Inject
        public TransportAction(Settings settings, TransportService transportService, ThreadPool threadPool,
                               ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver,
                               ClusterService clusterService) {
            super(settings, IsolateDatafeedAction.NAME, threadPool, clusterService, transportService, actionFilters,
                    indexNameExpressionResolver, Request::new, Response::new, MachineLearning.UTILITY_THREAD_POOL_NAME);
        }

        @Override
        protected void doExecute(Task task, Request request, ActionListener<Response> listener) {
            final ClusterState state = clusterService.state();
            PersistentTasksCustomMetaData tasks = state.getMetaData().custom(PersistentTasksCustomMetaData.TYPE);
            PersistentTasksCustomMetaData.PersistentTask<?> datafeedTask = MlMetadata.getDatafeedTask(request.getDatafeedId(), tasks);

            if (datafeedTask == null || datafeedTask.getExecutorNode() == null) {
                // No running datafeed task to isolate
                listener.onResponse(new Response());
                return;
            }

            String executorNode = datafeedTask.getExecutorNode();
            DiscoveryNodes nodes = state.nodes();
            if (nodes.resolveNode(executorNode).getVersion().before(Version.V_5_5_0)) {
                listener.onFailure(new ElasticsearchException("Force delete datafeed is not supported because the datafeed task " +
                        "is running on a node [" + executorNode + "] with a version prior to " + Version.V_5_5_0));
                return;
            }

            request.setNodes(datafeedTask.getExecutorNode());
            super.doExecute(task, request, listener);
        }

        @Override
        protected Response newResponse(Request request, List<Response> tasks, List<TaskOperationFailure> taskOperationFailures,
                                       List<FailedNodeException> failedNodeExceptions) {
            if (taskOperationFailures.isEmpty() == false) {
                throw org.elasticsearch.ExceptionsHelper
                        .convertToElastic(taskOperationFailures.get(0).getCause());
            } else if (failedNodeExceptions.isEmpty() == false) {
                throw org.elasticsearch.ExceptionsHelper
                        .convertToElastic(failedNodeExceptions.get(0));
            } else {
                return new Response();
            }
        }

        @Override
        protected void taskOperation(Request request, StartDatafeedAction.DatafeedTask datafeedTask, ActionListener<Response> listener) {
            datafeedTask.isolate();
            listener.onResponse(new Response());
        }

        @Override
        protected Response readTaskResponse(StreamInput in) throws IOException {
            return new Response(in);
        }
    }
}
