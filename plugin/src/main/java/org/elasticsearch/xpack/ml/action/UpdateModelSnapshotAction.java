/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.bulk.TransportBulkAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.StatusToXContentObject;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.ml.job.config.Job;
import org.elasticsearch.xpack.ml.job.messages.Messages;
import org.elasticsearch.xpack.ml.job.persistence.ElasticsearchMappings;
import org.elasticsearch.xpack.ml.job.persistence.JobProvider;
import org.elasticsearch.xpack.ml.job.process.autodetect.state.ModelSnapshot;
import org.elasticsearch.xpack.ml.job.results.Result;
import org.elasticsearch.xpack.ml.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.Objects;
import java.util.function.Consumer;

public class UpdateModelSnapshotAction extends Action<UpdateModelSnapshotAction.Request,
        UpdateModelSnapshotAction.Response, UpdateModelSnapshotAction.RequestBuilder> {

    public static final UpdateModelSnapshotAction INSTANCE = new UpdateModelSnapshotAction();
    public static final String NAME = "cluster:admin/xpack/ml/job/model_snapshots/update";

    private UpdateModelSnapshotAction() {
        super(NAME);
    }

    @Override
    public UpdateModelSnapshotAction.RequestBuilder newRequestBuilder(ElasticsearchClient client) {
        return new RequestBuilder(client, this);
    }

    @Override
    public UpdateModelSnapshotAction.Response newResponse() {
        return new Response();
    }

    public static class Request extends ActionRequest implements ToXContentObject {

        private static final ObjectParser<Request, Void> PARSER = new ObjectParser<>(NAME, Request::new);

        static {
            PARSER.declareString((request, jobId) -> request.jobId = jobId, Job.ID);
            PARSER.declareString((request, snapshotId) -> request.snapshotId = snapshotId, ModelSnapshot.SNAPSHOT_ID);
            PARSER.declareString(Request::setDescription, ModelSnapshot.DESCRIPTION);
            PARSER.declareBoolean(Request::setRetain, ModelSnapshot.RETAIN);
        }

        public static Request parseRequest(String jobId, String snapshotId, XContentParser parser) {
            Request request = PARSER.apply(parser, null);
            if (jobId != null) {
                request.jobId = jobId;
            }
            if (snapshotId != null) {
                request.snapshotId = snapshotId;
            }
            return request;
        }

        private String jobId;
        private String snapshotId;
        private String description;
        private Boolean retain;

        Request() {
        }

        public Request(String jobId, String snapshotId) {
            this.jobId = ExceptionsHelper.requireNonNull(jobId, Job.ID.getPreferredName());
            this.snapshotId = ExceptionsHelper.requireNonNull(snapshotId, ModelSnapshot.SNAPSHOT_ID.getPreferredName());
        }

        public String getJobId() {
            return jobId;
        }

        public String getSnapshotId() {
            return snapshotId;
        }

        public String getDescription() {
            return description;
        }

        public void setDescription(String description) {
            this.description = description;
        }

        public Boolean getRetain() {
            return retain;
        }

        public void setRetain(Boolean retain) {
            this.retain = retain;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            jobId = in.readString();
            snapshotId = in.readString();
            description = in.readOptionalString();
            retain = in.readOptionalBoolean();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(jobId);
            out.writeString(snapshotId);
            out.writeOptionalString(description);
            out.writeOptionalBoolean(retain);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(Job.ID.getPreferredName(), jobId);
            builder.field(ModelSnapshot.SNAPSHOT_ID.getPreferredName(), snapshotId);
            if (description != null) {
                builder.field(ModelSnapshot.DESCRIPTION.getPreferredName(), description);
            }
            if (retain != null) {
                builder.field(ModelSnapshot.RETAIN.getPreferredName(), retain);
            }
            builder.endObject();
            return builder;
        }

        @Override
        public int hashCode() {
            return Objects.hash(jobId, snapshotId, description, retain);
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
            return Objects.equals(jobId, other.jobId)
                    && Objects.equals(snapshotId, other.snapshotId)
                    && Objects.equals(description, other.description)
                    && Objects.equals(retain, other.retain);
        }
    }

    public static class Response extends ActionResponse implements StatusToXContentObject {

        private static final ParseField ACKNOWLEDGED = new ParseField("acknowledged");
        private static final ParseField MODEL = new ParseField("model");

        private ModelSnapshot model;

        Response() {

        }

        public Response(ModelSnapshot modelSnapshot) {
            model = modelSnapshot;
        }

        public ModelSnapshot getModel() {
            return model;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            model = new ModelSnapshot(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            model.writeTo(out);
        }

        @Override
        public RestStatus status() {
            return RestStatus.OK;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(ACKNOWLEDGED.getPreferredName(), true);
            builder.field(MODEL.getPreferredName());
            builder = model.toXContent(builder, params);
            builder.endObject();
            return builder;
        }

        @Override
        public int hashCode() {
            return Objects.hash(model);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            Response other = (Response) obj;
            return Objects.equals(model, other.model);
        }

        @Override
        public final String toString() {
            return Strings.toString(this);
        }
    }

    public static class RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder> {

        public RequestBuilder(ElasticsearchClient client, UpdateModelSnapshotAction action) {
            super(client, action, new Request());
        }
    }

    public static class TransportAction extends HandledTransportAction<Request, Response> {

        private final JobProvider jobProvider;
        private final TransportBulkAction transportBulkAction;

        @Inject
        public TransportAction(Settings settings, TransportService transportService, ThreadPool threadPool, ActionFilters actionFilters,
                IndexNameExpressionResolver indexNameExpressionResolver, JobProvider jobProvider, TransportBulkAction transportBulkAction) {
            super(settings, NAME, threadPool, transportService, actionFilters, indexNameExpressionResolver, Request::new);
            this.jobProvider = jobProvider;
            this.transportBulkAction = transportBulkAction;
        }

        @Override
        protected void doExecute(Request request, ActionListener<Response> listener) {
            logger.debug("Received request to update model snapshot [{}] for job [{}]", request.getSnapshotId(), request.getJobId());
            jobProvider.getModelSnapshot(request.getJobId(), request.getSnapshotId(), modelSnapshot -> {
                if (modelSnapshot == null) {
                    listener.onFailure(new ResourceNotFoundException(Messages.getMessage(
                            Messages.REST_NO_SUCH_MODEL_SNAPSHOT, request.getSnapshotId(), request.getJobId())));
                } else {
                    Result<ModelSnapshot> updatedSnapshot = applyUpdate(request, modelSnapshot);
                    indexModelSnapshot(updatedSnapshot, b -> {
                        // The quantiles can be large, and totally dominate the output -
                        // it's clearer to remove them
                        listener.onResponse(new Response(new ModelSnapshot.Builder(updatedSnapshot.result).setQuantiles(null).build()));
                    }, listener::onFailure);
                }
            }, listener::onFailure);
        }

        private static Result<ModelSnapshot> applyUpdate(Request request, Result<ModelSnapshot> target) {
            ModelSnapshot.Builder updatedSnapshotBuilder = new ModelSnapshot.Builder(target.result);
            if (request.getDescription() != null) {
                updatedSnapshotBuilder.setDescription(request.getDescription());
            }
            if (request.getRetain() != null) {
                updatedSnapshotBuilder.setRetain(request.getRetain());
            }
            return new Result(target.index, updatedSnapshotBuilder.build());
        }

        private void indexModelSnapshot(Result<ModelSnapshot> modelSnapshot, Consumer<Boolean> handler, Consumer<Exception> errorHandler) {
            IndexRequest indexRequest = new IndexRequest(modelSnapshot.index, ElasticsearchMappings.DOC_TYPE,
                    ModelSnapshot.documentId(modelSnapshot.result));
            try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
                modelSnapshot.result.toXContent(builder, ToXContent.EMPTY_PARAMS);
                indexRequest.source(builder);
            } catch (IOException e) {
                errorHandler.accept(e);
                return;
            }
            BulkRequest bulkRequest = new BulkRequest().add(indexRequest);
            transportBulkAction.execute(bulkRequest, new ActionListener<BulkResponse>() {
                @Override
                public void onResponse(BulkResponse indexResponse) {
                    handler.accept(true);
                }

                @Override
                public void onFailure(Exception e) {
                    errorHandler.accept(e);
                }
            });
        }
    }
}
