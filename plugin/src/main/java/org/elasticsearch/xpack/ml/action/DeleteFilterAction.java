/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.bulk.TransportBulkAction;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.MasterNodeOperationRequestBuilder;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.ml.MlMetadata;
import org.elasticsearch.xpack.ml.job.config.Detector;
import org.elasticsearch.xpack.ml.job.config.Job;
import org.elasticsearch.xpack.ml.job.config.MlFilter;
import org.elasticsearch.xpack.ml.job.persistence.JobProvider;
import org.elasticsearch.xpack.ml.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;


public class DeleteFilterAction extends Action<DeleteFilterAction.Request, DeleteFilterAction.Response, DeleteFilterAction.RequestBuilder> {

    public static final DeleteFilterAction INSTANCE = new DeleteFilterAction();
    public static final String NAME = "cluster:admin/ml/filters/delete";

    private DeleteFilterAction() {
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

    public static class Request extends AcknowledgedRequest<Request> {

        public static final ParseField FILTER_ID = new ParseField("filter_id");

        private String filterId;

        Request() {

        }

        public Request(String filterId) {
            this.filterId = ExceptionsHelper.requireNonNull(filterId, FILTER_ID.getPreferredName());
        }

        public String getFilterId() {
            return filterId;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            filterId = in.readString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(filterId);
        }

        @Override
        public int hashCode() {
            return Objects.hash(filterId);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            Request other = (Request) obj;
            return Objects.equals(filterId, other.filterId);
        }
    }

    public static class RequestBuilder extends MasterNodeOperationRequestBuilder<Request, Response, RequestBuilder> {

        public RequestBuilder(ElasticsearchClient client, DeleteFilterAction action) {
            super(client, action, new Request());
        }
    }

    public static class Response extends AcknowledgedResponse {

        public Response(boolean acknowledged) {
            super(acknowledged);
        }

        private Response() {}

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            readAcknowledged(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            writeAcknowledged(out);
        }
    }

    public static class TransportAction extends TransportMasterNodeAction<Request, Response> {

        private final TransportBulkAction transportAction;

        @Inject
        public TransportAction(Settings settings, TransportService transportService, ClusterService clusterService,
                               ThreadPool threadPool, ActionFilters actionFilters,
                               IndexNameExpressionResolver indexNameExpressionResolver,
                               TransportBulkAction transportAction) {
            super(settings, DeleteFilterAction.NAME, transportService, clusterService, threadPool, actionFilters,
                    indexNameExpressionResolver, Request::new);
            this.transportAction = transportAction;
        }

        @Override
        protected String executor() {
            return ThreadPool.Names.SAME;
        }

        @Override
        protected Response newResponse() {
            return new Response();
        }

        @Override
        protected void masterOperation(Request request, ClusterState state, ActionListener<Response> listener) throws Exception {

            final String filterId = request.getFilterId();
            MlMetadata currentMlMetadata = state.metaData().custom(MlMetadata.TYPE);
            Map<String, Job> jobs = currentMlMetadata.getJobs();
            List<String> currentlyUsedBy = new ArrayList<>();
            for (Job job : jobs.values()) {
                List<Detector> detectors = job.getAnalysisConfig().getDetectors();
                for (Detector detector : detectors) {
                    if (detector.extractReferencedFilters().contains(filterId)) {
                        currentlyUsedBy.add(job.getId());
                        break;
                    }
                }
            }
            if (!currentlyUsedBy.isEmpty()) {
                throw ExceptionsHelper.conflictStatusException("Cannot delete filter, currently used by jobs: "
                        + currentlyUsedBy);
            }

            DeleteRequest deleteRequest = new DeleteRequest(JobProvider.ML_META_INDEX, MlFilter.TYPE.getPreferredName(), filterId);
            BulkRequest bulkRequest = new BulkRequest();
            bulkRequest.add(deleteRequest);
            transportAction.execute(bulkRequest, new ActionListener<BulkResponse>() {
                @Override
                public void onResponse(BulkResponse bulkResponse) {
                    if (bulkResponse.getItems()[0].status() == RestStatus.NOT_FOUND) {
                        listener.onFailure(new ResourceNotFoundException("Could not delete filter with ID [" + filterId
                                + "] because it does not exist"));
                    } else {
                        listener.onResponse(new Response(true));
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    logger.error("Could not delete filter with ID [" + filterId + "]", e);
                    listener.onFailure(new IllegalStateException("Could not delete filter with ID [" + filterId + "]", e));
                }
            });
        }

        @Override
        protected ClusterBlockException checkBlock(Request request, ClusterState state) {
            return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
        }
    }
}

