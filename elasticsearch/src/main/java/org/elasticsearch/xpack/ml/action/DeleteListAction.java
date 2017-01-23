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
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.delete.TransportDeleteAction;
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
import org.elasticsearch.xpack.ml.job.config.Detector;
import org.elasticsearch.xpack.ml.job.config.Job;
import org.elasticsearch.xpack.ml.job.metadata.MlMetadata;
import org.elasticsearch.xpack.ml.job.config.ListDocument;
import org.elasticsearch.xpack.ml.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;


public class DeleteListAction extends Action<DeleteListAction.Request, DeleteListAction.Response, DeleteListAction.RequestBuilder> {

    public static final DeleteListAction INSTANCE = new DeleteListAction();
    public static final String NAME = "cluster:admin/ml/list/delete";

    private DeleteListAction() {
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

        public static final ParseField LIST_ID = new ParseField("list_id");

        private String listId;

        Request() {

        }

        public Request(String listId) {
            this.listId = ExceptionsHelper.requireNonNull(listId, LIST_ID.getPreferredName());
        }

        public String getListId() {
            return listId;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            listId = in.readString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(listId);
        }

        @Override
        public int hashCode() {
            return Objects.hash(listId);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            Request other = (Request) obj;
            return Objects.equals(listId, other.listId);
        }
    }

    public static class RequestBuilder extends MasterNodeOperationRequestBuilder<Request, Response, RequestBuilder> {

        public RequestBuilder(ElasticsearchClient client, DeleteListAction action) {
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

        private final TransportDeleteAction transportAction;

        // TODO these need to be moved to a settings object later. See #20
        private static final String ML_INFO_INDEX = "ml-int";

        @Inject
        public TransportAction(Settings settings, TransportService transportService, ClusterService clusterService,
                               ThreadPool threadPool, ActionFilters actionFilters,
                               IndexNameExpressionResolver indexNameExpressionResolver,
                               TransportDeleteAction transportAction) {
            super(settings, DeleteListAction.NAME, transportService, clusterService, threadPool, actionFilters,
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

            final String listId = request.getListId();
            MlMetadata currentMlMetadata = state.metaData().custom(MlMetadata.TYPE);
            Map<String, Job> jobs = currentMlMetadata.getJobs();
            List<String> currentlyUsedBy = new ArrayList<>();
            for (Job job : jobs.values()) {
                List<Detector> detectors = job.getAnalysisConfig().getDetectors();
                for (Detector detector : detectors) {
                    if (detector.extractReferencedLists().contains(listId)) {
                        currentlyUsedBy.add(job.getId());
                        break;
                    }
                }
            }
            if (!currentlyUsedBy.isEmpty()) {
                throw ExceptionsHelper.conflictStatusException("Cannot delete List, currently used by jobs: "
                        + currentlyUsedBy);
            }

            DeleteRequest deleteRequest = new DeleteRequest(ML_INFO_INDEX, ListDocument.TYPE.getPreferredName(), listId);
            transportAction.execute(deleteRequest, new ActionListener<DeleteResponse>() {
                @Override
                public void onResponse(DeleteResponse deleteResponse) {
                    if (deleteResponse.status().equals(RestStatus.NOT_FOUND)) {
                        listener.onFailure(new ResourceNotFoundException("Could not delete list with ID [" + listId
                                + "] because it does not exist"));
                    } else {
                        listener.onResponse(new Response(true));
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    logger.error("Could not delete list with ID [" + listId + "]", e);
                    listener.onFailure(new IllegalStateException("Could not delete list with ID [" + listId + "]", e));
                }
            });
        }

        @Override
        protected ClusterBlockException checkBlock(Request request, ClusterState state) {
            return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
        }
    }
}

