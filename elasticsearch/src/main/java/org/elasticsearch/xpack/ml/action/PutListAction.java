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
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.index.TransportIndexAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.MasterNodeOperationRequestBuilder;
import org.elasticsearch.action.support.master.MasterNodeReadRequest;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.ml.lists.ListDocument;
import org.elasticsearch.xpack.ml.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.Objects;


public class PutListAction extends Action<PutListAction.Request, PutListAction.Response, PutListAction.RequestBuilder> {

    public static final PutListAction INSTANCE = new PutListAction();
    public static final String NAME = "cluster:admin/ml/list/put";

    private PutListAction() {
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

    public static class Request extends MasterNodeReadRequest<Request> implements ToXContent {

        public static Request parseRequest(XContentParser parser) {
            ListDocument listDocument = ListDocument.PARSER.apply(parser, null);
            return new Request(listDocument);
        }

        private ListDocument listDocument;

        Request() {

        }

        public Request(ListDocument listDocument) {
            this.listDocument = ExceptionsHelper.requireNonNull(listDocument, "list_document");
        }

        public ListDocument getListDocument() {
            return this.listDocument;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            listDocument = new ListDocument(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            listDocument.writeTo(out);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            listDocument.toXContent(builder, params);
            return builder;
        }

        @Override
        public int hashCode() {
            return Objects.hash(listDocument);
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
            return Objects.equals(listDocument, other.listDocument);
        }
    }

    public static class RequestBuilder extends MasterNodeOperationRequestBuilder<Request, Response, RequestBuilder> {

        public RequestBuilder(ElasticsearchClient client, PutListAction action) {
            super(client, action, new Request());
        }
    }
    public static class Response extends AcknowledgedResponse {

        public Response() {
            super(true);
        }

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

    // extends TransportMasterNodeAction, because we will store in cluster state.
    public static class TransportAction extends TransportMasterNodeAction<Request, Response> {

        private final TransportIndexAction transportIndexAction;

        // TODO these need to be moved to a settings object later. See #20
        private static final String ML_INFO_INDEX = "ml-int";

        @Inject
        public TransportAction(Settings settings, TransportService transportService, ClusterService clusterService,
                ThreadPool threadPool, ActionFilters actionFilters,
                IndexNameExpressionResolver indexNameExpressionResolver,
                TransportIndexAction transportIndexAction) {
            super(settings, PutListAction.NAME, transportService, clusterService, threadPool, actionFilters,
                    indexNameExpressionResolver, Request::new);
            this.transportIndexAction = transportIndexAction;
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
            ListDocument listDocument = request.getListDocument();
            final String listId = listDocument.getId();
            IndexRequest indexRequest = new IndexRequest(ML_INFO_INDEX, ListDocument.TYPE.getPreferredName(), listId);
            XContentBuilder builder = XContentFactory.jsonBuilder();
            indexRequest.source(listDocument.toXContent(builder, ToXContent.EMPTY_PARAMS));
            transportIndexAction.execute(indexRequest, new ActionListener<IndexResponse>() {
                @Override
                public void onResponse(IndexResponse indexResponse) {
                    listener.onResponse(new Response());
                }

                @Override
                public void onFailure(Exception e) {
                    logger.error("Could not create list with ID [" + listId + "]", e);
                    throw new ResourceNotFoundException("Could not create list with ID [" + listId + "]", e);
                }
            });
        }

        @Override
        protected ClusterBlockException checkBlock(Request request, ClusterState state) {
            return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
        }
    }
}

