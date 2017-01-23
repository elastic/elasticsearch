/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.TransportGetAction;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.TransportSearchAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.MasterNodeReadOperationRequestBuilder;
import org.elasticsearch.action.support.master.MasterNodeReadRequest;
import org.elasticsearch.action.support.master.TransportMasterNodeReadAction;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.StatusToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.ml.action.util.QueryPage;
import org.elasticsearch.xpack.ml.action.util.PageParams;
import org.elasticsearch.xpack.ml.job.config.ListDocument;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;


public class GetListAction extends Action<GetListAction.Request, GetListAction.Response, GetListAction.RequestBuilder> {

    public static final GetListAction INSTANCE = new GetListAction();
    public static final String NAME = "cluster:admin/ml/list/get";

    private GetListAction() {
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

    public static class Request extends MasterNodeReadRequest<Request> {

        private String listId;
        private PageParams pageParams;

        public Request() {
        }

        public void setListId(String listId) {
            if (pageParams != null) {
                throw new IllegalArgumentException("Param [" + ListDocument.ID.getPreferredName() + "] is incompatible with ["
                        + PageParams.FROM.getPreferredName()+ ", " + PageParams.SIZE.getPreferredName() + "].");
            }
            this.listId = listId;
        }

        public String getListId() {
            return listId;
        }

        public PageParams getPageParams() {
            return pageParams;
        }

        public void setPageParams(PageParams pageParams) {
            if (listId != null) {
                throw new IllegalArgumentException("Param [" + PageParams.FROM.getPreferredName()
                        + ", " + PageParams.SIZE.getPreferredName() + "] is incompatible with ["
                        + ListDocument.ID.getPreferredName() + "].");
            }
            this.pageParams = pageParams;
        }

        @Override
        public ActionRequestValidationException validate() {
            ActionRequestValidationException validationException = null;
            if (pageParams == null && listId == null) {
                validationException = addValidationError("Both [" + ListDocument.ID.getPreferredName() + "] and ["
                        + PageParams.FROM.getPreferredName() + ", " + PageParams.SIZE.getPreferredName() + "] "
                        + "cannot be null" , validationException);
            }
            return validationException;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            listId = in.readOptionalString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeOptionalString(listId);
        }

        @Override
        public int hashCode() {
            return Objects.hash(listId);
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
            return Objects.equals(listId, other.listId);
        }
    }

    public static class RequestBuilder extends MasterNodeReadOperationRequestBuilder<Request, Response, RequestBuilder> {

        public RequestBuilder(ElasticsearchClient client, GetListAction action) {
            super(client, action, new Request());
        }
    }

    public static class Response extends ActionResponse implements StatusToXContentObject {

        private QueryPage<ListDocument> lists;

        public Response(QueryPage<ListDocument> lists) {
            this.lists = lists;
        }

        Response() {
        }

        public QueryPage<ListDocument> getLists() {
            return lists;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            lists = new QueryPage<>(in, ListDocument::new);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            lists.writeTo(out);
        }

        @Override
        public RestStatus status() {
            return RestStatus.OK;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            lists.doXContentBody(builder, params);
            builder.endObject();
            return builder;
        }

        @Override
        public int hashCode() {
            return Objects.hash(lists);
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
            return Objects.equals(lists, other.lists);
        }

        @SuppressWarnings("deprecation")
        @Override
        public final String toString() {
            try {
                XContentBuilder builder = XContentFactory.jsonBuilder();
                builder.prettyPrint();
                toXContent(builder, EMPTY_PARAMS);
                return builder.string();
            } catch (Exception e) {
                // So we have a stack trace logged somewhere
                return "{ \"error\" : \"" + org.elasticsearch.ExceptionsHelper.detailedMessage(e) + "\"}";
            }
        }
    }

    public static class TransportAction extends TransportMasterNodeReadAction<Request, Response> {

        private final TransportGetAction transportGetAction;
        private final TransportSearchAction transportSearchAction;

        // TODO these need to be moved to a settings object later
        // See #20
        private static final String ML_INFO_INDEX = "ml-int";

        @Inject
        public TransportAction(Settings settings, TransportService transportService, ClusterService clusterService,
                               ThreadPool threadPool, ActionFilters actionFilters,
                               IndexNameExpressionResolver indexNameExpressionResolver,
                               TransportGetAction transportGetAction, TransportSearchAction transportSearchAction) {
            super(settings, GetListAction.NAME, transportService, clusterService, threadPool, actionFilters,
                    indexNameExpressionResolver, Request::new);
            this.transportGetAction = transportGetAction;
            this.transportSearchAction = transportSearchAction;
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
            if (!Strings.isNullOrEmpty(listId)) {
                getList(listId, listener);
            } else if (request.getPageParams() != null) {
                getLists(request.getPageParams(), listener);
            } else {
                throw new IllegalStateException("Both listId and pageParams are null");
            }
        }

        @Override
        protected ClusterBlockException checkBlock(Request request, ClusterState state) {
            return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
        }

        private void getList(String listId, ActionListener<Response> listener) {
            GetRequest getRequest = new GetRequest(ML_INFO_INDEX, ListDocument.TYPE.getPreferredName(), listId);
            transportGetAction.execute(getRequest, new ActionListener<GetResponse>() {
                @Override
                public void onResponse(GetResponse getDocResponse) {

                    try {
                        QueryPage<ListDocument> responseBody;
                        if (getDocResponse.isExists()) {
                            BytesReference docSource = getDocResponse.getSourceAsBytesRef();
                            XContentParser parser =
                                    XContentFactory.xContent(docSource).createParser(NamedXContentRegistry.EMPTY, docSource);
                            ListDocument listDocument = ListDocument.PARSER.apply(parser, null);
                            responseBody = new QueryPage<>(Collections.singletonList(listDocument), 1, ListDocument.RESULTS_FIELD);

                            Response listResponse = new Response(responseBody);
                            listener.onResponse(listResponse);
                        } else {
                            this.onFailure(QueryPage.emptyQueryPage(ListDocument.RESULTS_FIELD));
                        }

                    } catch (Exception e) {
                        this.onFailure(e);
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    listener.onFailure(e);
                }
            });
        }

        private void getLists(PageParams pageParams, ActionListener<Response> listener) {
            SearchSourceBuilder sourceBuilder = new SearchSourceBuilder()
                    .from(pageParams.getFrom())
                    .size(pageParams.getSize());

            SearchRequest searchRequest = new SearchRequest(new String[]{ML_INFO_INDEX}, sourceBuilder)
                    .types(ListDocument.TYPE.getPreferredName());

            transportSearchAction.execute(searchRequest, new ActionListener<SearchResponse>() {
                @Override
                public void onResponse(SearchResponse response) {

                    try {
                        List<ListDocument> docs = new ArrayList<>();
                        for (SearchHit hit : response.getHits().getHits()) {
                            BytesReference docSource = hit.sourceRef();
                            XContentParser parser =
                                    XContentFactory.xContent(docSource).createParser(NamedXContentRegistry.EMPTY, docSource);
                            docs.add(ListDocument.PARSER.apply(parser, null));
                        }

                        Response listResponse = new Response(new QueryPage<>(docs, docs.size(), ListDocument.RESULTS_FIELD));
                        listener.onResponse(listResponse);

                    } catch (Exception e) {
                        this.onFailure(e);
                    }
                }


                @Override
                public void onFailure(Exception e) {
                    listener.onFailure(e);
                }
            });
        }
    }

}

