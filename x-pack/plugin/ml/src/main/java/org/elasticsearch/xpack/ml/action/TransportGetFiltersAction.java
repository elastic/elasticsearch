/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.get.GetAction;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ml.MlMetaIndex;
import org.elasticsearch.xpack.core.ml.action.GetFiltersAction;
import org.elasticsearch.xpack.core.ml.action.util.PageParams;
import org.elasticsearch.xpack.core.ml.action.util.QueryPage;
import org.elasticsearch.xpack.core.ml.job.config.MlFilter;
import org.elasticsearch.xpack.ml.utils.MlIndicesUtils;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;

public class TransportGetFiltersAction extends HandledTransportAction<GetFiltersAction.Request, GetFiltersAction.Response> {

    private final Client client;

    @Inject
    public TransportGetFiltersAction(TransportService transportService, ActionFilters actionFilters, Client client) {
        super(GetFiltersAction.NAME, transportService, actionFilters, GetFiltersAction.Request::new);
        this.client = client;
    }

    @Override
    protected void doExecute(Task task, GetFiltersAction.Request request, ActionListener<GetFiltersAction.Response> listener) {
        final String filterId = request.getFilterId();
        if (!Strings.isNullOrEmpty(filterId)) {
            getFilter(filterId, listener);
        } else  {
            PageParams pageParams = request.getPageParams();
            if (pageParams == null) {
                pageParams = PageParams.defaultParams();
            }
            getFilters(pageParams, listener);
        }
    }

    private void getFilter(String filterId, ActionListener<GetFiltersAction.Response> listener) {
        GetRequest getRequest = new GetRequest(MlMetaIndex.INDEX_NAME, MlMetaIndex.TYPE, MlFilter.documentId(filterId));
        executeAsyncWithOrigin(client, ML_ORIGIN, GetAction.INSTANCE, getRequest, new ActionListener<GetResponse>() {
            @Override
            public void onResponse(GetResponse getDocResponse) {

                try {
                    QueryPage<MlFilter> responseBody;
                    if (getDocResponse.isExists()) {
                        BytesReference docSource = getDocResponse.getSourceAsBytesRef();
                        try (InputStream stream = docSource.streamInput();
                             XContentParser parser = XContentFactory.xContent(XContentType.JSON)
                                     .createParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, stream)) {
                            MlFilter filter = MlFilter.LENIENT_PARSER.apply(parser, null).build();
                            responseBody = new QueryPage<>(Collections.singletonList(filter), 1, MlFilter.RESULTS_FIELD);

                            GetFiltersAction.Response filterResponse = new GetFiltersAction.Response(responseBody);
                            listener.onResponse(filterResponse);
                        }
                    } else {
                        this.onFailure(QueryPage.emptyQueryPage(MlFilter.RESULTS_FIELD));
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

    private void getFilters(PageParams pageParams, ActionListener<GetFiltersAction.Response> listener) {
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder()
                .from(pageParams.getFrom())
                .size(pageParams.getSize())
                .query(QueryBuilders.termQuery(MlFilter.TYPE.getPreferredName(), MlFilter.FILTER_TYPE));

        SearchRequest searchRequest = new SearchRequest(MlMetaIndex.INDEX_NAME)
                .indicesOptions(MlIndicesUtils.addIgnoreUnavailable(SearchRequest.DEFAULT_INDICES_OPTIONS))
                .source(sourceBuilder);

        executeAsyncWithOrigin(client.threadPool().getThreadContext(), ML_ORIGIN, searchRequest, new ActionListener<SearchResponse>() {
            @Override
            public void onResponse(SearchResponse response) {
                List<MlFilter> docs = new ArrayList<>();
                for (SearchHit hit : response.getHits().getHits()) {
                    BytesReference docSource = hit.getSourceRef();
                    try (InputStream stream = docSource.streamInput();
                         XContentParser parser = XContentFactory.xContent(XContentType.JSON).createParser(
                                 NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, stream)) {
                        docs.add(MlFilter.LENIENT_PARSER.apply(parser, null).build());
                    } catch (IOException e) {
                        this.onFailure(e);
                    }
                }

                GetFiltersAction.Response filterResponse = new GetFiltersAction.Response(new QueryPage<>(docs, docs.size(),
                        MlFilter.RESULTS_FIELD));
                listener.onResponse(filterResponse);
            }


            @Override
            public void onFailure(Exception e) {
                listener.onFailure(e);
            }
        },
        client::search);
    }
}
