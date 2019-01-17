/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ml.action.AbstractGetResourcesRequest;
import org.elasticsearch.xpack.core.ml.action.AbstractGetResourcesResponse;
import org.elasticsearch.xpack.core.ml.action.util.QueryPage;
import org.elasticsearch.xpack.ml.utils.MlIndicesUtils;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;

public abstract class AbstractTransportGetResourcesAction<Resource extends ToXContent & Writeable,
        Request extends AbstractGetResourcesRequest, Response extends AbstractGetResourcesResponse<Resource>>
            extends HandledTransportAction<Request, Response> {

    private Client client;

    protected AbstractTransportGetResourcesAction(String actionName, TransportService transportService, ActionFilters actionFilters,
                                                  Supplier<Request> request, Client client) {
        super(actionName, transportService, actionFilters, request);
        this.client = Objects.requireNonNull(client);
    }

    protected void searchResources(AbstractGetResourcesRequest request, ActionListener<QueryPage<Resource>> listener) {
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder()
            .sort(request.getResourceIdField())
            .from(request.getPageParams().getFrom())
            .size(request.getPageParams().getSize())
            .query(buildQuery(request));

        SearchRequest searchRequest = new SearchRequest(getIndices())
            .indicesOptions(MlIndicesUtils.addIgnoreUnavailable(SearchRequest.DEFAULT_INDICES_OPTIONS))
            .source(sourceBuilder);

        executeAsyncWithOrigin(client.threadPool().getThreadContext(), ML_ORIGIN, searchRequest, new ActionListener<SearchResponse>() {
                @Override
                public void onResponse(SearchResponse response) {
                    List<Resource> docs = new ArrayList<>();
                    for (SearchHit hit : response.getHits().getHits()) {
                        BytesReference docSource = hit.getSourceRef();
                        try (InputStream stream = docSource.streamInput();
                             XContentParser parser = XContentFactory.xContent(XContentType.JSON).createParser(
                                 NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, stream)) {
                            docs.add(parse(parser));
                        } catch (IOException e) {
                            this.onFailure(e);
                        }
                    }

                    if (docs.isEmpty() && Regex.isSimpleMatchPattern(request.getResourceId()) == false) {
                        listener.onFailure(notFoundException(request.getResourceId()));
                    } else {
                        listener.onResponse(new QueryPage<>(docs, docs.size(), getResultsField()));
                    }
                }


                @Override
                public void onFailure(Exception e) {
                    listener.onFailure(e);
                }
            },
            client::search);
    }

    private QueryBuilder buildQuery(AbstractGetResourcesRequest request) {
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
        if (Strings.isNullOrEmpty(request.getResourceId()) == false) {
            boolQuery.filter(QueryBuilders.wildcardQuery(request.getResourceIdField(), request.getResourceId()));
        }
        QueryBuilder additionalQuery = additionalQuery();
        if (additionalQuery != null) {
            boolQuery.filter(additionalQuery);
        }
        return boolQuery.hasClauses() ? boolQuery : QueryBuilders.matchAllQuery();
    }

    @Nullable
    protected QueryBuilder additionalQuery() {
        return null;
    }

    protected abstract ParseField getResultsField();

    protected abstract String[] getIndices();

    protected abstract Resource parse(XContentParser parser);

    protected abstract ResourceNotFoundException notFoundException(String resourceId);
}
