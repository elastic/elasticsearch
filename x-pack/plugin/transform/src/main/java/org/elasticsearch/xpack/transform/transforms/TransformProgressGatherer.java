/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.transform.transforms;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfig;
import org.elasticsearch.xpack.core.transform.transforms.TransformProgress;

import java.util.function.Function;

/**
 * Utility class to gather the progress information for a given config and its cursor position
 */
public final class TransformProgressGatherer {

    private Client client;

    TransformProgressGatherer(Client client) {
        this.client = client;
    }

    /**
     * This gathers the total docs given the config and search
     *
     * @param filterQuery The adapted filter that can optionally take into account checkpoint information
     * @param config The transform config containing headers, source, pivot, etc. information
     * @param progressListener The listener to notify when progress object has been created
     */
    public void getInitialProgress(QueryBuilder filterQuery, TransformConfig config, ActionListener<TransformProgress> progressListener) {
        SearchRequest request = getSearchRequest(config, filterQuery);

        ActionListener<SearchResponse> searchResponseActionListener = ActionListener
            .wrap(
                searchResponse -> progressListener.onResponse(searchResponseToTransformProgressFunction().apply(searchResponse)),
                progressListener::onFailure
            );
        ClientHelper
            .executeWithHeadersAsync(
                config.getHeaders(),
                ClientHelper.TRANSFORM_ORIGIN,
                client,
                SearchAction.INSTANCE,
                request,
                searchResponseActionListener
            );
    }

    public static SearchRequest getSearchRequest(TransformConfig config, QueryBuilder filteredQuery) {
        SearchRequest request = new SearchRequest(config.getSource().getIndex());
        request.allowPartialSearchResults(false);
        BoolQueryBuilder existsClauses = QueryBuilders.boolQuery();
        config
            .getPivotConfig()
            .getGroupConfig()
            .getGroups()
            .values()
            // TODO change once we allow missing_buckets
            .forEach(src -> existsClauses.must(QueryBuilders.existsQuery(src.getField())));

        request
            .source(
                new SearchSourceBuilder()
                    .size(0)
                    .trackTotalHits(true)
                    .query(QueryBuilders.boolQuery().filter(filteredQuery).filter(existsClauses))
            );
        return request;
    }

    public static Function<SearchResponse, TransformProgress> searchResponseToTransformProgressFunction() {
        return searchResponse -> searchResponse != null
            ? new TransformProgress(searchResponse.getHits().getTotalHits().value, 0L, 0L)
            : null;
    }
}
