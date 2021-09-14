/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.datafeed.extractor.aggregation;

import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.xpack.ml.datafeed.DatafeedTimingStatsReporter;

/**
 * An implementation that extracts data from elasticsearch using search with aggregations on a client.
 * The first time {@link #next()} is called, the search is executed. The result aggregations are
 * stored and they are then processed in batches. Cancellation is supported between batches.
 * Note that this class is NOT thread-safe.
 */
class AggregationDataExtractor extends AbstractAggregationDataExtractor<SearchRequestBuilder> {

    AggregationDataExtractor(
            Client client, AggregationDataExtractorContext dataExtractorContext, DatafeedTimingStatsReporter timingStatsReporter) {
        super(client, dataExtractorContext, timingStatsReporter);
    }

    @Override
    protected SearchRequestBuilder buildSearchRequest(SearchSourceBuilder searchSourceBuilder) {
        return new SearchRequestBuilder(client, SearchAction.INSTANCE)
            .setSource(searchSourceBuilder)
            .setIndicesOptions(context.indicesOptions)
            .setAllowPartialSearchResults(false)
            .setIndices(context.indices);
    }
}
