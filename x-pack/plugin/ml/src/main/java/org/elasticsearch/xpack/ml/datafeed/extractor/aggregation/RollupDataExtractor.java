/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.datafeed.extractor.aggregation;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.xpack.core.rollup.action.RollupSearchAction;
import org.elasticsearch.xpack.ml.datafeed.DatafeedTimingStatsReporter;

/**
 * An implementation that extracts data from elasticsearch using search with aggregations against rollup indexes on a client.
 * The first time {@link #next()} is called, the search is executed. The result aggregations are
 * stored and they are then processed in batches. Cancellation is supported between batches.
 * Note that this class is NOT thread-safe.
 */
class RollupDataExtractor extends AbstractAggregationDataExtractor<RollupSearchAction.RequestBuilder> {

    RollupDataExtractor(
            Client client, AggregationDataExtractorContext dataExtractorContext, DatafeedTimingStatsReporter timingStatsReporter) {
        super(client, dataExtractorContext, timingStatsReporter);
    }

    @Override
    protected RollupSearchAction.RequestBuilder buildSearchRequest(SearchSourceBuilder searchSourceBuilder) {
        SearchRequest searchRequest = new SearchRequest().indices(context.indices)
            .indicesOptions(context.indicesOptions)
            .allowPartialSearchResults(false)
            .source(searchSourceBuilder);

        return new RollupSearchAction.RequestBuilder(client, searchRequest);
    }
}
