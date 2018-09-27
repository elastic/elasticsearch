/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.datafeed.extractor.rollup;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.xpack.core.ml.datafeed.extractor.ExtractorUtils;
import org.elasticsearch.xpack.core.rollup.action.RollupSearchAction;
import org.elasticsearch.xpack.ml.datafeed.extractor.AbstractAggregationDataExtractor;
import org.elasticsearch.xpack.ml.datafeed.extractor.aggregation.AggregationDataExtractorContext;

/**
 * An implementation that extracts data from elasticsearch using search with aggregations against rollup indexes on a client.
 * The first time {@link #next()} is called, the search is executed. The result aggregations are
 * stored and they are then processed in batches. Cancellation is supported between batches.
 * Note that this class is NOT thread-safe.
 */
class RollupDataExtractor extends AbstractAggregationDataExtractor<RollupSearchAction.RequestBuilder> {

    RollupDataExtractor(Client client, AggregationDataExtractorContext dataExtractorContext) {
        super(client, dataExtractorContext);
    }

    @Override
    protected RollupSearchAction.RequestBuilder buildSearchRequest() {
        // For derivative aggregations the first bucket will always be null
        // so query one extra histogram bucket back and hope there is data
        // in that bucket
        long histogramSearchStartTime = Math.max(0, context.start - ExtractorUtils.getHistogramIntervalMillis(context.aggs));

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder()
            .size(0)
            .query(ExtractorUtils.wrapInTimeRangeQuery(context.query, context.timeField, histogramSearchStartTime, context.end));

        context.aggs.getAggregatorFactories().forEach(searchSourceBuilder::aggregation);
        context.aggs.getPipelineAggregatorFactories().forEach(searchSourceBuilder::aggregation);

        SearchRequest searchRequest = new SearchRequest().indices(context.indices)
            .types(context.types)
            .source(searchSourceBuilder);

        return new RollupSearchAction.RequestBuilder(client, searchRequest);
    }

}
