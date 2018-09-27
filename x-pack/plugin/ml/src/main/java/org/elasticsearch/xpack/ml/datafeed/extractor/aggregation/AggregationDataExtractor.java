/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.datafeed.extractor.aggregation;

import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.xpack.core.ml.datafeed.extractor.ExtractorUtils;
import org.elasticsearch.xpack.ml.datafeed.extractor.AbstractAggregationDataExtractor;

/**
 * An implementation that extracts data from elasticsearch using search with aggregations on a client.
 * The first time {@link #next()} is called, the search is executed. The result aggregations are
 * stored and they are then processed in batches. Cancellation is supported between batches.
 * Note that this class is NOT thread-safe.
 */
class AggregationDataExtractor extends AbstractAggregationDataExtractor<SearchRequestBuilder> {

    AggregationDataExtractor(Client client, AggregationDataExtractorContext dataExtractorContext) {
        super(client, dataExtractorContext);
    }

    @Override
    protected SearchRequestBuilder buildSearchRequest() {
        // For derivative aggregations the first bucket will always be null
        // so query one extra histogram bucket back and hope there is data
        // in that bucket
        long histogramSearchStartTime = Math.max(0, context.start - getHistogramInterval());

        SearchRequestBuilder searchRequestBuilder = new SearchRequestBuilder(client, SearchAction.INSTANCE)
                .setIndices(context.indices)
                .setTypes(context.types)
                .setSize(0)
                .setQuery(ExtractorUtils.wrapInTimeRangeQuery(context.query, context.timeField, histogramSearchStartTime, context.end));

        context.aggs.getAggregatorFactories().forEach(searchRequestBuilder::addAggregation);
        context.aggs.getPipelineAggregatorFactories().forEach(searchRequestBuilder::addAggregation);
        return searchRequestBuilder;
    }

}
