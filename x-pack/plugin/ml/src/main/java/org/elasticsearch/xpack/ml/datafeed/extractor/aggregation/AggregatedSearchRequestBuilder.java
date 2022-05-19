/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.datafeed.extractor.aggregation;

import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.builder.SearchSourceBuilder;

/**
 * This is used when building search actions for aggregated data.
 *
 * Implementations can be found for regular searches and rollup searches.
 */
public interface AggregatedSearchRequestBuilder {
    ActionRequestBuilder<SearchRequest, SearchResponse> build(SearchSourceBuilder searchSourceBuilder);
}
