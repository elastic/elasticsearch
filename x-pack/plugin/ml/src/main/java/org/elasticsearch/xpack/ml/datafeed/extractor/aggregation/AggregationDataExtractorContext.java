/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.datafeed.extractor.aggregation;

import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.xpack.ml.datafeed.extractor.DataExtractorQueryContext;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

class AggregationDataExtractorContext {
    final String jobId;
    final Set<String> fields;
    final AggregatorFactories.Builder aggs;
    final boolean includeDocCount;
    final DataExtractorQueryContext queryContext;

    AggregationDataExtractorContext(
        String jobId,
        String timeField,
        Set<String> fields,
        List<String> indices,
        QueryBuilder query,
        AggregatorFactories.Builder aggs,
        long start,
        long end,
        boolean includeDocCount,
        Map<String, String> headers,
        IndicesOptions indicesOptions,
        Map<String, Object> runtimeMappings
    ) {
        this.jobId = jobId;
        this.fields = Objects.requireNonNull(fields);
        this.aggs = Objects.requireNonNull(aggs);
        this.includeDocCount = includeDocCount;
        this.queryContext = new DataExtractorQueryContext(
            indices,
            query,
            Objects.requireNonNull(timeField),
            start,
            end,
            headers,
            indicesOptions,
            runtimeMappings
        );
    }
}
