/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.datafeed.extractor.aggregation;

import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.aggregations.AggregatorFactories;

import java.util.List;
import java.util.Objects;

class AggregationDataExtractorContext {

    final String jobId;
    final String timeField;
    final String[] indexes;
    final String[] types;
    final QueryBuilder query;
    final AggregatorFactories.Builder aggs;
    final long start;
    final long end;
    final boolean includeDocCount;

    AggregationDataExtractorContext(String jobId, String timeField, List<String> indexes, List<String> types, QueryBuilder query,
                                           AggregatorFactories.Builder aggs, long start, long end, boolean includeDocCount) {
        this.jobId = Objects.requireNonNull(jobId);
        this.timeField = Objects.requireNonNull(timeField);
        this.indexes = indexes.toArray(new String[indexes.size()]);
        this.types = types.toArray(new String[types.size()]);
        this.query = Objects.requireNonNull(query);
        this.aggs = Objects.requireNonNull(aggs);
        this.start = start;
        this.end = end;
        this.includeDocCount = includeDocCount;
    }
}
