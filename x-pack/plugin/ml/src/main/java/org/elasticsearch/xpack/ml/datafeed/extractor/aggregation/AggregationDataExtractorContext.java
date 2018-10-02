/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.datafeed.extractor.aggregation;

import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.aggregations.AggregatorFactories;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class AggregationDataExtractorContext {

    public final String jobId;
    public final String timeField;
    public final Set<String> fields;
    public final String[] indices;
    public final String[] types;
    public final QueryBuilder query;
    public final AggregatorFactories.Builder aggs;
    public final long start;
    public final long end;
    public final boolean includeDocCount;
    public final Map<String, String> headers;

    public AggregationDataExtractorContext(String jobId, String timeField, Set<String> fields, List<String> indices, List<String> types,
                                    QueryBuilder query, AggregatorFactories.Builder aggs, long start, long end, boolean includeDocCount,
                                    Map<String, String> headers) {
        this.jobId = Objects.requireNonNull(jobId);
        this.timeField = Objects.requireNonNull(timeField);
        this.fields = Objects.requireNonNull(fields);
        this.indices = indices.toArray(new String[indices.size()]);
        this.types = types.toArray(new String[types.size()]);
        this.query = Objects.requireNonNull(query);
        this.aggs = Objects.requireNonNull(aggs);
        this.start = start;
        this.end = end;
        this.includeDocCount = includeDocCount;
        this.headers = headers;
    }
}
