/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.bucket.histogram;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.search.aggregations.AggregationReduceContext;
import org.elasticsearch.search.aggregations.CollectedAggregator;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.aggregations.support.SamplingContext;

import java.util.List;
import java.util.Map;

public class CollectedDateHistogram extends CollectedAggregator {

    public CollectedDateHistogram(String name, Map<String, Object> metadata) {
        super(name, metadata);
    }

    @Override
    public void close() {

    }

    @Override
    public String getWriteableName() {
        return null;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return null;
    }

    @Override
    public CollectedAggregator reduce(List<CollectedAggregator> aggregations, AggregationReduceContext reduceContext) {
        return null;
    }

    @Override
    protected boolean mustReduceOnSingleInternalAgg() {
        return false;
    }

    @Override
    public CollectedAggregator finalizeSampling(SamplingContext samplingContext) {
        return null;
    }

    @Override
    public CollectedAggregator reducePipelines(
        CollectedAggregator agg,
        AggregationReduceContext context,
        PipelineAggregator.PipelineTree pipelines
    ) {
        return null;
    }

    @Override
    public InternalAggregation convertToLegacy(long bucketOrdinal) {
        return null;
    }
}
