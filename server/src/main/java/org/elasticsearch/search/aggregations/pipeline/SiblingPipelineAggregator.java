/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.pipeline;

import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregation.ReduceContext;
import org.elasticsearch.search.aggregations.InternalAggregations;

import java.util.List;
import java.util.Map;

public abstract class SiblingPipelineAggregator extends PipelineAggregator {
    protected SiblingPipelineAggregator(String name, String[] bucketsPaths, Map<String, Object> metadata) {
        super(name, bucketsPaths, metadata);
    }

    @Override
    public InternalAggregation reduce(InternalAggregation aggregation, ReduceContext reduceContext) {
        return aggregation.copyWithRewritenBuckets(aggregations -> {
            List<InternalAggregation> aggs = aggregations.copyResults();
            aggs.add(doReduce(aggregations, reduceContext));
            return InternalAggregations.from(aggs);
        });
    }

    public abstract InternalAggregation doReduce(Aggregations aggregations, ReduceContext context);
}
