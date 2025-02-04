/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.aggregations.pipeline;

import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.search.aggregations.AggregationReduceContext;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;

import java.util.Map;

public abstract class SiblingPipelineAggregator extends PipelineAggregator {
    protected SiblingPipelineAggregator(String name, String[] bucketsPaths, Map<String, Object> metadata) {
        super(name, bucketsPaths, metadata);
    }

    @Override
    public InternalAggregation reduce(InternalAggregation aggregation, AggregationReduceContext reduceContext) {
        return aggregation.copyWithRewritenBuckets(
            aggregations -> InternalAggregations.from(
                CollectionUtils.appendToCopyNoNullElements(aggregations.copyResults(), doReduce(aggregations, reduceContext))
            )
        );
    }

    public abstract InternalAggregation doReduce(InternalAggregations aggregations, AggregationReduceContext context);
}
