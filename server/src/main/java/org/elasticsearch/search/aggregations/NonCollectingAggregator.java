/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.aggregations;

import org.elasticsearch.common.util.LongArray;
import org.elasticsearch.search.aggregations.support.AggregationContext;

import java.io.IOException;
import java.util.Map;

/**
 * An aggregator that is not collected, this can typically be used when running an aggregation over a field that doesn't have
 * a mapping.
 */
public abstract class NonCollectingAggregator extends AggregatorBase {
    /**
     * Build a {@linkplain NonCollectingAggregator} for any aggregator.
     */
    protected NonCollectingAggregator(
        String name,
        AggregationContext context,
        Aggregator parent,
        AggregatorFactories subFactories,
        Map<String, Object> metadata
    ) throws IOException {
        super(name, subFactories, context, parent, CardinalityUpperBound.NONE, metadata);
    }

    @Override
    public final LeafBucketCollector getLeafCollector(AggregationExecutionContext aggCtx, LeafBucketCollector sub) {
        // the framework will automatically eliminate it
        return LeafBucketCollector.NO_OP_COLLECTOR;
    }

    @Override
    public InternalAggregation[] buildAggregations(LongArray owningBucketOrds) throws IOException {
        return buildAggregations(Math.toIntExact(owningBucketOrds.size()), ordIdx -> buildEmptyAggregation());
    }
}
