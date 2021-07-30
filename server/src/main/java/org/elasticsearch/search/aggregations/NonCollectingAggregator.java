/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations;

import org.apache.lucene.index.LeafReaderContext;
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
    public final LeafBucketCollector getLeafCollector(LeafReaderContext reader, LeafBucketCollector sub) {
        // the framework will automatically eliminate it
        return LeafBucketCollector.NO_OP_COLLECTOR;
    }

    @Override
    public InternalAggregation[] buildAggregations(long[] owningBucketOrds) throws IOException {
        InternalAggregation[] results = new InternalAggregation[owningBucketOrds.length];
        for (int ordIdx = 0; ordIdx < owningBucketOrds.length; ordIdx++) {
            results[ordIdx] = buildEmptyAggregation();
        }
        return results;
    }
}
