/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.bucket;

import org.elasticsearch.search.aggregations.AggregationReduceContext;
import org.elasticsearch.search.aggregations.AggregatorsReducer;
import org.elasticsearch.search.aggregations.InternalAggregations;

import java.util.ArrayList;
import java.util.List;

/**
 *  Class for reducing a list of {@link MultiBucketsAggregation.Bucket} to a single
 *  {@link InternalAggregations} and the number of documents in a delayable fashion.
 *
 *  This class can be reused by calling {@link #reset()}.
 *
 * @see MultiBucketAggregatorsReducer
 */
public final class DelayedMultiBucketAggregatorsReducer {

    private final AggregationReduceContext context;
    // the maximum size of this array is the number of shards to be reduced. We currently do it in a batches of 256
    // if we expect bigger batches, we might consider to use ObjectArray.
    private final List<InternalAggregations> internalAggregations;
    long count = 0;

    public DelayedMultiBucketAggregatorsReducer(AggregationReduceContext context) {
        this.context = context;
        this.internalAggregations = new ArrayList<>();
    }

    /**
     * Adds a {@link MultiBucketsAggregation.Bucket} for reduction.
     */
    public void accept(MultiBucketsAggregation.Bucket bucket) {
        count += bucket.getDocCount();
        internalAggregations.add(bucket.getAggregations());
    }

    /**
     * Reset the content of this reducer.
     */
    public void reset() {
        count = 0L;
        internalAggregations.clear();
    }

    /**
     * returns the reduced {@link InternalAggregations}.
     */
    public InternalAggregations get() {
        try (AggregatorsReducer aggregatorsReducer = new AggregatorsReducer(context, internalAggregations.size())) {
            for (InternalAggregations agg : internalAggregations) {
                aggregatorsReducer.accept(agg);
            }
            return aggregatorsReducer.get();
        }
    }

    /**
     * returns the number of docs
     */
    public long getDocCount() {
        return count;
    }
}
