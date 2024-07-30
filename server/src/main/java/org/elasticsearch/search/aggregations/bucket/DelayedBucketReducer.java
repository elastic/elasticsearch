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
 *  Class for reducing a list of {@link B} to a single {@link InternalAggregations}
 *  and the number of documents in a delayable fashion.
 *
 *  This class can be reused by calling {@link #reset(B)}.
 *
 * @see BucketReducer
 */
public final class DelayedBucketReducer<B extends MultiBucketsAggregation.Bucket> {

    private final AggregationReduceContext context;
    // changes at reset time
    private B proto;
    // the maximum size of this array is the number of shards to be reduced. We currently do it in a batches of 256
    // by default. if we expect bigger batches, we might consider to use ObjectArray.
    private final List<InternalAggregations> internalAggregations;
    private long count = 0;

    public DelayedBucketReducer(B proto, AggregationReduceContext context) {
        this.proto = proto;
        this.context = context;
        this.internalAggregations = new ArrayList<>();
    }

    /**
     * Adds a {@link B} for reduction.
     */
    public void accept(B bucket) {
        count += bucket.getDocCount();
        internalAggregations.add(bucket.getAggregations());
    }

    /**
     * returns the bucket prototype.
     */
    public B getProto() {
        return proto;
    }

    /**
     * Reset the content of this reducer.
     */
    public void reset(B proto) {
        this.proto = proto;
        count = 0L;
        internalAggregations.clear();
    }

    /**
     * returns the reduced {@link InternalAggregations}.
     */
    public InternalAggregations getAggregations() {
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
