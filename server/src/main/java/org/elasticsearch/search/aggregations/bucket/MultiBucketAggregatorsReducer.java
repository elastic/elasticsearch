/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.bucket;

import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.search.aggregations.AggregationReduceContext;
import org.elasticsearch.search.aggregations.AggregatorsReducer;
import org.elasticsearch.search.aggregations.InternalAggregations;

/**
 *  Class for reducing a list of {@link MultiBucketsAggregation.Bucket} to a single
 *  {@link InternalAggregations} and the number of documents.
 */
public final class MultiBucketAggregatorsReducer implements Releasable {

    private final AggregatorsReducer aggregatorsReducer;
    long count = 0;

    public MultiBucketAggregatorsReducer(AggregationReduceContext context, int size) {
        this.aggregatorsReducer = new AggregatorsReducer(context, size);
    }

    /**
     * Adds a {@link MultiBucketsAggregation.Bucket} for reduction.
     */
    public void accept(MultiBucketsAggregation.Bucket bucket) {
        count += bucket.getDocCount();
        aggregatorsReducer.accept(bucket.getAggregations());
    }

    /**
     * returns the reduced {@link InternalAggregations}.
     */
    public InternalAggregations get() {
        return aggregatorsReducer.get();

    }

    /**
     * returns the number of docs
     */
    public long getDocCount() {
        return count;
    }

    @Override
    public void close() {
        Releasables.close(aggregatorsReducer);
    }
}
