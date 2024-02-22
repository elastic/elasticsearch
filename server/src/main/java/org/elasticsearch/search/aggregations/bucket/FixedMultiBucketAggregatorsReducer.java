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
import org.elasticsearch.search.aggregations.InternalAggregations;

import java.util.ArrayList;
import java.util.List;

/**
 *  Class for reducing many fixed lists of {@link B} to a single reduced list.
 *
 */
public abstract class FixedMultiBucketAggregatorsReducer<B extends MultiBucketsAggregation.Bucket> implements Releasable {

    // we could use an ObjectArray here but these arrays are in normally small, so it is not worthy
    private final MultiBucketAggregatorsReducer[] bucketsReducer;
    private final List<B> protoList;

    public FixedMultiBucketAggregatorsReducer(AggregationReduceContext reduceContext, int size, List<B> protoList) {
        reduceContext.consumeBucketsAndMaybeBreak(protoList.size());
        this.protoList = protoList;
        this.bucketsReducer = new MultiBucketAggregatorsReducer[protoList.size()];
        for (int i = 0; i < protoList.size(); ++i) {
            bucketsReducer[i] = new MultiBucketAggregatorsReducer(reduceContext, size);
        }
    }

    /**
     * Adds a list of buckets for reduction. The size of the list must be the same as the size
     * of the list passed on the constructor
     */
    public final void accept(List<B> buckets) {
        assert buckets.size() == protoList.size();
        int i = 0;
        for (B bucket : buckets) {
            bucketsReducer[i++].accept(bucket);
        }
    }

    /**
     * returns the reduced buckets.
     */
    public final List<B> get() {
        final List<B> reduceBuckets = new ArrayList<>(protoList.size());
        for (int i = 0; i < protoList.size(); i++) {
            final B proto = protoList.get(i);
            final MultiBucketAggregatorsReducer reducer = bucketsReducer[i];
            reduceBuckets.add(createBucket(proto, reducer.getDocCount(), reducer.get()));
        }
        return reduceBuckets;
    }

    protected abstract B createBucket(B proto, long focCount, InternalAggregations aggregations);

    @Override
    public final void close() {
        Releasables.close(bucketsReducer);
    }
}
