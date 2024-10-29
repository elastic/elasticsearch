/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.aggregations.bucket;

import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.search.aggregations.AggregationReduceContext;
import org.elasticsearch.search.aggregations.AggregatorsReducer;
import org.elasticsearch.search.aggregations.InternalAggregations;

/**
 *  Class for reducing a list of {@link B} to a single {@link InternalAggregations}
 *  and the number of documents.
 */
public final class BucketReducer<B extends MultiBucketsAggregation.Bucket> implements Releasable {

    private final AggregatorsReducer aggregatorsReducer;
    private final B proto;
    private long count = 0;

    public BucketReducer(B proto, AggregationReduceContext context, int size) {
        this.aggregatorsReducer = new AggregatorsReducer(proto.getAggregations(), context, size);
        this.proto = proto;
    }

    /**
     * Adds a {@link B} for reduction.
     */
    public void accept(B bucket) {
        count += bucket.getDocCount();
        aggregatorsReducer.accept(bucket.getAggregations());
    }

    /**
     * returns the bucket prototype.
     */
    public B getProto() {
        return proto;
    }

    /**
     * returns the reduced {@link InternalAggregations}.
     */
    public InternalAggregations getAggregations() {
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
