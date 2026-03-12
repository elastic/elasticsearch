/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.search.aggregations.bucket.geogrid;

import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.ObjectArrayPriorityQueue;

import java.util.function.Function;

class BucketPriorityQueue<A, B extends InternalGeoGridBucket> extends ObjectArrayPriorityQueue<A> {

    private final Function<A, B> bucketSupplier;

    BucketPriorityQueue(int size, BigArrays bigArrays, Function<A, B> bucketSupplier) {
        super(size, bigArrays);
        this.bucketSupplier = bucketSupplier;
    }

    @Override
    protected boolean lessThan(A o1, A o2) {
        final B b1 = bucketSupplier.apply(o1);
        final B b2 = bucketSupplier.apply(o2);
        int cmp = Long.compare(b2.getDocCount(), b1.getDocCount());
        if (cmp == 0) {
            cmp = b2.compareTo(b1);
            if (cmp == 0) {
                cmp = System.identityHashCode(o2) - System.identityHashCode(o1);
            }
        }
        return cmp > 0;
    }
}
