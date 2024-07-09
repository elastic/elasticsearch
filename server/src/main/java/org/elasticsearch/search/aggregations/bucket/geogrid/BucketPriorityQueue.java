/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.aggregations.bucket.geogrid;

import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.ObjectArrayPriorityQueue;

class BucketPriorityQueue<B extends InternalGeoGridBucket> extends ObjectArrayPriorityQueue<B> {

    BucketPriorityQueue(int size, BigArrays bigArrays) {
        super(size, bigArrays);
    }

    @Override
    protected boolean lessThan(InternalGeoGridBucket o1, InternalGeoGridBucket o2) {
        int cmp = Long.compare(o2.getDocCount(), o1.getDocCount());
        if (cmp == 0) {
            cmp = o2.compareTo(o1);
            if (cmp == 0) {
                cmp = System.identityHashCode(o2) - System.identityHashCode(o1);
            }
        }
        return cmp > 0;
    }
}
