/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.aggregations.bucket.terms;

import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.ObjectArrayPriorityQueue;

public class BucketSignificancePriorityQueue<B extends SignificantTerms.Bucket> extends ObjectArrayPriorityQueue<BucketAndOrd<B>> {

    public BucketSignificancePriorityQueue(int size, BigArrays bigArrays) {
        super(size, bigArrays);
    }

    @Override
    protected boolean lessThan(BucketAndOrd<B> o1, BucketAndOrd<B> o2) {
        return o1.bucket.getSignificanceScore() < o2.bucket.getSignificanceScore();
    }
}
