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

import java.util.Comparator;

public class BucketPriorityQueue<B> extends ObjectArrayPriorityQueue<BucketAndOrd<B>> {

    private final Comparator<BucketAndOrd<B>> comparator;

    public BucketPriorityQueue(int size, BigArrays bigArrays, Comparator<BucketAndOrd<B>> comparator) {
        super(size, bigArrays);
        this.comparator = comparator;
    }

    @Override
    protected boolean lessThan(BucketAndOrd<B> a, BucketAndOrd<B> b) {
        return comparator.compare(a, b) > 0; // reverse, since we reverse again when adding to a list
    }
}
