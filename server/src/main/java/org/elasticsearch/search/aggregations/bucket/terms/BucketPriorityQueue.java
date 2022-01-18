/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.aggregations.bucket.terms;

import org.apache.lucene.util.PriorityQueue;

import java.util.Comparator;

public class BucketPriorityQueue<B> extends PriorityQueue<B> {

    private final Comparator<? super B> comparator;

    public BucketPriorityQueue(int size, Comparator<? super B> comparator) {
        super(size);
        this.comparator = comparator;
    }

    @Override
    protected boolean lessThan(B a, B b) {
        return comparator.compare(a, b) > 0; // reverse, since we reverse again when adding to a list
    }
}
