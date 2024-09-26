/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.aggregations.bucket;

import org.elasticsearch.search.aggregations.InternalMultiBucketAggregation;

import java.util.Deque;
import java.util.Iterator;

public class DequeAndCurrent<B extends InternalMultiBucketAggregation.InternalBucket> implements Iterator<B> {
    private final Deque<B> deque;
    private B current;

    public DequeAndCurrent(Deque<B> deque) {
        this.deque = deque;
        this.current = deque.poll();
    }

    public B current() {
        return current;
    }

    @Override
    public boolean hasNext() {
        return deque.isEmpty() == false;
    }

    @Override
    public B next() {
        return current = deque.poll();
    }
}
