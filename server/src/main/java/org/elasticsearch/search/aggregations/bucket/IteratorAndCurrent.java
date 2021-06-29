/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.bucket;

import org.elasticsearch.search.aggregations.InternalMultiBucketAggregation;

import java.util.Iterator;

public class IteratorAndCurrent<B extends InternalMultiBucketAggregation.InternalBucket> implements Iterator<B> {
    private final Iterator<B> iterator;
    private B current;

    public IteratorAndCurrent(Iterator<B> iterator) {
        this.iterator = iterator;
        this.current = iterator.next();
    }

    public Iterator<B> getIterator() {
        return iterator;
    }

    public B current() {
        return current;
    }

    @Override
    public boolean hasNext() {
        return iterator.hasNext();
    }

    @Override
    public B next() {
        return current = iterator.next();
    }
}

