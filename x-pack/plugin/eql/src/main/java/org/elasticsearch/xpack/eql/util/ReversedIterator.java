/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.eql.util;

import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

public class ReversedIterator<T> implements Iterator<T> {

    private final ListIterator<T> delegate;

    public ReversedIterator(List<T> delegate) {
        this.delegate = delegate.listIterator(delegate.size());
    }

    @Override
    public boolean hasNext() {
        return delegate.hasPrevious();
    }

    @Override
    public T next() {
        return delegate.previous();
    }

    @Override
    public void remove() {
        delegate.remove();
    }
}
