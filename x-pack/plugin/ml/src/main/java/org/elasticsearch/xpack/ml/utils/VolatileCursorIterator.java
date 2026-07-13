/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.utils;

import java.util.Iterator;
import java.util.List;

/**
 * An iterator whose cursor is volatile. The intended usage
 * is to allow safe iteration which is done serially but
 * from potentially different threads. In particular, this
 * allows iterating over a collection via callbacks, where
 * each call deals with the next item and only calls the next
 * callback once it's finished.
 */
public class VolatileCursorIterator<T> implements Iterator<T> {

    private final List<T> items;
    private volatile int cursor;

    public VolatileCursorIterator(List<T> items) {
        this.items = items;
        this.cursor = 0;
    }

    @Override
    public boolean hasNext() {
        return cursor < items.size();
    }

    @Override
    public T next() {
        return items.get(cursor++);
    }
}
