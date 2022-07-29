/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.watcher.support;

import java.lang.reflect.Array;
import java.util.Iterator;

public class ArrayObjectIterator implements Iterator<Object> {

    private final Object array;
    private final int length;
    private int index;

    public ArrayObjectIterator(Object array) {
        this.array = array;
        this.length = Array.getLength(array);
        this.index = 0;
    }

    @Override
    public boolean hasNext() {
        return index < length;
    }

    @Override
    public Object next() {
        return Array.get(array, index++);
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException("array iterator does not support removing elements");
    }

    public static class Iterable implements java.lang.Iterable<Object> {

        private Object array;

        public Iterable(Object array) {
            this.array = array;
        }

        @Override
        public Iterator<Object> iterator() {
            return new ArrayObjectIterator(array);
        }
    }
}
