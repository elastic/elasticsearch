/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.collect;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Objects;

public class Iterators {

    /**
     * Returns a single element iterator over the supplied value.
     */
    public static <T> Iterator<T> single(T element) {
        return new Iterator<T>() {

            private T value = Objects.requireNonNull(element);

            @Override
            public boolean hasNext() {
                return value != null;
            }

            @Override
            public T next() {
                final T res = value;
                value = null;
                return res;
            }
        };
    }

    @SafeVarargs
    @SuppressWarnings("varargs")
    public static <T> Iterator<T> concat(Iterator<? extends T>... iterators) {
        if (iterators == null) {
            throw new NullPointerException("iterators");
        }

        // explicit generic type argument needed for type inference
        return new ConcatenatedIterator<T>(iterators);
    }

    static class ConcatenatedIterator<T> implements Iterator<T> {
        private final Iterator<? extends T>[] iterators;
        private int index = 0;

        @SafeVarargs
        @SuppressWarnings("varargs")
        ConcatenatedIterator(Iterator<? extends T>... iterators) {
            if (iterators == null) {
                throw new NullPointerException("iterators");
            }
            for (int i = 0; i < iterators.length; i++) {
                if (iterators[i] == null) {
                    throw new NullPointerException("iterators[" + i  + "]");
                }
            }
            this.iterators = iterators;
        }

        @Override
        public boolean hasNext() {
            boolean hasNext = false;
            while (index < iterators.length && (hasNext = iterators[index].hasNext()) == false) {
                index++;
            }

            return hasNext;
        }

        @Override
        public T next() {
            if (hasNext() == false) {
                throw new NoSuchElementException();
            }
            return iterators[index].next();
        }
    }
}
