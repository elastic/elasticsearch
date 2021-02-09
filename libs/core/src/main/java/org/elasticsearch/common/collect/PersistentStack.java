/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.collect;

import java.util.Collections;
import java.util.Iterator;
import java.util.NoSuchElementException;

public abstract class PersistentStack<V> implements Iterable<V> {

    private static final PersistentStack<?> EMPTY = new Empty<>();

    @SuppressWarnings("unchecked")
    public static <V> PersistentStack<V> empty() {
        return (PersistentStack<V>) EMPTY;
    }

    private PersistentStack() {
    }

    public abstract V head();

    public abstract PersistentStack<V> tail();

    public abstract boolean isEmpty();

    public abstract int size();

    public PersistentStack<V> push(V item) {
        return new Head<>(item, this);
    }

    private static final class Head<V> extends PersistentStack<V> {

        private final V head;
        private final int size;
        private final PersistentStack<V> tail;

        private Head(V head, PersistentStack<V> tail) {
            this.head = head;
            this.tail = tail;
            this.size = tail.size() + 1;
        }

        @Override
        public V head() {
            return head;
        }

        @Override
        public PersistentStack<V> tail() {
            return tail;
        }

        @Override
        public boolean isEmpty() {
            return false;
        }

        @Override
        public int size() {
            return size;
        }

        @Override
        public Iterator<V> iterator() {
            return new PersistentStackIterator<>(this);
        }
    }

    private static final class Empty<V> extends PersistentStack<V> {

        @Override
        public V head() {
            throw new NoSuchElementException();
        }

        @Override
        public PersistentStack<V> tail() {
            return empty();
        }

        @Override
        public boolean isEmpty() {
            return true;
        }

        @Override
        public int size() {
            return 0;
        }

        @Override
        public Iterator<V> iterator() {
            return Collections.emptyIterator();
        }
    }

    private static final class PersistentStackIterator<V> implements Iterator<V> {
        PersistentStack<V> curr;

        private PersistentStackIterator(PersistentStack<V> curr) {
            this.curr = curr;
        }

        @Override
        public boolean hasNext() {
            return curr.isEmpty() == false;
        }

        @Override
        public V next() {
            V h = curr.head();
            curr = curr.tail();
            return h;
        }
    }
}
