/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest;

import java.util.Collection;
import java.util.Iterator;
import java.util.Set;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.function.IntFunction;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;

/**
 * {@link Set} implementation for use in {@link WriteReportingWrapper}. This reports write mutations directly on this set, or on derived
 * objects such as those returned by calling {@link #iterator}. It does <strong>not</strong> report mutations on elements contained within
 * the set — although the caller may populate the wrapped set with elements which report their own mutations.
 */
public class WriteReportingSet<E> implements Set<E> {

    private final Set<E> delegate;
    private final Consumer<String> reporter;
    private final String label;

    public WriteReportingSet(Set<E> delegate, Consumer<String> reporter, String label) {
        this.delegate = requireNonNull(delegate);
        this.reporter = requireNonNull(reporter);
        this.label = label;
    }

    @Override
    public int size() {
        return delegate.size();
    }

    @Override
    public boolean isEmpty() {
        return delegate.isEmpty();
    }

    @Override
    public boolean contains(Object o) {
        return delegate.contains(o);
    }

    @Override
    public Iterator<E> iterator() {
        return new WriteReportingIterator(delegate.iterator());
    }

    @Override
    public void forEach(Consumer<? super E> action) {
        delegate.forEach(action);
    }

    @Override
    public Object[] toArray() {
        return delegate.toArray();
    }

    @Override
    public <T> T[] toArray(T[] a) {
        return delegate.toArray(a);
    }

    @Override
    public <T> T[] toArray(IntFunction<T[]> generator) {
        return delegate.toArray(generator);
    }

    @Override
    public boolean add(E e) {
        reporter.accept(label + ".add(E)");
        return delegate.add(e);
    }

    @Override
    public boolean remove(Object o) {
        reporter.accept(label + ".remove(Object)");
        return delegate.remove(o);
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        return delegate.containsAll(c);
    }

    @Override
    public boolean addAll(Collection<? extends E> c) {
        reporter.accept(label + ".addAll(Collection<? extends E>)");
        return delegate.addAll(c);
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        reporter.accept(label + ".retainAll(Collection<? extends E>)");
        return delegate.retainAll(c);
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        reporter.accept(label + ".removeAll(Collection<? extends E>)");
        return delegate.removeAll(c);
    }

    @Override
    public boolean removeIf(Predicate<? super E> filter) {
        reporter.accept(label + ".removeIf(Predicate<? super E>)");
        return delegate.removeIf(filter);
    }

    @Override
    public void clear() {
        reporter.accept(label + ".clear()");
        delegate.clear();
    }

    @Override
    public Spliterator<E> spliterator() {
        return delegate.spliterator();
    }

    @Override
    public Stream<E> stream() {
        return delegate.stream();
    }

    @Override
    public Stream<E> parallelStream() {
        return delegate.parallelStream();
    }

    @Override
    public boolean equals(Object o) {
        return delegate.equals(o);
    }

    @Override
    public int hashCode() {
        return delegate.hashCode();
    }

    @Override
    public String toString() {
        return delegate.toString();
    }

    private class WriteReportingIterator implements Iterator<E> {

        private final Iterator<E> iteratorDelegate;

        WriteReportingIterator(Iterator<E> iteratorDelegate) {
            this.iteratorDelegate = iteratorDelegate;
        }

        @Override
        public boolean hasNext() {
            return iteratorDelegate.hasNext();
        }

        @Override
        public E next() {
            return iteratorDelegate.next();
        }

        @Override
        public void remove() {
            reporter.accept(label + ".iterator()...remove()");
            iteratorDelegate.remove();
        }

        @Override
        public void forEachRemaining(Consumer<? super E> action) {
            iteratorDelegate.forEachRemaining(action);
        }
    }
}
