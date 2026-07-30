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
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.function.IntFunction;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;

/**
 * {@link List} implementation for use in {@link WriteReportingWrapper}. This reports write mutations directly on this list, or on derived
 * objects such as those returned by calling {@link #iterator} or {@link #subList}. It does <strong>not</strong> report mutations on
 * elements contained within the list — although the caller may populate the wrapped list with elements which report their own mutations.
 */
class WriteReportingList<E> implements List<E> {

    private final List<E> delegate;
    private final Consumer<String> reporter;
    private final String label;

    WriteReportingList(List<E> delegate, Consumer<String> reporter, String label) {
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
    public <E1> E1[] toArray(E1[] a) {
        return delegate.toArray(a);
    }

    @Override
    public <E1> E1[] toArray(IntFunction<E1[]> generator) {
        return delegate.toArray(generator);
    }

    @Override
    public boolean add(E t) {
        reporter.accept(label + ".add(E)");
        return delegate.add(t);
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
    public boolean addAll(int index, Collection<? extends E> c) {
        reporter.accept(label + ".addAll(int, Collection<? extends E>)");
        return delegate.addAll(index, c);
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        reporter.accept(label + ".removeAll(Collection<?>)");
        return delegate.removeAll(c);
    }

    @Override
    public boolean removeIf(Predicate<? super E> filter) {
        reporter.accept(label + ".removeIf(Predicate<? super E>)");
        return delegate.removeIf(filter);
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        reporter.accept(label + ".retainAll(Collection<?>)");
        return delegate.retainAll(c);
    }

    @Override
    public void replaceAll(UnaryOperator<E> operator) {
        reporter.accept(label + ".replaceAll(UnaryOperator<E>)");
        delegate.replaceAll(operator);
    }

    @Override
    public void sort(Comparator<? super E> c) {
        reporter.accept(label + ".sort(Comparator<? super E>)");
        delegate.sort(c);
    }

    @Override
    public void clear() {
        reporter.accept(label + ".clear()");
        delegate.clear();
    }

    @Override
    public E get(int index) {
        return delegate.get(index);
    }

    @Override
    public E set(int index, E element) {
        reporter.accept(label + ".set(int, E)");
        return delegate.set(index, element);
    }

    @Override
    public void add(int index, E element) {
        reporter.accept(label + ".add(int, E)");
        delegate.add(index, element);
    }

    @Override
    public E remove(int index) {
        reporter.accept(label + ".remove(index)");
        return delegate.remove(index);
    }

    @Override
    public int indexOf(Object o) {
        return delegate.indexOf(o);
    }

    @Override
    public int lastIndexOf(Object o) {
        return delegate.lastIndexOf(o);
    }

    @Override
    public ListIterator<E> listIterator() {
        return new WriteReportingListIterator(delegate.listIterator());
    }

    @Override
    public ListIterator<E> listIterator(int index) {
        return new WriteReportingListIterator(delegate.listIterator(index));
    }

    @Override
    public List<E> subList(int fromIndex, int toIndex) {
        return new WriteReportingList<>(delegate.subList(fromIndex, toIndex), reporter, label + ".subList(int, int)");
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
    public void addFirst(E t) {
        reporter.accept(label + ".addFirst(E)");
        delegate.addFirst(t);
    }

    @Override
    public void addLast(E t) {
        reporter.accept(label + ".addLast(E)");
        delegate.addLast(t);
    }

    @Override
    public E getFirst() {
        return delegate.getFirst();
    }

    @Override
    public E getLast() {
        return delegate.getLast();
    }

    @Override
    public E removeFirst() {
        reporter.accept(label + ".removeFirst()");
        return delegate.removeFirst();
    }

    @Override
    public E removeLast() {
        reporter.accept(label + ".removeLast()");
        return delegate.removeLast();
    }

    @Override
    public List<E> reversed() {
        return new WriteReportingList<>(delegate.reversed(), reporter, label + ".reversed()");
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

        protected final Iterator<E> iteratorDelegate;

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

    private class WriteReportingListIterator extends WriteReportingIterator implements ListIterator<E> {

        // N.B. This constructor means we can safely downcast iteratorDelegate to ListIterator<E>:
        WriteReportingListIterator(ListIterator<E> iteratorDelegate) {
            super(iteratorDelegate);
        }

        @Override
        public boolean hasPrevious() {
            return ((ListIterator<E>) iteratorDelegate).hasPrevious();
        }

        @Override
        public E previous() {
            return ((ListIterator<E>) iteratorDelegate).previous();
        }

        @Override
        public int nextIndex() {
            return ((ListIterator<E>) iteratorDelegate).nextIndex();
        }

        @Override
        public int previousIndex() {
            return ((ListIterator<E>) iteratorDelegate).previousIndex();
        }

        @Override
        public void set(E t) {
            reporter.accept(label + ".listIterator()...set(E)");
            ((ListIterator<E>) iteratorDelegate).set(t);
        }

        @Override
        public void add(E t) {
            reporter.accept(label + ".listIterator()...add(E)");
            ((ListIterator<E>) iteratorDelegate).add(t);
        }
    }
}
