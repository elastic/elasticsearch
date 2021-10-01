/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.core;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.ListIterator;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;

public class List {

    /**
     * Returns an unmodifiable list containing zero elements.
     *
     * @param <T> the {@code List}'s element type
     * @return an empty {@code List}
     */
    public static <T> java.util.List<T> of() {
        return Collections.emptyList();
    }

    /**
     * Returns an unmodifiable list containing one element.
     *
     * @param <T> the {@code List}'s element type
     * @param e1  the single element
     * @return a {@code List} containing the specified element
     */
    public static <T> java.util.List<T> of(T e1) {
        return Collections.singletonList(e1);
    }

    /**
     * Returns an unmodifiable list containing two elements.
     *
     * @param <T> the {@code List}'s element type
     * @param e1  the first element
     * @param e2  the second element
     * @return a {@code List} containing the specified element
     */
    @SuppressWarnings("unchecked")
    public static <T> java.util.List<T> of(T e1, T e2) {
        return List.of((T[]) new Object[]{e1, e2});
    }

    /**
     * Returns an unmodifiable list containing an arbitrary number of elements.
     *
     * @param entries the elements to be contained in the list
     * @param <T>     the {@code List}'s element type
     * @return an unmodifiable list containing the specified elements.
     */
    @SafeVarargs
    @SuppressWarnings("varargs")
    public static <T> java.util.List<T> of(T... entries) {
        switch (entries.length) {
            case 0:
                return List.of();
            case 1:
                return List.of(entries[0]);
            default:
                return new ImmutableList<>(Collections.unmodifiableList(Arrays.asList(entries)));
        }
    }

    /**
     * Returns an unmodifiable {@code List} containing the elements of the given {@code Collection} in iteration order.
     *
     * @param <T>  the {@code List}'s element type
     * @param coll a {@code Collection} from which elements are drawn, must be non-null
     * @return a {@code List} containing the elements of the given {@code Collection}
     */
    @SuppressWarnings("unchecked")
    public static <T> java.util.List<T> copyOf(Collection<? extends T> coll) {
        if (coll.getClass() == ImmutableList.class) {
            return (java.util.List<T>) coll;
        }
        return (java.util.List<T>) List.of(coll.toArray());
    }

    private static class ImmutableList<E> implements java.util.List<E> {

        private final java.util.List<E> list;

        private ImmutableList(java.util.List<E> list) {
            this.list = list;
        }

        @Override
        public int size() {
            return list.size();
        }

        @Override
        public boolean isEmpty() {
            return list.isEmpty();
        }

        @Override
        public boolean contains(Object o) {
            return list.contains(o);
        }

        @Override
        public Iterator<E> iterator() {
            return list.iterator();
        }

        @Override
        public Object[] toArray() {
            return list.toArray();
        }

        @Override
        public <T> T[] toArray(T[] a) {
            return list.toArray(a);
        }

        @Override
        public boolean add(E e) {
            return list.add(e);
        }

        @Override
        public boolean remove(Object o) {
            return list.remove(o);
        }

        @Override
        public boolean containsAll(Collection<?> c) {
            return list.containsAll(c);
        }

        @Override
        public boolean addAll(Collection<? extends E> c) {
            return list.addAll(c);
        }

        @Override
        public boolean addAll(int index, Collection<? extends E> c) {
            return list.addAll(index, c);
        }

        @Override
        public boolean removeAll(Collection<?> c) {
            return list.removeAll(c);
        }

        @Override
        public boolean retainAll(Collection<?> c) {
            return list.retainAll(c);
        }

        @Override
        public void replaceAll(UnaryOperator<E> operator) {
            list.replaceAll(operator);
        }

        @Override
        public void sort(Comparator<? super E> c) {
            list.sort(c);
        }

        @Override
        public void clear() {
            list.clear();
        }

        @Override
        public boolean equals(Object o) {
            return list.equals(o);
        }

        @Override
        public int hashCode() {
            return list.hashCode();
        }

        @Override
        public E get(int index) {
            return list.get(index);
        }

        @Override
        public E set(int index, E element) {
            return list.set(index, element);
        }

        @Override
        public void add(int index, E element) {
            list.add(index, element);
        }

        @Override
        public E remove(int index) {
            return list.remove(index);
        }

        @Override
        public int indexOf(Object o) {
            return list.indexOf(o);
        }

        @Override
        public int lastIndexOf(Object o) {
            return list.lastIndexOf(o);
        }

        @Override
        public ListIterator<E> listIterator() {
            return list.listIterator();
        }

        @Override
        public ListIterator<E> listIterator(int index) {
            return list.listIterator(index);
        }

        @Override
        public java.util.List<E> subList(int fromIndex, int toIndex) {
            return list.subList(fromIndex, toIndex);
        }

        @Override
        public Spliterator<E> spliterator() {
            return list.spliterator();
        }

        @Override
        public boolean removeIf(Predicate<? super E> filter) {
            return list.removeIf(filter);
        }

        @Override
        public Stream<E> stream() {
            return list.stream();
        }

        @Override
        public Stream<E> parallelStream() {
            return list.parallelStream();
        }

        @Override
        public void forEach(Consumer<? super E> action) {
            list.forEach(action);
        }
    }
}
