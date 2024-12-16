/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.core.expression;

import java.util.Collection;
import java.util.Iterator;
import java.util.Set;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Stream;

/**
 * Set variant of {@link AttributeMap} - please see that class Javadoc.
 */
public class AttributeSet implements Set<Attribute> {

    public static final AttributeSet EMPTY = new AttributeSet(AttributeMap.emptyAttributeMap());

    // use the same name as in HashSet
    private static final Object PRESENT = new Object();

    private final AttributeMap<Object> delegate;

    public AttributeSet() {
        delegate = new AttributeMap<>();
    }

    public AttributeSet(Attribute attr) {
        delegate = new AttributeMap<>(attr, PRESENT);
    }

    public AttributeSet(Collection<? extends Attribute> attr) {
        delegate = new AttributeMap<>();

        for (Attribute a : attr) {
            delegate.add(a, PRESENT);
        }
    }

    private AttributeSet(AttributeMap<Object> delegate) {
        this.delegate = delegate;
    }

    public AttributeSet combine(AttributeSet other) {
        return new AttributeSet(delegate.combine(other.delegate));
    }

    public AttributeSet subtract(AttributeSet other) {
        return new AttributeSet(delegate.subtract(other.delegate));
    }

    public AttributeSet intersect(AttributeSet other) {
        return new AttributeSet(delegate.intersect(other.delegate));
    }

    public boolean subsetOf(AttributeSet other) {
        return delegate.subsetOf(other.delegate);
    }

    public Set<String> names() {
        return delegate.attributeNames();
    }

    @Override
    public void forEach(Consumer<? super Attribute> action) {
        delegate.forEach((k, v) -> action.accept(k));
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
        return delegate.containsKey(o);
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        for (Object o : c) {
            if (delegate.containsKey(o) == false) {
                return false;
            }
        }
        return true;
    }

    @Override
    public Iterator<Attribute> iterator() {
        return delegate.keySet().iterator();
    }

    @Override
    public Object[] toArray() {
        return delegate.keySet().toArray();
    }

    @Override
    public <T> T[] toArray(T[] a) {
        return delegate.keySet().toArray(a);
    }

    @Override
    public boolean add(Attribute e) {
        return delegate.put(e, PRESENT) == null;
    }

    @Override
    public boolean remove(Object o) {
        return delegate.remove(o) != null;
    }

    public void addAll(AttributeSet other) {
        delegate.addAll(other.delegate);
    }

    @Override
    public boolean addAll(Collection<? extends Attribute> c) {
        int size = delegate.size();
        for (var e : c) {
            delegate.put(e, PRESENT);
        }
        return delegate.size() != size;
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        return delegate.keySet().removeIf(e -> c.contains(e) == false);
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        int size = delegate.size();
        for (var e : c) {
            delegate.remove(e);
        }
        return delegate.size() != size;
    }

    @Override
    public void clear() {
        delegate.clear();
    }

    @Override
    public Spliterator<Attribute> spliterator() {
        return delegate.keySet().spliterator();
    }

    @Override
    public boolean removeIf(Predicate<? super Attribute> filter) {
        return delegate.keySet().removeIf(filter);
    }

    @Override
    public Stream<Attribute> stream() {
        return delegate.keySet().stream();
    }

    @Override
    public Stream<Attribute> parallelStream() {
        return delegate.keySet().parallelStream();
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
        return delegate.keySet().toString();
    }
}
