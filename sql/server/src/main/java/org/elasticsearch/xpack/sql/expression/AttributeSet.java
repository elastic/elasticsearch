/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression;

import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;

public class AttributeSet implements Set<Attribute> {

    public static final AttributeSet EMPTY = new AttributeSet(emptyList());

    private static class AttributeWrapper {

        private final Attribute attr;

        AttributeWrapper(Attribute attr) {
            this.attr = attr;
        }

        @Override
        public int hashCode() {
            return attr.id().hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            if (obj instanceof AttributeWrapper) {
                AttributeWrapper aw = (AttributeWrapper) obj;
                if (attr.getClass() == aw.attr.getClass()) {
                    return attr.id().equals(aw.attr.id());
                }
            }

            return false;
        }

        @Override
        public String toString() {
            return "AttrWrap[" + attr + "]";
        }
    }

    private final Set<AttributeWrapper> delegate;

    public AttributeSet() {
        this.delegate = new LinkedHashSet<>();
    }

    public AttributeSet(Collection<Attribute> attr) {
        if (attr.isEmpty()) {
            this.delegate = emptySet();
        }
        else {
            this.delegate = new LinkedHashSet<>(attr.size());

            for (Attribute a : attr) {
                delegate.add(new AttributeWrapper(a));
            }
        }
    }

    public AttributeSet(Attribute attr) {
        this.delegate = singleton(new AttributeWrapper(attr));
    }

    // package protected - should be called through Expressions to cheaply create 
    // a set from a collection of sets without too much copying
    void addAll(AttributeSet other) {
        this.delegate.addAll(other.delegate);
    }

    public AttributeSet substract(AttributeSet other) {
        AttributeSet diff = new AttributeSet();
        for (AttributeWrapper aw : this.delegate) {
            if (!other.delegate.contains(aw)) {
                diff.delegate.add(aw);
            }
        }

        return diff;
    }

    public AttributeSet intersect(AttributeSet other) {
        AttributeSet smaller = (other.size() > size() ? this : other);
        AttributeSet larger = (smaller == this ? other : this);

        AttributeSet intersect = new AttributeSet();
        for (AttributeWrapper aw : smaller.delegate) {
            if (larger.contains(aw)) {
                intersect.delegate.add(aw);
            }
        }

        return intersect;
    }

    public boolean subsetOf(AttributeSet other) {
        if (this.size() > other.size()) {
            return false;
        }
        for (AttributeWrapper aw : delegate) {
            if (!other.delegate.contains(aw)) {
                return false;
            }
        }

        return true;
    }

    public void forEach(Consumer<? super Attribute> action) {
        delegate.forEach(e -> action.accept(e.attr));
    }

    public int size() {
        return delegate.size();
    }

    public boolean isEmpty() {
        return delegate.isEmpty();
    }

    public boolean contains(Object o) {
        if (o instanceof NamedExpression) {
            return delegate.contains(new AttributeWrapper(((NamedExpression) o).toAttribute()));
        }
        return false;
    }

    public boolean containsAll(Collection<?> c) {
        for (Object o : c) {
            if (!delegate.contains(o)) {
                return false;
            }
        }
        return true;
    }

    public Iterator<Attribute> iterator() {
        return new Iterator<Attribute>() {
            Iterator<AttributeWrapper> internal = delegate.iterator();

            @Override
            public boolean hasNext() {
                return internal.hasNext();
            }

            @Override
            public Attribute next() {
                return internal.next().attr;
            }
        };
    }

    public Object[] toArray() {
        Object[] array = delegate.toArray();
        for (int i = 0; i < array.length; i++) {
            array[i] = ((AttributeWrapper) array[i]).attr;
        }
        return array;
    }

    @SuppressWarnings("unchecked")
    public <T> T[] toArray(T[] a) {
        if (a.length < size())
            a = (T[]) java.lang.reflect.Array.newInstance(a.getClass().getComponentType(), size());
        int i = 0;
        Object[] result = a;
        for (Attribute attr : this) {
            result[i++] = attr;
        }
        if (a.length > size()) {
            a[size()] = null;
        }
        return a;
    }

    public boolean add(Attribute e) {
        throw new UnsupportedOperationException();
    }

    public boolean remove(Object o) {
        throw new UnsupportedOperationException();
    }

    public boolean addAll(Collection<? extends Attribute> c) {
        throw new UnsupportedOperationException();
    }

    public boolean retainAll(Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    public boolean removeAll(Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    public void clear() {
        throw new UnsupportedOperationException();
    }

    public boolean equals(Object o) {
        return delegate.equals(o);
    }

    public int hashCode() {
        return delegate.hashCode();
    }

    public Spliterator<Attribute> spliterator() {
        throw new UnsupportedOperationException();
    }

    public boolean removeIf(Predicate<? super Attribute> filter) {
        throw new UnsupportedOperationException();
    }

    public Stream<Attribute> stream() {
        return delegate.stream().map(e -> e.attr);
    }

    public Stream<Attribute> parallelStream() {
        return delegate.parallelStream().map(e -> e.attr);
    }

    @Override
    public String toString() {
        return delegate.toString();
    }
}