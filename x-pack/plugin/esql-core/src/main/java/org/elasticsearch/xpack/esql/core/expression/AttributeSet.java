/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.core.expression;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Set;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.function.Function;
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
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean remove(Object o) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean addAll(Collection<? extends Attribute> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void clear() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Spliterator<Attribute> spliterator() {
        return delegate.keySet().spliterator();
    }

    @Override
    public boolean removeIf(Predicate<? super Attribute> filter) {
        throw new UnsupportedOperationException();
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
    public boolean equals(Object obj) {
        if (obj instanceof AttributeSet as) {
            obj = as.delegate;
        }

        return delegate.equals(obj);
    }

    @Override
    public int hashCode() {
        return delegate.hashCode();
    }

    @Override
    public String toString() {
        return delegate.keySet().toString();
    }

    public Builder asBuilder() {
        return builder(size()).addAll(this);
    }

    public static AttributeSet of(Attribute... attrs) {
        final AttributeMap.Builder<Object> mapBuilder = AttributeMap.builder(attrs.length);
        for (var a : attrs) {
            mapBuilder.put(a, PRESENT);
        }
        return new AttributeSet(mapBuilder.build());
    }

    public static AttributeSet of(Collection<? extends Attribute> c) {
        final AttributeMap.Builder<Object> mapBuilder = AttributeMap.builder(c.size());
        for (var a : c) {
            mapBuilder.put(a, PRESENT);
        }
        return new AttributeSet(mapBuilder.build());
    }

    public static <T> AttributeSet of(Collection<T> sources, Function<T, Collection<Attribute>> mapper) {
        if (sources.isEmpty()) {
            return AttributeSet.EMPTY;
        }
        var size = 0;
        var attributeSets = new ArrayList<Collection<Attribute>>(sources.size());
        for (T source : sources) {
            var attrs = mapper.apply(source);
            size += attrs.size();
            attributeSets.add(attrs);
        }
        var builder = AttributeSet.builder(size);
        for (Collection<Attribute> attributeSet : attributeSets) {
            builder.addAll(attributeSet);
        }
        return builder.build();
    }

    public static Builder builder() {
        return new Builder(AttributeMap.builder());
    }

    public static Builder builder(int expectedSize) {
        return new Builder(AttributeMap.builder(expectedSize));
    }

    public static class Builder {

        private final AttributeMap.Builder<Object> mapBuilder;

        private Builder(AttributeMap.Builder<Object> mapBuilder) {
            this.mapBuilder = mapBuilder;
        }

        public Builder add(Attribute attr) {
            mapBuilder.put(attr, PRESENT);
            return this;
        }

        public boolean remove(Object o) {
            return mapBuilder.remove(o) != null;
        }

        public Builder addAll(AttributeSet other) {
            mapBuilder.putAll(other.delegate);
            return this;
        }

        public Builder addAll(Builder other) {
            mapBuilder.putAll(other.mapBuilder.build());
            return this;
        }

        public Builder addAll(Collection<? extends Attribute> c) {
            for (var e : c) {
                mapBuilder.put(e, PRESENT);
            }
            return this;
        }

        public boolean removeIf(Predicate<? super Attribute> filter) {
            return mapBuilder.keySet().removeIf(filter);
        }

        public boolean contains(Object o) {
            return mapBuilder.containsKey(o);
        }

        public boolean isEmpty() {
            return mapBuilder.isEmpty();
        }

        public AttributeSet build() {
            return new AttributeSet(mapBuilder.build());
        }
    }
}
