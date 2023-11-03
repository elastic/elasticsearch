/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ql.expression;

import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.xpack.ql.QlIllegalArgumentException;

import java.util.AbstractSet;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.stream.Stream;

import static java.util.Collections.emptyMap;

/**
 * Dedicated map for checking {@link Attribute} equality.
 * This is typically the case when comparing the initial declaration of an Attribute, such as {@link FieldAttribute} with
 * references to it, namely {@link ReferenceAttribute}.
 * Using plain object equality, the two references are difference due to their type however semantically, they are the same.
 * Expressions support semantic equality through {@link Expression#semanticEquals(Expression)} - this map is dedicated solution
 * for attributes as its common case picked up by the plan rules.
 * <p>
 * The map implementation is mutable thus consumers need to be careful NOT to modify the content unless they have ownership.
 * Worth noting the {@link #combine(AttributeMap)}, {@link #intersect(AttributeMap)} and {@link #subtract(AttributeMap)} methods which
 * return copies, decoupled from the input maps. In other words the returned maps can be modified without affecting the input or vice-versa.
 */
public final class AttributeMap<E> implements Map<Attribute, E> {

    static class AttributeWrapper {

        private final Attribute attr;

        AttributeWrapper(Attribute attr) {
            this.attr = attr;
        }

        @Override
        public int hashCode() {
            return attr.semanticHash();
        }

        @Override
        public boolean equals(Object obj) {
            return obj instanceof AttributeWrapper aw ? attr.semanticEquals(aw.attr) : false;
        }

        @Override
        public String toString() {
            return attr.toString();
        }
    }

    /**
     * Set that does unwrapping of keys inside the keySet and iterator.
     */
    private abstract static class UnwrappingSet<W, U> extends AbstractSet<U> {
        private final Set<W> set;

        UnwrappingSet(Set<W> originalSet) {
            set = originalSet;
        }

        @Override
        public Iterator<U> iterator() {
            return new Iterator<>() {
                final Iterator<W> i = set.iterator();

                @Override
                public boolean hasNext() {
                    return i.hasNext();
                }

                @Override
                public U next() {
                    return unwrap(i.next());
                }

                @Override
                public void remove() {
                    i.remove();
                }
            };
        }

        protected abstract U unwrap(W next);

        @Override
        public Stream<U> stream() {
            return set.stream().map(this::unwrap);
        }

        @Override
        public Stream<U> parallelStream() {
            return set.parallelStream().map(this::unwrap);
        }

        @Override
        public int size() {
            return set.size();
        }

        @Override
        public boolean equals(Object o) {
            return set.equals(o);
        }

        @Override
        public int hashCode() {
            return set.hashCode();
        }

        @Override
        public Object[] toArray() {
            Object[] array = set.toArray();
            for (int i = 0; i < array.length; i++) {
                array[i] = ((AttributeWrapper) array[i]).attr;
            }
            return array;
        }

        @Override
        @SuppressWarnings("unchecked")
        public <A> A[] toArray(A[] a) {
            // collection is immutable so use that to our advantage
            if (a.length < size()) {
                a = (A[]) java.lang.reflect.Array.newInstance(a.getClass().getComponentType(), size());
            }
            int i = 0;
            Object[] result = a;
            for (U u : this) {
                result[i++] = u;
            }
            // array larger than size, mark the ending element as null
            if (a.length > size()) {
                a[size()] = null;
            }
            return a;
        }

        @Override
        public String toString() {
            return set.toString();
        }
    }

    @SuppressWarnings("rawtypes")
    private static final AttributeMap EMPTY = new AttributeMap<>(emptyMap());

    @SuppressWarnings("unchecked")
    public static <E> AttributeMap<E> emptyAttributeMap() {
        return EMPTY;
    }

    private final Map<AttributeWrapper, E> delegate;

    private AttributeMap(Map<AttributeWrapper, E> other) {
        delegate = other;
    }

    public AttributeMap() {
        delegate = new LinkedHashMap<>();
    }

    public AttributeMap(Attribute key, E value) {
        delegate = new LinkedHashMap<>();
        add(key, value);
    }

    public AttributeMap<E> combine(AttributeMap<E> other) {
        AttributeMap<E> combine = new AttributeMap<>();
        combine.addAll(this);
        combine.addAll(other);

        return combine;
    }

    public AttributeMap<E> subtract(AttributeMap<E> other) {
        AttributeMap<E> diff = new AttributeMap<>();
        for (Entry<AttributeWrapper, E> entry : this.delegate.entrySet()) {
            if (other.delegate.containsKey(entry.getKey()) == false) {
                diff.delegate.put(entry.getKey(), entry.getValue());
            }
        }

        return diff;
    }

    public AttributeMap<E> intersect(AttributeMap<E> other) {
        AttributeMap<E> smaller = (other.size() > size() ? this : other);
        AttributeMap<E> larger = (smaller == this ? other : this);

        AttributeMap<E> intersect = new AttributeMap<>();
        for (Entry<AttributeWrapper, E> entry : smaller.delegate.entrySet()) {
            if (larger.delegate.containsKey(entry.getKey())) {
                intersect.delegate.put(entry.getKey(), entry.getValue());
            }
        }

        return intersect;
    }

    public boolean subsetOf(AttributeMap<E> other) {
        if (this.size() > other.size()) {
            return false;
        }
        for (AttributeWrapper aw : delegate.keySet()) {
            if (other.delegate.containsKey(aw) == false) {
                return false;
            }
        }

        return true;
    }

    public void add(Attribute key, E value) {
        put(key, value);
    }

    public void addAll(AttributeMap<E> other) {
        putAll(other);
    }

    public Set<String> attributeNames() {
        Set<String> s = Sets.newLinkedHashSetWithExpectedSize(size());

        for (AttributeWrapper aw : delegate.keySet()) {
            s.add(aw.attr.name());
        }
        return s;
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
    public boolean containsKey(Object key) {
        return key instanceof NamedExpression ne ? delegate.containsKey(new AttributeWrapper(ne.toAttribute())) : false;
    }

    @Override
    public boolean containsValue(Object value) {
        return delegate.containsValue(value);
    }

    @Override
    public E get(Object key) {
        return key instanceof NamedExpression ne ? delegate.get(new AttributeWrapper(ne.toAttribute())) : null;
    }

    @Override
    public E getOrDefault(Object key, E defaultValue) {
        return key instanceof NamedExpression ne
            ? delegate.getOrDefault(new AttributeWrapper(ne.toAttribute()), defaultValue)
            : defaultValue;
    }

    public E resolve(Object key) {
        return resolve(key, null);
    }

    public E resolve(Object key, E defaultValue) {
        E value = defaultValue;
        E candidate = null;
        int allowedLookups = 1000;
        while ((candidate = get(key)) != null || containsKey(key)) {
            // instead of circling around, return
            if (candidate == key) {
                return candidate;
            }
            if (--allowedLookups == 0) {
                throw new QlIllegalArgumentException("Potential cycle detected");
            }
            key = candidate;
            value = candidate;
        }
        return value;
    }

    @Override
    public E put(Attribute key, E value) {
        return delegate.put(new AttributeWrapper(key), value);
    }

    @Override
    public void putAll(Map<? extends Attribute, ? extends E> m) {
        for (Entry<? extends Attribute, ? extends E> entry : m.entrySet()) {
            put(entry.getKey(), entry.getValue());
        }
    }

    @Override
    public E remove(Object key) {
        return key instanceof NamedExpression ne ? delegate.remove(new AttributeWrapper(ne.toAttribute())) : null;
    }

    @Override
    public void clear() {
        delegate.clear();
    }

    @Override
    public Set<Attribute> keySet() {
        return new UnwrappingSet<>(delegate.keySet()) {
            @Override
            protected Attribute unwrap(AttributeWrapper next) {
                return next.attr;
            }
        };
    }

    @Override
    public Collection<E> values() {
        return delegate.values();
    }

    @Override
    public Set<Entry<Attribute, E>> entrySet() {
        return new UnwrappingSet<>(delegate.entrySet()) {
            @Override
            protected Entry<Attribute, E> unwrap(final Entry<AttributeWrapper, E> next) {
                return new Entry<>() {
                    @Override
                    public Attribute getKey() {
                        return next.getKey().attr;
                    }

                    @Override
                    public E getValue() {
                        return next.getValue();
                    }

                    @Override
                    public E setValue(E value) {
                        return next.setValue(value);
                    }
                };
            }
        };
    }

    @Override
    public void forEach(BiConsumer<? super Attribute, ? super E> action) {
        delegate.forEach((k, v) -> action.accept(k.attr, v));
    }

    @Override
    public int hashCode() {
        return delegate.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof AttributeMap<?> am) {
            obj = am.delegate;
        }
        return delegate.equals(obj);
    }

    @Override
    public String toString() {
        return delegate.toString();
    }

    public static <E> Builder<E> builder() {
        return new Builder<>();
    }

    public static <E> Builder<E> builder(AttributeMap<E> map) {
        return new Builder<E>().putAll(map);
    }

    public static class Builder<E> {
        private AttributeMap<E> map = new AttributeMap<>();

        private Builder() {}

        public Builder<E> put(Attribute attr, E value) {
            map.add(attr, value);
            return this;
        }

        public Builder<E> putAll(AttributeMap<E> m) {
            map.addAll(m);
            return this;
        }

        public AttributeMap<E> build() {
            return map;
        }
    }
}
