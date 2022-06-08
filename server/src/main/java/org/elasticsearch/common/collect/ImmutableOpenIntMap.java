/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.collect;

import com.carrotsearch.hppc.IntObjectHashMap;
import com.carrotsearch.hppc.cursors.IntCursor;
import com.carrotsearch.hppc.cursors.IntObjectCursor;
import com.carrotsearch.hppc.cursors.ObjectCursor;

import java.util.AbstractCollection;
import java.util.AbstractSet;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.Consumer;

/**
 * An immutable map implementation based on open hash map.
 * <p>
 * Can be constructed using a {@link #builder()}, or using {@link #builder(org.elasticsearch.common.collect.ImmutableOpenIntMap)}
 * (which is an optimized option to copy over existing content and modify it).
 */
public final class ImmutableOpenIntMap<VType> implements Map<Integer, VType> {

    private final IntObjectHashMap<VType> map;

    /**
     * Holds cached entrySet().
     */
    private Set<Map.Entry<Integer, VType>> entrySet;
    private Set<Integer> keySet;

    private ImmutableOpenIntMap(IntObjectHashMap<VType> map) {
        this.map = map;
    }

    @Override
    public int size() {
        return map.size();
    }

    @Override
    public boolean isEmpty() {
        return map.isEmpty();
    }

    @Override
    public boolean containsKey(Object key) {
        return key instanceof Integer i && map.containsKey(i);
    }

    @Override
    public boolean containsValue(Object value) {
        for (ObjectCursor<VType> cursor : map.values()) {
            if (Objects.equals(cursor.value, value)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public VType get(Object key) {
        if (key instanceof Integer k) {
            return map.get(k);
        }
        return null;
    }

    @Override
    public VType put(Integer key, VType value) {
        throw new UnsupportedOperationException("modification is not supported");
    }

    @Override
    public VType remove(Object key) {
        throw new UnsupportedOperationException("modification is not supported");
    }

    @Override
    public void putAll(Map<? extends Integer, ? extends VType> m) {
        throw new UnsupportedOperationException("modification is not supported");
    }

    @Override
    public void clear() {
        throw new UnsupportedOperationException("modification is not supported");
    }

    @Override
    public Set<Integer> keySet() {
        if (keySet == null) {
            keySet = new KeySet<>(map.keys());
        }
        return keySet;
    }

    @Override
    public Collection<VType> values() {
        return new AbstractCollection<VType>() {
            @Override
            public Iterator<VType> iterator() {
                return ImmutableOpenMap.iterator(map.values());
            }

            @Override
            public int size() {
                return map.size();
            }
        };
    }

    @Override
    public Set<Map.Entry<Integer, VType>> entrySet() {
        Set<Map.Entry<Integer, VType>> es;
        return (es = entrySet) == null ? (entrySet = new EntrySet()) : es;
    }

    private final class ImmutableEntry implements Map.Entry<Integer, VType> {
        private final int key;
        private final VType value;

        ImmutableEntry(int key, VType value) {
            this.key = key;
            this.value = value;
        }

        @Override
        public Integer getKey() {
            return key;
        }

        @Override
        public VType getValue() {
            return value;
        }

        @Override
        public VType setValue(VType value) {
            throw new UnsupportedOperationException("collection is immutable");
        }
    }

    private final class EntryIterator implements Iterator<Map.Entry<Integer, VType>> {

        private final Iterator<IntObjectCursor<VType>> original;

        EntryIterator() {
            this.original = map.iterator();
        }

        @Override
        public boolean hasNext() {
            return original.hasNext();
        }

        @Override
        public Map.Entry<Integer, VType> next() {
            final IntObjectCursor<VType> obj = original.next();
            if (obj == null) {
                return null;
            }
            return new ImmutableEntry(obj.key, obj.value);
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("removal is unsupported");
        }
    }

    private abstract class UnmodifiableSetView<T> extends AbstractSet<T> {

        @Override
        public int size() {
            return map.size();
        }

        @Override
        public Spliterator<T> spliterator() {
            return Spliterators.spliterator(iterator(), size(), Spliterator.SIZED);
        }

        @Override
        public void clear() {
            throw new UnsupportedOperationException("removal is not supported");
        }

        @Override
        public boolean remove(Object o) {
            throw new UnsupportedOperationException("removal is not supported");
        }
    }

    private final class EntrySet extends UnmodifiableSetView<Map.Entry<Integer, VType>> {

        public Iterator<Map.Entry<Integer, VType>> iterator() {
            return new EntryIterator();
        }

        @SuppressWarnings("unchecked")
        public boolean contains(Object o) {
            if (o instanceof Map.Entry<?, ?> == false) {
                return false;
            }
            Map.Entry<Integer, ?> e = (Map.Entry<Integer, ?>) o;
            int key = e.getKey();
            if (map.containsKey(key) == false) {
                return false;
            }
            Object val = map.get(key);
            return Objects.equals(val, e.getValue());
        }

        public void forEach(Consumer<? super Map.Entry<Integer, VType>> action) {
            map.forEach((Consumer<? super IntObjectCursor<VType>>) cursor -> {
                ImmutableEntry entry = new ImmutableEntry(cursor.key, cursor.value);
                action.accept(entry);
            });
        }
    }

    private static final class KeySet<VType> extends AbstractSet<Integer> {
        private final IntObjectHashMap<VType>.KeysContainer keys;

        KeySet(IntObjectHashMap<VType>.KeysContainer keys) {
            this.keys = keys;
        }

        @Override
        public Iterator<Integer> iterator() {
            final Iterator<IntCursor> cursor = keys.iterator();
            return new Iterator<>() {
                @Override
                public boolean hasNext() {
                    return cursor.hasNext();
                }

                @Override
                public Integer next() {
                    return cursor.next().value;
                }

                @Override
                public void remove() {
                    throw new UnsupportedOperationException("removal is not supported");
                }
            };
        }

        @Override
        public int size() {
            return keys.size();
        }

        @Override
        public boolean contains(Object o) {
            return o instanceof Integer i && keys.contains(i);
        }
    }

    @Override
    public String toString() {
        return map.toString();
    }

    @Override
    @SuppressWarnings("rawtypes")
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ImmutableOpenIntMap that = (ImmutableOpenIntMap) o;

        if (map.equals(that.map) == false) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return map.hashCode();
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    private static final ImmutableOpenIntMap EMPTY = new ImmutableOpenIntMap(new IntObjectHashMap());

    @SuppressWarnings("unchecked")
    public static <VType> ImmutableOpenIntMap<VType> of() {
        return EMPTY;
    }

    public static <VType> Builder<VType> builder() {
        return new Builder<>();
    }

    public static <VType> Builder<VType> builder(int size) {
        return new Builder<>(size);
    }

    public static <VType> Builder<VType> builder(ImmutableOpenIntMap<VType> map) {
        return new Builder<>(map);
    }

    public static class Builder<VType> {

        private IntObjectHashMap<VType> map;

        @SuppressWarnings("unchecked")
        public Builder() {
            this(EMPTY);
        }

        public Builder(int size) {
            this.map = new IntObjectHashMap<>(size);
        }

        public Builder(ImmutableOpenIntMap<VType> map) {
            this.map = map.map.clone();
        }

        /**
         * Builds a new ImmutableOpenIntMap from this builder.
         */
        public ImmutableOpenIntMap<VType> build() {
            IntObjectHashMap<VType> map = this.map;
            this.map = null; // nullify the map, so any operation post build will fail! (hackish, but safest)
            return map.isEmpty() ? of() : new ImmutableOpenIntMap<>(map);
        }

        /**
         * Puts all the entries in the map to the builder.
         */
        public Builder<VType> putAll(Map<Integer, VType> map) {
            for (Map.Entry<Integer, VType> entry : map.entrySet()) {
                this.map.put(entry.getKey(), entry.getValue());
            }
            return this;
        }

        /**
         * A put operation that can be used in the fluent pattern.
         */
        public Builder<VType> fPut(int key, VType value) {
            map.put(key, value);
            return this;
        }

        public VType put(int key, VType value) {
            return map.put(key, value);
        }

        public VType get(int key) {
            return map.get(key);
        }

        public VType getOrDefault(int kType, VType vType) {
            return map.getOrDefault(kType, vType);
        }

        /**
         * Remove that can be used in the fluent pattern.
         */
        public Builder<VType> fRemove(int key) {
            map.remove(key);
            return this;
        }

        public VType remove(int key) {
            return map.remove(key);
        }

        public boolean containsKey(int key) {
            return map.containsKey(key);
        }

        public boolean isEmpty() {
            return map.isEmpty();
        }

        public Set<Integer> keys() {
            return new KeySet<>(map.keys());
        }
    }
}
