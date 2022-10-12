/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.collect;

import com.carrotsearch.hppc.ObjectCollection;
import com.carrotsearch.hppc.ObjectObjectHashMap;
import com.carrotsearch.hppc.cursors.ObjectCursor;
import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import com.carrotsearch.hppc.procedures.ObjectObjectProcedure;

import org.elasticsearch.common.util.Maps;

import java.util.AbstractCollection;
import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.BiConsumer;
import java.util.function.BiPredicate;
import java.util.function.Consumer;

/**
 * An immutable map implementation based on open hash map.
 * <p>
 * Can be constructed using a {@link #builder()}, or using {@link #builder(Map)} (which is an optimized
 * option to copy over existing content and modify it).
 */
public final class ImmutableOpenMap<KType, VType> extends AbstractMap<KType, VType> {

    private final ObjectObjectHashMap<KType, VType> map;

    /**
     * Holds cached entrySet().
     */
    private Set<Map.Entry<KType, VType>> entrySet;

    private ImmutableOpenMap(ObjectObjectHashMap<KType, VType> map) {
        this.map = map;
    }

    /**
     * @return Returns the value associated with the given key or the default value
     * for the key type, if the key is not associated with any value.
     * <p>
     * <b>Important note:</b> For primitive type values, the value returned for a non-existing
     * key may not be the default value of the primitive type (it may be any value previously
     * assigned to that slot).
     */
    @Override
    @SuppressWarnings("unchecked")
    public VType get(Object key) {
        return map.get((KType) key);
    }

    /**
     * @return Returns the value associated with the given key or the provided default value if the
     * key is not associated with any value.
     */
    @Override
    @SuppressWarnings("unchecked")
    public VType getOrDefault(Object key, VType defaultValue) {
        return map.getOrDefault((KType) key, defaultValue);
    }

    /**
     * Returns <code>true</code> if this container has an association to a value for
     * the given key.
     */
    @Override
    @SuppressWarnings("unchecked")
    public boolean containsKey(Object key) {
        return map.containsKey((KType) key);
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
    public VType put(KType key, VType value) {
        throw new UnsupportedOperationException("modification is not supported");
    }

    @Override
    public VType remove(Object key) {
        throw new UnsupportedOperationException("modification is not supported");
    }

    @Override
    public void putAll(Map<? extends KType, ? extends VType> m) {
        throw new UnsupportedOperationException("modification is not supported");
    }

    @Override
    public void clear() {
        throw new UnsupportedOperationException("modification is not supported");
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
    public Set<Map.Entry<KType, VType>> entrySet() {
        Set<Map.Entry<KType, VType>> es;
        return (es = entrySet) == null ? (entrySet = new EntrySet<>(map)) : es;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o instanceof ImmutableOpenMap<?, ?> immutableOpenMap) {
            return map.equals(immutableOpenMap.map);
        }
        return super.equals(o);
    }

    @Override
    public int hashCode() {
        // noop override to make checkstyle happy since we override equals
        return super.hashCode();
    }

    private static final class ConversionIterator<KType, VType> implements Iterator<Map.Entry<KType, VType>> {

        private final Iterator<ObjectObjectCursor<KType, VType>> original;

        ConversionIterator(Iterator<ObjectObjectCursor<KType, VType>> original) {
            this.original = original;
        }

        @Override
        public boolean hasNext() {
            return original.hasNext();
        }

        @Override
        public Map.Entry<KType, VType> next() {
            final ObjectObjectCursor<KType, VType> obj = original.next();
            if (obj == null) {
                return null;
            }
            return new Maps.ImmutableEntry<>(obj.key, obj.value);
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("removal is unsupported");
        }
    }

    private static final class EntrySet<KType, VType> extends AbstractSet<Map.Entry<KType, VType>> {
        private final ObjectObjectHashMap<KType, VType> map;

        private EntrySet(ObjectObjectHashMap<KType, VType> map) {
            this.map = map;
        }

        @Override
        public int size() {
            return map.size();
        }

        @Override
        public void clear() {
            throw new UnsupportedOperationException("removal is unsupported");
        }

        @Override
        public Iterator<Map.Entry<KType, VType>> iterator() {
            return new ConversionIterator<>(map.iterator());
        }

        @SuppressWarnings("unchecked")
        @Override
        public boolean contains(Object o) {
            if (o instanceof Map.Entry<?, ?> == false) {
                return false;
            }
            Map.Entry<?, ?> e = (Map.Entry<?, ?>) o;
            Object key = e.getKey();
            if (map.containsKey((KType) key) == false) {
                return false;
            }
            Object val = map.get((KType) key);
            return Objects.equals(val, e.getValue());
        }

        @Override
        public boolean remove(Object o) {
            throw new UnsupportedOperationException("removal is not supported");
        }

        @Override
        public Spliterator<Map.Entry<KType, VType>> spliterator() {
            return Spliterators.spliterator(iterator(), size(), Spliterator.SIZED);
        }

        @Override
        public void forEach(Consumer<? super Map.Entry<KType, VType>> action) {
            map.forEach((Consumer<? super ObjectObjectCursor<KType, VType>>) ooCursor -> {
                Maps.ImmutableEntry<KType, VType> entry = new Maps.ImmutableEntry<>(ooCursor.key, ooCursor.value);
                action.accept(entry);
            });
        }
    }

    private static final class KeySet<KType, VType> extends AbstractSet<KType> {

        private final ObjectObjectHashMap<KType, VType>.KeysContainer keys;

        private KeySet(ObjectObjectHashMap<KType, VType>.KeysContainer keys) {
            this.keys = keys;
        }

        @Override
        public Iterator<KType> iterator() {
            final Iterator<ObjectCursor<KType>> iterator = keys.iterator();
            return new Iterator<>() {
                @Override
                public boolean hasNext() {
                    return iterator.hasNext();
                }

                @Override
                public KType next() {
                    return iterator.next().value;
                }

                @Override
                public void remove() {
                    throw new UnsupportedOperationException();
                }
            };
        }

        @Override
        public int size() {
            return keys.size();
        }

        @Override
        @SuppressWarnings("unchecked")
        public boolean contains(Object o) {
            return keys.contains((KType) o);
        }
    };

    @Override
    public Set<KType> keySet() {
        return new KeySet<>(map.keys());
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
    public void forEach(BiConsumer<? super KType, ? super VType> action) {
        map.forEach((ObjectObjectProcedure<KType, VType>) action::accept);
    }

    static <T> Iterator<T> iterator(ObjectCollection<T> collection) {
        final Iterator<ObjectCursor<T>> iterator = collection.iterator();
        return new Iterator<>() {
            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public T next() {
                return iterator.next().value;
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }

    @Override
    public String toString() {
        return map.toString();
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private static final ImmutableOpenMap EMPTY = new ImmutableOpenMap(new ObjectObjectHashMap());

    @SuppressWarnings("unchecked")
    public static <KType, VType> ImmutableOpenMap<KType, VType> of() {
        return EMPTY;
    }

    public static <KType, VType> Builder<KType, VType> builder() {
        return new Builder<>();
    }

    public static <KType, VType> Builder<KType, VType> builder(int size) {
        return new Builder<>(size);
    }

    public static <KType, VType> Builder<KType, VType> builder(Map<KType, VType> map) {
        if (map instanceof ImmutableOpenMap<KType, VType> iom) {
            return new Builder<>(iom);
        }
        Builder<KType, VType> builder = new Builder<>(map.size());
        builder.putAllFromMap(map);
        return builder;
    }

    public static class Builder<KType, VType> {

        // if the Builder was constructed with a reference to an existing ImmutableOpenMap, then this will be non-null
        // (at least until the point where the builder has been used to actually make some changes to the map that is
        // being built -- see maybeCloneMap)
        private ImmutableOpenMap<KType, VType> reference;

        // if the Builder was constructed with a size (only), then this will be non-null (and reference will be null)
        private ObjectObjectHashMap<KType, VType> mutableMap;

        // n.b. a constructor can either set reference or it can set mutableMap, but it must not set both.

        /**
         * This method must be called before reading or writing via the {@code mutableMap} -- so every method
         * of builder should call this as the first thing it does. If {@code reference} is not null, it will be used to
         * populate {@code mutableMap} as a clone of the {@code reference} ImmutableOpenMap. It will then null out
         * {@code reference}.
         *
         * If {@code reference} is already null (and by extension, {@code mutableMap} is already *not* null),
         * then this method is a no-op.
         */
        private void maybeCloneMap() {
            if (reference != null) {
                this.mutableMap = reference.map.clone(); // make a mutable clone of the reference ImmutableOpenMap
                this.reference = null; // and throw away the reference, we now rely on mutableMap
            }
        }

        @SuppressWarnings("unchecked")
        public Builder() {
            this(EMPTY);
        }

        public Builder(int size) {
            this.mutableMap = new ObjectObjectHashMap<>(size);
        }

        public Builder(ImmutableOpenMap<KType, VType> immutableOpenMap) {
            this.reference = Objects.requireNonNull(immutableOpenMap);
        }

        /**
         * Builds a new ImmutableOpenMap from this builder.
         */
        public ImmutableOpenMap<KType, VType> build() {
            if (reference != null) {
                ImmutableOpenMap<KType, VType> reference = this.reference;
                this.reference = null; // null out the reference so that you can't reuse this builder
                return reference;
            } else {
                ObjectObjectHashMap<KType, VType> mutableMap = this.mutableMap;
                this.mutableMap = null; // null out the map so that you can't reuse this builder
                return mutableMap.isEmpty() ? of() : new ImmutableOpenMap<>(mutableMap);
            }
        }

        /**
         * Puts all the entries in the map to the builder.
         */
        public Builder<KType, VType> putAllFromMap(Map<KType, VType> map) {
            maybeCloneMap();
            for (Map.Entry<KType, VType> entry : map.entrySet()) {
                this.mutableMap.put(entry.getKey(), entry.getValue());
            }
            return this;
        }

        /**
         * A put operation that can be used in the fluent pattern.
         */
        public Builder<KType, VType> fPut(KType key, VType value) {
            maybeCloneMap();
            mutableMap.put(key, value);
            return this;
        }

        public VType put(KType key, VType value) {
            maybeCloneMap();
            return mutableMap.put(key, value);
        }

        public VType get(KType key) {
            maybeCloneMap();
            return mutableMap.get(key);
        }

        public VType getOrDefault(KType kType, VType vType) {
            maybeCloneMap();
            return mutableMap.getOrDefault(kType, vType);
        }

        public VType remove(KType key) {
            maybeCloneMap();
            return mutableMap.remove(key);
        }

        public boolean containsKey(KType key) {
            maybeCloneMap();
            return mutableMap.containsKey(key);
        }

        public int size() {
            maybeCloneMap();
            return mutableMap.size();
        }

        public void clear() {
            maybeCloneMap();
            mutableMap.clear();
        }

        public Set<KType> keys() {
            maybeCloneMap();
            return new KeySet<>(mutableMap.keys());
        }

        public int removeAll(BiPredicate<? super KType, ? super VType> predicate) {
            maybeCloneMap();
            return mutableMap.removeAll(predicate::test);
        }

    }
}
