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

import java.util.AbstractCollection;
import java.util.AbstractSet;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.BiPredicate;
import java.util.function.Consumer;
import java.util.function.Predicate;

/**
 * An immutable map implementation based on open hash map.
 * <p>
 * Can be constructed using a {@link #builder()}, or using {@link #builder(ImmutableOpenMap)} (which is an optimized
 * option to copy over existing content and modify it).
 */
public final class ImmutableOpenMap<KType, VType> implements Map<KType, VType> {

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

    private static final class ImmutableEntry<KType, VType> implements Map.Entry<KType, VType> {
        private final KType key;
        private final VType value;

        ImmutableEntry(KType key, VType value) {
            this.key = key;
            this.value = value;
        }

        @Override
        public KType getKey() {
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

        @Override
        @SuppressWarnings("rawtypes")
        public boolean equals(Object o) {
            if (this == o) return true;
            if ((o instanceof Map.Entry) == false) return false;
            Map.Entry that = (Map.Entry) o;
            return Objects.equals(key, that.getKey()) && Objects.equals(value, that.getValue());
        }

        @Override
        public int hashCode() {
            return Objects.hash(key, value);
        }
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
            return new ImmutableEntry<>(obj.key, obj.value);
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
                ImmutableEntry<KType, VType> entry = new ImmutableEntry<>(ooCursor.key, ooCursor.value);
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

    @Override
    @SuppressWarnings("rawtypes")
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ImmutableOpenMap that = (ImmutableOpenMap) o;

        if (map.equals(that.map) == false) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return map.hashCode();
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

    public static <KType, VType> Builder<KType, VType> builder(ImmutableOpenMap<KType, VType> map) {
        return new Builder<>(map);
    }

    public static class Builder<KType, VType> {
        private ObjectObjectHashMap<KType, VType> map;

        @SuppressWarnings("unchecked")
        public Builder() {
            this(EMPTY);
        }

        public Builder(int size) {
            this.map = new ObjectObjectHashMap<>(size);
        }

        public Builder(ImmutableOpenMap<KType, VType> map) {
            this.map = map.map.clone();
        }

        /**
         * Builds a new instance of the
         */
        public ImmutableOpenMap<KType, VType> build() {
            ObjectObjectHashMap<KType, VType> map = this.map;
            this.map = null; // nullify the map, so any operation post build will fail! (hackish, but safest)
            return map.isEmpty() ? of() : new ImmutableOpenMap<>(map);
        }

        /**
         * Puts all the entries in the map to the builder.
         */
        public Builder<KType, VType> putAllFromMap(Map<KType, VType> map) {
            for (Map.Entry<KType, VType> entry : map.entrySet()) {
                this.map.put(entry.getKey(), entry.getValue());
            }
            return this;
        }

        /**
         * A put operation that can be used in the fluent pattern.
         */
        public Builder<KType, VType> fPut(KType key, VType value) {
            map.put(key, value);
            return this;
        }

        public VType put(KType key, VType value) {
            return map.put(key, value);
        }

        public VType get(KType key) {
            return map.get(key);
        }

        public VType getOrDefault(KType kType, VType vType) {
            return map.getOrDefault(kType, vType);
        }

        public void putAll(Builder<KType, VType> builder) {
            for (var entry : builder.map) {
                map.put(entry.key, entry.value);
            }
        }

        /**
         * Remove that can be used in the fluent pattern.
         */
        public Builder<KType, VType> fRemove(KType key) {
            map.remove(key);
            return this;
        }

        public VType remove(KType key) {
            return map.remove(key);
        }

        public boolean containsKey(KType key) {
            return map.containsKey(key);
        }

        public int size() {
            return map.size();
        }

        public boolean isEmpty() {
            return map.isEmpty();
        }

        public int removeAll(Predicate<? super KType> predicate) {
            return map.removeAll(predicate::test);
        }

        public void removeAllFromCollection(Collection<KType> collection) {
            for (var k : collection) {
                map.remove(k);
            }
        }

        public void clear() {
            map.clear();
        }

        public Set<KType> keys() {
            return new KeySet<>(map.keys());
        }

        @SuppressWarnings("unchecked")
        public <K, V> Builder<K, V> cast() {
            return (Builder) this;
        }

        public int removeAll(BiPredicate<? super KType, ? super VType> predicate) {
            return map.removeAll(predicate::test);
        }

        public int indexOf(KType key) {
            return map.indexOf(key);
        }

        public boolean indexExists(int index) {
            return map.indexExists(index);
        }

        public VType indexGet(int index) {
            return map.indexGet(index);
        }

        public VType indexReplace(int index, VType newValue) {
            return map.indexReplace(index, newValue);
        }

        public void indexInsert(int index, KType key, VType value) {
            map.indexInsert(index, key, value);
        }

        public void release() {
            map.release();
        }

        public String visualizeKeyDistribution(int characters) {
            return map.visualizeKeyDistribution(characters);
        }
    }
}
