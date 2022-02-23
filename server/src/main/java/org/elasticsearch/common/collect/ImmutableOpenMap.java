/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.collect;

import com.carrotsearch.hppc.ObjectCollection;
import com.carrotsearch.hppc.ObjectContainer;
import com.carrotsearch.hppc.ObjectLookupContainer;
import com.carrotsearch.hppc.ObjectObjectAssociativeContainer;
import com.carrotsearch.hppc.ObjectObjectHashMap;
import com.carrotsearch.hppc.ObjectObjectMap;
import com.carrotsearch.hppc.cursors.ObjectCursor;
import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import com.carrotsearch.hppc.predicates.ObjectObjectPredicate;
import com.carrotsearch.hppc.predicates.ObjectPredicate;
import com.carrotsearch.hppc.procedures.ObjectObjectProcedure;

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
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * An immutable map implementation based on open hash map.
 * <p>
 * Can be constructed using a {@link #builder()}, or using {@link #builder(ImmutableOpenMap)} (which is an optimized
 * option to copy over existing content and modify it).
 */
public final class ImmutableOpenMap<KType, VType> implements Iterable<ObjectObjectCursor<KType, VType>> {

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
    public VType get(KType key) {
        return map.get(key);
    }

    /**
     * @return Returns the value associated with the given key or the provided default value if the
     * key is not associated with any value.
     */
    public VType getOrDefault(KType key, VType defaultValue) {
        return map.getOrDefault(key, defaultValue);
    }

    /**
     * Returns <code>true</code> if this container has an association to a value for
     * the given key.
     */
    public boolean containsKey(KType key) {
        return map.containsKey(key);
    }

    /**
     * @return Returns the current size (number of assigned keys) in the container.
     */
    public int size() {
        return map.size();
    }

    /**
     * @return Return <code>true</code> if this hash map contains no assigned keys.
     */
    public boolean isEmpty() {
        return map.isEmpty();
    }

    /**
     * Returns a cursor over the entries (key-value pairs) in this map. The iterator is
     * implemented as a cursor and it returns <b>the same cursor instance</b> on every
     * call to {@link Iterator#next()}. To read the current key and value use the cursor's
     * public fields. An example is shown below.
     * <pre>
     * for (IntShortCursor c : intShortMap)
     * {
     *     System.out.println(&quot;index=&quot; + c.index
     *       + &quot; key=&quot; + c.key
     *       + &quot; value=&quot; + c.value);
     * }
     * </pre>
     * <p>
     * The <code>index</code> field inside the cursor gives the internal index inside
     * the container's implementation. The interpretation of this index depends on
     * to the container.
     */
    @Override
    public Iterator<ObjectObjectCursor<KType, VType>> iterator() {
        return map.iterator();
    }

    public Set<Map.Entry<KType, VType>> entrySet() {
        Set<Map.Entry<KType, VType>> es;
        return (es = entrySet) == null ? (entrySet = new EntrySet()) : es;
    }

    private final class ImmutableEntry implements Map.Entry<KType, VType> {
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
    }

    private final class ConversionIterator implements Iterator<Map.Entry<KType, VType>> {

        private final Iterator<ObjectObjectCursor<KType, VType>> original;

        ConversionIterator() {
            this.original = map.iterator();
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
            return new ImmutableEntry(obj.key, obj.value);
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("removal is unsupported");
        }
    }

    private final class EntrySet extends AbstractSet<Map.Entry<KType, VType>> {
        public int size() {
            return map.size();
        }

        public void clear() {
            throw new UnsupportedOperationException("removal is unsupported");
        }

        public Iterator<Map.Entry<KType, VType>> iterator() {
            return new ConversionIterator();
        }

        @SuppressWarnings("unchecked")
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

        public boolean remove(Object o) {
            throw new UnsupportedOperationException("removal is not supported");
        }

        public Spliterator<Map.Entry<KType, VType>> spliterator() {
            return Spliterators.spliterator(iterator(), size(), Spliterator.SIZED);
        }

        public void forEach(Consumer<? super Map.Entry<KType, VType>> action) {
            map.forEach((Consumer<? super ObjectObjectCursor<KType, VType>>) ooCursor -> {
                ImmutableEntry entry = new ImmutableEntry(ooCursor.key, ooCursor.value);
                action.accept(entry);
            });
        }
    }

    /**
     * Returns a specialized view of the keys of this associated container.
     * The view additionally implements {@link ObjectLookupContainer}.
     */
    public ObjectLookupContainer<KType> keys() {
        return map.keys();
    }

    /**
     * Returns a direct iterator over the keys.
     */
    public Iterator<KType> keysIt() {
        final Iterator<ObjectCursor<KType>> iterator = map.keys().iterator();
        return new Iterator<KType>() {
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

    /**
     * Returns a {@link Set} view of the keys contained in this map.
     */
    public Set<KType> keySet() {
        return new AbstractSet<>() {
            @Override
            public Iterator<KType> iterator() {
                return keysIt();
            }

            @Override
            public int size() {
                return map.size();
            }

            @Override
            @SuppressWarnings("unchecked")
            public boolean contains(Object o) {
                return map.containsKey((KType) o);
            }
        };
    }

    /**
     * Returns a direct iterator over the keys.
     */
    public Iterator<VType> valuesIt() {
        return iterator(map.values());
    }

    /**
     * Returns a {@link Collection} view of the values contained in the map.
     */
    public Collection<VType> values() {
        return new AbstractCollection<VType>() {
            @Override
            public Iterator<VType> iterator() {
                return valuesIt();
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

    /**
     * Returns a sequential unordered stream of the map entries.
     *
     * @return a {@link Stream} of the map entries as {@link Map.Entry}
     */
    public Stream<Map.Entry<KType, VType>> stream() {
        final Iterator<ObjectObjectCursor<KType, VType>> mapIterator = map.iterator();
        return StreamSupport.stream(new Spliterators.AbstractSpliterator<>(map.size(), Spliterator.SIZED | Spliterator.DISTINCT) {
            @Override
            public boolean tryAdvance(Consumer<? super Map.Entry<KType, VType>> action) {
                if (mapIterator.hasNext() == false) {
                    return false;
                }
                ObjectObjectCursor<KType, VType> cursor = mapIterator.next();
                action.accept(new AbstractMap.SimpleImmutableEntry<>(cursor.key, cursor.value));
                return true;
            }
        }, false);
    }

    @Override
    public String toString() {
        return map.toString();
    }

    /**
     * Convert this ImmutableOpenMap to an immutable Java collection Map
     */
    public Map<KType, VType> toMap() {
        return entrySet().stream().collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue));
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

    /**
     * @return  An immutable copy of the given map
     */
    public static <KType, VType> ImmutableOpenMap<KType, VType> copyOf(ObjectObjectMap<KType, VType> map) {
        Builder<KType, VType> builder = builder();
        builder.putAll(map);
        return builder.build();
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

    public static class Builder<KType, VType> implements ObjectObjectMap<KType, VType> {
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
        public Builder<KType, VType> putAll(Map<KType, VType> map) {
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

        @Override
        public VType put(KType key, VType value) {
            return map.put(key, value);
        }

        @Override
        public VType get(KType key) {
            return map.get(key);
        }

        @Override
        public VType getOrDefault(KType kType, VType vType) {
            return map.getOrDefault(kType, vType);
        }

        @Override
        public int putAll(ObjectObjectAssociativeContainer<? extends KType, ? extends VType> container) {
            return map.putAll(container);
        }

        @Override
        public int putAll(Iterable<? extends ObjectObjectCursor<? extends KType, ? extends VType>> iterable) {
            return map.putAll(iterable);
        }

        /**
         * Remove that can be used in the fluent pattern.
         */
        public Builder<KType, VType> fRemove(KType key) {
            map.remove(key);
            return this;
        }

        @Override
        public VType remove(KType key) {
            return map.remove(key);
        }

        @Override
        public Iterator<ObjectObjectCursor<KType, VType>> iterator() {
            return map.iterator();
        }

        @Override
        public boolean containsKey(KType key) {
            return map.containsKey(key);
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
        public int removeAll(ObjectContainer<? super KType> container) {
            return map.removeAll(container);
        }

        @Override
        public int removeAll(ObjectPredicate<? super KType> predicate) {
            return map.removeAll(predicate);
        }

        @Override
        public <T extends ObjectObjectProcedure<? super KType, ? super VType>> T forEach(T procedure) {
            return map.forEach(procedure);
        }

        @Override
        public void clear() {
            map.clear();
        }

        @Override
        public ObjectCollection<KType> keys() {
            return map.keys();
        }

        @Override
        public ObjectContainer<VType> values() {
            return map.values();
        }

        @SuppressWarnings("unchecked")
        public <K, V> Builder<K, V> cast() {
            return (Builder) this;
        }

        @Override
        public int removeAll(ObjectObjectPredicate<? super KType, ? super VType> predicate) {
            return map.removeAll(predicate);
        }

        @Override
        public <T extends ObjectObjectPredicate<? super KType, ? super VType>> T forEach(T predicate) {
            return map.forEach(predicate);
        }

        @Override
        public int indexOf(KType key) {
            return map.indexOf(key);
        }

        @Override
        public boolean indexExists(int index) {
            return map.indexExists(index);
        }

        @Override
        public VType indexGet(int index) {
            return map.indexGet(index);
        }

        @Override
        public VType indexReplace(int index, VType newValue) {
            return map.indexReplace(index, newValue);
        }

        @Override
        public void indexInsert(int index, KType key, VType value) {
            map.indexInsert(index, key, value);
        }

        @Override
        public void release() {
            map.release();
        }

        @Override
        public String visualizeKeyDistribution(int characters) {
            return map.visualizeKeyDistribution(characters);
        }
    }
}
