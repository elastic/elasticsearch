/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.common.collect;

import com.carrotsearch.hppc.*;
import com.carrotsearch.hppc.cursors.LongCursor;
import com.carrotsearch.hppc.cursors.LongObjectCursor;
import com.carrotsearch.hppc.cursors.ObjectCursor;
import com.carrotsearch.hppc.predicates.LongPredicate;
import com.carrotsearch.hppc.procedures.LongObjectProcedure;
import com.google.common.collect.UnmodifiableIterator;

import java.util.Iterator;
import java.util.Map;

/**
 * An immutable map implementation based on open hash map.
 * <p/>
 * Can be constructed using a {@link #builder()}, or using {@link #builder(org.elasticsearch.common.collect.ImmutableOpenLongMap)} (which is an optimized
 * option to copy over existing content and modify it).
 */
public final class ImmutableOpenLongMap<VType> implements Iterable<LongObjectCursor<VType>> {

    private final LongObjectOpenHashMap<VType> map;

    private ImmutableOpenLongMap(LongObjectOpenHashMap<VType> map) {
        this.map = map;
    }

    /**
     * @return Returns the value associated with the given key or the default value
     * for the key type, if the key is not associated with any value.
     * <p/>
     * <b>Important note:</b> For primitive type values, the value returned for a non-existing
     * key may not be the default value of the primitive type (it may be any value previously
     * assigned to that slot).
     */
    public VType get(long key) {
        return map.get(key);
    }

    /**
     * Returns <code>true</code> if this container has an association to a value for
     * the given key.
     */
    public boolean containsKey(long key) {
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
     * call to {@link java.util.Iterator#next()}. To read the current key and value use the cursor's
     * public fields. An example is shown below.
     * <pre>
     * for (IntShortCursor c : intShortMap)
     * {
     *     System.out.println(&quot;index=&quot; + c.index
     *       + &quot; key=&quot; + c.key
     *       + &quot; value=&quot; + c.value);
     * }
     * </pre>
     * <p/>
     * <p>The <code>index</code> field inside the cursor gives the internal index inside
     * the container's implementation. The interpretation of this index depends on
     * to the container.
     */
    @Override
    public Iterator<LongObjectCursor<VType>> iterator() {
        return map.iterator();
    }

    /**
     * Returns a specialized view of the keys of this associated container.
     * The view additionally implements {@link com.carrotsearch.hppc.ObjectLookupContainer}.
     */
    public LongLookupContainer keys() {
        return map.keys();
    }

    /**
     * Returns a direct iterator over the keys.
     */
    public UnmodifiableIterator<Long> keysIt() {
        final Iterator<LongCursor> iterator = map.keys().iterator();
        return new UnmodifiableIterator<Long>() {
            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public Long next() {
                return iterator.next().value;
            }
        };
    }

    /**
     * @return Returns a container with all values stored in this map.
     */
    public ObjectContainer<VType> values() {
        return map.values();
    }

    /**
     * Returns a direct iterator over the keys.
     */
    public UnmodifiableIterator<VType> valuesIt() {
        final Iterator<ObjectCursor<VType>> iterator = map.values().iterator();
        return new UnmodifiableIterator<VType>() {
            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public VType next() {
                return iterator.next().value;
            }
        };
    }

    @Override
    public String toString() {
        return map.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ImmutableOpenLongMap that = (ImmutableOpenLongMap) o;

        if (!map.equals(that.map)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return map.hashCode();
    }

    @SuppressWarnings("unchecked")
    private static final ImmutableOpenLongMap EMPTY = new ImmutableOpenLongMap(new LongObjectOpenHashMap());

    @SuppressWarnings("unchecked")
    public static <VType> ImmutableOpenLongMap<VType> of() {
        return EMPTY;
    }

    public static <VType> Builder<VType> builder() {
        return new Builder<>();
    }

    public static <VType> Builder<VType> builder(int size) {
        return new Builder<>(size);
    }

    public static <VType> Builder<VType> builder(ImmutableOpenLongMap<VType> map) {
        return new Builder<>(map);
    }

    public static class Builder<VType> implements LongObjectMap<VType> {

        private LongObjectOpenHashMap<VType> map;

        public Builder() {
            //noinspection unchecked
            this(EMPTY);
        }

        public Builder(int size) {
            this.map = new LongObjectOpenHashMap<>(size);
        }

        public Builder(ImmutableOpenLongMap<VType> map) {
            this.map = map.map.clone();
        }

        /**
         * Builds a new instance of the
         */
        public ImmutableOpenLongMap<VType> build() {
            LongObjectOpenHashMap<VType> map = this.map;
            this.map = null; // nullify the map, so any operation post build will fail! (hackish, but safest)
            return new ImmutableOpenLongMap<>(map);
        }

        /**
         * Puts all the entries in the map to the builder.
         */
        public Builder<VType> putAll(Map<Long, VType> map) {
            for (Map.Entry<Long, VType> entry : map.entrySet()) {
                this.map.put(entry.getKey(), entry.getValue());
            }
            return this;
        }

        /**
         * A put operation that can be used in the fluent pattern.
         */
        public Builder<VType> fPut(long key, VType value) {
            map.put(key, value);
            return this;
        }

        @Override
        public VType put(long key, VType value) {
            return map.put(key, value);
        }

        @Override
        public VType get(long key) {
            return map.get(key);
        }

        @Override
        public VType getOrDefault(long kType, VType vType) {
            return map.getOrDefault(kType, vType);
        }

        /**
         * Remove that can be used in the fluent pattern.
         */
        public Builder<VType> fRemove(long key) {
            map.remove(key);
            return this;
        }

        @Override
        public VType remove(long key) {
            return map.remove(key);
        }

        @Override
        public Iterator<LongObjectCursor<VType>> iterator() {
            return map.iterator();
        }

        @Override
        public boolean containsKey(long key) {
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
        public void clear() {
            map.clear();
        }

        @Override
        public int putAll(LongObjectAssociativeContainer<? extends VType> container) {
            return map.putAll(container);
        }

        @Override
        public int putAll(Iterable<? extends LongObjectCursor<? extends VType>> iterable) {
            return map.putAll(iterable);
        }

        @Override
        public int removeAll(LongContainer container) {
            return map.removeAll(container);
        }

        @Override
        public int removeAll(LongPredicate predicate) {
            return map.removeAll(predicate);
        }

        @Override
        public <T extends LongObjectProcedure<? super VType>> T forEach(T procedure) {
            return map.forEach(procedure);
        }

        @Override
        public LongCollection keys() {
            return map.keys();
        }

        @Override
        public ObjectContainer<VType> values() {
            return map.values();
        }
    }
}
