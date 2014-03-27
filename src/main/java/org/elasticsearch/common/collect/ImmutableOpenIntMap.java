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
import com.carrotsearch.hppc.cursors.IntCursor;
import com.carrotsearch.hppc.cursors.IntObjectCursor;
import com.carrotsearch.hppc.cursors.ObjectCursor;
import com.carrotsearch.hppc.predicates.IntPredicate;
import com.carrotsearch.hppc.procedures.IntObjectProcedure;
import com.google.common.collect.UnmodifiableIterator;

import java.util.Iterator;
import java.util.Map;

/**
 * An immutable map implementation based on open hash map.
 * <p/>
 * Can be constructed using a {@link #builder()}, or using {@link #builder(org.elasticsearch.common.collect.ImmutableOpenIntMap)} (which is an optimized
 * option to copy over existing content and modify it).
 */
public final class ImmutableOpenIntMap<VType> implements Iterable<IntObjectCursor<VType>> {

    private final IntObjectOpenHashMap<VType> map;

    private ImmutableOpenIntMap(IntObjectOpenHashMap<VType> map) {
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
    public VType get(int key) {
        return map.get(key);
    }

    /**
     * Returns <code>true</code> if this container has an association to a value for
     * the given key.
     */
    public boolean containsKey(int key) {
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
    public Iterator<IntObjectCursor<VType>> iterator() {
        return map.iterator();
    }

    /**
     * Returns a specialized view of the keys of this associated container.
     * The view additionally implements {@link com.carrotsearch.hppc.ObjectLookupContainer}.
     */
    public IntLookupContainer keys() {
        return map.keys();
    }

    /**
     * Returns a direct iterator over the keys.
     */
    public UnmodifiableIterator<Integer> keysIt() {
        final Iterator<IntCursor> iterator = map.keys().iterator();
        return new UnmodifiableIterator<Integer>() {
            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public Integer next() {
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

        ImmutableOpenIntMap that = (ImmutableOpenIntMap) o;

        if (!map.equals(that.map)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return map.hashCode();
    }

    @SuppressWarnings("unchecked")
    private static final ImmutableOpenIntMap EMPTY = new ImmutableOpenIntMap(new IntObjectOpenHashMap());

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

    public static class Builder<VType> implements IntObjectMap<VType> {

        private IntObjectOpenHashMap<VType> map;

        public Builder() {
            //noinspection unchecked
            this(EMPTY);
        }

        public Builder(int size) {
            this.map = new IntObjectOpenHashMap<>(size);
        }

        public Builder(ImmutableOpenIntMap<VType> map) {
            this.map = map.map.clone();
        }

        /**
         * Builds a new instance of the
         */
        public ImmutableOpenIntMap<VType> build() {
            IntObjectOpenHashMap<VType> map = this.map;
            this.map = null; // nullify the map, so any operation post build will fail! (hackish, but safest)
            return new ImmutableOpenIntMap<>(map);
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

        @Override
        public VType put(int key, VType value) {
            return map.put(key, value);
        }

        @Override
        public VType get(int key) {
            return map.get(key);
        }

        @Override
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

        @Override
        public VType remove(int key) {
            return map.remove(key);
        }

        @Override
        public Iterator<IntObjectCursor<VType>> iterator() {
            return map.iterator();
        }

        @Override
        public boolean containsKey(int key) {
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
        public int putAll(IntObjectAssociativeContainer<? extends VType> container) {
            return map.putAll(container);
        }

        @Override
        public int putAll(Iterable<? extends IntObjectCursor<? extends VType>> iterable) {
            return map.putAll(iterable);
        }

        @Override
        public int removeAll(IntContainer container) {
            return map.removeAll(container);
        }

        @Override
        public int removeAll(IntPredicate predicate) {
            return map.removeAll(predicate);
        }

        @Override
        public <T extends IntObjectProcedure<? super VType>> T forEach(T procedure) {
            return map.forEach(procedure);
        }

        @Override
        public IntCollection keys() {
            return map.keys();
        }

        @Override
        public ObjectContainer<VType> values() {
            return map.values();
        }
    }
}
