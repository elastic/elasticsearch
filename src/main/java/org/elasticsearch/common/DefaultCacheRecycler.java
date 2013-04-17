/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.common;

import gnu.trove.map.hash.TByteIntHashMap;
import gnu.trove.map.hash.TDoubleIntHashMap;
import gnu.trove.map.hash.TFloatIntHashMap;
import gnu.trove.map.hash.TIntIntHashMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.map.hash.TLongIntHashMap;
import gnu.trove.map.hash.TLongLongHashMap;
import gnu.trove.map.hash.TObjectFloatHashMap;
import gnu.trove.map.hash.TObjectIntHashMap;
import gnu.trove.map.hash.TShortIntHashMap;
import gnu.trove.set.hash.THashSet;

import java.lang.ref.SoftReference;
import java.util.Arrays;
import java.util.Queue;

import org.elasticsearch.common.trove.ExtTDoubleObjectHashMap;
import org.elasticsearch.common.trove.ExtTHashMap;
import org.elasticsearch.common.trove.ExtTLongObjectHashMap;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;

public class DefaultCacheRecycler implements Recycler {
 
    
    
    @Override
    public void clear() {
        hashMap.clear();
        hashSet.clear();
        doubleObjectHashMap.clear();
        longObjectHashMap.clear();
        longLongHashMap.clear();
        intIntHashMap.clear();
        floatIntHashMap.clear();
        doubleIntHashMap.clear();
        shortIntHashMap.clear();
        longIntHashMap.clear();
        objectIntHashMap.clear();
        intObjectHashMap.clear();
        objectFloatHashMap.clear();
        objectArray.clear();
        intArray.clear();
    }

    static class SoftWrapper<T> {
        private SoftReference<T> ref;

        public SoftWrapper() {
        }

        public void set(T ref) {
            this.ref = new SoftReference<T>(ref);
        }

        public T get() {
            return ref == null ? null : ref.get();
        }

        public void clear() {
            ref = null;
        }
    }

    // ----- ExtTHashMap -----

    private final SoftWrapper<Queue<ExtTHashMap>> hashMap = new SoftWrapper<Queue<ExtTHashMap>>();

    /* (non-Javadoc)
     * @see org.elasticsearch.common.Recycler#popHashMap()
     */
    @Override
    @SuppressWarnings("unchecked")
    public <K, V> ExtTHashMap<K, V> popHashMap() {
        Queue<ExtTHashMap> ref = hashMap.get();
        if (ref == null) {
            return new ExtTHashMap<K, V>();
        }
        ExtTHashMap map = ref.poll();
        if (map == null) {
            return new ExtTHashMap<K, V>();
        }
        return map;
    }

    /* (non-Javadoc)
     * @see org.elasticsearch.common.Recycler#pushHashMap(org.elasticsearch.common.trove.ExtTHashMap)
     */
    @Override
    public void pushHashMap(ExtTHashMap map) {
        Queue<ExtTHashMap> ref = hashMap.get();
        if (ref == null) {
            ref = ConcurrentCollections.newQueue();
            hashMap.set(ref);
        }
        map.clear();
        ref.add(map);
    }

    // ----- THashSet -----

    private final SoftWrapper<Queue<THashSet>> hashSet = new SoftWrapper<Queue<THashSet>>();

    /* (non-Javadoc)
     * @see org.elasticsearch.common.Recycler#popHashSet()
     */
    @Override
    @SuppressWarnings("unchecked")
    public <T> THashSet<T> popHashSet() {
        Queue<THashSet> ref = hashSet.get();
        if (ref == null) {
            return new THashSet<T>();
        }
        THashSet set = ref.poll();
        if (set == null) {
            return new THashSet<T>();
        }
        return set;
    }

    /* (non-Javadoc)
     * @see org.elasticsearch.common.Recycler#pushHashSet(gnu.trove.set.hash.THashSet)
     */
    @Override
    public void pushHashSet(THashSet map) {
        Queue<THashSet> ref = hashSet.get();
        if (ref == null) {
            ref = ConcurrentCollections.newQueue();
            hashSet.set(ref);
        }
        map.clear();
        ref.add(map);
    }

    // ------ ExtTDoubleObjectHashMap -----

    private final SoftWrapper<Queue<ExtTDoubleObjectHashMap>> doubleObjectHashMap = new SoftWrapper<Queue<ExtTDoubleObjectHashMap>>();

    /* (non-Javadoc)
     * @see org.elasticsearch.common.Recycler#popDoubleObjectMap()
     */
    @Override
    @SuppressWarnings("unchecked")
    public <T> ExtTDoubleObjectHashMap<T> popDoubleObjectMap() {
        Queue<ExtTDoubleObjectHashMap> ref = doubleObjectHashMap.get();
        if (ref == null) {
            return new ExtTDoubleObjectHashMap();
        }
        ExtTDoubleObjectHashMap map = ref.poll();
        if (map == null) {
            return new ExtTDoubleObjectHashMap();
        }
        return map;
    }

    /* (non-Javadoc)
     * @see org.elasticsearch.common.Recycler#pushDoubleObjectMap(org.elasticsearch.common.trove.ExtTDoubleObjectHashMap)
     */
    @Override
    public void pushDoubleObjectMap(ExtTDoubleObjectHashMap map) {
        Queue<ExtTDoubleObjectHashMap> ref = doubleObjectHashMap.get();
        if (ref == null) {
            ref = ConcurrentCollections.newQueue();
            doubleObjectHashMap.set(ref);
        }
        map.clear();
        ref.add(map);
    }

    // ----- ExtTLongObjectHashMap ----

    private final SoftWrapper<Queue<ExtTLongObjectHashMap>> longObjectHashMap = new SoftWrapper<Queue<ExtTLongObjectHashMap>>();

    /* (non-Javadoc)
     * @see org.elasticsearch.common.Recycler#popLongObjectMap()
     */
    @Override
    @SuppressWarnings("unchecked")
    public <T> ExtTLongObjectHashMap<T> popLongObjectMap() {
        Queue<ExtTLongObjectHashMap> ref = longObjectHashMap.get();
        if (ref == null) {
            return new ExtTLongObjectHashMap();
        }
        ExtTLongObjectHashMap map = ref.poll();
        if (map == null) {
            return new ExtTLongObjectHashMap();
        }
        return map;
    }

    /* (non-Javadoc)
     * @see org.elasticsearch.common.Recycler#pushLongObjectMap(org.elasticsearch.common.trove.ExtTLongObjectHashMap)
     */
    @Override
    public void pushLongObjectMap(ExtTLongObjectHashMap map) {
        Queue<ExtTLongObjectHashMap> ref = longObjectHashMap.get();
        if (ref == null) {
            ref = ConcurrentCollections.newQueue();
            longObjectHashMap.set(ref);
        }
        map.clear();
        ref.add(map);
    }

    // ----- TLongLongHashMap ----

    private final SoftWrapper<Queue<TLongLongHashMap>> longLongHashMap = new SoftWrapper<Queue<TLongLongHashMap>>();

    /* (non-Javadoc)
     * @see org.elasticsearch.common.Recycler#popLongLongMap()
     */
    @Override
    public TLongLongHashMap popLongLongMap() {
        Queue<TLongLongHashMap> ref = longLongHashMap.get();
        if (ref == null) {
            return new TLongLongHashMap();
        }
        TLongLongHashMap map = ref.poll();
        if (map == null) {
            return new TLongLongHashMap();
        }
        return map;
    }

    /* (non-Javadoc)
     * @see org.elasticsearch.common.Recycler#pushLongLongMap(gnu.trove.map.hash.TLongLongHashMap)
     */
    @Override
    public void pushLongLongMap(TLongLongHashMap map) {
        Queue<TLongLongHashMap> ref = longLongHashMap.get();
        if (ref == null) {
            ref = ConcurrentCollections.newQueue();
            longLongHashMap.set(ref);
        }
        map.clear();
        ref.add(map);
    }

    // ----- TIntIntHashMap ----

    private final SoftWrapper<Queue<TIntIntHashMap>> intIntHashMap = new SoftWrapper<Queue<TIntIntHashMap>>();


    /* (non-Javadoc)
     * @see org.elasticsearch.common.Recycler#popIntIntMap()
     */
    @Override
    public TIntIntHashMap popIntIntMap() {
        Queue<TIntIntHashMap> ref = intIntHashMap.get();
        if (ref == null) {
            return new TIntIntHashMap();
        }
        TIntIntHashMap map = ref.poll();
        if (map == null) {
            return new TIntIntHashMap();
        }
        return map;
    }

    /* (non-Javadoc)
     * @see org.elasticsearch.common.Recycler#pushIntIntMap(gnu.trove.map.hash.TIntIntHashMap)
     */
    @Override
    public void pushIntIntMap(TIntIntHashMap map) {
        Queue<TIntIntHashMap> ref = intIntHashMap.get();
        if (ref == null) {
            ref = ConcurrentCollections.newQueue();
            intIntHashMap.set(ref);
        }
        map.clear();
        ref.add(map);
    }


    // ----- TFloatIntHashMap ---

    private final SoftWrapper<Queue<TFloatIntHashMap>> floatIntHashMap = new SoftWrapper<Queue<TFloatIntHashMap>>();


    /* (non-Javadoc)
     * @see org.elasticsearch.common.Recycler#popFloatIntMap()
     */
    @Override
    public TFloatIntHashMap popFloatIntMap() {
        Queue<TFloatIntHashMap> ref = floatIntHashMap.get();
        if (ref == null) {
            return new TFloatIntHashMap();
        }
        TFloatIntHashMap map = ref.poll();
        if (map == null) {
            return new TFloatIntHashMap();
        }
        return map;
    }

    /* (non-Javadoc)
     * @see org.elasticsearch.common.Recycler#pushFloatIntMap(gnu.trove.map.hash.TFloatIntHashMap)
     */
    @Override
    public void pushFloatIntMap(TFloatIntHashMap map) {
        Queue<TFloatIntHashMap> ref = floatIntHashMap.get();
        if (ref == null) {
            ref = ConcurrentCollections.newQueue();
            floatIntHashMap.set(ref);
        }
        map.clear();
        ref.add(map);
    }


    // ----- TDoubleIntHashMap ---

    private final SoftWrapper<Queue<TDoubleIntHashMap>> doubleIntHashMap = new SoftWrapper<Queue<TDoubleIntHashMap>>();


    /* (non-Javadoc)
     * @see org.elasticsearch.common.Recycler#popDoubleIntMap()
     */
    @Override
    public TDoubleIntHashMap popDoubleIntMap() {
        Queue<TDoubleIntHashMap> ref = doubleIntHashMap.get();
        if (ref == null) {
            return new TDoubleIntHashMap();
        }
        TDoubleIntHashMap map = ref.poll();
        if (map == null) {
            return new TDoubleIntHashMap();
        }
        return map;
    }

    /* (non-Javadoc)
     * @see org.elasticsearch.common.Recycler#pushDoubleIntMap(gnu.trove.map.hash.TDoubleIntHashMap)
     */
    @Override
    public void pushDoubleIntMap(TDoubleIntHashMap map) {
        Queue<TDoubleIntHashMap> ref = doubleIntHashMap.get();
        if (ref == null) {
            ref = ConcurrentCollections.newQueue();
            doubleIntHashMap.set(ref);
        }
        map.clear();
        ref.add(map);
    }


    // ----- TByteIntHashMap ---

    private final SoftWrapper<Queue<TByteIntHashMap>> byteIntHashMap = new SoftWrapper<Queue<TByteIntHashMap>>();


    /* (non-Javadoc)
     * @see org.elasticsearch.common.Recycler#popByteIntMap()
     */
    @Override
    public TByteIntHashMap popByteIntMap() {
        Queue<TByteIntHashMap> ref = byteIntHashMap.get();
        if (ref == null) {
            return new TByteIntHashMap();
        }
        TByteIntHashMap map = ref.poll();
        if (map == null) {
            return new TByteIntHashMap();
        }
        return map;
    }

    /* (non-Javadoc)
     * @see org.elasticsearch.common.Recycler#pushByteIntMap(gnu.trove.map.hash.TByteIntHashMap)
     */
    @Override
    public void pushByteIntMap(TByteIntHashMap map) {
        Queue<TByteIntHashMap> ref = byteIntHashMap.get();
        if (ref == null) {
            ref = ConcurrentCollections.newQueue();
            byteIntHashMap.set(ref);
        }
        map.clear();
        ref.add(map);
    }

    // ----- TShortIntHashMap ---

    private final SoftWrapper<Queue<TShortIntHashMap>> shortIntHashMap = new SoftWrapper<Queue<TShortIntHashMap>>();


    /* (non-Javadoc)
     * @see org.elasticsearch.common.Recycler#popShortIntMap()
     */
    @Override
    public TShortIntHashMap popShortIntMap() {
        Queue<TShortIntHashMap> ref = shortIntHashMap.get();
        if (ref == null) {
            return new TShortIntHashMap();
        }
        TShortIntHashMap map = ref.poll();
        if (map == null) {
            return new TShortIntHashMap();
        }
        return map;
    }

    /* (non-Javadoc)
     * @see org.elasticsearch.common.Recycler#pushShortIntMap(gnu.trove.map.hash.TShortIntHashMap)
     */
    @Override
    public void pushShortIntMap(TShortIntHashMap map) {
        Queue<TShortIntHashMap> ref = shortIntHashMap.get();
        if (ref == null) {
            ref = ConcurrentCollections.newQueue();
            shortIntHashMap.set(ref);
        }
        map.clear();
        ref.add(map);
    }


    // ----- TLongIntHashMap ----

    private final SoftWrapper<Queue<TLongIntHashMap>> longIntHashMap = new SoftWrapper<Queue<TLongIntHashMap>>();


    /* (non-Javadoc)
     * @see org.elasticsearch.common.Recycler#popLongIntMap()
     */
    @Override
    public TLongIntHashMap popLongIntMap() {
        Queue<TLongIntHashMap> ref = longIntHashMap.get();
        if (ref == null) {
            return new TLongIntHashMap();
        }
        TLongIntHashMap map = ref.poll();
        if (map == null) {
            return new TLongIntHashMap();
        }
        return map;
    }

    /* (non-Javadoc)
     * @see org.elasticsearch.common.Recycler#pushLongIntMap(gnu.trove.map.hash.TLongIntHashMap)
     */
    @Override
    public void pushLongIntMap(TLongIntHashMap map) {
        Queue<TLongIntHashMap> ref = longIntHashMap.get();
        if (ref == null) {
            ref = ConcurrentCollections.newQueue();
            longIntHashMap.set(ref);
        }
        map.clear();
        ref.add(map);
    }

    // ------ TObjectIntHashMap -----

    private final SoftWrapper<Queue<TObjectIntHashMap>> objectIntHashMap = new SoftWrapper<Queue<TObjectIntHashMap>>();


    /* (non-Javadoc)
     * @see org.elasticsearch.common.Recycler#popObjectIntMap()
     */
    @Override
    @SuppressWarnings({"unchecked"})
    public <T> TObjectIntHashMap<T> popObjectIntMap() {
        Queue<TObjectIntHashMap> ref = objectIntHashMap.get();
        if (ref == null) {
            return new TObjectIntHashMap();
        }
        TObjectIntHashMap map = ref.poll();
        if (map == null) {
            return new TObjectIntHashMap();
        }
        return map;
    }

    /* (non-Javadoc)
     * @see org.elasticsearch.common.Recycler#pushObjectIntMap(gnu.trove.map.hash.TObjectIntHashMap)
     */
    @Override
    public <T> void pushObjectIntMap(TObjectIntHashMap<T> map) {
        Queue<TObjectIntHashMap> ref = objectIntHashMap.get();
        if (ref == null) {
            ref = ConcurrentCollections.newQueue();
            objectIntHashMap.set(ref);
        }
        map.clear();
        ref.add(map);
    }

    // ------ TIntObjectHashMap -----

    private final SoftWrapper<Queue<TIntObjectHashMap>> intObjectHashMap = new SoftWrapper<Queue<TIntObjectHashMap>>();


    /* (non-Javadoc)
     * @see org.elasticsearch.common.Recycler#popIntObjectMap()
     */
    @Override
    @SuppressWarnings({"unchecked"})
    public <T> TIntObjectHashMap<T> popIntObjectMap() {
        Queue<TIntObjectHashMap> ref = intObjectHashMap.get();
        if (ref == null) {
            return new TIntObjectHashMap<T>();
        }
        TIntObjectHashMap<T> map = ref.poll();
        if (map == null) {
            return new TIntObjectHashMap<T>();
        }
        return map;
    }

    /* (non-Javadoc)
     * @see org.elasticsearch.common.Recycler#pushIntObjectMap(gnu.trove.map.hash.TIntObjectHashMap)
     */
    @Override
    public <T> void pushIntObjectMap(TIntObjectHashMap<T> map) {
        Queue<TIntObjectHashMap> ref = intObjectHashMap.get();
        if (ref == null) {
            ref = ConcurrentCollections.newQueue();
            intObjectHashMap.set(ref);
        }
        map.clear();
        ref.add(map);
    }

    // ------ TObjectFloatHashMap -----

    private final SoftWrapper<Queue<TObjectFloatHashMap>> objectFloatHashMap = new SoftWrapper<Queue<TObjectFloatHashMap>>();

    /* (non-Javadoc)
     * @see org.elasticsearch.common.Recycler#popObjectFloatMap()
     */
    @Override
    @SuppressWarnings({"unchecked"})
    public <T> TObjectFloatHashMap<T> popObjectFloatMap() {
        Queue<TObjectFloatHashMap> ref = objectFloatHashMap.get();
        if (ref == null) {
            return new TObjectFloatHashMap();
        }
        TObjectFloatHashMap map = ref.poll();
        if (map == null) {
            return new TObjectFloatHashMap();
        }
        return map;
    }

    /* (non-Javadoc)
     * @see org.elasticsearch.common.Recycler#pushObjectFloatMap(gnu.trove.map.hash.TObjectFloatHashMap)
     */
    @Override
    public <T> void pushObjectFloatMap(TObjectFloatHashMap<T> map) {
        Queue<TObjectFloatHashMap> ref = objectFloatHashMap.get();
        if (ref == null) {
            ref = ConcurrentCollections.newQueue();
            objectFloatHashMap.set(ref);
        }
        map.clear();
        ref.add(map);
    }

    // ----- int[] -----

    private final SoftWrapper<Queue<Object[]>> objectArray = new SoftWrapper<Queue<Object[]>>();

    /* (non-Javadoc)
     * @see org.elasticsearch.common.Recycler#popObjectArray(int)
     */
    @Override
    public Object[] popObjectArray(int size) {
        size = size < 100 ? 100 : size;
        Queue<Object[]> ref = objectArray.get();
        if (ref == null) {
            return new Object[size];
        }
        Object[] objects = ref.poll();
        if (objects == null) {
            return new Object[size];
        }
        if (objects.length < size) {
            return new Object[size];
        }
        return objects;
    }

    /* (non-Javadoc)
     * @see org.elasticsearch.common.Recycler#pushObjectArray(java.lang.Object[])
     */
    @Override
    public void pushObjectArray(Object[] objects) {
        Queue<Object[]> ref = objectArray.get();
        if (ref == null) {
            ref = ConcurrentCollections.newQueue();
            objectArray.set(ref);
        }
        Arrays.fill(objects, null);
        ref.add(objects);
    }


    private final SoftWrapper<Queue<int[]>> intArray = new SoftWrapper<Queue<int[]>>();

    /* (non-Javadoc)
     * @see org.elasticsearch.common.Recycler#popIntArray(int)
     */
    @Override
    public int[] popIntArray(int size) {
        return popIntArray(size, 0);
    }

    /* (non-Javadoc)
     * @see org.elasticsearch.common.Recycler#popIntArray(int, int)
     */
    @Override
    public int[] popIntArray(int size, int sentinal) {
        size = size < 100 ? 100 : size;
        Queue<int[]> ref = intArray.get();
        if (ref == null) {
            int[] ints = new int[size];
            if (sentinal != 0) {
                Arrays.fill(ints, sentinal);
            }
            return ints;
        }
        int[] ints = ref.poll();
        if (ints == null) {
            ints = new int[size];
            if (sentinal != 0) {
                Arrays.fill(ints, sentinal);
            }
            return ints;
        }
        if (ints.length < size) {
            ints = new int[size];
            if (sentinal != 0) {
                Arrays.fill(ints, sentinal);
            }
            return ints;
        }
        return ints;
    }

    /* (non-Javadoc)
     * @see org.elasticsearch.common.Recycler#pushIntArray(int[])
     */
    @Override
    public void pushIntArray(int[] ints) {
        pushIntArray(ints, 0);
    }

    /* (non-Javadoc)
     * @see org.elasticsearch.common.Recycler#pushIntArray(int[], int)
     */
    @Override
    public void pushIntArray(int[] ints, int sentinal) {
        Queue<int[]> ref = intArray.get();
        if (ref == null) {
            ref = ConcurrentCollections.newQueue();
            intArray.set(ref);
        }
        Arrays.fill(ints, sentinal);
        ref.add(ints);
    }
}