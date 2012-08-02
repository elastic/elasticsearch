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

import gnu.trove.map.hash.*;
import jsr166y.LinkedTransferQueue;
import org.elasticsearch.common.trove.ExtTDoubleObjectHashMap;
import org.elasticsearch.common.trove.ExtTHashMap;
import org.elasticsearch.common.trove.ExtTLongObjectHashMap;

import java.lang.ref.SoftReference;
import java.util.Arrays;
import java.util.Queue;

public class CacheRecycler {

    public static void clear() {
        hashMap.clear();
        doubleObjectHashMap.clear();
        longObjectHashMap.clear();
        longLongHashMap.clear();
        intIntHashMap.clear();
        floatIntHashMap.clear();
        doubleIntHashMap.clear();
        shortIntHashMap.clear();
        longIntHashMap.clear();
        objectIntHashMap.clear();
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

    private static SoftWrapper<Queue<ExtTHashMap>> hashMap = new SoftWrapper<Queue<ExtTHashMap>>();

    public static <K, V> ExtTHashMap<K, V> popHashMap() {
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

    public static void pushHashMap(ExtTHashMap map) {
        Queue<ExtTHashMap> ref = hashMap.get();
        if (ref == null) {
            ref = new LinkedTransferQueue<ExtTHashMap>();
            hashMap.set(ref);
        }
        map.clear();
        ref.add(map);
    }

    // ------ ExtTDoubleObjectHashMap -----

    private static SoftWrapper<Queue<ExtTDoubleObjectHashMap>> doubleObjectHashMap = new SoftWrapper<Queue<ExtTDoubleObjectHashMap>>();

    public static <T> ExtTDoubleObjectHashMap<T> popDoubleObjectMap() {
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

    public static void pushDoubleObjectMap(ExtTDoubleObjectHashMap map) {
        Queue<ExtTDoubleObjectHashMap> ref = doubleObjectHashMap.get();
        if (ref == null) {
            ref = new LinkedTransferQueue<ExtTDoubleObjectHashMap>();
            doubleObjectHashMap.set(ref);
        }
        map.clear();
        ref.add(map);
    }

    // ----- ExtTLongObjectHashMap ----

    private static SoftWrapper<Queue<ExtTLongObjectHashMap>> longObjectHashMap = new SoftWrapper<Queue<ExtTLongObjectHashMap>>();

    public static <T> ExtTLongObjectHashMap<T> popLongObjectMap() {
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

    public static void pushLongObjectMap(ExtTLongObjectHashMap map) {
        Queue<ExtTLongObjectHashMap> ref = longObjectHashMap.get();
        if (ref == null) {
            ref = new LinkedTransferQueue<ExtTLongObjectHashMap>();
            longObjectHashMap.set(ref);
        }
        map.clear();
        ref.add(map);
    }

    // ----- TLongLongHashMap ----

    private static SoftWrapper<Queue<TLongLongHashMap>> longLongHashMap = new SoftWrapper<Queue<TLongLongHashMap>>();

    public static TLongLongHashMap popLongLongMap() {
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

    public static void pushLongLongMap(TLongLongHashMap map) {
        Queue<TLongLongHashMap> ref = longLongHashMap.get();
        if (ref == null) {
            ref = new LinkedTransferQueue<TLongLongHashMap>();
            longLongHashMap.set(ref);
        }
        map.clear();
        ref.add(map);
    }

    // ----- TIntIntHashMap ----

    private static SoftWrapper<Queue<TIntIntHashMap>> intIntHashMap = new SoftWrapper<Queue<TIntIntHashMap>>();


    public static TIntIntHashMap popIntIntMap() {
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

    public static void pushIntIntMap(TIntIntHashMap map) {
        Queue<TIntIntHashMap> ref = intIntHashMap.get();
        if (ref == null) {
            ref = new LinkedTransferQueue<TIntIntHashMap>();
            intIntHashMap.set(ref);
        }
        map.clear();
        ref.add(map);
    }


    // ----- TFloatIntHashMap ---

    private static SoftWrapper<Queue<TFloatIntHashMap>> floatIntHashMap = new SoftWrapper<Queue<TFloatIntHashMap>>();


    public static TFloatIntHashMap popFloatIntMap() {
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

    public static void pushFloatIntMap(TFloatIntHashMap map) {
        Queue<TFloatIntHashMap> ref = floatIntHashMap.get();
        if (ref == null) {
            ref = new LinkedTransferQueue<TFloatIntHashMap>();
            floatIntHashMap.set(ref);
        }
        map.clear();
        ref.add(map);
    }


    // ----- TDoubleIntHashMap ---

    private static SoftWrapper<Queue<TDoubleIntHashMap>> doubleIntHashMap = new SoftWrapper<Queue<TDoubleIntHashMap>>();


    public static TDoubleIntHashMap popDoubleIntMap() {
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

    public static void pushDoubleIntMap(TDoubleIntHashMap map) {
        Queue<TDoubleIntHashMap> ref = doubleIntHashMap.get();
        if (ref == null) {
            ref = new LinkedTransferQueue<TDoubleIntHashMap>();
            doubleIntHashMap.set(ref);
        }
        map.clear();
        ref.add(map);
    }


    // ----- TByteIntHashMap ---

    private static SoftWrapper<Queue<TByteIntHashMap>> byteIntHashMap = new SoftWrapper<Queue<TByteIntHashMap>>();


    public static TByteIntHashMap popByteIntMap() {
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

    public static void pushByteIntMap(TByteIntHashMap map) {
        Queue<TByteIntHashMap> ref = byteIntHashMap.get();
        if (ref == null) {
            ref = new LinkedTransferQueue<TByteIntHashMap>();
            byteIntHashMap.set(ref);
        }
        map.clear();
        ref.add(map);
    }

    // ----- TShortIntHashMap ---

    private static SoftWrapper<Queue<TShortIntHashMap>> shortIntHashMap = new SoftWrapper<Queue<TShortIntHashMap>>();


    public static TShortIntHashMap popShortIntMap() {
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

    public static void pushShortIntMap(TShortIntHashMap map) {
        Queue<TShortIntHashMap> ref = shortIntHashMap.get();
        if (ref == null) {
            ref = new LinkedTransferQueue<TShortIntHashMap>();
            shortIntHashMap.set(ref);
        }
        map.clear();
        ref.add(map);
    }


    // ----- TLongIntHashMap ----

    private static SoftWrapper<Queue<TLongIntHashMap>> longIntHashMap = new SoftWrapper<Queue<TLongIntHashMap>>();


    public static TLongIntHashMap popLongIntMap() {
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

    public static void pushLongIntMap(TLongIntHashMap map) {
        Queue<TLongIntHashMap> ref = longIntHashMap.get();
        if (ref == null) {
            ref = new LinkedTransferQueue<TLongIntHashMap>();
            longIntHashMap.set(ref);
        }
        map.clear();
        ref.add(map);
    }

    // ------ TObjectIntHashMap -----

    private static SoftWrapper<Queue<TObjectIntHashMap>> objectIntHashMap = new SoftWrapper<Queue<TObjectIntHashMap>>();


    @SuppressWarnings({"unchecked"})
    public static <T> TObjectIntHashMap<T> popObjectIntMap() {
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

    public static <T> void pushObjectIntMap(TObjectIntHashMap<T> map) {
        Queue<TObjectIntHashMap> ref = objectIntHashMap.get();
        if (ref == null) {
            ref = new LinkedTransferQueue<TObjectIntHashMap>();
            objectIntHashMap.set(ref);
        }
        map.clear();
        ref.add(map);
    }

    // ----- int[] -----

    private static SoftWrapper<Queue<Object[]>> objectArray = new SoftWrapper<Queue<Object[]>>();

    public static Object[] popObjectArray(int size) {
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

    public static void pushObjectArray(Object[] objects) {
        Queue<Object[]> ref = objectArray.get();
        if (ref == null) {
            ref = new LinkedTransferQueue<Object[]>();
            objectArray.set(ref);
        }
        Arrays.fill(objects, null);
        ref.add(objects);
    }


    private static SoftWrapper<Queue<int[]>> intArray = new SoftWrapper<Queue<int[]>>();

    public static int[] popIntArray(int size) {
        return popIntArray(size, 0);
    }

    public static int[] popIntArray(int size, int sentinal) {
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

    public static void pushIntArray(int[] ints) {
        pushIntArray(ints, 0);
    }

    public static void pushIntArray(int[] ints, int sentinal) {
        Queue<int[]> ref = intArray.get();
        if (ref == null) {
            ref = new LinkedTransferQueue<int[]>();
            intArray.set(ref);
        }
        Arrays.fill(ints, sentinal);
        ref.add(ints);
    }
}