/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
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

import org.elasticsearch.common.compress.lzf.BufferRecycler;
import org.elasticsearch.common.trove.ExtTDoubleObjectHashMap;
import org.elasticsearch.common.trove.ExtTHashMap;
import org.elasticsearch.common.trove.ExtTLongObjectHashMap;
import org.elasticsearch.common.trove.map.hash.*;

import java.lang.ref.SoftReference;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Deque;

public class CacheRecycler {

    public static void clear() {
        BufferRecycler.clean();
        bytes.remove();
        doubleObjectHashMap.remove();
        longObjectHashMap.remove();
        longLongHashMap.remove();
        intIntHashMap.remove();
        floatIntHashMap.remove();
        doubleIntHashMap.remove();
        shortIntHashMap.remove();
        longIntHashMap.remove();
        objectIntHashMap.remove();
        intArray.remove();
    }

    // Bytes
    private static ThreadLocal<SoftReference<byte[]>> bytes = new ThreadLocal<SoftReference<byte[]>>();

    public static byte[] popBytes() {
        SoftReference<byte[]> ref = bytes.get();
        byte[] bb = ref == null ? null : ref.get();
        if (bb == null) {
            bb = new byte[1024];
            bytes.set(new SoftReference<byte[]>(bb));
        }
        return bb;
    }

    public static void pushBytes(byte[] bb) {
        SoftReference<byte[]> ref = bytes.get();
        byte[] bb1 = ref == null ? null : ref.get();
        if (bb1 != null && bb1.length < bb.length) {
            bytes.set(new SoftReference<byte[]>(bb));
        }
    }

    // ----- ExtTHashMap -----

    private static ThreadLocal<SoftReference<Deque<ExtTHashMap>>> hashMap = new ThreadLocal<SoftReference<Deque<ExtTHashMap>>>();

    public static <K, V> ExtTHashMap<K, V> popHashMap() {
        SoftReference<Deque<ExtTHashMap>> ref = hashMap.get();
        Deque<ExtTHashMap> deque = ref == null ? null : ref.get();
        if (deque == null) {
            deque = new ArrayDeque<ExtTHashMap>();
            hashMap.set(new SoftReference<Deque<ExtTHashMap>>(deque));
        }
        if (deque.isEmpty()) {
            return new ExtTHashMap();
        }
        ExtTHashMap map = deque.pollFirst();
        map.clear();
        return map;
    }

    public static void pushHashMap(ExtTHashMap map) {
        SoftReference<Deque<ExtTHashMap>> ref = hashMap.get();
        Deque<ExtTHashMap> deque = ref == null ? null : ref.get();
        if (deque == null) {
            deque = new ArrayDeque<ExtTHashMap>();
            hashMap.set(new SoftReference<Deque<ExtTHashMap>>(deque));
        }
        deque.add(map);
    }

    // ------ ExtTDoubleObjectHashMap -----

    private static ThreadLocal<SoftReference<Deque<ExtTDoubleObjectHashMap>>> doubleObjectHashMap = new ThreadLocal<SoftReference<Deque<ExtTDoubleObjectHashMap>>>();

    public static <T> ExtTDoubleObjectHashMap<T> popDoubleObjectMap() {
        SoftReference<Deque<ExtTDoubleObjectHashMap>> ref = doubleObjectHashMap.get();
        Deque<ExtTDoubleObjectHashMap> deque = ref == null ? null : ref.get();
        if (deque == null) {
            deque = new ArrayDeque<ExtTDoubleObjectHashMap>();
            doubleObjectHashMap.set(new SoftReference<Deque<ExtTDoubleObjectHashMap>>(deque));
        }
        if (deque.isEmpty()) {
            return new ExtTDoubleObjectHashMap();
        }
        ExtTDoubleObjectHashMap map = deque.pollFirst();
        map.clear();
        return map;
    }

    public static void pushDoubleObjectMap(ExtTDoubleObjectHashMap map) {
        SoftReference<Deque<ExtTDoubleObjectHashMap>> ref = doubleObjectHashMap.get();
        Deque<ExtTDoubleObjectHashMap> deque = ref == null ? null : ref.get();
        if (deque == null) {
            deque = new ArrayDeque<ExtTDoubleObjectHashMap>();
            doubleObjectHashMap.set(new SoftReference<Deque<ExtTDoubleObjectHashMap>>(deque));
        }
        deque.add(map);
    }

    // ----- ExtTLongObjectHashMap ----

    private static ThreadLocal<SoftReference<Deque<ExtTLongObjectHashMap>>> longObjectHashMap = new ThreadLocal<SoftReference<Deque<ExtTLongObjectHashMap>>>();

    public static <T> ExtTLongObjectHashMap<T> popLongObjectMap() {
        SoftReference<Deque<ExtTLongObjectHashMap>> ref = longObjectHashMap.get();
        Deque<ExtTLongObjectHashMap> deque = ref == null ? null : ref.get();
        if (deque == null) {
            deque = new ArrayDeque<ExtTLongObjectHashMap>();
            longObjectHashMap.set(new SoftReference<Deque<ExtTLongObjectHashMap>>(deque));
        }
        if (deque.isEmpty()) {
            return new ExtTLongObjectHashMap();
        }
        ExtTLongObjectHashMap map = deque.pollFirst();
        map.clear();
        return map;
    }

    public static void pushLongObjectMap(ExtTLongObjectHashMap map) {
        SoftReference<Deque<ExtTLongObjectHashMap>> ref = longObjectHashMap.get();
        Deque<ExtTLongObjectHashMap> deque = ref == null ? null : ref.get();
        if (deque == null) {
            deque = new ArrayDeque<ExtTLongObjectHashMap>();
            longObjectHashMap.set(new SoftReference<Deque<ExtTLongObjectHashMap>>(deque));
        }
        deque.add(map);
    }

    // ----- TLongLongHashMap ----

    private static ThreadLocal<SoftReference<Deque<TLongLongHashMap>>> longLongHashMap = new ThreadLocal<SoftReference<Deque<TLongLongHashMap>>>();

    public static TLongLongHashMap popLongLongMap() {
        SoftReference<Deque<TLongLongHashMap>> ref = longLongHashMap.get();
        Deque<TLongLongHashMap> deque = ref == null ? null : ref.get();
        if (deque == null) {
            deque = new ArrayDeque<TLongLongHashMap>();
            longLongHashMap.set(new SoftReference<Deque<TLongLongHashMap>>(deque));
        }
        if (deque.isEmpty()) {
            return new TLongLongHashMap();
        }
        TLongLongHashMap map = deque.pollFirst();
        map.clear();
        return map;
    }

    public static void pushLongLongMap(TLongLongHashMap map) {
        SoftReference<Deque<TLongLongHashMap>> ref = longLongHashMap.get();
        Deque<TLongLongHashMap> deque = ref == null ? null : ref.get();
        if (deque == null) {
            deque = new ArrayDeque<TLongLongHashMap>();
            longLongHashMap.set(new SoftReference<Deque<TLongLongHashMap>>(deque));
        }
        deque.add(map);
    }

    // ----- TIntIntHashMap ----

    private static ThreadLocal<SoftReference<Deque<TIntIntHashMap>>> intIntHashMap = new ThreadLocal<SoftReference<Deque<TIntIntHashMap>>>();


    public static TIntIntHashMap popIntIntMap() {
        SoftReference<Deque<TIntIntHashMap>> ref = intIntHashMap.get();
        Deque<TIntIntHashMap> deque = ref == null ? null : ref.get();
        if (deque == null) {
            deque = new ArrayDeque<TIntIntHashMap>();
            intIntHashMap.set(new SoftReference<Deque<TIntIntHashMap>>(deque));
        }
        if (deque.isEmpty()) {
            return new TIntIntHashMap();
        }
        TIntIntHashMap map = deque.pollFirst();
        map.clear();
        return map;
    }

    public static void pushIntIntMap(TIntIntHashMap map) {
        SoftReference<Deque<TIntIntHashMap>> ref = intIntHashMap.get();
        Deque<TIntIntHashMap> deque = ref == null ? null : ref.get();
        if (deque == null) {
            deque = new ArrayDeque<TIntIntHashMap>();
            intIntHashMap.set(new SoftReference<Deque<TIntIntHashMap>>(deque));
        }
        deque.add(map);
    }


    // ----- TFloatIntHashMap ---

    private static ThreadLocal<SoftReference<Deque<TFloatIntHashMap>>> floatIntHashMap = new ThreadLocal<SoftReference<Deque<TFloatIntHashMap>>>();


    public static TFloatIntHashMap popFloatIntMap() {
        SoftReference<Deque<TFloatIntHashMap>> ref = floatIntHashMap.get();
        Deque<TFloatIntHashMap> deque = ref == null ? null : ref.get();
        if (deque == null) {
            deque = new ArrayDeque<TFloatIntHashMap>();
            floatIntHashMap.set(new SoftReference<Deque<TFloatIntHashMap>>(deque));
        }
        if (deque.isEmpty()) {
            return new TFloatIntHashMap();
        }
        TFloatIntHashMap map = deque.pollFirst();
        map.clear();
        return map;
    }

    public static void pushFloatIntMap(TFloatIntHashMap map) {
        SoftReference<Deque<TFloatIntHashMap>> ref = floatIntHashMap.get();
        Deque<TFloatIntHashMap> deque = ref == null ? null : ref.get();
        if (deque == null) {
            deque = new ArrayDeque<TFloatIntHashMap>();
            floatIntHashMap.set(new SoftReference<Deque<TFloatIntHashMap>>(deque));
        }
        deque.add(map);
    }


    // ----- TDoubleIntHashMap ---

    private static ThreadLocal<SoftReference<Deque<TDoubleIntHashMap>>> doubleIntHashMap = new ThreadLocal<SoftReference<Deque<TDoubleIntHashMap>>>();


    public static TDoubleIntHashMap popDoubleIntMap() {
        SoftReference<Deque<TDoubleIntHashMap>> ref = doubleIntHashMap.get();
        Deque<TDoubleIntHashMap> deque = ref == null ? null : ref.get();
        if (deque == null) {
            deque = new ArrayDeque<TDoubleIntHashMap>();
            doubleIntHashMap.set(new SoftReference<Deque<TDoubleIntHashMap>>(deque));
        }
        if (deque.isEmpty()) {
            return new TDoubleIntHashMap();
        }
        TDoubleIntHashMap map = deque.pollFirst();
        map.clear();
        return map;
    }

    public static void pushDoubleIntMap(TDoubleIntHashMap map) {
        SoftReference<Deque<TDoubleIntHashMap>> ref = doubleIntHashMap.get();
        Deque<TDoubleIntHashMap> deque = ref == null ? null : ref.get();
        if (deque == null) {
            deque = new ArrayDeque<TDoubleIntHashMap>();
            doubleIntHashMap.set(new SoftReference<Deque<TDoubleIntHashMap>>(deque));
        }
        deque.add(map);
    }


    // ----- TByteIntHashMap ---

    private static ThreadLocal<SoftReference<Deque<TByteIntHashMap>>> byteIntHashMap = new ThreadLocal<SoftReference<Deque<TByteIntHashMap>>>();


    public static TByteIntHashMap popByteIntMap() {
        SoftReference<Deque<TByteIntHashMap>> ref = byteIntHashMap.get();
        Deque<TByteIntHashMap> deque = ref == null ? null : ref.get();
        if (deque == null) {
            deque = new ArrayDeque<TByteIntHashMap>();
            byteIntHashMap.set(new SoftReference<Deque<TByteIntHashMap>>(deque));
        }
        if (deque.isEmpty()) {
            return new TByteIntHashMap();
        }
        TByteIntHashMap map = deque.pollFirst();
        map.clear();
        return map;
    }

    public static void pushByteIntMap(TByteIntHashMap map) {
        SoftReference<Deque<TByteIntHashMap>> ref = byteIntHashMap.get();
        Deque<TByteIntHashMap> deque = ref == null ? null : ref.get();
        if (deque == null) {
            deque = new ArrayDeque<TByteIntHashMap>();
            byteIntHashMap.set(new SoftReference<Deque<TByteIntHashMap>>(deque));
        }
        deque.add(map);
    }

    // ----- TShortIntHashMap ---

    private static ThreadLocal<SoftReference<Deque<TShortIntHashMap>>> shortIntHashMap = new ThreadLocal<SoftReference<Deque<TShortIntHashMap>>>();


    public static TShortIntHashMap popShortIntMap() {
        SoftReference<Deque<TShortIntHashMap>> ref = shortIntHashMap.get();
        Deque<TShortIntHashMap> deque = ref == null ? null : ref.get();
        if (deque == null) {
            deque = new ArrayDeque<TShortIntHashMap>();
            shortIntHashMap.set(new SoftReference<Deque<TShortIntHashMap>>(deque));
        }
        if (deque.isEmpty()) {
            return new TShortIntHashMap();
        }
        TShortIntHashMap map = deque.pollFirst();
        map.clear();
        return map;
    }

    public static void pushShortIntMap(TShortIntHashMap map) {
        SoftReference<Deque<TShortIntHashMap>> ref = shortIntHashMap.get();
        Deque<TShortIntHashMap> deque = ref == null ? null : ref.get();
        if (deque == null) {
            deque = new ArrayDeque<TShortIntHashMap>();
            shortIntHashMap.set(new SoftReference<Deque<TShortIntHashMap>>(deque));
        }
        deque.add(map);
    }


    // ----- TLongIntHashMap ----

    private static ThreadLocal<SoftReference<Deque<TLongIntHashMap>>> longIntHashMap = new ThreadLocal<SoftReference<Deque<TLongIntHashMap>>>();


    public static TLongIntHashMap popLongIntMap() {
        SoftReference<Deque<TLongIntHashMap>> ref = longIntHashMap.get();
        Deque<TLongIntHashMap> deque = ref == null ? null : ref.get();
        if (deque == null) {
            deque = new ArrayDeque<TLongIntHashMap>();
            longIntHashMap.set(new SoftReference<Deque<TLongIntHashMap>>(deque));
        }
        if (deque.isEmpty()) {
            return new TLongIntHashMap();
        }
        TLongIntHashMap map = deque.pollFirst();
        map.clear();
        return map;
    }

    public static void pushLongIntMap(TLongIntHashMap map) {
        SoftReference<Deque<TLongIntHashMap>> ref = longIntHashMap.get();
        Deque<TLongIntHashMap> deque = ref == null ? null : ref.get();
        if (deque == null) {
            deque = new ArrayDeque<TLongIntHashMap>();
            longIntHashMap.set(new SoftReference<Deque<TLongIntHashMap>>(deque));
        }
        deque.add(map);
    }

    // ------ TObjectIntHashMap -----

    private static ThreadLocal<SoftReference<Deque<TObjectIntHashMap>>> objectIntHashMap = new ThreadLocal<SoftReference<Deque<TObjectIntHashMap>>>();


    @SuppressWarnings({"unchecked"})
    public static <T> TObjectIntHashMap<T> popObjectIntMap() {
        SoftReference<Deque<TObjectIntHashMap>> ref = objectIntHashMap.get();
        Deque<TObjectIntHashMap> deque = ref == null ? null : ref.get();
        if (deque == null) {
            deque = new ArrayDeque<TObjectIntHashMap>();
            objectIntHashMap.set(new SoftReference<Deque<TObjectIntHashMap>>(deque));
        }
        if (deque.isEmpty()) {
            return new TObjectIntHashMap();
        }
        TObjectIntHashMap map = deque.pollFirst();
        map.clear();
        return map;
    }

    public static <T> void pushObjectIntMap(TObjectIntHashMap<T> map) {
        SoftReference<Deque<TObjectIntHashMap>> ref = objectIntHashMap.get();
        Deque<TObjectIntHashMap> deque = ref == null ? null : ref.get();
        if (deque == null) {
            deque = new ArrayDeque<TObjectIntHashMap>();
            objectIntHashMap.set(new SoftReference<Deque<TObjectIntHashMap>>(deque));
        }
        deque.add(map);
    }

    // ----- int[] -----

    private static ThreadLocal<SoftReference<Deque<Object[]>>> objectArray = new ThreadLocal<SoftReference<Deque<Object[]>>>();

    public static Object[] popObjectArray(int size) {
        size = size < 100 ? 100 : size;
        SoftReference<Deque<Object[]>> ref = objectArray.get();
        Deque<Object[]> deque = ref == null ? null : ref.get();
        if (deque == null) {
            deque = new ArrayDeque<Object[]>();
            objectArray.set(new SoftReference<Deque<Object[]>>(deque));
        }
        if (deque.isEmpty()) {
            return new Object[size];
        }
        Object[] objects = deque.pollFirst();
        if (objects.length < size) {
            return new Object[size];
        }
        Arrays.fill(objects, null);
        return objects;
    }

    public static void pushObjectArray(Object[] objects) {
        SoftReference<Deque<Object[]>> ref = objectArray.get();
        Deque<Object[]> deque = ref == null ? null : ref.get();
        if (deque == null) {
            deque = new ArrayDeque<Object[]>();
            objectArray.set(new SoftReference<Deque<Object[]>>(deque));
        }
        deque.add(objects);
    }


    private static ThreadLocal<SoftReference<Deque<int[]>>> intArray = new ThreadLocal<SoftReference<Deque<int[]>>>();

    public static int[] popIntArray(int size) {
        return popIntArray(size, 0);
    }

    public static int[] popIntArray(int size, int sentinal) {
        size = size < 100 ? 100 : size;
        SoftReference<Deque<int[]>> ref = intArray.get();
        Deque<int[]> deque = ref == null ? null : ref.get();
        if (deque == null) {
            deque = new ArrayDeque<int[]>();
            intArray.set(new SoftReference<Deque<int[]>>(deque));
        }
        if (deque.isEmpty()) {
            int[] ints = new int[size];
            if (sentinal != 0) {
                Arrays.fill(ints, sentinal);
            }
            return ints;
        }
        int[] ints = deque.pollFirst();
        if (ints.length < size) {
            ints = new int[size];
            if (sentinal != 0) {
                Arrays.fill(ints, sentinal);
            }
            return ints;
        }
        Arrays.fill(ints, sentinal);
        return ints;
    }

    public static void pushIntArray(int[] ints) {
        SoftReference<Deque<int[]>> ref = intArray.get();
        Deque<int[]> deque = ref == null ? null : ref.get();
        if (deque == null) {
            deque = new ArrayDeque<int[]>();
            intArray.set(new SoftReference<Deque<int[]>>(deque));
        }
        deque.add(ints);
    }
}