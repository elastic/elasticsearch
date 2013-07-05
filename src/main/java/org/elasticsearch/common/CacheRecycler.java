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
import gnu.trove.set.hash.THashSet;

import org.elasticsearch.common.trove.ExtTDoubleObjectHashMap;
import org.elasticsearch.common.trove.ExtTHashMap;
import org.elasticsearch.common.trove.ExtTLongObjectHashMap;

public final class CacheRecycler  {

    private static final Recycler INSTANCE;
    static {
        String property = System.getProperty("es.cache.recycle");
        if (property != null && !Boolean.parseBoolean(property)) {
            INSTANCE = new NoCacheCacheRecycler();
        } else {
            INSTANCE = new DefaultCacheRecycler();
        }
    }
    
    private CacheRecycler() {
        // no instance
    }

    public static void clear() {
        INSTANCE.clear();
    }

    public static <K, V> ExtTHashMap<K, V> popHashMap() {
        return INSTANCE.popHashMap();
    }

    public static void pushHashMap(ExtTHashMap map) {
        INSTANCE.pushHashMap(map);
    }

    public static <T> THashSet<T> popHashSet() {
        return INSTANCE.popHashSet();
    }

    public static void pushHashSet(THashSet map) {
        INSTANCE.pushHashSet(map);
    }

    public static <T> ExtTDoubleObjectHashMap<T> popDoubleObjectMap() {
        return INSTANCE.popDoubleObjectMap();
    }

    public static void pushDoubleObjectMap(ExtTDoubleObjectHashMap map) {
        INSTANCE.pushDoubleObjectMap(map);
    }

    public static <T> ExtTLongObjectHashMap<T> popLongObjectMap() {
        return INSTANCE.popLongObjectMap();
    }

    public static void pushLongObjectMap(ExtTLongObjectHashMap map) {
        INSTANCE.pushLongObjectMap(map);
    }

    public static TLongLongHashMap popLongLongMap() {
        return INSTANCE.popLongLongMap();
    }

    public static void pushLongLongMap(TLongLongHashMap map) {
        INSTANCE.pushLongLongMap(map);
    }

    public static TIntIntHashMap popIntIntMap() {
        return INSTANCE.popIntIntMap();
    }

    public static void pushIntIntMap(TIntIntHashMap map) {
        INSTANCE.pushIntIntMap(map);
    }

    public static TFloatIntHashMap popFloatIntMap() {
        return INSTANCE.popFloatIntMap();
    }

    public static void pushFloatIntMap(TFloatIntHashMap map) {
        INSTANCE.pushFloatIntMap(map);
    }

    public static TDoubleIntHashMap popDoubleIntMap() {
        return INSTANCE.popDoubleIntMap();
    }

    public static void pushDoubleIntMap(TDoubleIntHashMap map) {
        INSTANCE.pushDoubleIntMap(map);
    }

    public static TByteIntHashMap popByteIntMap() {
        return INSTANCE.popByteIntMap();
    }

    public static void pushByteIntMap(TByteIntHashMap map) {
        INSTANCE.pushByteIntMap(map);
    }

    public static TShortIntHashMap popShortIntMap() {
        return INSTANCE.popShortIntMap();
    }

    public static void pushShortIntMap(TShortIntHashMap map) {
        INSTANCE.pushShortIntMap(map);
    }

    public static TLongIntHashMap popLongIntMap() {
        return INSTANCE.popLongIntMap();
    }

    public static void pushLongIntMap(TLongIntHashMap map) {
        INSTANCE.pushLongIntMap(map);
    }

    public static <T> TObjectIntHashMap<T> popObjectIntMap() {
        return INSTANCE.popObjectIntMap();
    }

    public static <T> void pushObjectIntMap(TObjectIntHashMap<T> map) {
        INSTANCE.pushObjectIntMap(map);
    }

    public static <T> TIntObjectHashMap<T> popIntObjectMap() {
        return INSTANCE.popIntObjectMap();
    }

    public static <T> void pushIntObjectMap(TIntObjectHashMap<T> map) {
        INSTANCE.pushIntObjectMap(map);
    }

    public static <T> TObjectFloatHashMap<T> popObjectFloatMap() {
        return INSTANCE.popObjectFloatMap();
    }

    public static <T> void pushObjectFloatMap(TObjectFloatHashMap<T> map) {
        INSTANCE.pushObjectFloatMap(map);
    }

    public static Object[] popObjectArray(int size) {
        return INSTANCE.popObjectArray(size);
    }

    public static void pushObjectArray(Object[] objects) {
        INSTANCE.pushObjectArray(objects);
    }

    public static int[] popIntArray(int size) {
        return INSTANCE.popIntArray(size);
    }

    public static int[] popIntArray(int size, int sentinal) {
        return INSTANCE.popIntArray(size, sentinal);
    }

    public static void pushIntArray(int[] ints) {
        INSTANCE.pushIntArray(ints);
    }

    public static void pushIntArray(int[] ints, int sentinal) {
        INSTANCE.pushIntArray(ints, sentinal);
    }
   
}