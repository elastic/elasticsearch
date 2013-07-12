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

package org.elasticsearch.cache.recycler;

import gnu.trove.map.hash.*;
import gnu.trove.set.hash.THashSet;
import org.elasticsearch.common.trove.ExtTDoubleObjectHashMap;
import org.elasticsearch.common.trove.ExtTHashMap;
import org.elasticsearch.common.trove.ExtTLongObjectHashMap;

public interface CacheRecycler {

    public abstract void clear();

    public abstract <K, V> ExtTHashMap<K, V> popHashMap();

    public abstract void pushHashMap(ExtTHashMap map);

    public abstract <T> THashSet<T> popHashSet();

    public abstract void pushHashSet(THashSet map);

    public abstract <T> ExtTDoubleObjectHashMap<T> popDoubleObjectMap();

    public abstract void pushDoubleObjectMap(ExtTDoubleObjectHashMap map);

    public abstract <T> ExtTLongObjectHashMap<T> popLongObjectMap();

    public abstract void pushLongObjectMap(ExtTLongObjectHashMap map);

    public abstract TLongLongHashMap popLongLongMap();

    public abstract void pushLongLongMap(TLongLongHashMap map);

    public abstract TIntIntHashMap popIntIntMap();

    public abstract void pushIntIntMap(TIntIntHashMap map);

    public abstract TFloatIntHashMap popFloatIntMap();

    public abstract void pushFloatIntMap(TFloatIntHashMap map);

    public abstract TDoubleIntHashMap popDoubleIntMap();

    public abstract void pushDoubleIntMap(TDoubleIntHashMap map);

    public abstract TByteIntHashMap popByteIntMap();

    public abstract void pushByteIntMap(TByteIntHashMap map);

    public abstract TShortIntHashMap popShortIntMap();

    public abstract void pushShortIntMap(TShortIntHashMap map);

    public abstract TLongIntHashMap popLongIntMap();

    public abstract void pushLongIntMap(TLongIntHashMap map);

    public abstract <T> TObjectIntHashMap<T> popObjectIntMap();

    public abstract <T> void pushObjectIntMap(TObjectIntHashMap<T> map);

    public abstract <T> TIntObjectHashMap<T> popIntObjectMap();

    public abstract <T> void pushIntObjectMap(TIntObjectHashMap<T> map);

    public abstract <T> TObjectFloatHashMap<T> popObjectFloatMap();

    public abstract <T> void pushObjectFloatMap(TObjectFloatHashMap<T> map);

    public abstract Object[] popObjectArray(int size);

    public abstract void pushObjectArray(Object[] objects);

    public abstract int[] popIntArray(int size);

    public abstract int[] popIntArray(int size, int sentinal);

    public abstract void pushIntArray(int[] ints);

    public abstract void pushIntArray(int[] ints, int sentinal);

}