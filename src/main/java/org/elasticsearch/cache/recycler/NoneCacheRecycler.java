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

import java.util.Arrays;

public class NoneCacheRecycler implements CacheRecycler {

    @Override
    public void clear() {
    }

    @Override
    @SuppressWarnings("unchecked")
    public <K, V> ExtTHashMap<K, V> popHashMap() {
        return new ExtTHashMap<K, V>();
    }

    @Override
    public void pushHashMap(ExtTHashMap map) {
        map.clear();
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> THashSet<T> popHashSet() {
        return new THashSet<T>();
    }

    @Override
    public void pushHashSet(THashSet map) {
        map.clear();
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> ExtTDoubleObjectHashMap<T> popDoubleObjectMap() {
        return new ExtTDoubleObjectHashMap();
    }

    @Override
    public void pushDoubleObjectMap(ExtTDoubleObjectHashMap map) {
        map.clear();
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> ExtTLongObjectHashMap<T> popLongObjectMap() {
        return new ExtTLongObjectHashMap();
    }

    @Override
    public void pushLongObjectMap(ExtTLongObjectHashMap map) {
        map.clear();
    }

    @Override
    public TLongLongHashMap popLongLongMap() {
        return new TLongLongHashMap();
    }

    @Override
    public void pushLongLongMap(TLongLongHashMap map) {
    }

    @Override
    public TIntIntHashMap popIntIntMap() {
        return new TIntIntHashMap();
    }

    @Override
    public void pushIntIntMap(TIntIntHashMap map) {
    }

    @Override
    public TFloatIntHashMap popFloatIntMap() {
        return new TFloatIntHashMap();
    }

    @Override
    public void pushFloatIntMap(TFloatIntHashMap map) {
    }

    @Override
    public TDoubleIntHashMap popDoubleIntMap() {
        return new TDoubleIntHashMap();
    }

    @Override
    public void pushDoubleIntMap(TDoubleIntHashMap map) {
    }

    @Override
    public TByteIntHashMap popByteIntMap() {
        return new TByteIntHashMap();
    }

    @Override
    public void pushByteIntMap(TByteIntHashMap map) {
    }

    @Override
    public TShortIntHashMap popShortIntMap() {
        return new TShortIntHashMap();
    }

    @Override
    public void pushShortIntMap(TShortIntHashMap map) {
    }

    @Override
    public TLongIntHashMap popLongIntMap() {
        return new TLongIntHashMap();
    }

    @Override
    public void pushLongIntMap(TLongIntHashMap map) {
    }

    @Override
    @SuppressWarnings({"unchecked"})
    public <T> TObjectIntHashMap<T> popObjectIntMap() {
        return new TObjectIntHashMap();
    }

    @Override
    public <T> void pushObjectIntMap(TObjectIntHashMap<T> map) {
    }

    @Override
    @SuppressWarnings({"unchecked"})
    public <T> TIntObjectHashMap<T> popIntObjectMap() {
        return new TIntObjectHashMap<T>();
    }

    @Override
    public <T> void pushIntObjectMap(TIntObjectHashMap<T> map) {
    }

    @Override
    @SuppressWarnings({"unchecked"})
    public <T> TObjectFloatHashMap<T> popObjectFloatMap() {
        return new TObjectFloatHashMap();
    }

    @Override
    public <T> void pushObjectFloatMap(TObjectFloatHashMap<T> map) {
    }

    @Override
    public Object[] popObjectArray(int size) {
        return new Object[size];
    }

    @Override
    public void pushObjectArray(Object[] objects) {
    }

    @Override
    public int[] popIntArray(int size) {
        return popIntArray(size, 0);
    }

    @Override
    public int[] popIntArray(int size, int sentinal) {
        int[] ints = new int[size];
        if (sentinal != 0) {
            Arrays.fill(ints, sentinal);
        }
        return ints;
    }

    @Override
    public void pushIntArray(int[] ints) {
        pushIntArray(ints, 0);
    }

    @Override
    public void pushIntArray(int[] ints, int sentinal) {
    }
}