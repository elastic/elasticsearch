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

package org.elasticsearch.common.util;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

public abstract class MinimalMap<K, V> implements Map<K, V> {

    @Override
    public boolean isEmpty() {
        throw new UnsupportedOperationException("entrySet() not supported!");
    }

    @Override
    public V put(K key, V value) {
        throw new UnsupportedOperationException("put(Object, Object) not supported!");
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> m) {
        throw new UnsupportedOperationException("putAll(Map<? extends K, ? extends V>) not supported!");
    }

    @Override
    public V remove(Object key) {
        throw new UnsupportedOperationException("remove(Object) not supported!");
    }

    @Override
    public void clear() {
        throw new UnsupportedOperationException("clear() not supported!");
    }

    @Override
    public Set<K> keySet() {
        throw new UnsupportedOperationException("keySet() not supported!");
    }

    @Override
    public Collection<V> values() {
        throw new UnsupportedOperationException("values() not supported!");
    }

    @Override
    public Set<Entry<K, V>> entrySet() {
        throw new UnsupportedOperationException("entrySet() not supported!");
    }

    @Override
    public boolean containsValue(Object value) {
        throw new UnsupportedOperationException("containsValue(Object) not supported!");
    }

    @Override
    public int size() {
        throw new UnsupportedOperationException("size() not supported!");
    }

    @Override
    public boolean containsKey(Object k) {
        throw new UnsupportedOperationException("containsKey(Object) not supported!");
    }
}