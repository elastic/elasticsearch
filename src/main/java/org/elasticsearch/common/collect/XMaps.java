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

package org.elasticsearch.common.collect;

import com.google.common.collect.ForwardingMap;
import gnu.trove.impl.Constants;
import org.elasticsearch.ElasticSearchIllegalArgumentException;
import org.elasticsearch.common.trove.ExtTHashMap;

import java.util.Collections;
import java.util.Map;

/**
 * This class provides factory methods for Maps. The returned {@link Map}
 * instances are general purpose maps and non of the method guarantees a
 * concrete implementation unless the return type is a concrete type. The
 * implementations used might change over time, if you rely on a specific
 * Implementation you should use a concrete constructor.
 */
public final class XMaps {

    public static final int DEFAULT_CAPACITY = Constants.DEFAULT_CAPACITY;

    /**
     * Returns a new map with the given initial capacity
     */
    public static <K, V> Map<K, V> newMap(int capacity) {
        return new ExtTHashMap<K, V>(capacity, Constants.DEFAULT_LOAD_FACTOR);
    }

    /**
     * Returns a new map with a default initial capacity of
     * {@value #DEFAULT_CAPACITY}
     */
    public static <K, V> Map<K, V> newMap() {
        return newMap(DEFAULT_CAPACITY);
    }

    /**
     * Returns a map like {@link #newMap()} that does not accept <code>null</code> keys
     */
    public static <K, V> Map<K, V> newNoNullKeysMap() {
        Map<K, V> delegate = newMap();
        return ensureNoNullKeys(delegate);
    }

    /**
     * Returns a map like {@link #newMap(in)} that does not accept <code>null</code> keys
     */
    public static <K, V> Map<K, V> newNoNullKeysMap(int capacity) {
        Map<K, V> delegate = newMap(capacity);
        return ensureNoNullKeys(delegate);
    }

    /**
     * Wraps the given map and prevent adding of <code>null</code> keys.
     */
    public static <K, V> Map<K, V> ensureNoNullKeys(final Map<K, V> delegate) {
        return new ForwardingMap<K, V>() {
            @Override
            public V put(K key, V value) {
                if (key == null) {
                    throw new ElasticSearchIllegalArgumentException("Map key must not be null");
                }
                return super.put(key, value);
            }

            @Override
            protected Map<K, V> delegate() {
                return delegate;
            }
        };
    }

    /**
     * Wraps the given map into a read only implementation.
     */
    public static <K, V> Map<K, V> makeReadOnly(Map<K, V> map) {
        return Collections.unmodifiableMap(map);
    }

}
