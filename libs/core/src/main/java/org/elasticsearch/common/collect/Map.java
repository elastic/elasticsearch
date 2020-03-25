/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.common.collect;

import java.util.Collections;
import java.util.HashMap;

public class Map {

    /**
     * Returns an unmodifiable map containing one mapping.
     */
    public static <K, V> java.util.Map<K, V> of(K k1, V v1) {
        return mapN(k1, v1);
    }

    /**
     * Returns an unmodifiable map containing two mappings.
     */
    public static <K, V> java.util.Map<K, V> of(K k1, V v1, K k2, V v2) {
        return mapN(k1, v1, k2, v2);
    }

    /**
     * Returns an unmodifiable map containing three mappings.
     */
    public static <K, V> java.util.Map<K, V> of(K k1, V v1, K k2, V v2, K k3, V v3) {
        return mapN(k1, v1, k2, v2, k3, v3);
    }

    /**
     * Returns an unmodifiable map containing four mappings.
     */
    public static <K, V> java.util.Map<K, V> of(K k1, V v1, K k2, V v2, K k3, V v3, K k4, V v4) {
        return mapN(k1, v1, k2, v2, k3, v3, k4, v4);
    }

    /**
     * Returns an unmodifiable map containing five mappings.
     */
    public static <K, V> java.util.Map<K, V> of(K k1, V v1, K k2, V v2, K k3, V v3, K k4, V v4, K k5, V v5) {
        return mapN(k1, v1, k2, v2, k3, v3, k4, v4, k5, v5);
    }

    /**
     * Returns an unmodifiable map containing six mappings.
     */
    public static <K, V> java.util.Map<K, V> of(K k1, V v1, K k2, V v2, K k3, V v3, K k4, V v4, K k5, V v5,
                                                K k6, V v6) {
        return mapN(k1, v1, k2, v2, k3, v3, k4, v4, k5, v5, k6, v6);
    }

    /**
     * Returns an unmodifiable map containing seven mappings.
     */
    public static <K, V> java.util.Map<K, V> of(K k1, V v1, K k2, V v2, K k3, V v3, K k4, V v4, K k5, V v5,
                                                K k6, V v6, K k7, V v7) {
        return mapN(k1, v1, k2, v2, k3, v3, k4, v4, k5, v5, k6, v6, k7, v7);
    }

    /**
     * Returns an unmodifiable map containing eight mappings.
     */
    public static <K, V> java.util.Map<K, V> of(K k1, V v1, K k2, V v2, K k3, V v3, K k4, V v4, K k5, V v5,
                                                K k6, V v6, K k7, V v7, K k8, V v8) {
        return mapN(k1, v1, k2, v2, k3, v3, k4, v4, k5, v5, k6, v6, k7, v7, k8, v8);
    }

    /**
     * Returns an unmodifiable map containing nine mappings.
     */
    public static <K, V> java.util.Map<K, V> of(K k1, V v1, K k2, V v2, K k3, V v3, K k4, V v4, K k5, V v5,
                                                K k6, V v6, K k7, V v7, K k8, V v8, K k9, V v9) {
        return mapN(k1, v1, k2, v2, k3, v3, k4, v4, k5, v5, k6, v6, k7, v7, k8, v8, k9, v9);
    }

    /**
     * Returns an unmodifiable map containing ten mappings.
     */
    public static <K, V> java.util.Map<K, V> of(K k1, V v1, K k2, V v2, K k3, V v3, K k4, V v4, K k5, V v5,
                                                K k6, V v6, K k7, V v7, K k8, V v8, K k9, V v9, K k10, V v10) {
        return mapN(k1, v1, k2, v2, k3, v3, k4, v4, k5, v5, k6, v6, k7, v7, k8, v8, k9, v9, k10, v10);
    }

    @SuppressWarnings("unchecked")
    private static <K, V> java.util.Map<K, V> mapN(Object... objects) {
        if (objects.length % 2 != 0) {
            throw new IllegalStateException("Must provide an even number of arguments to Map::of method");
        }
        HashMap<K, V> map = new HashMap<>();
        for (int k = 0; k < objects.length / 2; k++) {
            map.put((K) objects[k * 2], (V) objects[k * 2 + 1]);
        }
       return Collections.unmodifiableMap(map);
    }
}
