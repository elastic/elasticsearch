/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.core;

public class Map {

    /**
     * Delegates to the Java9 {@code Map.of()} method.
     */
    public static <K, V> java.util.Map<K, V> of() {
        return java.util.Map.of();
    }

    /**
     * Delegates to the Java9 {@code Map.of()} method.
     */
    public static <K, V> java.util.Map<K, V> of(K k1, V v1) {
        return java.util.Map.of(k1, v1);
    }

    /**
     * Delegates to the Java9 {@code Map.of()} method.
     */
    public static <K, V> java.util.Map<K, V> of(K k1, V v1, K k2, V v2) {
        return java.util.Map.of(k1, v1, k2, v2);
    }

    /**
     * Delegates to the Java9 {@code Map.of()} method.
     */
    public static <K, V> java.util.Map<K, V> of(K k1, V v1, K k2, V v2, K k3, V v3) {
        return java.util.Map.of(k1, v1, k2, v2, k3, v3);
    }

    /**
     * Delegates to the Java9 {@code Map.of()} method.
     */
    public static <K, V> java.util.Map<K, V> of(K k1, V v1, K k2, V v2, K k3, V v3, K k4, V v4) {
        return java.util.Map.of(k1, v1, k2, v2, k3, v3, k4, v4);
    }

    /**
     * Delegates to the Java9 {@code Map.of()} method.
     */
    public static <K, V> java.util.Map<K, V> of(K k1, V v1, K k2, V v2, K k3, V v3, K k4, V v4, K k5, V v5) {
        return java.util.Map.of(k1, v1, k2, v2, k3, v3, k4, v4, k5, v5);
    }

    /**
     * Delegates to the Java9 {@code Map.of()} method.
     */
    public static <K, V> java.util.Map<K, V> of(K k1, V v1, K k2, V v2, K k3, V v3, K k4, V v4, K k5, V v5, K k6, V v6) {
        return java.util.Map.of(k1, v1, k2, v2, k3, v3, k4, v4, k5, v5, k6, v6);
    }

    /**
     * Delegates to the Java9 {@code Map.of()} method.
     */
    public static <K, V> java.util.Map<K, V> of(K k1, V v1, K k2, V v2, K k3, V v3, K k4, V v4, K k5, V v5, K k6, V v6, K k7, V v7) {
        return java.util.Map.of(k1, v1, k2, v2, k3, v3, k4, v4, k5, v5, k6, v6, k7, v7);
    }

    /**
     * Delegates to the Java9 {@code Map.of()} method.
     */
    public static <K, V> java.util.Map<K, V> of(
        K k1,
        V v1,
        K k2,
        V v2,
        K k3,
        V v3,
        K k4,
        V v4,
        K k5,
        V v5,
        K k6,
        V v6,
        K k7,
        V v7,
        K k8,
        V v8
    ) {
        return java.util.Map.of(k1, v1, k2, v2, k3, v3, k4, v4, k5, v5, k6, v6, k7, v7, k8, v8);
    }

    /**
     * Delegates to the Java9 {@code Map.of()} method.
     */
    public static <K, V> java.util.Map<K, V> of(
        K k1,
        V v1,
        K k2,
        V v2,
        K k3,
        V v3,
        K k4,
        V v4,
        K k5,
        V v5,
        K k6,
        V v6,
        K k7,
        V v7,
        K k8,
        V v8,
        K k9,
        V v9
    ) {
        return java.util.Map.of(k1, v1, k2, v2, k3, v3, k4, v4, k5, v5, k6, v6, k7, v7, k8, v8, k9, v9);
    }

    /**
     * Delegates to the Java9 {@code Map.of()} method.
     */
    public static <K, V> java.util.Map<K, V> of(
        K k1,
        V v1,
        K k2,
        V v2,
        K k3,
        V v3,
        K k4,
        V v4,
        K k5,
        V v5,
        K k6,
        V v6,
        K k7,
        V v7,
        K k8,
        V v8,
        K k9,
        V v9,
        K k10,
        V v10
    ) {
        return java.util.Map.of(k1, v1, k2, v2, k3, v3, k4, v4, k5, v5, k6, v6, k7, v7, k8, v8, k9, v9, k10, v10);
    }

    /**
     * Delegates to the Java9 {@code Map.ofEntries()} method.
     */
    @SafeVarargs
    @SuppressWarnings("varargs")
    public static <K, V> java.util.Map<K, V> ofEntries(java.util.Map.Entry<? extends K, ? extends V>... entries) {
        return java.util.Map.ofEntries(entries);
    }

    /**
     * Delegates to the Java9 {@code Map.entry()} method.
     */
    public static <K, V> java.util.Map.Entry<K, V> entry(K k, V v) {
        return java.util.Map.entry(k, v);
    }

    /**
     * Delegates to the Java10 {@code Map.copyOf()} method.
     */
    public static <K, V> java.util.Map<K, V> copyOf(java.util.Map<? extends K, ? extends V> map) {
        return java.util.Map.copyOf(map);
    }

}
