/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.cache;

/**
 * A function that computes or retrieves values to be stored in a {@link Cache}.
 * This interface is typically used with {@link Cache#computeIfAbsent(Object, CacheLoader)}
 * to automatically load values into the cache when they are not present.
 *
 * <p><b>Usage Examples:</b></p>
 * <pre>{@code
 * Cache<String, User> userCache = CacheBuilder.<String, User>builder()
 *     .setMaximumWeight(1000)
 *     .build();
 *
 * // Define a cache loader
 * CacheLoader<String, User> loader = userId -> {
 *     return database.loadUser(userId);
 * };
 *
 * // Use the loader to populate cache on miss
 * User user = userCache.computeIfAbsent("user123", loader);
 * }</pre>
 *
 * @param <K> the type of keys used to compute values
 * @param <V> the type of values returned by this loader
 */
@FunctionalInterface
public interface CacheLoader<K, V> {
    /**
     * Computes or retrieves the value corresponding to the given key.
     * This method is called when a cache lookup misses and needs to load the value.
     *
     * @param key the key whose associated value is to be loaded
     * @return the computed or retrieved value
     * @throws Exception if unable to compute or retrieve the value
     */
    V load(K key) throws Exception;
}
