/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.cache;

import org.elasticsearch.core.TimeValue;

import java.util.Objects;
import java.util.function.ToLongBiFunction;

/**
 * A builder for constructing {@link Cache} instances with various configuration options.
 * This builder uses a fluent API pattern, allowing method chaining for configuration.
 *
 * <p><b>Usage Examples:</b></p>
 * <pre>{@code
 * // Create a simple cache with maximum weight
 * Cache<String, String> cache = CacheBuilder.<String, String>builder()
 *     .setMaximumWeight(1000)
 *     .build();
 *
 * // Create a cache with expiration and custom weigher
 * Cache<String, byte[]> cache = CacheBuilder.<String, byte[]>builder()
 *     .setMaximumWeight(10000)
 *     .setExpireAfterAccess(TimeValue.timeValueMinutes(5))
 *     .weigher((key, value) -> value.length)
 *     .removalListener(notification -> {
 *         System.out.println("Removed: " + notification.getKey());
 *     })
 *     .build();
 * }</pre>
 *
 * @param <K> the type of keys maintained by caches created by this builder
 * @param <V> the type of values maintained by caches created by this builder
 */
public class CacheBuilder<K, V> {
    private long maximumWeight = -1;
    private long expireAfterAccessNanos = -1;
    private long expireAfterWriteNanos = -1;
    private ToLongBiFunction<K, V> weigher;
    private RemovalListener<K, V> removalListener;

    /**
     * Creates a new cache builder instance.
     *
     * @param <K> the type of keys
     * @param <V> the type of values
     * @return a new cache builder
     */
    public static <K, V> CacheBuilder<K, V> builder() {
        return new CacheBuilder<>();
    }

    private CacheBuilder() {}

    /**
     * Sets the maximum weight of entries the cache may contain. Weight is determined by the
     * configured weigher, or defaults to counting entries if no weigher is set.
     *
     * @param maximumWeight the maximum total weight of entries the cache may contain
     * @return this builder instance
     * @throws IllegalArgumentException if maximumWeight is negative
     */
    public CacheBuilder<K, V> setMaximumWeight(long maximumWeight) {
        if (maximumWeight < 0) {
            throw new IllegalArgumentException("maximumWeight < 0");
        }
        this.maximumWeight = maximumWeight;
        return this;
    }

    /**
     * Sets the amount of time before an entry in the cache expires after it was last accessed.
     *
     * @param expireAfterAccess The amount of time before an entry expires after it was last accessed. Must not be {@code null} and must
     *                          be greater than 0.
     */
    public CacheBuilder<K, V> setExpireAfterAccess(TimeValue expireAfterAccess) {
        Objects.requireNonNull(expireAfterAccess);
        final long expireAfterAccessNanos = expireAfterAccess.getNanos();
        if (expireAfterAccessNanos <= 0) {
            throw new IllegalArgumentException("expireAfterAccess <= 0");
        }
        this.expireAfterAccessNanos = expireAfterAccessNanos;
        return this;
    }

    /**
     * Sets the amount of time before an entry in the cache expires after it was written.
     *
     * @param expireAfterWrite The amount of time before an entry expires after it was written. Must not be {@code null} and must be
     *                         greater than 0.
     */
    public CacheBuilder<K, V> setExpireAfterWrite(TimeValue expireAfterWrite) {
        Objects.requireNonNull(expireAfterWrite);
        final long expireAfterWriteNanos = expireAfterWrite.getNanos();
        if (expireAfterWriteNanos <= 0) {
            throw new IllegalArgumentException("expireAfterWrite <= 0");
        }
        this.expireAfterWriteNanos = expireAfterWriteNanos;
        return this;
    }

    /**
     * Sets the weigher function to determine the weight of cache entries.
     * The weigher is called for each entry to determine how much weight it contributes
     * toward the maximum weight limit.
     *
     * @param weigher the weigher function
     * @return this builder instance
     * @throws NullPointerException if weigher is null
     */
    public CacheBuilder<K, V> weigher(ToLongBiFunction<K, V> weigher) {
        Objects.requireNonNull(weigher);
        this.weigher = weigher;
        return this;
    }

    /**
     * Sets the removal listener to be notified when entries are removed from the cache.
     * The listener is called for all removals, whether due to eviction, expiration, or explicit invalidation.
     *
     * @param removalListener the removal listener
     * @return this builder instance
     * @throws NullPointerException if removalListener is null
     */
    public CacheBuilder<K, V> removalListener(RemovalListener<K, V> removalListener) {
        Objects.requireNonNull(removalListener);
        this.removalListener = removalListener;
        return this;
    }

    /**
     * Builds a cache instance with the configured settings.
     *
     * @return a new cache instance
     */
    public Cache<K, V> build() {
        Cache<K, V> cache = new Cache<>();
        if (maximumWeight != -1) {
            cache.setMaximumWeight(maximumWeight);
        }
        if (expireAfterAccessNanos != -1) {
            cache.setExpireAfterAccessNanos(expireAfterAccessNanos);
        }
        if (expireAfterWriteNanos != -1) {
            cache.setExpireAfterWriteNanos(expireAfterWriteNanos);
        }
        if (weigher != null) {
            cache.setWeigher(weigher);
        }
        if (removalListener != null) {
            cache.setRemovalListener(removalListener);
        }
        return cache;
    }
}
