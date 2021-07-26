/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.cache;

import org.elasticsearch.core.TimeValue;

import java.util.Objects;
import java.util.function.ToLongBiFunction;

public class CacheBuilder<K, V> {
    private long maximumWeight = -1;
    private long expireAfterAccessNanos = -1;
    private long expireAfterWriteNanos = -1;
    private ToLongBiFunction<K, V> weigher;
    private RemovalListener<K, V> removalListener;

    public static <K, V> CacheBuilder<K, V> builder() {
        return new CacheBuilder<>();
    }

    private CacheBuilder() {
    }

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

    public CacheBuilder<K, V> weigher(ToLongBiFunction<K, V> weigher) {
        Objects.requireNonNull(weigher);
        this.weigher = weigher;
        return this;
    }

    public CacheBuilder<K, V> removalListener(RemovalListener<K, V> removalListener) {
        Objects.requireNonNull(removalListener);
        this.removalListener = removalListener;
        return this;
    }

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
