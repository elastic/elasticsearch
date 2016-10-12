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

package org.elasticsearch.common.cache;

import org.elasticsearch.common.unit.TimeValue;

import java.util.Objects;
import java.util.function.ToLongBiFunction;

public class CacheBuilder<K, V> {
    private long maximumWeight = -1;
    private long expireAfterAccess = -1;
    private long expireAfterWrite = -1;
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

    public CacheBuilder<K, V> setExpireAfterAccess(TimeValue expireAfterAccess) {
        Objects.requireNonNull(expireAfterAccess);
        final long expireAfterAccessNanos = expireAfterAccess.getNanos();
        if (expireAfterAccessNanos <= 0) {
            throw new IllegalArgumentException("expireAfterAccess <= 0");
        }
        this.expireAfterAccess = expireAfterAccessNanos;
        return this;
    }

    public CacheBuilder<K, V> setExpireAfterWrite(TimeValue expireAfterWrite) {
        Objects.requireNonNull(expireAfterWrite);
        final long expireAfterWriteNanos = expireAfterWrite.getNanos();
        if (expireAfterWriteNanos <= 0) {
            throw new IllegalArgumentException("expireAfterWrite <= 0");
        }
        this.expireAfterWrite = expireAfterWriteNanos;
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
        Cache<K, V> cache = new Cache();
        if (maximumWeight != -1) {
            cache.setMaximumWeight(maximumWeight);
        }
        if (expireAfterAccess != -1) {
            cache.setExpireAfterAccess(expireAfterAccess);
        }
        if (expireAfterWrite != -1) {
            cache.setExpireAfterWrite(expireAfterWrite);
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
