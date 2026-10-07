/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.cache;

import org.elasticsearch.common.cache.Cache;
import org.elasticsearch.common.cache.CacheBuilder;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;

import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.ToLongFunction;

/**
 * One byte-bounded store of one kind of cached fact: a {@link Cache}, the slice of the total budget it may
 * occupy, and the per-entry ceiling that keeps a single oversized value from flushing the working set.
 * <p>
 * It exists because those three travel together and were previously copied per store - the budget division,
 * {@link #perEntryCeiling}, the refuse-before-put, the invalidate-on-oversize, and five
 * {@code usageStats} lines each. Separating the caches by kind of fact multiplies the number of stores, so the
 * duplication is what would have grown.
 * <p>
 * <b>Not a supertype for the values.</b> Weight is supplied as a {@link ToLongFunction} over the value, so a
 * value type needs no interface and no base class to be stored here: a constant-weight value passes
 * {@code v -> BYTES} and a variable one passes its own accessor. An {@code estimatedBytes()} interface
 * implemented by every cached type would exist only so that one lambda could be written once, and would put a
 * cache concern into types that are otherwise plain values.
 * <p>
 * <b>The weigher is called more than once per entry.</b> {@code Cache.promote} relinks through
 * {@code unlink} (weight minus weigher) and {@code linkAtHead} (weight plus weigher) on every hit that is not
 * already at the LRU head, so a weight function that walks a value's fields runs twice per warm hit. Values
 * here therefore precompute their own weight at construction, and this type's contract is that
 * {@code weigher} is cheap and total rather than that it is called once.
 */
final class WeightedStore<K, V> {

    private final String name;
    private final Cache<K, V> cache;
    private final ToLongFunction<V> weigher;
    private final long budgetBytes;
    private final long maxEntryBytes;

    /**
     * @param name          the {@code usageStats} prefix, e.g. {@code schema_cache}
     * @param budgetBytes   this store's slice of the total cache budget
     * @param weigher       the retained size of one value; cheap, because the shared cache calls it twice per
     *                      non-head hit
     * @param expireAfterWrite a freshness clock, for a store whose contents can go stale without the key
     *                      changing; {@code null} for an identity-keyed store, where a changed fact derives a
     *                      different key and the stale entry ages out through the LRU
     */
    static <K, V> WeightedStore<K, V> of(String name, long budgetBytes, ToLongFunction<V> weigher, @Nullable TimeValue expireAfterWrite) {
        CacheBuilder<K, V> builder = CacheBuilder.<K, V>builder().setMaximumWeight(budgetBytes).weigher((k, v) -> weigher.applyAsLong(v));
        if (expireAfterWrite != null) {
            builder.setExpireAfterWrite(expireAfterWrite);
        }
        return new WeightedStore<>(name, builder.build(), weigher, budgetBytes);
    }

    private WeightedStore(String name, Cache<K, V> cache, ToLongFunction<V> weigher, long budgetBytes) {
        this.name = name;
        this.cache = cache;
        this.weigher = weigher;
        this.budgetBytes = budgetBytes;
        this.maxEntryBytes = perEntryCeiling(budgetBytes);
    }

    /**
     * Per-entry admission ceiling: a quarter of the slice, floored at {@link #PER_ENTRY_CEILING_FLOOR_BYTES} so
     * a deliberately tiny slice does not refuse every realistic entry, and never above the slice itself.
     */
    static long perEntryCeiling(long sliceBudget) {
        if (sliceBudget <= 0L) {
            return 0L;
        }
        long quarter = Math.max(1L, sliceBudget / 4);
        long floored = Math.max(quarter, Math.min(PER_ENTRY_CEILING_FLOOR_BYTES, sliceBudget));
        return Math.min(sliceBudget, floored);
    }

    /**
     * Soft floor for {@link #perEntryCeiling}: when a slice is deliberately tiny - the warm-fold regression
     * suites run on tens of kilobytes to force eviction - a strict quarter would refuse entries the test needs
     * resident, so the ceiling floors here instead, capped by the slice.
     */
    static final long PER_ENTRY_CEILING_FLOOR_BYTES = 16 * 1024L;

    /**
     * Stores {@code value}, unless it is heavier than the per-entry ceiling - in which case any value already
     * at this address is dropped, so a tightened budget cannot leave a resident entry the ceiling now forbids.
     * The caller keeps its value either way; refusing to retain is not refusing to answer.
     */
    void putIfWithinCeiling(K key, V value) {
        if (weigher.applyAsLong(value) > maxEntryBytes) {
            cache.invalidate(key);
            return;
        }
        cache.put(key, value);
    }

    @Nullable
    V get(K key) {
        return cache.get(key);
    }

    void put(K key, V value) {
        cache.put(key, value);
    }

    void invalidate(K key) {
        cache.invalidate(key);
    }

    void invalidateAll() {
        cache.invalidateAll();
    }

    void forEach(BiConsumer<K, V> consumer) {
        cache.forEach(consumer);
    }

    long budgetBytes() {
        return budgetBytes;
    }

    long maxEntryBytes() {
        return maxEntryBytes;
    }

    /** The underlying cache, for the loader-bearing paths and the test accessors that need it. */
    Cache<K, V> cache() {
        return cache;
    }

    /** The five counters this store contributes to {@code usageStats}, under its own prefix. */
    void reportInto(Map<String, Object> stats) {
        stats.put(name + ".count", cache.count());
        stats.put(name + ".weight_bytes", cache.weight());
        stats.put(name + ".hits", cache.stats().getHits());
        stats.put(name + ".misses", cache.stats().getMisses());
        stats.put(name + ".evictions", cache.stats().getEvictions());
    }
}
