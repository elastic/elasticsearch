/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.elasticsearch.index.store.remote.utils.cache;


import org.elasticsearch.common.cache.RemovalListener;
import org.elasticsearch.common.cache.RemovalNotification;
import org.elasticsearch.common.cache.Weigher;
import org.elasticsearch.index.store.remote.utils.cache.stats.CacheStats;

import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.Predicate;

/**
 * Segmented {@link LRUCache} to offer concurrent access with less contention.
 * @param <K> type of the key
 * @param <V> type of th value
 *
 * @opensearch.internal
 */
public class SegmentedCache<K, V> implements RefCountedCache<K, V> {

    private static final int HASH_BITS = 0x7fffffff;

    @SuppressWarnings("checkstyle:RedundantModifier")
    private static final int ceilingNextPowerOfTwo(int x) {
        // From Hacker's Delight, Chapter 3, Harry S. Warren Jr.
        return 1 << (Integer.SIZE - Integer.numberOfLeadingZeros(x - 1));
    }

    private final long capacity;

    private final long perSegmentCapacity;
    /**
     * The array of bins. Size is always a power of two.
     */
    private final RefCountedCache<K, V>[] table;

    /**
     * Mask value for indexing into segments.
     */
    private final int segmentMask;

    private final Weigher<V> weigher;

    public SegmentedCache(Builder<K, V> builder) {
        this.capacity = builder.capacity;
        final int segments = ceilingNextPowerOfTwo(builder.concurrencyLevel);
        this.segmentMask = segments - 1;
        this.table = newSegmentArray(segments);
        this.perSegmentCapacity = (capacity + (segments - 1)) / segments;
        this.weigher = builder.weigher;
        for (int i = 0; i < table.length; i++) {
            table[i] = new LRUCache<>(perSegmentCapacity, builder.listener, builder.weigher);
        }
    }

    @SuppressWarnings("unchecked")
    final RefCountedCache<K, V>[] newSegmentArray(int size) {
        return new RefCountedCache[size];
    }

    RefCountedCache<K, V> segmentFor(K key) {
        int h = key.hashCode();
        // Based on this answer https://stackoverflow.com/a/12996028
        h = ((h >>> 16) ^ h) * 0x45d9f3b;
        h = ((h >>> 16) ^ h) * 0x45d9f3b;
        h = (h >>> 16) ^ h;
        return table[h & HASH_BITS & segmentMask];
    }

    public long capacity() {
        return capacity;
    }

    @Override
    public V get(K key) {
        if (key == null) throw new NullPointerException();
        return segmentFor(key).get(key);
    }

    @Override
    public V put(K key, V value) {
        if (key == null || value == null) throw new NullPointerException();
        return segmentFor(key).put(key, value);
    }

    @Override
    public V compute(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
        if (key == null || remappingFunction == null) throw new NullPointerException();
        return segmentFor(key).compute(key, remappingFunction);
    }

    @Override
    public void remove(K key) {
        if (key == null) throw new NullPointerException();
        segmentFor(key).remove(key);
    }

    @Override
    public void clear() {
        for (RefCountedCache<K, V> cache : table) {
            cache.clear();
        }
    }

    @Override
    public long size() {
        long size = 0;
        for (RefCountedCache<K, V> cache : table) {
            size += cache.size();
        }
        return size;
    }

    @Override
    public void incRef(K key) {
        if (key == null) throw new NullPointerException();
        segmentFor(key).incRef(key);
    }

    @Override
    public void decRef(K key) {
        if (key == null) throw new NullPointerException();
        segmentFor(key).decRef(key);
    }

    @Override
    public long prune() {
        long sum = 0L;
        for (RefCountedCache<K, V> cache : table) {
            sum += cache.prune();
        }
        return sum;
    }

    @Override
    public long prune(Predicate<K> keyPredicate) {
        long sum = 0L;
        for (RefCountedCache<K, V> cache : table) {
            sum += cache.prune(keyPredicate);
        }
        return sum;
    }

    @Override
    public CacheUsage usage() {
        long usage = 0L;
        long activeUsage = 0L;
        for (RefCountedCache<K, V> cache : table) {
            CacheUsage c = cache.usage();
            usage += c.usage();
            activeUsage += c.activeUsage();
        }
        return new CacheUsage(usage, activeUsage);
    }

    @Override
    public CacheStats stats() {
        long hitCount = 0L;
        long missCount = 0L;
        long removeCount = 0L;
        long removeWeight = 0L;
        long replaceCount = 0L;
        long evictionCount = 0L;
        long evictionWeight = 0L;

        for (RefCountedCache<K, V> cache : table) {
            CacheStats c = cache.stats();
            hitCount += c.hitCount();
            missCount += c.missCount();
            removeCount += c.removeCount();
            removeWeight += c.removeWeight();
            replaceCount += c.replaceCount();
            evictionCount += c.evictionCount();
            evictionWeight += c.evictionWeight();
        }
        return new CacheStats(hitCount, missCount, removeCount, removeWeight, replaceCount, evictionCount, evictionWeight);
    }

    enum SingletonWeigher implements Weigher<Object> {
        INSTANCE;

        @Override
        public long weightOf(Object value) {
            return 1;
        }
    }

    /**
     * A listener that ignores all notifications.
     */
    enum DiscardingListener implements RemovalListener<Object, Object> {
        INSTANCE;

        @Override
        public void onRemoval(RemovalNotification<Object, Object> notification) {}
    }

    /**
     * @return the capacity per internal segment
     */
    public long getPerSegmentCapacity() {
        return perSegmentCapacity;
    }

    /**
     * @return the weigher used for IndexInput
     */
    public Weigher<V> getWeigher() {
        return weigher;
    }

    /**
     * A builder that creates {@link SegmentedCache} instances. It
     * provides a flexible approach for constructing customized instances with
     * a named parameter syntax.
     */
    public static final class Builder<K, V> {

        static final int DEFAULT_CONCURRENCY_LEVEL = Runtime.getRuntime().availableProcessors();

        RemovalListener<K, V> listener;
        Weigher<V> weigher;

        int concurrencyLevel;

        long capacity;

        @SuppressWarnings("unchecked")
        Builder() {
            capacity = -1;
            weigher = (Weigher<V>) SingletonWeigher.INSTANCE;
            concurrencyLevel = DEFAULT_CONCURRENCY_LEVEL;
            listener = (RemovalListener<K, V>) DiscardingListener.INSTANCE;
        }

        /**
         * Specifies the maximum weighted capacity to coerce the map to and may
         * exceed it temporarily.
         *
         * @param capacity the weighted threshold to bound the map by
         * @throws IllegalArgumentException if the maximumWeightedCapacity is
         *                                  negative
         */
        public Builder<K, V> capacity(long capacity) {
            checkArgument(capacity >= 0, "capacity has to be greater or equal to 0");
            this.capacity = capacity;
            return this;
        }

        /**
         * Specifies the estimated number of concurrently updating threads. The
         * implementation performs internal sizing to try to accommodate this many
         * threads (default Runtime.getRuntime().availableProcessors()).
         *
         * @param concurrencyLevel the estimated number of concurrently updating
         *                         threads
         * @throws IllegalArgumentException if the concurrencyLevel is less than or
         *                                  equal to zero
         */
        public Builder<K, V> concurrencyLevel(int concurrencyLevel) {
            checkArgument(concurrencyLevel > 0, "concurrencyLevel has to be greater than 0");
            this.concurrencyLevel = concurrencyLevel;
            return this;
        }

        /**
         * Specifies an optional listener that is registered for notification when
         * an entry is removed.
         *
         * @param listener the object to forward removed entries to
         * @throws NullPointerException if the listener is null
         */
        public Builder<K, V> listener(RemovalListener<K, V> listener) {
            Objects.requireNonNull(listener);
            this.listener = listener;
            return this;
        }

        /**
         * Specifies an algorithm to determine how many the units of capacity a
         * value consumes. The default algorithm bounds the map by the number of
         * key-value pairs by giving each entry a weight of 1.
         *
         * @param weigher the algorithm to determine a value's weight
         * @throws NullPointerException if the weigher is null
         */
        public Builder<K, V> weigher(Weigher<V> weigher) {
            Objects.requireNonNull(weigher);
            this.weigher = weigher;
            return this;
        }

        /**
         * Ensures that the argument expression is true.
         */
        @SuppressWarnings("checkstyle:DescendantToken")
        private static void checkArgument(boolean expression, String message) {
            if (!expression) {
                throw new IllegalArgumentException(message);
            }
        }

        /**
         * Creates a new {@link SegmentedCache} instance.
         *
         * @throws IllegalStateException if the maximum weighted capacity was
         *                               not set
         */
        public SegmentedCache<K, V> build() {
            return new SegmentedCache<>(this);
        }
    }

    public static <K, V> Builder<K, V> builder() {
        return new Builder<>();
    }
}
