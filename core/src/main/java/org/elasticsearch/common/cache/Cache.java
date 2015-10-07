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

import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.util.concurrent.ReleasableLock;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;
import java.util.function.ToLongBiFunction;

/**
 * A simple concurrent cache.
 * <p>
 * Cache is a simple concurrent cache that supports time-based and weight-based evictions, with notifications for all
 * evictions. The design goals for this cache were simplicity and read performance. This means that we are willing to
 * accept reduced write performance in exchange for easy-to-understand code. Cache statistics for hits, misses and
 * evictions are exposed.
 * <p>
 * The design of the cache is relatively simple. The cache is segmented into 256 segments which are backed by HashMaps.
 * Each segment is protected by a re-entrant read/write lock. The read/write locks permit multiple concurrent readers
 * without contention, and the segments gives us write throughput without impacting readers (so readers are blocked only
 * if they are reading a segment that a writer is writing to).
 * <p>
 * The LRU functionality is backed by a single doubly-linked list chaining the entries in order of insertion. This
 * LRU list is protected by a lock that serializes all writes to it. There are opportunities for improvements
 * here if write throughput is a concern.
 * <ol>
 * <li>LRU list mutations could be inserted into a blocking queue that a single thread is reading from
 * and applying to the LRU list.</li>
 * <li>Promotions could be deferred for entries that were "recently" promoted.</li>
 * <li>Locks on the list could be taken per node being modified instead of globally.</li>
 * </ol>
 * <p>
 * Evictions only occur after a mutation to the cache (meaning an entry promotion, a cache insertion, or a manual
 * invalidation) or an explicit call to {@link #refresh()}.
 *
 * @param <K> The type of the keys
 * @param <V> The type of the values
 */
public class Cache<K, V> {
    // positive if entries have an expiration
    private long expireAfterAccess = -1;

    // true if entries can expire after access
    private boolean entriesExpireAfterAccess;

    // positive if entries have an expiration after write
    private long expireAfterWrite = -1;

    // true if entries can expire after initial insertion
    private boolean entriesExpireAfterWrite;

    // the number of entries in the cache
    private int count = 0;

    // the weight of the entries in the cache
    private long weight = 0;

    // the maximum weight that this cache supports
    private long maximumWeight = -1;

    // the weigher of entries
    private ToLongBiFunction<K, V> weigher = (k, v) -> 1;

    // the removal callback
    private RemovalListener<K, V> removalListener = notification -> {
    };

    // use CacheBuilder to construct
    Cache() {
    }

    void setExpireAfterAccess(long expireAfterAccess) {
        if (expireAfterAccess <= 0) {
            throw new IllegalArgumentException("expireAfterAccess <= 0");
        }
        this.expireAfterAccess = expireAfterAccess;
        this.entriesExpireAfterAccess = true;
    }

    void setExpireAfterWrite(long expireAfterWrite) {
        if (expireAfterWrite <= 0) {
            throw new IllegalArgumentException("expireAfterWrite <= 0");
        }
        this.expireAfterWrite = expireAfterWrite;
        this.entriesExpireAfterWrite = true;
    }

    void setMaximumWeight(long maximumWeight) {
        if (maximumWeight < 0) {
            throw new IllegalArgumentException("maximumWeight < 0");
        }
        this.maximumWeight = maximumWeight;
    }

    void setWeigher(ToLongBiFunction<K, V> weigher) {
        Objects.requireNonNull(weigher);
        this.weigher = weigher;
    }

    void setRemovalListener(RemovalListener<K, V> removalListener) {
        this.removalListener = removalListener;
    }

    /**
     * The relative time used to track time-based evictions.
     *
     * @return the current relative time
     */
    protected long now() {
        // System.nanoTime takes non-negligible time, so we only use it if we need it
        return entriesExpireAfterAccess || entriesExpireAfterWrite ? System.nanoTime() : 0;
    }

    // the state of an entry in the LRU list
    enum State {
        NEW, EXISTING, DELETED
    }

    static class Entry<K, V> {
        final K key;
        final V value;
        long writeTime;
        long accessTime;
        Entry<K, V> before;
        Entry<K, V> after;
        State state = State.NEW;

        public Entry(K key, V value, long writeTime) {
            this.key = key;
            this.value = value;
            this.writeTime = this.accessTime = writeTime;
        }
    }

    /**
     * A cache segment.
     * <p>
     * A CacheSegment is backed by a HashMap and is protected by a read/write lock.
     *
     * @param <K> the type of the keys
     * @param <V> the type of the values
     */
    private static class CacheSegment<K, V> {
        // read/write lock protecting mutations to the segment
        ReadWriteLock lock = new ReentrantReadWriteLock();

        ReleasableLock readLock = new ReleasableLock(lock.readLock());
        ReleasableLock writeLock = new ReleasableLock(lock.writeLock());

        Map<K, Entry<K, V>> map = new HashMap<>();
        SegmentStats segmentStats = new SegmentStats();

        /**
         * get an entry from the segment
         *
         * @param key the key of the entry to get from the cache
         * @param now the access time of this entry
         * @return the entry if there was one, otherwise null
         */
        Entry<K, V> get(K key, long now) {
            Entry<K, V> entry;
            try (ReleasableLock ignored = readLock.acquire()) {
                entry = map.get(key);
            }
            if (entry != null) {
                segmentStats.hit();
                entry.accessTime = now;
            } else {
                segmentStats.miss();
            }
            return entry;
        }

        /**
         * put an entry into the segment
         *
         * @param key   the key of the entry to add to the cache
         * @param value the value of the entry to add to the cache
         * @param now   the access time of this entry
         * @return a tuple of the new entry and the existing entry, if there was one otherwise null
         */
        Tuple<Entry<K, V>, Entry<K, V>> put(K key, V value, long now) {
            Entry<K, V> entry = new Entry<>(key, value, now);
            Entry<K, V> existing;
            try (ReleasableLock ignored = writeLock.acquire()) {
                existing = map.put(key, entry);
            }
            return Tuple.tuple(entry, existing);
        }

        /**
         * remove an entry from the segment
         *
         * @param key the key of the entry to remove from the cache
         * @return the removed entry if there was one, otherwise null
         */
        Entry<K, V> remove(K key) {
            Entry<K, V> entry;
            try (ReleasableLock ignored = writeLock.acquire()) {
                entry = map.remove(key);
            }
            if (entry != null) {
                segmentStats.eviction();
            }
            return entry;
        }

        private static class SegmentStats {
            private final LongAdder hits = new LongAdder();
            private final LongAdder misses = new LongAdder();
            private final LongAdder evictions = new LongAdder();

            void hit() {
                hits.increment();
            }

            void miss() {
                misses.increment();
            }

            void eviction() {
                evictions.increment();
            }
        }
    }

    private CacheSegment<K, V>[] segments = new CacheSegment[256];

    {
        for (int i = 0; i < segments.length; i++) {
            segments[i] = new CacheSegment<>();
        }
    }

    Entry<K, V> head;
    Entry<K, V> tail;

    // lock protecting mutations to the LRU list
    private ReleasableLock lock = new ReleasableLock(new ReentrantLock());

    /**
     * Returns the value to which the specified key is mapped, or null if this map contains no mapping for the key.
     *
     * @param key the key whose associated value is to be returned
     * @return the value to which the specified key is mapped, or null if this map contains no mapping for the key
     */
    public V get(K key) {
        return get(key, now());
    }

    private V get(K key, long now) {
        CacheSegment<K, V> segment = getCacheSegment(key);
        Entry<K, V> entry = segment.get(key, now);
        if (entry == null || isExpired(entry, now)) {
            return null;
        } else {
            promote(entry, now);
            return entry.value;
        }
    }

    /**
     * If the specified key is not already associated with a value (or is mapped to null), attempts to compute its
     * value using the given mapping function and enters it into this map unless null.
     *
     * @param key    the key whose associated value is to be returned or computed for if non-existant
     * @param loader the function to compute a value given a key
     * @return the current (existing or computed) value associated with the specified key, or null if the computed
     * value is null
     * @throws ExecutionException thrown if loader throws an exception
     */
    public V computeIfAbsent(K key, CacheLoader<K, V> loader) throws ExecutionException {
        long now = now();
        V value = get(key, now);
        if (value == null) {
            CacheSegment<K, V> segment = getCacheSegment(key);
            try (ReleasableLock ignored = segment.writeLock.acquire()) {
                value = get(key, now);
                if (value == null) {
                    try {
                        value = loader.load(key);
                    } catch (Exception e) {
                        throw new ExecutionException(e);
                    }
                }
                if (value != null) {
                    put(key, value, now);
                }
            }
        }
        return value;
    }

    /**
     * Associates the specified value with the specified key in this map. If the map previously contained a mapping for
     * the key, the old value is replaced.
     *
     * @param key   key with which the specified value is to be associated
     * @param value value to be associated with the specified key
     */
    public void put(K key, V value) {
        long now = now();
        put(key, value, now);
    }

    private void put(K key, V value, long now) {
        CacheSegment<K, V> segment = getCacheSegment(key);
        Tuple<Entry<K, V>, Entry<K, V>> tuple = segment.put(key, value, now);
        boolean replaced = false;
        try (ReleasableLock ignored = lock.acquire()) {
            if (tuple.v2() != null && tuple.v2().state == State.EXISTING) {
                if (unlink(tuple.v2())) {
                    replaced = true;
                }
            }
            promote(tuple.v1(), now);
        }
        if (replaced) {
            removalListener.onRemoval(new RemovalNotification(tuple.v2().key, tuple.v2().value, RemovalNotification.RemovalReason.REPLACED));
        }
    }

    /**
     * Invalidate the association for the specified key. A removal notification will be issued for invalidated
     * entries with {@link org.elasticsearch.common.cache.RemovalNotification.RemovalReason} INVALIDATED.
     *
     * @param key the key whose mapping is to be invalidated from the cache
     */
    public void invalidate(K key) {
        CacheSegment<K, V> segment = getCacheSegment(key);
        Entry<K, V> entry = segment.remove(key);
        if (entry != null) {
            try (ReleasableLock ignored = lock.acquire()) {
                delete(entry, RemovalNotification.RemovalReason.INVALIDATED);
            }
        }
    }

    /**
     * Invalidate all cache entries. A removal notification will be issued for invalidated entries with
     * {@link org.elasticsearch.common.cache.RemovalNotification.RemovalReason} INVALIDATED.
     */
    public void invalidateAll() {
        Entry<K, V> h = head;
        Arrays.stream(segments).forEach(segment -> segment.lock.writeLock().lock());
        try (ReleasableLock ignored = lock.acquire()) {
            Arrays.stream(segments).forEach(segment -> segment.map = new HashMap<>());
            Entry<K, V> current = head;
            while (current != null) {
                current.state = State.DELETED;
                current = current.after;
            }
            head = tail = null;
            count = 0;
            weight = 0;
        }
        Arrays.stream(segments).forEach(segment -> segment.lock.writeLock().unlock());
        while (h != null) {
            removalListener.onRemoval(new RemovalNotification<>(h.key, h.value, RemovalNotification.RemovalReason.INVALIDATED));
            h = h.after;
        }
    }

    /**
     * Force any outstanding size-based and time-based evictions to occur
     */
    public void refresh() {
        long now = now();
        try (ReleasableLock ignored = lock.acquire()) {
            evict(now);
        }
    }

    /**
     * The number of entries in the cache.
     *
     * @return the number of entries in the cache
     */
    public int count() {
        return count;
    }

    /**
     * The weight of the entries in the cache.
     *
     * @return the weight of the entries in the cache
     */
    public long weight() {
        return weight;
    }

    /**
     * An LRU sequencing of the keys in the cache that supports removal.
     *
     * @return an LRU-ordered {@link Iterable} over the keys in the cache
     */
    public Iterable<K> keys() {
        return () -> new Iterator<K>() {
            private CacheIterator iterator = new CacheIterator(head);

            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public K next() {
                return iterator.next().key;
            }

            @Override
            public void remove() {
                iterator.remove();
            }
        };
    }

    /**
     * An LRU sequencing of the values in the cache.
     *
     * @return an LRU-ordered {@link Iterable} over the values in the cache
     */
    public Iterable<V> values() {
        return () -> new Iterator<V>() {
            private CacheIterator iterator = new CacheIterator(head);

            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public V next() {
                return iterator.next().value;
            }
        };
    }

    private class CacheIterator implements Iterator<Entry<K, V>> {
        private Entry<K, V> current;
        private Entry<K, V> next;

        CacheIterator(Entry<K, V> head) {
            current = null;
            next = head;
        }

        @Override
        public boolean hasNext() {
            return next != null;
        }

        @Override
        public Entry<K, V> next() {
            current = next;
            next = next.after;
            return current;
        }

        @Override
        public void remove() {
            Entry<K, V> entry = current;
            if (entry != null) {
                CacheSegment<K, V> segment = getCacheSegment(entry.key);
                segment.remove(entry.key);
                try (ReleasableLock ignored = lock.acquire()) {
                    current = null;
                    delete(entry, RemovalNotification.RemovalReason.INVALIDATED);
                }
            }
        }
    }

    /**
     * The cache statistics tracking hits, misses and evictions. These are taken on a best-effort basis meaning that
     * they could be out-of-date mid-flight.
     *
     * @return the current cache statistics
     */
    public CacheStats stats() {
        int hits = 0;
        int misses = 0;
        int evictions = 0;
        for (int i = 0; i < segments.length; i++) {
            hits += segments[i].segmentStats.hits.intValue();
            misses += segments[i].segmentStats.misses.intValue();
            evictions += segments[i].segmentStats.evictions.intValue();
        }
        return new CacheStats(hits, misses, evictions);
    }

    public static class CacheStats {
        private int hits;
        private int misses;
        private int evictions;

        public CacheStats(int hits, int misses, int evictions) {
            this.hits = hits;
            this.misses = misses;
            this.evictions = evictions;
        }

        public int getHits() {
            return hits;
        }

        public int getMisses() {
            return misses;
        }

        public int getEvictions() {
            return evictions;
        }
    }

    private boolean promote(Entry<K, V> entry, long now) {
        boolean promoted = true;
        try (ReleasableLock ignored = lock.acquire()) {
            switch (entry.state) {
                case DELETED:
                    promoted = false;
                    break;
                case EXISTING:
                    relinkAtHead(entry);
                    break;
                case NEW:
                    linkAtHead(entry);
                    break;
            }
            if (promoted) {
                evict(now);
            }
        }
        return promoted;
    }

    private void evict(long now) {
        assert lock.isHeldByCurrentThread();

        while (tail != null && shouldPrune(tail, now)) {
            CacheSegment<K, V> segment = getCacheSegment(tail.key);
            Entry<K, V> entry = tail;
            if (segment != null) {
                segment.remove(tail.key);
            }
            delete(entry, RemovalNotification.RemovalReason.EVICTED);
        }
    }

    private void delete(Entry<K, V> entry, RemovalNotification.RemovalReason removalReason) {
        assert lock.isHeldByCurrentThread();

        if (unlink(entry)) {
            removalListener.onRemoval(new RemovalNotification<>(entry.key, entry.value, removalReason));
        }
    }

    private boolean shouldPrune(Entry<K, V> entry, long now) {
        return exceedsSize() || isExpired(entry, now);
    }

    private boolean exceedsSize() {
        return maximumWeight != -1 && weight > maximumWeight;
    }

    private boolean isExpired(Entry<K, V> entry, long now) {
        return (entriesExpireAfterAccess && now - entry.accessTime > expireAfterAccess) ||
                (entriesExpireAfterWrite && now - entry.writeTime > expireAfterWrite);
    }

    private boolean unlink(Entry<K, V> entry) {
        assert lock.isHeldByCurrentThread();

        if (entry.state == State.EXISTING) {
            final Entry<K, V> before = entry.before;
            final Entry<K, V> after = entry.after;

            if (before == null) {
                // removing the head
                head = after;
                if (head != null) {
                    head.before = null;
                }
            } else {
                // removing inner element
                before.after = after;
                entry.before = null;
            }

            if (after == null) {
                // removing tail
                tail = before;
                if (tail != null) {
                    tail.after = null;
                }
            } else {
                // removing inner element
                after.before = before;
                entry.after = null;
            }

            count--;
            weight -= weigher.applyAsLong(entry.key, entry.value);
            entry.state = State.DELETED;
            return true;
        } else {
            return false;
        }
    }

    private void linkAtHead(Entry<K, V> entry) {
        assert lock.isHeldByCurrentThread();

        Entry<K, V> h = head;
        entry.before = null;
        entry.after = head;
        head = entry;
        if (h == null) {
            tail = entry;
        } else {
            h.before = entry;
        }

        count++;
        weight += weigher.applyAsLong(entry.key, entry.value);
        entry.state = State.EXISTING;
    }

    private void relinkAtHead(Entry<K, V> entry) {
        assert lock.isHeldByCurrentThread();

        if (head != entry) {
            unlink(entry);
            linkAtHead(entry);
        }
    }

    private CacheSegment<K, V> getCacheSegment(K key) {
        return segments[key.hashCode() & 0xff];
    }
}
