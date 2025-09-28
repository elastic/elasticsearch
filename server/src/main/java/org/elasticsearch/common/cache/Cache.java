/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.common.cache;

import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Objects;
import java.util.Spliterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.ToLongBiFunction;

import static org.elasticsearch.common.cache.RemovalNotification.RemovalReason.EVICTED;
import static org.elasticsearch.common.cache.RemovalNotification.RemovalReason.INVALIDATED;
import static org.elasticsearch.common.cache.RemovalNotification.RemovalReason.REPLACED;

/**
 * Implementation of <a href="https://junchengyang.com/publication/nsdi24-SIEVE.pdf">SIEVE</a>
 * <p>
 * SIEVE, a simple, efficient, fast, and scalable cache eviction algorithm for web caches that leverages “lazy
 * promotion” and “quick demotion.” The high efficiency in SIEVE comes from gradually sifting out the unpopular objects.
 * </p>
 * maxSize and maxWeight are soft limits and we might surge over.
 *
 * @param <Key> type of keys used for lookup
 * @param <Value> type of values this cache can hold.
 */
public class Cache<Key, Value> {

    private static class EntryHolder<Key, Value> {
        public final Key key;
        public final Value value;
        public final AtomicBoolean visited = new AtomicBoolean(false);
        public final long writeTime;
        public volatile long accessTime;

        EntryHolder(Key key, Value value, long writeTime) {
            this.key = key;
            this.value = value;
            this.writeTime = this.accessTime = writeTime;
        }
    }

    private final ConcurrentMap<Key, EntryHolder<Key, Value>> cache = new ConcurrentHashMap<>();
    private final ConcurrentLinkedDeque<EntryHolder<Key, Value>> queue = new ConcurrentLinkedDeque<>();
    private final LongAdder size = new LongAdder();
    private final LongAdder weight = new LongAdder();
    private final LongAdder hits = new LongAdder();
    private final LongAdder misses = new LongAdder();
    private final LongAdder evictions = new LongAdder();
    private final AtomicBoolean sieving = new AtomicBoolean();
    private final Long maxCapacity;
    private final Long maxWeight;
    private final ToLongBiFunction<Key, Value> weigher;
    private final RemovalListener<Key, Value> removalListener;

    private final ExecutorService siever = Executors.newSingleThreadExecutor();
    private volatile Iterator<EntryHolder<Key, Value>> sieve;
    // positive if entries have an expiration
    private final long expireAfterAccessNanos;
    // true if entries can expire after access
    private final boolean entriesExpireAfterAccess;
    // positive if entries have an expiration after write
    private final long expireAfterWriteNanos;
    // true if entries can expire after initial insertion
    private final boolean entriesExpireAfterWrite;

    public Cache() {
        this(null,null,null,null, -1, -1);
    }

    public Cache(Long maxCapacity, Long maxWeight, RemovalListener<Key, Value> removalListener, ToLongBiFunction<Key, Value> weigher) {
        this(maxCapacity, maxWeight, removalListener, weigher, -1, -1);
    }

    public Cache(
        Long maxCapacity,
        Long maxWeight,
        RemovalListener<Key, Value> removalListener,
        ToLongBiFunction<Key, Value> weigher,
        long expireAfterAccessNanos,
        long expireAfterWriteNanos
    ) {
        this.maxCapacity = maxCapacity;
        this.maxWeight = maxWeight;
        this.removalListener = removalListener != null ? removalListener : (notification) -> {} ;
        this.weigher = weigher != null ? weigher : (key, value) -> 1;
        this.expireAfterAccessNanos = expireAfterAccessNanos;
        this.entriesExpireAfterAccess = expireAfterAccessNanos > 0;
        this.expireAfterWriteNanos = expireAfterWriteNanos;
        this.entriesExpireAfterWrite = expireAfterWriteNanos > 0;
    }

    public Value get(Key key) {
        EntryHolder<Key, Value> entry = cache.get(key);
        if(entry != null) {
            markHit(entry);
            return entry.value;
        }
        misses.increment();
        return null;
    }

    public void put(Key key, Value value) {
        EntryHolder<Key, Value> newHead = new EntryHolder<>(key, value, now());
        EntryHolder<Key, Value> oldValue = cache.put(key, newHead);
        size.increment();
        weight.add(weigher.applyAsLong(key, value));
        appendToHead(newHead);
        if(oldValue!=null) {
            size.decrement();
            weight.add(-weigher.applyAsLong(oldValue.key, oldValue.value));
            removeFromQueue(oldValue, REPLACED);
        }
        siever.submit(this::sieveUntilSpace);
    }

    public Value computeIfAbsent(Key key, CacheLoader<Key, Value> loader) throws ExecutionException {
        Objects.requireNonNull(loader);
        var created = new AtomicBoolean(false);
        try {
            EntryHolder<Key, Value> result = cache.computeIfAbsent(key, (loadKey) -> {
                try {
                    var loadedValue = loader.load(loadKey);
                    if(loadedValue == null) {
                        return null;
                    }
                    created.set(true);
                    var entry = new EntryHolder<>(loadKey, loadedValue, now());
                    size.increment();
                    weight.add(weigher.applyAsLong(entry.key, entry.value));
                    return entry;
                } catch (Exception e) {
                    throw new CacheLoaderException(e);
                }
            });
            if(created.get()) {
                appendToHead(result);
                siever.submit(this::sieveUntilSpace);
                assert result != null;
                return result.value;
            } else {
                if(result == null) {
                    return null;
                }
                markHit(result);
                return result.value;
            }
        } catch (CacheLoaderException e) {
            throw new ExecutionException(e.getCause());
        }
    }

    public void invalidate(Key key) {
        EntryHolder<Key, Value> removedEntry = cache.remove(key);
        if(removedEntry != null) {
            size.decrement();
            weight.add(-weigher.applyAsLong(removedEntry.key, removedEntry.value));
            removeFromQueue(removedEntry, INVALIDATED);
        }
    }

    public void invalidate(Key key, Value value) {
        EntryHolder<Key, Value> entry = cache.get(key);
        if(entry != null && Objects.equals(entry.value, value)) {
            if(cache.remove(key, entry)) {
                size.decrement();
                weight.add(-weigher.applyAsLong(entry.key, entry.value));
                removeFromQueue(entry, INVALIDATED);
            } else {
                // Value already replaced before we could remove it. Invalidating is no longer necessary
            }
        }
    }

    public void invalidateAll() {
        while(true) {
            EntryHolder<Key, Value> entry = queue.pollLast();
            if(entry == null) {
                break;
            }
            if(cache.remove(entry.key, entry)) {
                size.decrement();
                weight.add(-weigher.applyAsLong(entry.key, entry.value));
                removalListener.onRemoval(new RemovalNotification<>(entry.key, entry.value, INVALIDATED));
            }
        }
    }

    public void refresh() {
        var iterator = queue.descendingIterator();
        while(iterator.hasNext()) {
            var entry = iterator.next();
            if(isExpired(entry, now())) {
                if(cache.remove(entry.key, entry)) {
                    size.decrement();
                    weight.add(-weigher.applyAsLong(entry.key, entry.value));
                    removalListener.onRemoval(new RemovalNotification<>(entry.key, entry.value, EVICTED));
                    evictions.increment();
                }
                iterator.remove();
            }
        }
    }

    public int count() {
        return size.intValue();
    }

    public long weight() {
        return weight.sum();
    }

    public Iterable<Key> keys() {
        return new BookkeepingIterable<Key>(cache.values()) {
            @Override
            protected Key map(EntryHolder<Key, Value> holder) {
                return holder.key;
            }
        };
    }

    public Iterable<Value> values() {
        return new BookkeepingIterable<Value>(cache.values()) {
            @Override
            protected Value map(EntryHolder<Key, Value> holder) {
                return holder.value;
            }
        };
    }

    public Stats stats() {
        return new Stats(hits.sum(),misses.sum(),evictions.sum());
    }

    public void forEach(BiConsumer<Key, Value> consumer) {
        cache.forEach((key, entry) -> consumer.accept(key, entry.value));
    }

    private void sieveUntilSpace() {
        if(hasSpace()) {
            return;
        }
        if (sieving.compareAndSet(false, true) == false) {
            // Another thread is already sieving; avoid contention.
            return;
        }
        try {
            while (hasSpace() == false) {
                if (sieve == null || sieve.hasNext() == false) {
                    if(queue.isEmpty()) {
                        return; // protect against queue.clear() etc.
                    }
                    sieve = queue.descendingIterator();
                }
                EntryHolder<Key, Value> entry = sieve.next();
                if(isExpired(entry, now()) || entry.visited.getAndSet(false) == false) {
                    if(cache.remove(entry.key, entry)) {
                        size.decrement();
                        weight.add(-weigher.applyAsLong(entry.key, entry.value));
                        removalListener.onRemoval(new RemovalNotification<>(entry.key, entry.value, EVICTED));
                        evictions.increment();
                    }
                    sieve.remove();
                }
            }
        } finally {
            sieving.set(false);
        }

    }

    private boolean hasSpace() {
        return (maxCapacity==null || count()<maxCapacity) && (maxWeight==null || weight()<maxWeight);
    }

    private void appendToHead(EntryHolder<Key, Value> newHead) {
        queue.addFirst(newHead);
    }

    private void removeFromQueue(EntryHolder<Key, Value> entry, RemovalNotification.RemovalReason reason) {
        //queue.remove(entry); we're setting the flag to false instead and let the sieve remove it at O(1)
        entry.visited.lazySet(false);
        removalListener.onRemoval(new RemovalNotification<>(entry.key, entry.value, reason));
    }

    private void markHit(EntryHolder<Key, Value> result) {
        hits.increment();
        if (result.visited.get() == false) {
            result.visited.lazySet(true);
        }
        if(entriesExpireAfterAccess) {
            result.accessTime = now();
        }
    }

    private boolean isExpired(EntryHolder<Key, Value> entry, long now) {
        return (entriesExpireAfterAccess && now - entry.accessTime > expireAfterAccessNanos)
            || (entriesExpireAfterWrite && now - entry.writeTime > expireAfterWriteNanos);
    }

    /**
     * The relative time used to track time-based evictions.
     *
     * @return the current relative time
     */
    protected long now() {
        // System.nanoTime takes non-negligible time, so we only use it if we need it
        // use System.nanoTime because we want relative time, not absolute time
        return entriesExpireAfterAccess || entriesExpireAfterWrite ? System.nanoTime() : 0;
    }

    private static class CacheLoaderException extends RuntimeException {
        CacheLoaderException(Throwable throwable) {
            super(throwable);
        }
    }

    private abstract class BookkeepingIterable<Type> implements Iterable<Type> {
        private final Collection<EntryHolder<Key, Value>> entries;

        private BookkeepingIterable(Collection<EntryHolder<Key, Value>> entries) {
            this.entries = entries;
        }

        @Override
        public BookkeepingIterator<Type> iterator() {
            return new BookkeepingIterator<Type>(entries.iterator()) {
                @Override
                protected Type map(EntryHolder<Key, Value> holder) {
                    return BookkeepingIterable.this.map(holder);
                }
            };
        }

        @Override
        public Spliterator<Type> spliterator() {
            return new BookkeepingSpliterator<Type>(entries.spliterator()) {
                @Override
                protected Type map(EntryHolder<Key, Value> holder) {
                    return BookkeepingIterable.this.map(holder);
                }
            };
        }

        protected abstract Type map(EntryHolder<Key, Value> holder);
    }

    private abstract class BookkeepingSpliterator<Type> implements Spliterator<Type> {
        private final Spliterator<EntryHolder<Key, Value>> spliterator;

        BookkeepingSpliterator(Spliterator<EntryHolder<Key, Value>> spliterator) {
            Objects.requireNonNull(spliterator);
            if(spliterator.hasCharacteristics(ORDERED) || spliterator.hasCharacteristics(SORTED)){
                throw new UnsupportedOperationException(
                    "Because we erase context(key), #getComparator() can't be implemented in any efficient way."
                );
            }
            this.spliterator = spliterator;
        }

        @Override
        public boolean tryAdvance(Consumer<? super Type> action) {
            return spliterator.tryAdvance(entry -> action.accept(map(entry)));
        }

        @Override
        public Spliterator<Type> trySplit() {
            var split = spliterator.trySplit();
            if(split == null) {
                return null;
            }
            return new BookkeepingSpliterator<Type>(split) {
                @Override
                protected Type map(EntryHolder<Key, Value> entry) {
                    return BookkeepingSpliterator.this.map(entry);
                }
            };
        }

        @Override
        public long estimateSize() {
            return spliterator.estimateSize();
        }

        @Override
        public int characteristics() {
            return spliterator.characteristics();
        }

        @Override
        public void forEachRemaining(Consumer<? super Type> action) {
            spliterator.forEachRemaining((entry) -> action.accept(map(entry)));
        }

        protected abstract Type map(EntryHolder<Key, Value> entry);

        @Override
        public Comparator<? super Type> getComparator() {
            throw new IllegalStateException();
        }

        @Override
        public long getExactSizeIfKnown() {
            return spliterator.getExactSizeIfKnown();
        }

        @Override
        public boolean hasCharacteristics(int characteristics) {
            return spliterator.hasCharacteristics(characteristics);
        }
    }

    private abstract class BookkeepingIterator<Type> implements Iterator<Type> {
        private final Iterator<EntryHolder<Key, Value>> iterator;
        private EntryHolder<Key, Value> last;

        BookkeepingIterator(Iterator<EntryHolder<Key, Value>> iterator) {
            this.iterator = iterator;
        }

        protected abstract Type map(EntryHolder<Key, Value> holder);

        @Override
        public boolean hasNext() {
            return iterator.hasNext();
        }

        @Override
        public Type next() {
            last = iterator.next();
            return map(last);
        }

        @Override
        public void remove() {
            if (last == null) {
                throw new IllegalStateException();
            }
            if (cache.remove(last.key, last)) {
                size.decrement();
                weight.add(-weigher.applyAsLong(last.key, last.value));
                queue.remove(last);
                removalListener.onRemoval(new RemovalNotification<>(last.key, last.value, INVALIDATED));
            } else {
                queue.remove(last);
            }
            last = null;
        }

        @Override
        public void forEachRemaining(Consumer<? super Type> action) {
            iterator.forEachRemaining(holder -> {
                last = holder;
                action.accept(map(holder));
            });
        }
    }

    /**
     * Record holding the cache stats
     * @param hits number of cache hits since creation.
     * @param misses number of cache misses since creation.
     * @param evictions number of cache ections since creation.
     */
    public record Stats(long hits, long misses, long evictions) {
        public long getHits() {
            return hits;
        }
        public long getMisses() {
            return misses;
        }
        public long getEvictions() {
            return evictions;
        }
    }
}
