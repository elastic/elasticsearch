/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.common.cache;

import java.util.Comparator;
import java.util.Iterator;
import java.util.Objects;
import java.util.Spliterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
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
 * promotion” and “quick demotion”. The high efficiency in SIEVE comes from gradually sifting out the unpopular objects.
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

        EntryHolder(Key key, Value value) {
            this.key = key;
            this.value = value;
        }
    }

    private final ConcurrentMap<Key, EntryHolder<Key, Value>> cache = new ConcurrentHashMap<>();
    private final ConcurrentLinkedDeque<EntryHolder<Key, Value>> queue = new ConcurrentLinkedDeque<>();
    private final AtomicLong weight = new AtomicLong();
    private final LongAdder hits = new LongAdder();
    private final LongAdder misses = new LongAdder();
    private final LongAdder evictions = new LongAdder();
    private final Object sieveLock = new Object();
    private volatile Iterator<EntryHolder<Key, Value>> sieve;
    private final Long maxCapacity;
    private final Long maxWeight;
    private final ToLongBiFunction<Key, Value> weigher;
    private final RemovalListener<Key, Value> removalListener;

    public Cache() {
        this(null,null,null,null);
    }

    public Cache(Long maxCapacity, Long maxWeight, RemovalListener<Key, Value> removalListener, ToLongBiFunction<Key, Value> weigher) {
        this.maxCapacity = maxCapacity;
        this.maxWeight = maxWeight;
        this.removalListener = removalListener != null ? removalListener : (notification) -> {} ;
        this.weigher = weigher != null ? weigher : (key, value) -> 0;
    }

    public Value get(Key key) {
        EntryHolder<Key, Value> entry = cache.get(key);
        if(entry != null) {
            hits.increment();
            entry.visited.set(true);
            return entry.value;
        }
        misses.increment();
        return null;
    }

    public void put(Key key, Value value) {
        EntryHolder<Key, Value> newHead = new EntryHolder<>(key, value);
        EntryHolder<Key, Value> oldValue = cache.put(key, newHead);
        weight.getAndAdd(weigher.applyAsLong(key, value));
        appendToHead(newHead);
        if(oldValue!=null) {
            weight.getAndAdd(-weigher.applyAsLong(oldValue.key, oldValue.value));
            removeFromQueue(oldValue, REPLACED);
        }
        sieveUntilSpace();
    }

    public Value computeIfAbsent(Key key, CacheLoader<Key, Value> loader) throws ExecutionException {
        Objects.requireNonNull(loader);
        try {
            EntryHolder<Key, Value> entry = cache.computeIfAbsent(key, (loadKey) -> {
                try {
                    return new EntryHolder<>(loadKey, loader.load(loadKey));
                } catch (Exception e) {
                    throw new CacheLoaderException(e);
                }
            });
            weight.getAndAdd(weigher.applyAsLong(entry.key, entry.value));
            appendToHead(entry);
            sieveUntilSpace();
            return entry.value;
        } catch (CacheLoaderException e) {
            throw new ExecutionException(e.getCause());
        }
    }

    public void invalidate(Key key) {
        EntryHolder<Key, Value> removedEntry = cache.remove(key);
        if(removedEntry != null) {
            weight.getAndAdd(-weigher.applyAsLong(removedEntry.key, removedEntry.value));
            removeFromQueue(removedEntry, INVALIDATED);
        }
    }

    public void invalidate(Key key, Value value) {
        EntryHolder<Key, Value> entry = cache.get(key);
        if(entry != null && entry.value.equals(value)) {
            if(cache.remove(key, entry)) {
                weight.getAndAdd(-weigher.applyAsLong(entry.key, entry.value));
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
                weight.getAndAdd(-weigher.applyAsLong(entry.key, entry.value));
                removalListener.onRemoval(new RemovalNotification<>(entry.key, entry.value, INVALIDATED));
            }
        }
    }

    public void refresh() {
        sieveUntilSpace();
    }

    public int count() {
        return cache.size();
    }

    public long weight() {
        return weight.get();
    }

    public Iterable<Key> keys() {
        return cache.keySet();
    }

    public Iterable<Value> values() {
        return new Iterable<>() {
            @Override
            public Iterator<Value> iterator() {
                return new UnwrappingIterator(cache.values().iterator());
            }

            @Override
            public Spliterator<Value> spliterator() {
                return new UnwrappingSpliterator(cache.values().spliterator());
            }
        };
    }

    public Stats stats() {
        return new Stats(hits.sum(),misses.sum(),evictions.sum());
    }

    public void forEach(BiConsumer<Key, Value> consumer) {
        cache.forEach((key, entry) -> {
            consumer.accept(key, entry.value);
        });
    }

    private void sieveUntilSpace() {
        if(hasSpace()) {
            return;
        }
        synchronized (sieveLock) {
            while (hasSpace()==false) {
                if (sieve == null || sieve.hasNext() == false) {
                    if(queue.isEmpty()) {
                        return; // protect against queue.clear() etc.
                    }
                    sieve = queue.descendingIterator();
                }
                EntryHolder<Key, Value> entry = sieve.next();
                if(entry.visited.get() == false) {
                    if(cache.remove(entry.key, entry)) {
                        weight.getAndAdd(-weigher.applyAsLong(entry.key, entry.value));
                    }
                    sieve.remove();
                    removalListener.onRemoval(new RemovalNotification<>(entry.key, entry.value, EVICTED));
                    evictions.increment();
                }
            }
        }
    }

    private boolean hasSpace() {
        return (maxCapacity==null || count()<maxCapacity) && (maxWeight==null || weight()<maxWeight);
    }

    private void appendToHead(EntryHolder<Key, Value> newHead) {
        queue.addFirst(newHead);
    }

    /**
     * We should hold that regardless of which entry, we should be able to transverse to the head.
     * @param entry currently linked entry
     */
    private void removeFromQueue(EntryHolder<Key, Value> entry, RemovalNotification.RemovalReason reason) {
        queue.remove(entry);
        removalListener.onRemoval(new RemovalNotification<>(entry.key, entry.value, reason));
    }

    private static class CacheLoaderException extends RuntimeException {
        CacheLoaderException(Throwable throwable) {
            super(throwable);
        }
    }

    private class UnwrappingSpliterator implements Spliterator<Value> {
        private final Spliterator<EntryHolder<Key, Value>> spliterator;

        UnwrappingSpliterator(Spliterator<EntryHolder<Key, Value>> spliterator) {
            Objects.requireNonNull(spliterator);
            if(spliterator.hasCharacteristics(ORDERED) || spliterator.hasCharacteristics(SORTED)){
                throw new UnsupportedOperationException("Because we erase context(key), #getComparator() can't be implemented in any efficient way.");
            }
            this.spliterator = spliterator;
        }

        @Override
        public boolean tryAdvance(Consumer<? super Value> action) {
            return spliterator.tryAdvance(entry -> action.accept(entry.value));
        }

        @Override
        public Spliterator<Value> trySplit() {
            return new UnwrappingSpliterator(spliterator.trySplit());
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
        public void forEachRemaining(Consumer<? super Value> action) {
            spliterator.forEachRemaining((entry) -> action.accept(entry.value));
        }

        @Override
        public Comparator<? super Value> getComparator() {
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

    private class UnwrappingIterator implements Iterator<Value> {
        private final Iterator<EntryHolder<Key, Value>> iterator;

        UnwrappingIterator(Iterator<EntryHolder<Key, Value>> iterator) {
            this.iterator = iterator;
        }

        @Override
        public boolean hasNext() {
            return iterator.hasNext();
        }

        @Override
        public Value next() {
            EntryHolder<Key, Value> entry = iterator.next();
            if(entry == null) {
                return null;
            }
            return entry.value;
        }

        @Override
        public void remove() {
            iterator.remove();
        }

        @Override
        public void forEachRemaining(Consumer<? super Value> action) {
            iterator.forEachRemaining((entry) -> action.accept(entry.value));
        }
    }

    /**
     * Record holding the cache stats
     * @param hits number of cache hits since creation.
     * @param misses number of cache misses since creation.
     * @param evictions number of cache ections since creation.
     */
    public record Stats(long hits, long misses, long evictions) {
        long getHits() {
            return hits;
        }
        long getMisses() {
            return misses;
        }
        long getEvictions() {
            return evictions;
        }
    }
}
