/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.common;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;

/**
 * Provides a limited functionality queue that can have its capacity adjusted.
 * @param <K> the items to store in the queue
 */
public class AdjustableCapacityBlockingQueue<K> {

    private BlockingQueue<K> queue;
    private final Function<Integer, BlockingQueue<K>> createQueue;
    private final ReentrantReadWriteLock lock;

    public AdjustableCapacityBlockingQueue(BlockingQueue<K> queue, Function<Integer, BlockingQueue<K>> createQueue) {
        this.queue = Objects.requireNonNull(queue);
        this.createQueue = Objects.requireNonNull(createQueue);
        lock = new ReentrantReadWriteLock();
    }

    /**
     * Sets the capacity of the queue. If the new capacity is smaller than the current number of elements in the queue, the
     * elements that exceed the new capacity size are returned and will not be present in the queue.
     * This is potentially an expensive operation because a new internal queue is instantiated.
     * @param capacity the new capacity for the queue.
     * @return a list of elements that could not fit within the queue given the new capacity requirement.
     */
    public List<K> setCapacity(int capacity) {
        final ReentrantReadWriteLock.WriteLock writeLock = lock.writeLock();
        List<K> remainingItems = new ArrayList<>(queue.size());

        writeLock.lock();
        try {
            BlockingQueue<K> newQueue = createQueue.apply(capacity);
            queue.drainTo(newQueue, capacity);
            queue.drainTo(remainingItems);
            queue = newQueue;
        } finally {
            writeLock.unlock();
        }

        return remainingItems;
    }

    public boolean offer(K item) {
        final ReentrantReadWriteLock.ReadLock readLock = lock.readLock();

        readLock.lock();
        try {
            return queue.offer(item);
        } finally {
            readLock.unlock();
        }
    }

    public int drainTo(Collection<? super K> c) {
        return drainTo(c, Integer.MAX_VALUE);
    }

    public int drainTo(Collection<? super K> c, int maxElements) {
        final ReentrantReadWriteLock.ReadLock readLock = lock.readLock();

        readLock.lock();
        try {
            return queue.drainTo(c, maxElements);
        } finally {
            readLock.unlock();
        }
    }

    public K poll(long timeout, TimeUnit timeUnit) throws InterruptedException {
        final ReentrantReadWriteLock.ReadLock readLock = lock.readLock();

        readLock.lock();
        try {
            return queue.poll(timeout, timeUnit);
        } finally {
            readLock.unlock();
        }
    }

    public int size() {
        return queue.size();
    }

    public int remainingCapacity() {
        return queue.remainingCapacity();
    }
}
