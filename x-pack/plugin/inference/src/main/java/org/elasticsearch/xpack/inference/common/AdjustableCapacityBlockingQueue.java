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
 * @param <E> the items to store in the queue
 */
public class AdjustableCapacityBlockingQueue<E> {

    private BlockingQueue<E> queue;
    private final Function<Integer, BlockingQueue<E>> createQueue;
    private final ReentrantReadWriteLock lock;

    /**
     * Constructs the adjustable capacity queue
     * @param initialCapacity the initial capacity of the queue
     * @param createQueue a function for creating a new backing queue with the specified capacity
     */
    public AdjustableCapacityBlockingQueue(int initialCapacity, Function<Integer, BlockingQueue<E>> createQueue) {
        this.createQueue = Objects.requireNonNull(createQueue);
        queue = createQueue.apply(initialCapacity);
        lock = new ReentrantReadWriteLock();
    }

    /**
     * Sets the capacity of the queue. If the new capacity is smaller than the current number of elements in the queue, the
     * elements that exceed the new capacity size are returned and will not be present in the queue.
     * This is potentially an expensive operation because a new internal queue is instantiated.
     * @param capacity the new capacity for the queue.
     * @return a list of elements that could not fit within the queue given the new capacity requirement.
     */
    public List<E> setCapacity(int capacity) {
        final ReentrantReadWriteLock.WriteLock writeLock = lock.writeLock();
        List<E> remainingItems = new ArrayList<>(queue.size());

        writeLock.lock();
        try {
            BlockingQueue<E> newQueue = createQueue.apply(capacity);
            queue.drainTo(newQueue, capacity);
            queue.drainTo(remainingItems);
            queue = newQueue;
        } finally {
            writeLock.unlock();
        }

        return remainingItems;
    }

    public boolean offer(E item) {
        final ReentrantReadWriteLock.ReadLock readLock = lock.readLock();

        readLock.lock();
        try {
            return queue.offer(item);
        } finally {
            readLock.unlock();
        }
    }

    public int drainTo(Collection<? super E> c) {
        return drainTo(c, Integer.MAX_VALUE);
    }

    public int drainTo(Collection<? super E> c, int maxElements) {
        final ReentrantReadWriteLock.ReadLock readLock = lock.readLock();

        readLock.lock();
        try {
            return queue.drainTo(c, maxElements);
        } finally {
            readLock.unlock();
        }
    }

    public E poll(long timeout, TimeUnit timeUnit) throws InterruptedException {
        final ReentrantReadWriteLock.ReadLock readLock = lock.readLock();

        readLock.lockInterruptibly();
        try {
            return queue.poll(timeout, timeUnit);
        } finally {
            readLock.unlock();
        }
    }

    public E take() throws InterruptedException {
        final ReentrantReadWriteLock.ReadLock readLock = lock.readLock();

        readLock.lockInterruptibly();
        try {
            return queue.take();
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
