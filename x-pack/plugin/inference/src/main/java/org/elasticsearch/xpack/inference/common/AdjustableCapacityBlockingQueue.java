/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.common;

import org.elasticsearch.core.Nullable;

import java.util.Collection;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Provides a limited functionality queue that can have its capacity adjusted.
 * @param <E> the items to store in the queue
 */
public class AdjustableCapacityBlockingQueue<E> {

    private BlockingQueue<E> currentQueue;
    /**
     * When the capacity of the {@link AdjustableCapacityBlockingQueue#currentQueue} changes, any items that can't fit in the new queue
     * will be placed in this secondary queue (the items from the front of the queue first). Then when an operation occurs to remove an
     * item from the queue we'll attempt to grab it from this secondary queue first. That way we guarantee that items that were in the
     * old queue will be read first.
     */
    private final BlockingQueue<E> prioritizedReadingQueue;
    private final QueueCreator<E> queueCreator;
    private final ReentrantReadWriteLock lock;

    /**
     * Constructs the adjustable capacity queue
     * @param queueCreator a {@link QueueCreator} object for handling how to create the {@link BlockingQueue}
     * @param initialCapacity the initial capacity of the queue, if null the queue will be unbounded
     */
    public AdjustableCapacityBlockingQueue(QueueCreator<E> queueCreator, @Nullable Integer initialCapacity) {
        this.queueCreator = Objects.requireNonNull(queueCreator);
        currentQueue = createCurrentQueue(queueCreator, initialCapacity);
        lock = new ReentrantReadWriteLock();
        prioritizedReadingQueue = queueCreator.create();
    }

    private static <E> BlockingQueue<E> createCurrentQueue(QueueCreator<E> queueCreator, @Nullable Integer initialCapacity) {
        if (initialCapacity == null) {
            return queueCreator.create();
        }

        return queueCreator.create(initialCapacity);
    }

    /**
     * Sets the capacity of the queue. If the new capacity is smaller than the current number of elements in the queue, the
     * elements that exceed the new capacity are retained. In this situation the {@link AdjustableCapacityBlockingQueue#size()} method
     * could return a value greater than the specified capacity.
     * <br/>
     * <b>This is potentially an expensive operation because a new internal queue is instantiated.</b>
     * @param newCapacity the new capacity for the queue
     */
    public void setCapacity(int newCapacity) {
        final ReentrantReadWriteLock.WriteLock writeLock = lock.writeLock();

        writeLock.lock();
        try {
            BlockingQueue<E> newQueue = queueCreator.create(newCapacity);
            // Drain the first items from the queue, so they will get read first.
            // Only drain the amount that wouldn't fit in the new queue
            // If the new capacity is larger than the current queue size then we don't need to drain any
            // they will all fit within the newly created queue. In this situation the queue size - capacity
            // would result in a negative value which is ignored
            if (currentQueue.size() > newCapacity) {
                currentQueue.drainTo(prioritizedReadingQueue, currentQueue.size() - newCapacity);
            }
            currentQueue.drainTo(newQueue, newCapacity);
            currentQueue = newQueue;
        } finally {
            writeLock.unlock();
        }
    }

    public boolean offer(E item) {
        final ReentrantReadWriteLock.ReadLock readLock = lock.readLock();

        readLock.lock();
        try {
            return currentQueue.offer(item);
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
            var numberOfDrainedOldItems = prioritizedReadingQueue.drainTo(c, maxElements);
            var numberOfDrainedCurrentItems = currentQueue.drainTo(c, maxElements - numberOfDrainedOldItems);

            return numberOfDrainedCurrentItems + numberOfDrainedOldItems;
        } finally {
            readLock.unlock();
        }
    }

    public E poll(long timeout, TimeUnit timeUnit) throws InterruptedException {
        final ReentrantReadWriteLock.ReadLock readLock = lock.readLock();

        readLock.lockInterruptibly();
        try {
            // no new items should be added to the old queue, so we shouldn't need to wait on it
            var oldItem = prioritizedReadingQueue.poll();

            if (oldItem != null) {
                return oldItem;
            }

            return currentQueue.poll(timeout, timeUnit);
        } finally {
            readLock.unlock();
        }
    }

    public E take() throws InterruptedException {
        final ReentrantReadWriteLock.ReadLock readLock = lock.readLock();

        readLock.lockInterruptibly();
        try {
            var oldItem = prioritizedReadingQueue.poll();

            if (oldItem != null) {
                return oldItem;
            }

            return currentQueue.take();
        } finally {
            readLock.unlock();
        }
    }

    public E peek() {
        final ReentrantReadWriteLock.ReadLock readLock = lock.readLock();

        readLock.lock();
        try {
            var oldItem = prioritizedReadingQueue.peek();

            if (oldItem != null) {
                return oldItem;
            }

            return currentQueue.peek();
        } finally {
            readLock.unlock();
        }
    }

    public E poll() {
        final ReentrantReadWriteLock.ReadLock readLock = lock.readLock();

        readLock.lock();
        try {
            var oldItem = prioritizedReadingQueue.poll();

            if (oldItem != null) {
                return oldItem;
            }

            return currentQueue.poll();
        } finally {
            readLock.unlock();
        }
    }

    /**
     * Returns the number of elements stored in the queue. If the capacity was recently changed, the value returned could be
     * greater than the capacity. This occurs when the capacity was reduced and there were more elements in the queue than the
     * new capacity.
     * @return the number of elements in the queue.
     */
    public int size() {
        return currentQueue.size() + prioritizedReadingQueue.size();
    }

    /**
     * The number of additional elements that his queue can accept without blocking.
     */
    public int remainingCapacity() {
        return currentQueue.remainingCapacity();
    }

    /**
     * Provides a contract for creating a {@link BlockingQueue}
     * @param <E> items to store in the queue
     */
    public interface QueueCreator<E> {

        /**
         * Creates a new {@link BlockingQueue} with the specified capacity.
         * @param capacity the number of items that can be stored in the queue
         * @return a new {@link BlockingQueue}
         */
        BlockingQueue<E> create(int capacity);

        /**
         * Creates a new {@link BlockingQueue} with an unbounded capacity.
         * @return a new {@link BlockingQueue}
         */
        BlockingQueue<E> create();
    }
}
