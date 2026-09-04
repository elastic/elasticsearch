/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.repositories.azure;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.core.Nullable;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

/**
 * An iterator that allows to subscribe to a reactive publisher and request more elements
 * in batches as the iterator is consumed so an slow consumer is not overwhelmed by a fast
 * producer. Additionally it provides the ability to cancel the subscription before the entire
 * flux is consumed, for these cases it possible to provide a cleaner function that would be
 * invoked for all the elements that weren't consumed before the cancellation. (i.e. it's
 * possible to free the memory allocated for a byte buffer).
 */
class CancellableRateLimitedFluxIterator<T> implements Subscriber<T>, Iterator<T> {
    private static final Subscription CANCELLED_SUBSCRIPTION = new Subscription() {
        @Override
        public void request(long n) {
            // no op
        }

        @Override
        public void cancel() {
            // no op
        }
    };

    private final int elementsPerBatch;
    private final Queue<T> queue;
    private final Lock lock;
    private final Condition condition;
    private final Consumer<T> cleaner;
    private final AtomicReference<Subscription> subscription = new AtomicReference<>();
    private static final Logger logger = LogManager.getLogger(CancellableRateLimitedFluxIterator.class);
    private final AtomicReference<DoneState> doneState = new AtomicReference<>(DoneState.STILL_READING);
    private int emittedElements;

    /**
     * This is used to set 'done' and 'error' atomically
     *
     * @param done whether the iterator is done
     * @param error the terminal error, if one occurred
     * @param fromProducer whether the doneState was a result of a producer event
     */
    private record DoneState(boolean done, @Nullable Throwable error, boolean fromProducer) {

        static final DoneState STILL_READING = new DoneState(false, null, false);
        static final DoneState COMPLETE = new DoneState(true, null, true);

        public DoneState {
            assert done || error == null : "Must be done to specify an error";
        }

        public boolean isDoneWithError() {
            return done && error != null;
        }

        public boolean isErrorFromProducer() {
            return fromProducer && error != null;
        }

        public boolean isErrorFromConsumer() {
            return fromProducer == false && error != null;
        }
    }

    /**
     * Creates a new CancellableRateLimitedFluxIterator that would request to it's upstream publisher
     * in batches as specified in {@code elementsPerBatch}. Additionally, it's possible to provide a
     * function that would be invoked after cancellation for possibly outstanding elements that won't by
     * consumed downstream but need to be cleaned in any case.
     * @param elementsPerBatch the number of elements to request upstream
     * @param cleaner the function that would be used to clean unused elements
     */
    CancellableRateLimitedFluxIterator(int elementsPerBatch, Consumer<T> cleaner) {
        this.elementsPerBatch = elementsPerBatch;
        this.queue = new ArrayBlockingQueue<>(elementsPerBatch);
        this.lock = new ReentrantLock();
        this.condition = lock.newCondition();
        this.cleaner = cleaner;
    }

    @Override
    public boolean hasNext() {
        // This method acts as a barrier between producers and consumers
        // and it's possible that the consumer thread is blocked
        // waiting until the producer emits an element.
        for (;;) {
            final DoneState lastDoneState = this.doneState.get();
            boolean isQueueEmpty = queue.isEmpty();

            if (lastDoneState.done()) {
                Throwable e = lastDoneState.error();
                if (e != null) {
                    throw new RuntimeException(e);
                } else if (isQueueEmpty) {
                    return false;
                }
            }

            if (isQueueEmpty == false) {
                return true;
            }

            // Provide visibility guarantees for the modified queue
            lock.lock();
            try {
                while (doneState.get().done() == false && queue.isEmpty()) {
                    condition.await();
                }
            } catch (InterruptedException e) {
                cancelSubscription();
                throw new RuntimeException(e);
            } finally {
                lock.unlock();
            }
        }
    }

    @Override
    public T next() {
        // We block here until the producer has emitted an element.
        if (hasNext() == false) {
            throw new NoSuchElementException();
        }

        T nextElement = queue.poll();

        final var currentDoneState = doneState.get();
        if (currentDoneState.isDoneWithError()) {
            // We can't trust anything we read after doneState is done with an error or cancellation
            // as we may have begun clearing the queue
            if (nextElement != null) {
                cleanElement(nextElement);
            }
            throw new RuntimeException(currentDoneState.error());
        } else if (nextElement == null) {
            final var illegalStateException = new IllegalStateException(
                "Queue is empty: Expected one element to be available from the Reactive Streams source."
            );
            updateDoneState(new DoneState(true, illegalStateException, false));
            cancelSubscription();
            signalConsumer();
            throw illegalStateException;
        }

        int totalEmittedElements = emittedElements + 1;
        if (totalEmittedElements == elementsPerBatch) {
            emittedElements = 0;
            subscription.get().request(totalEmittedElements);
        } else {
            emittedElements = totalEmittedElements;
        }

        return nextElement;
    }

    @Override
    public void onSubscribe(Subscription s) {
        if (subscription.compareAndSet(null, s)) {
            s.request(elementsPerBatch);
        } else {
            s.cancel();
        }
    }

    @Override
    public void onNext(T element) {
        // It's possible that we receive more elements after cancelling the subscription
        // since it might have outstanding requests before the cancellation. In that case
        // we just clean the resources.
        if (doneState.get().done()) {
            cleanElement(element);
            return;
        }

        if (queue.offer(element) == false) {
            // If the source doesn't respect backpressure, we might lose elements,
            // in that case we cancel the subscription and mark this consumer as failed
            // cleaning possibly non-consumed outstanding elements.
            cancelSubscription();
            onError(new RuntimeException("Queue is full: Reactive Streams source doesn't respect backpressure"));
        }
        signalConsumer();
    }

    public void cancel() {
        updateDoneState(new DoneState(true, new CancellationException(), false));
        cancelSubscription();
        clearQueue();
        // cancel should be called from the consumer
        // thread, but to avoid potential deadlocks
        // we just try to release a possibly blocked
        // consumer
        signalConsumer();
    }

    @Override
    public void onError(Throwable t) {
        updateDoneState(new DoneState(true, t, true));
        clearQueue();
        signalConsumer();
    }

    @Override
    public void onComplete() {
        updateDoneState(DoneState.COMPLETE);
        signalConsumer();
    }

    // visible for testing
    Queue<T> getQueue() {
        return queue;
    }

    private void signalConsumer() {
        lock.lock();
        try {
            condition.signalAll();
        } finally {
            lock.unlock();
        }
    }

    private void clearQueue() {
        T element;
        while ((element = queue.poll()) != null) {
            cleanElement(element);
        }
    }

    private void cleanElement(T element) {
        try {
            cleaner.accept(element);
        } catch (Exception e) {
            logger.warn("Unable to clean unused element", e);
        }
    }

    private void cancelSubscription() {
        Subscription previousSubscription = subscription.getAndSet(CANCELLED_SUBSCRIPTION);
        previousSubscription.cancel();
    }

    private void updateDoneState(DoneState newState) {
        assert newState.done() : "We only ever update to done states";
        this.doneState.updateAndGet(existing -> {
            // If we're not already done, allow the update
            if (existing.done() == false) {
                return newState;
            }

            // Errors always overwrite non-errors
            if (existing.error() == null && newState.error() != null) {
                return newState;
            }

            // If the existing error is not from the producer, allow it to be overwritten by one from the producer
            if (existing.isErrorFromConsumer() && newState.isErrorFromProducer()) {
                return newState;
            }

            // Otherwise keep the existing state
            return existing;
        });
    }
}
