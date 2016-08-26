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
package org.elasticsearch.common.util.concurrent;

import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.logging.ESLogger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.function.Consumer;

/**
 * This async IO processor allows to batch IO operations and have a single writer processing the write operations.
 * This can be used to ensure that threads can continue with other work while the actual IO operation is still processed
 * by a single worker. A worker in this context can be any caller of the {@link #put(Object, Consumer)} method since it will
 * hijack a worker if nobody else is currently processing queued items. If the internal queue has reached it's capacity incoming threads
 * might be blocked until other items are processed
 */
public abstract class AsyncIOProcessor<Item> {
    private final ESLogger logger;
    private final ArrayBlockingQueue<Tuple<Item, Consumer<Exception>>> queue;
    private final Semaphore promiseSemaphore = new Semaphore(1);

    protected AsyncIOProcessor(ESLogger logger, int queueSize) {
        this.logger = logger;
        this.queue = new ArrayBlockingQueue<>(queueSize);
    }

    /**
     * Adds the given item to the queue. The listener is notified once the item is processed
     */
    public final void put(Item item, Consumer<Exception> listener) {
        ensureOpen();
        // the algorithm here tires to reduce the load on each individual caller.
        // we try to have only one caller that processes pending items to disc while others just add to the queue but
        // at the same time never overload the node by pushing too many items into the queue.

        // we first try make a promise that we are responsible for the processing
        final boolean promised = promiseSemaphore.tryAcquire();
        final List<Tuple<Item, Consumer<Exception>>> candidates = new ArrayList<>();
        if (promised) {
            // if we are responsible we can't wait for others to make space - we have to process until we can add to the queue
            while (queue.offer(new Tuple<>(item, listener)) == false) {
                drainAndSync(candidates);
            }
        } else {
            // in this case we are not responsible and can just block until there is space
            try {
                queue.put(new Tuple<>(item, listener));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        // here we have to try to make the promise again otherwise there is a race when a thread puts an entry without making the promise
        // while we are draining that mean we might exit below too early in the while loop if the drainAndSync call is fast.
        if (promised || promiseSemaphore.tryAcquire()) {
            // since we made the promise to process we gotta do it here at least once
            drainAndSync(candidates);
            promiseSemaphore.release(); // now to ensure we are passing it on we release the promise so another thread can take over
            while (queue.isEmpty() == false && promiseSemaphore.availablePermits() > 0) {
                // yet if the queue is not empty AND nobody else has yet made the promise to take over we continue processing
                drainAndSync(candidates);
            }
        }
    }

    private void drainAndSync(List<Tuple<Item, Consumer<Exception>>> candidates) {
        candidates.clear();
        queue.drainTo(candidates);
        processList(candidates);
    }

    private void processList(List<Tuple<Item, Consumer<Exception>>> candidates) {
        Exception exception = null;
        if (candidates.isEmpty() == false) {
            try {
                write(candidates);
            } catch (Exception ex) { // if this fails we are in deep shit - fail the request
                logger.debug("failed to ", ex);
                // this exception is passed to all listeners - we don't retry. if this doesn't work we are in deep shit
                exception = ex;
            }
        }
        for (Tuple<Item, Consumer<Exception>> tuple : candidates) {
            Consumer<Exception> consumer = tuple.v2();
            try {
                consumer.accept(exception);
            } catch (Exception ex) {
                logger.warn("failed to notify callback", ex);
            }
        }
    }

    /**
     * Writes or processes the items out or to disk.
     */
    protected abstract void write(List<Tuple<Item, Consumer<Exception>>> candidates) throws IOException;

    /**
     * method called before entering the {@link #put(Object, Consumer)} section to ensure this processor can still process items
     */
    protected abstract void ensureOpen();
}
