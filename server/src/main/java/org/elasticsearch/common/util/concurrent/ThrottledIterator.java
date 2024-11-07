/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.util.concurrent;

import org.elasticsearch.core.AbstractRefCounted;
import org.elasticsearch.core.RefCounted;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.Strings;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.util.Iterator;
import java.util.Objects;
import java.util.concurrent.Semaphore;
import java.util.function.BiConsumer;

public class ThrottledIterator<T> implements Releasable {

    private static final Logger logger = LogManager.getLogger(ThrottledIterator.class);

    /**
     * Iterate through the given collection, performing an operation on each item which may fork background tasks, but with a limit on the
     * number of such background tasks running concurrently to avoid overwhelming the rest of the system (e.g. starving other work of access
     * to an executor).
     *
     * @param iterator The items to iterate. May be accessed by multiple threads, but accesses are all protected by synchronizing on itself.
     * @param itemConsumer The operation to perform on each item. Each operation receives a {@link RefCounted} which can be used to track
     *                     the execution of any background tasks spawned for this item. This operation may run on the thread which
     *                     originally called {@link #run}, if this method has not yet returned. Otherwise it will run on a thread on which a
     *                     background task previously called {@link RefCounted#decRef()} on its ref count. This operation should not throw
     *                     any exceptions.
     * @param maxConcurrency The maximum number of ongoing operations at any time.
     * @param onCompletion   Executed when all items are completed.
     */
    public static <T> void run(Iterator<T> iterator, BiConsumer<Releasable, T> itemConsumer, int maxConcurrency, Runnable onCompletion) {
        try (var throttledIterator = new ThrottledIterator<>(iterator, itemConsumer, maxConcurrency, onCompletion)) {
            throttledIterator.run();
        }
    }

    private final RefCounted refs; // one ref for each running item, plus one for the iterator if incomplete
    private final Iterator<T> iterator;
    private final BiConsumer<Releasable, T> itemConsumer;
    private final Semaphore permits;

    private ThrottledIterator(Iterator<T> iterator, BiConsumer<Releasable, T> itemConsumer, int maxConcurrency, Runnable onCompletion) {
        this.iterator = Objects.requireNonNull(iterator);
        this.itemConsumer = Objects.requireNonNull(itemConsumer);
        if (maxConcurrency <= 0) {
            throw new IllegalArgumentException("maxConcurrency must be positive");
        }
        this.permits = new Semaphore(maxConcurrency);
        this.refs = AbstractRefCounted.of(onCompletion);
    }

    private void run() {
        while (permits.tryAcquire()) {
            final T item;
            synchronized (iterator) {
                if (iterator.hasNext()) {
                    item = iterator.next();
                } else {
                    permits.release();
                    return;
                }
            }
            try (var itemRefs = new ItemRefCounted()) {
                itemRefs.mustIncRef();
                itemConsumer.accept(Releasables.releaseOnce(itemRefs::decRef), item);
            } catch (Exception e) {
                logger.error(Strings.format("exception when processing [%s] with [%s]", item, itemConsumer), e);
                assert false : e;
            }
        }
    }

    @Override
    public void close() {
        refs.decRef();
    }

    // A RefCounted for a single item, including protection against calling back into run() if it's created and closed within a single
    // invocation of run().
    private class ItemRefCounted extends AbstractRefCounted implements Releasable {
        private boolean isRecursive = true;

        ItemRefCounted() {
            refs.mustIncRef();
        }

        @Override
        protected void closeInternal() {
            permits.release();
            try {
                // Someone must now pick up the next item. Here we might be called from the run() invocation which started processing the
                // just-completed item (via close() -> decRef()) if that item's processing didn't fork or all its forked tasks finished
                // first. If so, there's no need to call run() here, we can just return and the next iteration of the run() loop will
                // continue the processing; moreover calling run() in this situation could lead to a stack overflow. However if we're not
                // within that run() invocation then ...
                if (isRecursive() == false) {
                    // ... we're not within any other run() invocation either, so it's safe (and necessary) to call run() here.
                    run();
                }
            } finally {
                refs.decRef();
            }
        }

        // Note on blocking: we call both of these synchronized methods exactly once (and must enter close() before calling isRecursive()).
        // If close() releases the last ref and calls closeInternal(), and hence isRecursive(), then there's no other threads involved and
        // hence no blocking. In contrast if close() doesn't release the last ref then it exits immediately, so the call to isRecursive()
        // will proceed without delay in this case too.

        private synchronized boolean isRecursive() {
            return isRecursive;
        }

        @Override
        public synchronized void close() {
            decRef();
            isRecursive = false;
        }
    }
}
