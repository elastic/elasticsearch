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
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

public class ThrottledIterator<T> implements Releasable {

    private static final Logger logger = LogManager.getLogger(ThrottledIterator.class);

    /**
     * Iterate through the given collection, performing an operation on each item which may fork background tasks, but with a limit on the
     * number of such background tasks running concurrently to avoid overwhelming the rest of the system (e.g. starving other work of access
     * to an executor).
     *
     * @param iterator The items to iterate. May be accessed by multiple threads, but accesses are always strictly sequential: for any two
     *                 method calls {@code M1} and {@code M2} on this iterator, either the return of {@code M1} _happens-before_ the
     *                 invocation of {@code M2} or vice versa.
     *
     * @param itemConsumer The operation to perform on each item. Each operation receives a {@link Releasable} resource which can be used to
     *                     track the execution of any background tasks spawned for this item and which must be closed when all processing of
     *                     the item has finished. If the {@code iterator} has not been fully consumed then closing an operation's resource
     *                     triggers the start of an operation on the next item.
     *                     <p>
     *                     This operation may run on the thread which originally called {@link #run}, if this method has not yet returned.
     *                     Otherwise, it will run on a thread on which a background task previously closed its tracking resource.
     *                     <p>
     *                     This operation must not throw any exceptions.
     *
     * @param maxConcurrency The maximum number of ongoing operations at any time.
     *
     * @param onCompletion   Executed when all items are completed.
     */
    public static <T> void run(Iterator<T> iterator, BiConsumer<Releasable, T> itemConsumer, int maxConcurrency, Runnable onCompletion) {
        run(iterator, itemConsumer, maxConcurrency, onCompletion, EsExecutors.DIRECT_EXECUTOR_SERVICE, e -> {});
    }

    /**
     * Like {@link #run(Iterator, BiConsumer, int, Runnable)} but dispatches continuation work to the given executor when an item's
     * {@link Releasable} is closed after the initial call to {@link #run} has returned. Up to {@code maxConcurrency} items may be
     * processed on the calling thread before {@link #run} returns; once it has returned, further items are processed on threads
     * that the supplied executor picks. Note that if an {@code itemConsumer} closes its {@link Releasable} synchronously, the
     * resulting continuation can be dispatched to the executor even while the initial call is still looping.
     * <p>
     * When using {@link EsExecutors#DIRECT_EXECUTOR_SERVICE}, exceptions from {@code iterator.hasNext()}/{@code iterator.next()} propagate
     * directly to the caller of {@link Releasable#close()}, preserving the same behavior as the executor-less overload. With a real
     * executor, such exceptions cannot propagate across threads; they are surfaced to the caller via {@code onContinuationFailure}
     * and the iteration terminates.
     *
     * @param executor Executor to dispatch {@link #run()} from {@link #onItemRelease()}.
     *                 Must not be {@code null}. Use {@link EsExecutors#DIRECT_EXECUTOR_SERVICE} for inline execution
     *                 (same behaviour as the executor-less overload).
     *
     * @param onContinuationFailure Invoked when a continuation fails or is rejected by {@code executor}.
     */
    public static <T> void run(
        Iterator<T> iterator,
        BiConsumer<Releasable, T> itemConsumer,
        int maxConcurrency,
        Runnable onCompletion,
        Executor executor,
        Consumer<Exception> onContinuationFailure
    ) {
        try (
            var throttledIterator = new ThrottledIterator<>(
                iterator,
                itemConsumer,
                maxConcurrency,
                onCompletion,
                executor,
                onContinuationFailure
            )
        ) {
            throttledIterator.run();
        }
    }

    private final RefCounted refs; // one ref for each running item, plus one for the iterator if incomplete
    private final Iterator<T> iterator;
    private final BiConsumer<Releasable, T> itemConsumer;
    private final AtomicInteger permits;
    private final Executor executor;
    private final Consumer<Exception> onContinuationFailure;
    private final Releasable itemReleasable = this::onItemRelease;

    private ThrottledIterator(
        Iterator<T> iterator,
        BiConsumer<Releasable, T> itemConsumer,
        int maxConcurrency,
        Runnable onCompletion,
        Executor executor,
        Consumer<Exception> onContinuationFailure
    ) {
        this.iterator = Objects.requireNonNull(iterator);
        this.itemConsumer = Objects.requireNonNull(itemConsumer);
        if (maxConcurrency <= 0) {
            throw new IllegalArgumentException("maxConcurrency must be positive");
        }
        this.permits = new AtomicInteger(maxConcurrency);
        this.refs = AbstractRefCounted.of(onCompletion);
        this.executor = Objects.requireNonNull(executor);
        this.onContinuationFailure = Objects.requireNonNull(onContinuationFailure);
    }

    private void run() {
        do {
            final T item;
            if (iterator.hasNext()) {
                item = iterator.next();
            } else {
                return;
            }
            try {
                refs.mustIncRef();
                itemConsumer.accept(Releasables.releaseOnce(itemReleasable), item);
            } catch (Exception e) {
                logger.error(Strings.format("exception when processing [%s] with [%s]", item, itemConsumer), e);
                assert false : e;
            }
            assert permits.get() > 0; // i.e. only one thread is in this loop at once
        } while (permits.decrementAndGet() > 0);
    }

    @Override
    public void close() {
        refs.decRef();
    }

    private void onItemRelease() {
        try {
            if (permits.getAndIncrement() == 0) {
                if (executor == EsExecutors.DIRECT_EXECUTOR_SERVICE) {
                    run();
                } else {
                    refs.mustIncRef();
                    executor.execute(new AbstractRunnable() {
                        @Override
                        protected void doRun() {
                            ThrottledIterator.this.run();
                        }

                        @Override
                        public void onFailure(Exception e) {
                            onContinuationFailure.accept(e);
                        }

                        @Override
                        public void onRejection(Exception e) {
                            onContinuationFailure.accept(e);
                        }

                        @Override
                        public void onAfter() {
                            refs.decRef();
                        }
                    });
                }
            }
        } finally {
            refs.decRef();
        }
    }
}
