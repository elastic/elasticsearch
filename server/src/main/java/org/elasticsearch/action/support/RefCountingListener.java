/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.support;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.exception.ElasticsearchException;

import java.util.Objects;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A mechanism to complete a listener on the completion of some (dynamic) collection of other actions. Basic usage is as follows:
 *
 * <pre>
 * try (var refs = new RefCountingListener(finalListener)) {
 *     for (var item : collection) {
 *         runAsyncAction(item, refs.acquire()); // completes the acquired listener on completion
 *     }
 * }
 * </pre>
 *
 * The delegate listener is completed when execution leaves the try-with-resources block and every acquired reference is released. The
 * {@link RefCountingListener} collects (a bounded number of) exceptions received by its subsidiary listeners, and completes the delegate
 * listener with an exception if (and only if) any subsidiary listener fails. However, unlike a {@link GroupedActionListener} it leaves it
 * to the caller to collect the results of successful completions by accumulating them in a data structure of its choice. Also unlike a
 * {@link GroupedActionListener} there is no need to declare the number of subsidiary listeners up front: listeners can be acquired
 * dynamically as needed. Finally, you can continue to acquire additional listeners even outside the try-with-resources block, perhaps in a
 * separate thread, as long as there's at least one listener outstanding:
 *
 * <pre>
 * try (var refs = new RefCountingListener(finalListener)) {
 *     for (var item : collection) {
 *         if (condition(item)) {
 *             runAsyncAction(item, refs.acquire(results::add));
 *         }
 *     }
 *     if (flag) {
 *         runOneOffAsyncAction(refs.acquire(results::add));
 *         return;
 *     }
 *     for (var item : otherCollection) {
 *         var itemRef = refs.acquire(); // delays completion while the background action is pending
 *         executorService.execute(() -> {
 *             try {
 *                 if (condition(item)) {
 *                     runOtherAsyncAction(item, refs.acquire(results::add));
 *                 }
 *             } finally {
 *                 itemRef.onResponse(null);
 *             }
 *         });
 *     }
 * }
 * </pre>
 *
 * In particular (and also unlike a {@link GroupedActionListener}) this works even if you don't acquire any extra refs at all: in that case,
 * the delegate listener is completed at the end of the try-with-resources block.
 */
public final class RefCountingListener implements Releasable {

    private final ActionListener<Void> delegate;
    private final RefCountingRunnable refs = new RefCountingRunnable(this::finish);

    private final AtomicReference<Exception> exceptionRef = new AtomicReference<>();
    private final Semaphore exceptionPermits;
    private final AtomicInteger droppedExceptionsRef = new AtomicInteger();

    /**
     * Construct a {@link RefCountingListener} which completes {@code delegate} when all refs are released.
     * @param delegate The listener to complete when all refs are released. This listener must not throw any exception on completion. If all
     *                 the acquired listeners completed successfully then so is the delegate. If any of the acquired listeners completed
     *                 with failure then the delegate is completed with the first exception received, with other exceptions added to its
     *                 collection of suppressed exceptions.
     */
    public RefCountingListener(ActionListener<Void> delegate) {
        this(10, delegate);
    }

    /**
     * Construct a {@link RefCountingListener} which completes {@code delegate} when all refs are released.
     * @param delegate The listener to complete when all refs are released. This listener must not throw any exception on completion. If all
     *                 the acquired listeners completed successfully then so is the delegate. If any of the acquired listeners completed
     *                 with failure then the delegate is completed with the first exception received, with other exceptions added to its
     *                 collection of suppressed exceptions.
     * @param maxExceptions The maximum number of exceptions to accumulate on failure.
     */
    public RefCountingListener(int maxExceptions, ActionListener<Void> delegate) {
        if (maxExceptions <= 0) {
            assert false : maxExceptions;
            throw new IllegalArgumentException("maxExceptions must be positive");
        }
        this.delegate = ActionListener.assertOnce(Objects.requireNonNull(delegate));
        this.exceptionPermits = new Semaphore(maxExceptions);
    }

    /**
     * Release the original reference to this object, which completes the delegate {@link ActionListener} if there are no other references.
     *
     * It is invalid to call this method more than once. Doing so will trip an assertion if assertions are enabled, but will be ignored
     * otherwise. This deviates from the contract of {@link java.io.Closeable}.
     */
    @Override
    public void close() {
        refs.close();
    }

    private void finish() {
        try {
            var exception = exceptionRef.get();
            if (exception == null) {
                delegate.onResponse(null);
            } else {
                final var droppedExceptions = droppedExceptionsRef.getAndSet(0);
                if (droppedExceptions > 0) {
                    exception.addSuppressed(new ElasticsearchException(droppedExceptions + " further exceptions were dropped"));
                }
                delegate.onFailure(exception);
            }
        } catch (Exception e) {
            assert false : e;
            throw e;
        }
    }

    /**
     * Acquire a reference to this object and return a listener which releases it. The delegate {@link ActionListener} is called when all
     * its references have been released.
     *
     * It is invalid to call this method once all references are released. Doing so will trip an assertion if assertions are enabled, and
     * will throw an {@link IllegalStateException} otherwise.
     *
     * It is also invalid to complete the returned listener more than once. Doing so will trip an assertion if assertions are enabled, but
     * will be ignored otherwise.
     */
    public ActionListener<Void> acquire() {
        return new ActionListener<>() {
            private final Releasable ref = refs.acquire();

            @Override
            public void onResponse(Void unused) {
                ref.close();
            }

            @Override
            public void onFailure(Exception e) {
                try (ref) {
                    addException(e);
                }
            }

            @Override
            public String toString() {
                return RefCountingListener.this.toString();
            }
        };
    }

    /**
     * Acquire a reference to this object and return a listener which consumes a response and releases the reference. The delegate {@link
     * ActionListener} is called when all its references have been released. If the consumer throws an exception, the exception is passed
     * to the final listener as if the returned listener was completed exceptionally.
     *
     * It is invalid to call this method once all references are released. Doing so will trip an assertion if assertions are enabled, and
     * will throw an {@link IllegalStateException} otherwise.
     *
     * It is also invalid to complete the returned listener more than once. Doing so will trip an assertion if assertions are enabled, but
     * will be ignored otherwise.
     */
    public <Response> ActionListener<Response> acquire(CheckedConsumer<Response, Exception> consumer) {
        final var ref = refs.acquire();
        final var consumerRef = new AtomicReference<>(Objects.requireNonNull(consumer));
        return new ActionListener<>() {
            @Override
            public void onResponse(Response response) {
                try (ref) {
                    var acquiredConsumer = consumerRef.getAndSet(null);
                    if (acquiredConsumer == null) {
                        assert false : "already closed";
                    } else {
                        try {
                            acquiredConsumer.accept(response);
                        } catch (Exception e) {
                            addException(e);
                        }
                    }
                }
            }

            @Override
            public void onFailure(Exception e) {
                try (ref) {
                    var acquiredConsumer = consumerRef.getAndSet(null);
                    assert acquiredConsumer != null : "already closed";
                    addException(e);
                }
            }

            @Override
            public String toString() {
                return RefCountingListener.this + "[" + consumerRef.get() + "]";
            }
        };
    }

    private void addException(Exception e) {
        if (exceptionPermits.tryAcquire()) {
            final var firstException = exceptionRef.compareAndExchange(null, e);
            if (firstException != null && firstException != e) {
                firstException.addSuppressed(e);
            }
        } else {
            droppedExceptionsRef.incrementAndGet();
        }
    }

    @Override
    public String toString() {
        return "refCounting[" + delegate + "]";
    }

    /**
     * @return {@code true} if at least one acquired listener has completed exceptionally, which means that the delegate listener will also
     *         complete exceptionally once all acquired listeners are completed.
     */
    public boolean isFailing() {
        return exceptionRef.get() != null;
    }
}
