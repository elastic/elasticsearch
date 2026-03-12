/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.support;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.Releasable;

import java.util.Objects;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A mechanism to complete a listener on the completion of some (dynamic) collection of other actions. Basic usage is as follows:
 *
 * <pre>
 * try (var listeners = new RefCountingListener(finalListener)) {
 *     for (var item : collection) {
 *         runAsyncAction(item, listeners.acquire()); // completes the acquired listener on completion
 *     }
 * }
 * </pre>
 *
 * The delegate listener is completed when execution leaves the try-with-resources block and every acquired listener is completed. The
 * {@link RefCountingListener} collects a bounded number of the exceptions received by the acquired listeners, and completes the delegate
 * listener with an exception if and only if any acquired listener fails. If more than one acquired listener fails, the resulting exception
 * is the first such failure, to which other tracked exceptions are added using {@link Exception#addSuppressed}.
 * <p>
 * A {@link RefCountingListener}, unlike a {@link GroupedActionListener}, leaves it to the caller to collect the results of successful
 * completions by accumulating them in a data structure of its choice: a {@link RefCountingListener} itself adds only a small and
 * {@code O(1)} amount of heap overhead. Also unlike a {@link GroupedActionListener} there is no need to declare the number of subsidiary
 * listeners up front: listeners can be acquired dynamically as needed, and you can continue to acquire additional listeners outside the
 * try-with-resources block, even in a separate thread, as long as you ensure there's at least one listener outstanding:
 *
 * <pre>
 * try (var listeners = new RefCountingListener(finalListener)) {
 *     for (var item : collection) {
 *         if (condition(item)) {
 *             runAsyncAction(item, listeners.acquire(results::add));
 *         }
 *     }
 *     if (flag) {
 *         runOneOffAsyncAction(listeners.acquire(results::add));
 *         return;
 *     }
 *     for (var item : otherCollection) {
 *         var itemListener = listeners.acquire(); // delays completion while the background action is pending
 *         executorService.execute(() -> {
 *             try {
 *                 if (condition(item)) {
 *                     runOtherAsyncAction(item, listeners.acquire(results::add));
 *                 }
 *             } finally {
 *                 itemListener.onResponse(null);
 *             }
 *         });
 *     }
 * }
 * </pre>
 *
 * In particular (and also unlike a {@link GroupedActionListener}) this works even if you don't acquire any extra listeners at all: in that
 * case, the delegate listener is completed at the end of the try-with-resources block.
 * <p>
 * The delegate listener is completed on the thread that completes the last acquired listener, or the thread that closes the
 * try-with-resources block if there are no incomplete acquired listeners when this happens.
 * <p>
 * See also {@link RefCountingRunnable}, which fulfils a similar role in situations where the subsidiary actions cannot fail (or at least
 * where you do not need such failures to propagate automatically to the final action).
 */
public final class RefCountingListener implements Releasable {

    private final ActionListener<Void> delegate;
    private final RefCountingRunnable refs = new RefCountingRunnable(this::finish);

    private final AtomicReference<Exception> exceptionRef = new AtomicReference<>();
    private final Semaphore exceptionPermits;
    private final AtomicInteger droppedExceptionsRef = new AtomicInteger();

    /**
     * Construct a {@link RefCountingListener} which completes {@code delegate} when all acquired listeners have been completed.
     * @param delegate The listener to complete when all acquired listeners are completed. This listener must not throw any exception on
     *                 completion. If all the acquired listeners completed successfully then so is the delegate. If any of the acquired
     *                 listeners completed exceptionally then the delegate is completed with the first exception received, with up to 10
     *                 other exceptions added to its collection of suppressed exceptions.
     */
    public RefCountingListener(ActionListener<Void> delegate) {
        this(10, delegate);
    }

    /**
     * Construct a {@link RefCountingListener} which completes {@code delegate} when all acquired listeners have been completed.
     * @param delegate The listener to complete when all acquired listeners are completed. This listener must not throw any exception on
     *                 completion. If all the acquired listeners completed successfully then so is the delegate. If any of the acquired
     *                 listeners completed exceptionally then the delegate is completed with the first exception received, with other
     *                 exceptions added to its collection of suppressed exceptions.
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
     * Release the original reference to this object, which completes the delegate {@link ActionListener} if there are no incomplete
     * acquired listeners.
     * <p>
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
     * Acquire a listener which awaits a {@link Void} response. The delegate {@link ActionListener} is called when all such acquired
     * listeners are completed, on the thread that completes the last such listener.
     * <p>
     * It is invalid to call this method once all acquired listeners have been completed and the original try-with-resources block is
     * closed. Doing so will trip an assertion if assertions are enabled, and will throw an {@link IllegalStateException} otherwise.
     * <p>
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
     * Acquire a listener which consumes a response, for instance by storing the response in some kind of data structure. The delegate
     * {@link ActionListener} is called when all such acquired listeners are completed, on the thread that completes the last such listener.
     * If the {@code consumer} throws an exception then that exception is passed to the final listener as if the returned listener was
     * completed exceptionally.
     * <p>
     * It is invalid to call this method once all acquired listeners have been completed and the original try-with-resources block is
     * closed. Doing so will trip an assertion if assertions are enabled, and will throw an {@link IllegalStateException} otherwise.
     * <p>
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
