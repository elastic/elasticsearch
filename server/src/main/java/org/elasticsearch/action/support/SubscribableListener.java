/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.support;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.ListenableFuture;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.util.concurrent.UncategorizedExecutionException;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;

/**
 * An {@link ActionListener} to which other {@link ActionListener} instances can subscribe, such that when this listener is completed it
 * fans-out its result to the subscribed listeners.
 *
 * Similar to {@link ListenableActionFuture} and {@link ListenableFuture} except for its handling of exceptions: if this listener is
 * completed exceptionally then the exception is passed to subscribed listeners without modification.
 */
public class SubscribableListener<T> implements ActionListener<T> {

    private static final Logger logger = LogManager.getLogger(SubscribableListener.class);
    private static final Object EMPTY = new Object();

    /**
     * If we are incomplete, {@code ref} may refer to one of the following depending on how many waiting subscribers there are:
     * <ul>
     * <li>If there are no subscribers yet, {@code ref} refers to {@link #EMPTY}.
     * <li>If there is one subscriber, {@code ref} refers to it directly.
     * <li>If there are more than one subscriber, {@code ref} refers to the head of a linked list of subscribers in reverse order of their
     * subscriptions.
     * </ul>
     * If we are complete, {@code ref} refers to a {@code Result<T>} which will be used to complete any subsequent subscribers.
     */
    private final AtomicReference<Object> ref = new AtomicReference<>(EMPTY);

    /**
     * Add a listener to this listener's collection of subscribers. If this listener is complete, this method completes the subscribing
     * listener immediately with the result with which this listener was completed. Otherwise, the subscribing listener is retained and
     * completed when this listener is completed.
     * <p>
     * Subscribed listeners must not throw any exceptions. Use {@link ActionListener#wrap(ActionListener)} if you have a listener for which
     * exceptions from its {@link ActionListener#onResponse} method should be handled by its own {@link ActionListener#onFailure} method.
     * <p>
     * Listeners added strictly before this listener is completed will themselves be completed in the order in which their subscriptions
     * were received. However, there are no guarantees about the ordering of the completions of listeners which are added concurrently with
     * (or after) the completion of this listener.
     * <p>
     * If the subscribed listener is not completed immediately then it will be completed on the thread, and in the {@link ThreadContext}, of
     * the thread which completes this listener.
     */
    public final void addListener(ActionListener<T> listener) {
        addListener(listener, EsExecutors.DIRECT_EXECUTOR_SERVICE, null);
    }

    /**
     * Add a listener to this listener's collection of subscribers. If this listener is complete, this method completes the subscribing
     * listener immediately with the result with which this listener was completed. Otherwise, the subscribing listener is retained and
     * completed when this listener is completed.
     * <p>
     * Subscribed listeners must not throw any exceptions. Use {@link ActionListener#wrap(ActionListener)} if you have a listener for which
     * exceptions from its {@link ActionListener#onResponse} method should be handled by its own {@link ActionListener#onFailure} method.
     * <p>
     * Listeners added strictly before this listener is completed will themselves be completed in the order in which their subscriptions
     * were received. However, there are no guarantees about the ordering of the completions of listeners which are added concurrently with
     * (or after) the completion of this listener.
     *
     * @param executor      If not {@link EsExecutors#DIRECT_EXECUTOR_SERVICE}, and the subscribing listener is not completed immediately,
     *                      then it will be completed using the given executor. If the subscribing listener is completed immediately then
     *                      this completion happens on the subscribing thread.
     * @param threadContext If not {@code null}, and the subscribing listener is not completed immediately, then it will be completed in
     *                      the given thread context. If {@code null}, and the subscribing listener is not completed immediately, then it
     *                      will be completed in the {@link ThreadContext} of the completing thread. If the subscribing listener is
     *                      completed immediately then this completion happens in the {@link ThreadContext} of the subscribing thread.
     */
    @SuppressWarnings({ "rawtypes" })
    public final void addListener(ActionListener<T> listener, Executor executor, @Nullable ThreadContext threadContext) {
        if (tryComplete(ref.get(), listener)) {
            return;
        }

        final ActionListener<T> wrappedListener = fork(executor, preserveContext(threadContext, listener));
        Object currentValue = ref.compareAndExchange(EMPTY, wrappedListener);
        if (currentValue == EMPTY) {
            return;
        }
        Cell newCell = null;
        while (true) {
            if (tryComplete(currentValue, listener)) {
                return;
            }
            if (currentValue instanceof ActionListener firstListener) {
                final Cell tail = new Cell(firstListener, null);
                currentValue = ref.compareAndExchange(firstListener, tail);
                if (currentValue == firstListener) {
                    currentValue = tail;
                }
                continue;
            }
            if (currentValue instanceof Cell headCell) {
                if (newCell == null) {
                    newCell = new Cell(wrappedListener, headCell);
                } else {
                    newCell.next = headCell;
                }
                currentValue = ref.compareAndExchange(headCell, newCell);
                if (currentValue == headCell) {
                    return;
                }
            } else {
                assert false : "unexpected witness: " + currentValue;
            }
        }
    }

    @Override
    public final void onResponse(T result) {
        setResult(new SuccessResult<T>(result));
    }

    @Override
    public final void onFailure(Exception exception) {
        setResult(new FailureResult(exception, wrapException(exception)));
    }

    protected Exception wrapException(Exception exception) {
        return exception;
    }

    /**
     * @return {@code true} if and only if this listener has been completed (either successfully or exceptionally).
     */
    public final boolean isDone() {
        return isDone(ref.get());
    }

    /**
     * @return the result with which this listener completed successfully, or throw the exception with which it failed.
     *
     * @throws AssertionError if this listener is not complete yet and assertions are enabled.
     * @throws IllegalStateException if this listener is not complete yet and assertions are disabled.
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    protected final T rawResult() throws Exception {
        final Object refValue = ref.get();
        if (refValue instanceof SuccessResult result) {
            return (T) result.result();
        } else if (refValue instanceof FailureResult result) {
            throw result.exception();
        } else {
            assert false : "not done";
            throw new IllegalStateException("listener is not done, cannot get result yet");
        }
    }

    protected static RuntimeException wrapAsExecutionException(Throwable t) {
        if (t instanceof RuntimeException runtimeException) {
            return runtimeException;
        } else {
            return new UncategorizedExecutionException("Failed execution", new ExecutionException(t));
        }
    }

    private static <T> ActionListener<T> preserveContext(@Nullable ThreadContext threadContext, ActionListener<T> listener) {
        return threadContext == null ? listener : ContextPreservingActionListener.wrapPreservingContext(listener, threadContext);
    }

    private static <T> ActionListener<T> fork(Executor executor, ActionListener<T> listener) {
        return executor == EsExecutors.DIRECT_EXECUTOR_SERVICE ? listener : new ThreadedActionListener<>(executor, listener);
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private static <T> boolean tryComplete(Object refValue, ActionListener<T> listener) {
        if (refValue instanceof SuccessResult successResult) {
            successResult.complete(listener);
            return true;
        }
        if (refValue instanceof FailureResult failureResult) {
            failureResult.complete(listener);
            return true;
        }
        return false;
    }

    /**
     * If incomplete, atomically update {@link #ref} with the given result and use it to complete any pending listeners.
     */
    @SuppressWarnings("unchecked")
    private void setResult(Object result) {
        assert isDone(result);

        Object currentValue = ref.get();
        while (true) {
            if (isDone(currentValue)) {
                // already complete - nothing to do
                return;
            }

            final Object witness = ref.compareAndExchange(currentValue, result);
            if (witness == currentValue) {
                // we won the race to complete the listener
                if (currentValue instanceof ActionListener<?> listener) {
                    // unique subscriber - complete it
                    boolean completed = tryComplete(result, listener);
                    assert completed;
                } else if (currentValue instanceof Cell currCell) {
                    // multiple subscribers, but they are currently in reverse order of subscription so reverse them back
                    Cell prevCell = null;
                    while (true) {
                        final Cell nextCell = currCell.next;
                        currCell.next = prevCell;
                        if (nextCell == null) {
                            break;
                        }
                        prevCell = currCell;
                        currCell = nextCell;
                    }
                    // now they are in subscription order, complete them
                    while (currCell != null) {
                        boolean completed = tryComplete(result, (ActionListener<T>) currCell.listener);
                        assert completed;
                        currCell = currCell.next;
                    }
                } else {
                    assert currentValue == EMPTY : "unexpected witness: " + currentValue;
                }
                return;
            }

            // we lost a race with another setResult or addListener call - retry
            currentValue = witness;
        }
    }

    private static boolean isDone(Object refValue) {
        return refValue instanceof SubscribableListener.SuccessResult<?> || refValue instanceof SubscribableListener.FailureResult;
    }

    /**
     * A cell in the linked list of pending listeners.
     */
    private static class Cell {
        final ActionListener<?> listener;
        Cell next;

        Cell(ActionListener<?> listener, Cell next) {
            this.listener = listener;
            this.next = next;
        }
    }

    private record SuccessResult<T>(T result) {
        public void complete(ActionListener<T> listener) {
            try {
                listener.onResponse(result);
            } catch (Exception exception) {
                logger.error(Strings.format("exception thrown while handling response in listener [%s]", listener), exception);
                assert false : exception;
                // nothing more can be done here
            }
        }
    }

    private record FailureResult(Exception exception, Exception wrappedException) {
        public void complete(ActionListener<?> listener) {
            try {
                listener.onFailure(wrappedException);
            } catch (Exception innerException) {
                if (wrappedException != innerException) {
                    innerException.addSuppressed(wrappedException);
                }
                logger.error(
                    Strings.format("exception thrown while handling another exception in listener [%s]", listener),
                    innerException
                );
                assert false : innerException;
                // nothing more can be done here
            }
        }
    }

    /**
     * Adds a timeout to this listener, such that if the timeout elapses before the listener is completed then it will be completed with an
     * {@link ElasticsearchTimeoutException}.
     * <p>
     * The process which is racing against this timeout should stop and clean up promptly when the timeout occurs to avoid unnecessary
     * work. For instance, it could check that the race is not lost by calling {@link #isDone} whenever appropriate, or it could subscribe
     * another listener which performs any necessary cleanup steps.
     */
    public void addTimeout(TimeValue timeout, ThreadPool threadPool, String timeoutExecutor) {
        if (isDone()) {
            return;
        }
        addListener(ActionListener.running(scheduleTimeout(timeout, threadPool, timeoutExecutor)));
    }

    private Runnable scheduleTimeout(TimeValue timeout, ThreadPool threadPool, String timeoutExecutor) {
        try {
            final var cancellable = threadPool.schedule(
                () -> onFailure(new ElasticsearchTimeoutException(Strings.format("timed out after [%s/%dms]", timeout, timeout.millis()))),
                timeout,
                timeoutExecutor
            );
            return cancellable::cancel;
        } catch (Exception e) {
            onFailure(e);
            return () -> {};
        }
    }
}
