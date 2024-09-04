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
import org.elasticsearch.common.CheckedBiConsumer;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.ListenableFuture;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.util.concurrent.UncategorizedExecutionException;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.threadpool.ThreadPool;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;

/**
 * An {@link ActionListener} to which other {@link ActionListener} instances can subscribe, such that when this listener is
 * completed it fans-out its result to the subscribed listeners.
 * <p>
 * If this listener is complete, {@link #addListener} completes the subscribing listener immediately
 * with the result with which this listener was completed. Otherwise, the subscribing listener is retained
 * and completed when this listener is completed.
 * <p>
 * Exceptions are passed to subscribed listeners without modification. {@link ListenableActionFuture} and {@link ListenableFuture} are child
 * classes that provide additional exception handling.
 * <p>
 * A sequence of async steps can be chained together using a series of {@link SubscribableListener}s, similar to {@link CompletionStage}
 * (without the {@code catch (Throwable t)}). Listeners can be created for each step, where the next step subscribes to the result of the
 * previous, using utilities like {@link #andThen(CheckedBiConsumer)}. The following example demonstrates how this might be used:
 * <pre>{@code
 * private void exampleAsyncMethod(String request, List<Long> items, ActionListener<Boolean> finalListener) {
 *     SubscribableListener
 *
 *         // Start the chain and run the first step by creating a SubscribableListener using newForked():
 *         .<String>newForked(l -> firstAsyncStep(request, l))
 *
 *         // Run a second step when the first step completes using andThen(); if the first step fails then the exception falls through to
 *         // the end without executing the intervening steps.
 *         .<Integer>andThen((l, firstStepResult) -> secondAsyncStep(request, firstStepResult, l))
 *
 *         // Run another step when the second step completes with another andThen() call; as above this only runs if the first two steps
 *         // succeed.
 *         .<Boolean>andThen((l, secondStepResult) -> {
 *             if (condition) {
 *                 // Steps are exception-safe: an exception thrown here will be passed to the listener rather than escaping to the
 *                 // caller.
 *                 throw new IOException("failure");
 *             }
 *
 *             // Steps can fan out to multiple subsidiary async actions using utilities like RefCountingListener.
 *             final var result = new AtomicBoolean();
 *             try (var listeners = new RefCountingListener(l.map(v -> result.get()))) {
 *                 for (final var item : items) {
 *                     thirdAsyncStep(secondStepResult, item, listeners.acquire());
 *                 }
 *             }
 *         })
 *
 *         // Synchronous (non-forking) steps which do not return a result can be expressed using andThenAccept() with a consumer:
 *         .andThenAccept(thirdStepResult -> {
 *             if (condition) {
 *                 // andThenAccept() is also exception-safe
 *                 throw new ElasticsearchException("some other problem");
 *             }
 *             consumeThirdStepResult(thirdStepResult);
 *         })
 *
 *         // Synchronous (non-forking) steps which do return a result can be expressed using andThenApply() with a function:
 *         .andThenApply(voidFromStep4 -> {
 *             if (condition) {
 *                 // andThenApply() is also exception-safe
 *                 throw new IllegalArgumentException("failure");
 *             }
 *             return computeFifthStepResult();
 *         })
 *
 *         // To complete the chain, add the outer listener which will be completed with the result of the previous step if all steps were
 *         // successful, or the exception if any step failed.
 *         .addListener(finalListener);
 * }
 * }</pre>
 */
public class SubscribableListener<T> implements ActionListener<T> {

    private static final Logger logger = LogManager.getLogger(SubscribableListener.class);
    private static final Object EMPTY = new Object();

    /**
     * Create a {@link SubscribableListener} which is incomplete.
     */
    public SubscribableListener() {
        this(EMPTY);
    }

    /**
     * Create a {@link SubscribableListener} which has already succeeded with the given result.
     */
    public static <T> SubscribableListener<T> newSucceeded(T result) {
        return new SubscribableListener<>(new SuccessResult<>(result));
    }

    /**
     * Create a {@link SubscribableListener} which has already failed with the given exception.
     */
    public static <T> SubscribableListener<T> newFailed(Exception exception) {
        return new SubscribableListener<>(new FailureResult(exception, exception));
    }

    /**
     * Create a {@link SubscribableListener}, fork a computation to complete it, and return the listener. If the forking itself throws an
     * exception then the exception is caught and fed to the returned listener.
     */
    public static <T> SubscribableListener<T> newForked(CheckedConsumer<ActionListener<T>, ? extends Exception> fork) {
        final var listener = new SubscribableListener<T>();
        ActionListener.run(listener, fork::accept);
        return listener;
    }

    private SubscribableListener(Object initialState) {
        state = initialState;
    }

    /**
     * If we are incomplete, {@code state} may be one of the following depending on how many waiting subscribers there are:
     * <ul>
     * <li>If there are no subscribers yet, {@code state} is {@link #EMPTY}.
     * <li>If there is one subscriber, {@code state} is that subscriber.
     * <li>If there are multiple subscribers, {@code state} is the head of a linked list of subscribers in reverse order of their
     * subscriptions.
     * </ul>
     * If we are complete, {@code state} is the {@code SuccessResult<T>} or {@code FailureResult} which will be used to complete any
     * subsequent subscribers.
     */
    @SuppressWarnings("FieldMayBeFinal") // updated via VH_STATE_FIELD (and _only_ via VH_STATE_FIELD)
    private volatile Object state;

    /**
     * Add a listener to this listener's collection of subscribers. If this listener is complete, this method completes the subscribing
     * listener immediately with the result with which this listener was completed. Otherwise, the subscribing listener is retained and
     * completed when this listener is completed.
     * <p>
     * Subscribed listeners must not throw any exceptions.
     * <p>
     * Listeners added strictly before this listener is completed will themselves be completed in the order in which their subscriptions
     * were received. However, there are no guarantees about the ordering of the completions of listeners which are added concurrently with
     * (or after) the completion of this listener.
     * <p>
     * If the subscribed listener is not completed immediately then it will be completed on the thread, and in the {@link ThreadContext}, of
     * the thread which completes this listener. In other words, if you want to ensure that {@code listener} is completed using a particular
     * executor, then you must do both of:
     * <ul>
     * <li>Ensure that this {@link SubscribableListener} is always completed using that executor, and</li>
     * <li>Invoke {@link #addListener} using that executor.</li>
     * </ul>
     */
    public final void addListener(ActionListener<T> listener) {
        addListener(listener, EsExecutors.DIRECT_EXECUTOR_SERVICE, null);
    }

    /**
     * Add a listener to this listener's collection of subscribers. If this listener is complete, this method completes the subscribing
     * listener immediately with the result with which this listener was completed. Otherwise, the subscribing listener is retained and
     * completed when this listener is completed.
     * <p>
     * Subscribed listeners must not throw any exceptions.
     * <p>
     * Listeners added strictly before this listener is completed will themselves be completed in the order in which their subscriptions
     * were received. However, there are no guarantees about the ordering of the completions of listeners which are added concurrently with
     * (or after) the completion of this listener.
     *
     * @param executor      If not {@link EsExecutors#DIRECT_EXECUTOR_SERVICE}, and the subscribing listener is not completed immediately,
     *                      then it will be completed using the given executor. If the subscribing listener is completed immediately then
     *                      this completion happens on the subscribing thread.
     *                      <p>
     *                      In other words, if you want to ensure that {@code listener} is completed using a particular executor, then you
     *                      must do both of:
     *                      <ul>
     *                      <li>Pass the desired executor in as {@code executor}, and</li>
     *                      <li>Invoke {@link #addListener} using that executor.</li>
     *                      </ul>
     *                      <p>
     *                      If {@code executor} rejects the execution of the completion of the subscribing listener then the result is
     *                      discarded and the subscribing listener is completed with a rejection exception on the thread which completes
     *                      this listener.
     * @param threadContext If not {@code null}, and the subscribing listener is not completed immediately, then it will be completed in
     *                      the given thread context. If {@code null}, and the subscribing listener is not completed immediately, then it
     *                      will be completed in the {@link ThreadContext} of the completing thread. If the subscribing listener is
     *                      completed immediately then this completion happens in the {@link ThreadContext} of the subscribing thread.
     */
    @SuppressWarnings({ "rawtypes" })
    public final void addListener(ActionListener<T> listener, Executor executor, @Nullable ThreadContext threadContext) {
        if (tryComplete(state, listener)) {
            return;
        }

        final ActionListener<T> wrappedListener = fork(executor, preserveContext(threadContext, listener));
        Object currentValue = compareAndExchangeState(EMPTY, wrappedListener);
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
                currentValue = compareAndExchangeState(firstListener, tail);
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
                currentValue = compareAndExchangeState(headCell, newCell);
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
        return isDone(state);
    }

    /**
     * @return the result with which this listener completed successfully, or throw the exception with which it failed.
     *
     * @throws AssertionError if this listener is not complete yet and assertions are enabled.
     * @throws IllegalStateException if this listener is not complete yet and assertions are disabled.
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    protected final T rawResult() throws Exception {
        final Object currentState = state;
        if (currentState instanceof SuccessResult result) {
            return (T) result.result();
        } else if (currentState instanceof FailureResult result) {
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
    private static <T> boolean tryComplete(Object currentState, ActionListener<T> listener) {
        if (currentState instanceof SuccessResult successResult) {
            successResult.complete(listener);
            return true;
        }
        if (currentState instanceof FailureResult failureResult) {
            failureResult.complete(listener);
            return true;
        }
        return false;
    }

    /**
     * If incomplete, atomically update {@link #state} with the given result and use it to complete any pending listeners.
     */
    @SuppressWarnings("unchecked")
    private void setResult(Object result) {
        assert isDone(result);

        Object currentState = state;
        while (true) {
            if (isDone(currentState)) {
                // already complete - nothing to do
                return;
            }

            final Object witness = compareAndExchangeState(currentState, result);
            if (witness == currentState) {
                // we won the race to complete the listener
                if (currentState instanceof ActionListener<?> listener) {
                    // unique subscriber - complete it
                    boolean completed = tryComplete(result, listener);
                    assert completed;
                } else if (currentState instanceof Cell currCell) {
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
                    assert currentState == EMPTY : "unexpected witness: " + currentState;
                }
                return;
            }

            // we lost a race with another setResult or addListener call - retry
            currentState = witness;
        }
    }

    private static boolean isDone(Object currentState) {
        return currentState instanceof SuccessResult<?> || currentState instanceof FailureResult;
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
     * Creates and returns a new {@link SubscribableListener} {@code L} and subscribes {@code nextStep} to this listener such that if this
     * listener is completed successfully then the result is discarded and {@code nextStep} is invoked with argument {@code L}. If this
     * listener is completed with exception {@code E} then so is {@code L}.
     * <p>
     * This can be used to construct a sequence of async actions, each ignoring the result of the previous ones:
     * <pre>
     * l.andThen(l1 -> forkAction1(args1, l1)).andThen(l2 -> forkAction2(args2, l2)).addListener(finalListener);
     * </pre>
     * After creating this chain, completing {@code l} with a successful response will call {@code forkAction1}, which will on completion
     * call {@code forkAction2}, which will in turn pass its response to {@code finalListener}. A failure of any step will bypass the
     * remaining steps and ultimately fail {@code finalListener}.
     * <p>
     * The threading of the {@code nextStep} callback is the same as for listeners added with {@link #addListener}: if this listener is
     * already complete then {@code nextStep} is invoked on the thread calling {@link #andThen} and in its thread context, but if this
     * listener is incomplete then {@code nextStep} is invoked on the completing thread and in its thread context. In other words, if you
     * want to ensure that {@code nextStep} is invoked using a particular executor, then you must do both of:
     * <ul>
     * <li>Ensure that this {@link SubscribableListener} is always completed using that executor, and</li>
     * <li>Invoke {@link #andThen} using that executor.</li>
     * </ul>
     */
    public <U> SubscribableListener<U> andThen(CheckedConsumer<ActionListener<U>, ? extends Exception> nextStep) {
        return newForked(l -> addListener(l.delegateFailureIgnoreResponseAndWrap(nextStep)));
    }

    /**
     * Creates and returns a new {@link SubscribableListener} {@code L} and subscribes {@code nextStep} to this listener such that if this
     * listener is completed successfully with result {@code R} then {@code nextStep} is invoked with arguments {@code L} and {@code R}. If
     * this listener is completed with exception {@code E} then so is {@code L}.
     * <p>
     * This can be used to construct a sequence of async actions, each invoked with the result of the previous one:
     * <pre>
     * l.andThen((l1, o1) -> forkAction1(o1, args1, l1)).andThen((l2, o2) -> forkAction2(o2, args2, l2)).addListener(finalListener);
     * </pre>
     * After creating this chain, completing {@code l} with a successful response will pass the response to {@code forkAction1}, which will
     * on completion pass its response to {@code forkAction2}, which will in turn pass its response to {@code finalListener}. A failure of
     * any step will bypass the remaining steps and ultimately fail {@code finalListener}.
     * <p>
     * The threading of the {@code nextStep} callback is the same as for listeners added with {@link #addListener}: if this listener is
     * already complete then {@code nextStep} is invoked on the thread calling {@link #andThen} and in its thread context, but if this
     * listener is incomplete then {@code nextStep} is invoked on the completing thread and in its thread context. In other words, if you
     * want to ensure that {@code nextStep} is invoked using a particular executor, then you must do
     * both of:
     * <ul>
     * <li>Ensure that this {@link SubscribableListener} is always completed using that executor, and</li>
     * <li>Invoke {@link #andThen} using that executor.</li>
     * </ul>
     */
    public <U> SubscribableListener<U> andThen(CheckedBiConsumer<ActionListener<U>, T, ? extends Exception> nextStep) {
        return andThen(EsExecutors.DIRECT_EXECUTOR_SERVICE, null, nextStep);
    }

    /**
     * Creates and returns a new {@link SubscribableListener} {@code L} and subscribes {@code nextStep} to this listener such that if this
     * listener is completed successfully with result {@code R} then {@code nextStep} is invoked with arguments {@code L} and {@code R}. If
     * this listener is completed with exception {@code E} then so is {@code L}.
     * <p>
     * This can be used to construct a sequence of async actions, each invoked with the result of the previous one:
     * <pre>
     * l.andThen(x, t, (l1,o1) -> forkAction1(o1,args1,l1)).andThen(x, t, (l2,o2) -> forkAction2(o2,args2,l2)).addListener(finalListener);
     * </pre>
     * After creating this chain, completing {@code l} with a successful response will pass the response to {@code forkAction1}, which will
     * on completion pass its response to {@code forkAction2}, which will in turn pass its response to {@code finalListener}. A failure of
     * any step will bypass the remaining steps and ultimately fail {@code finalListener}.
     * <p>
     * The threading of the {@code nextStep} callback is the same as for listeners added with {@link #addListener}: if this listener is
     * already complete then {@code nextStep} is invoked on the thread calling {@link #andThen} and in its thread context, but if this
     * listener is incomplete then {@code nextStep} is invoked using {@code executor}, in a thread context captured when {@link #andThen}
     * was called. In other words, if you want to ensure that {@code nextStep} is invoked using a particular executor, then you must do
     * both of:
     * <ul>
     * <li>Pass the desired executor in as {@code executor}, and</li>
     * <li>Invoke {@link #andThen} using that executor.</li>
     * </ul>
     * <p>
     * If {@code executor} rejects the execution of {@code nextStep} then the result is discarded and the returned listener is completed
     * with a rejection exception on the thread which completes this listener. Likewise if this listener is completed exceptionally but
     * {@code executor} rejects the execution of the completion of the returned listener then the returned listener is completed with a
     * rejection exception on the thread which completes this listener.
     */
    public <U> SubscribableListener<U> andThen(
        Executor executor,
        @Nullable ThreadContext threadContext,
        CheckedBiConsumer<ActionListener<U>, T, ? extends Exception> nextStep
    ) {
        return newForked(l -> addListener(l.delegateFailureAndWrap(nextStep), executor, threadContext));
    }

    /**
     * Creates and returns a new {@link SubscribableListener} {@code L} such that if this listener is completed successfully with result
     * {@code R} then {@code fn} is invoked with argument {@code R}, and {@code L} is completed with the result of that invocation. If this
     * listener is completed exceptionally, or {@code fn} throws an exception, then {@code L} is completed with that exception.
     * <p>
     * This is essentially a shorthand for a call to {@link #andThen} with a {@code nextStep} argument that is fully synchronous.
     * <p>
     * The threading of the {@code fn} invocation is the same as for listeners added with {@link #addListener}: if this listener is
     * already complete then {@code fn} is invoked on the thread calling {@link #andThenApply} and in its thread context, but if this
     * listener is incomplete then {@code fn} is invoked on the thread, and in the thread context, on which this listener is completed.
     */
    public <U> SubscribableListener<U> andThenApply(CheckedFunction<T, U, Exception> fn) {
        return newForked(l -> addListener(l.map(fn)));
    }

    /**
     * Creates and returns a new {@link SubscribableListener} {@code L} such that if this listener is completed successfully with result
     * {@code R} then {@code consumer} is applied to argument {@code R}, and {@code L} is completed with {@code null} when {@code
     * consumer} returns. If this listener is completed exceptionally, or {@code consumer} throws an exception, then {@code L} is
     * completed with that exception.
     * <p>
     * This is essentially a shorthand for a call to {@link #andThen} with a {@code nextStep} argument that is fully synchronous.
     * <p>
     * The threading of the {@code consumer} invocation is the same as for listeners added with {@link #addListener}: if this listener is
     * already complete then {@code consumer} is invoked on the thread calling {@link #andThenAccept} and in its thread context, but if
     * this listener is incomplete then {@code consumer} is invoked on the thread, and in the thread context, on which this listener is
     * completed.
     */
    public SubscribableListener<Void> andThenAccept(CheckedConsumer<T, Exception> consumer) {
        return newForked(l -> addListener(l.map(r -> {
            consumer.accept(r);
            return null;
        })));
    }

    /**
     * Adds a timeout to this listener, such that if the timeout elapses before the listener is completed then it will be completed with an
     * {@link ElasticsearchTimeoutException}.
     * <p>
     * The process which is racing against this timeout should stop and clean up promptly when the timeout occurs to avoid unnecessary
     * work. For instance, it could check that the race is not lost by calling {@link #isDone} whenever appropriate, or it could subscribe
     * another listener which performs any necessary cleanup steps.
     */
    public void addTimeout(TimeValue timeout, ThreadPool threadPool, Executor timeoutExecutor) {
        if (isDone()) {
            return;
        }
        addListener(ActionListener.running(scheduleTimeout(timeout, threadPool, timeoutExecutor)));
    }

    private Runnable scheduleTimeout(TimeValue timeout, ThreadPool threadPool, Executor timeoutExecutor) {
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

    private static final VarHandle VH_STATE_FIELD;

    static {
        try {
            VH_STATE_FIELD = MethodHandles.lookup()
                .in(SubscribableListener.class)
                .findVarHandle(SubscribableListener.class, "state", Object.class);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    private Object compareAndExchangeState(Object expectedValue, Object newValue) {
        return VH_STATE_FIELD.compareAndExchange(this, expectedValue, newValue);
    }
}
