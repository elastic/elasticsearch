/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action;

import org.elasticsearch.common.CheckedSupplier;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.CheckedRunnable;
import org.elasticsearch.core.RefCounted;
import org.elasticsearch.core.Releasable;

/**
 * Base class for {@link Runnable}s that need to call {@link ActionListener#onFailure(Exception)} in case
 * an uncaught exception or error is thrown while the actual action is run.
 *
 * <p>This class extends {@link AbstractRunnable} and automatically handles exceptions by forwarding them
 * to the listener's {@link ActionListener#onFailure} method. This is particularly useful for executing
 * asynchronous operations on thread pools where exceptions need to be properly propagated.
 *
 * <p><b>Usage Examples:</b></p>
 * <pre>{@code
 * // Execute a simple runnable that completes the listener with null
 * ActionListener<Void> listener = ActionListener.wrap(
 *     v -> System.out.println("Completed"),
 *     e -> System.err.println("Failed: " + e)
 * );
 * Runnable task = ActionRunnable.run(listener, () -> {
 *     // Do some work
 *     System.out.println("Work done");
 * });
 * threadPool.executor(ThreadPool.Names.GENERIC).execute(task);
 *
 * // Supply a result to the listener
 * ActionListener<String> stringListener = ActionListener.wrap(
 *     result -> System.out.println("Result: " + result),
 *     e -> System.err.println("Failed: " + e)
 * );
 * Runnable supplier = ActionRunnable.supply(stringListener, () -> {
 *     // Compute result
 *     return "computed value";
 * });
 * threadPool.executor(ThreadPool.Names.GENERIC).execute(supplier);
 * }</pre>
 */
public abstract class ActionRunnable<Response> extends AbstractRunnable {

    /**
     * The listener to be notified of success or failure when this runnable completes.
     */
    protected final ActionListener<Response> listener;

    /**
     * Creates a {@link Runnable} that invokes the given listener with {@code null} after the given
     * runnable has executed successfully. If the runnable throws an exception, the listener's
     * {@link ActionListener#onFailure} is called instead.
     *
     * @param <T> the type of the listener's response
     * @param listener the listener to invoke upon completion
     * @param runnable the runnable to execute
     * @return a wrapped {@code Runnable} that handles exceptions
     */
    public static <T> ActionRunnable<T> run(ActionListener<T> listener, CheckedRunnable<Exception> runnable) {
        return new ActionRunnable<>(listener) {
            @Override
            protected void doRun() throws Exception {
                runnable.run();
                listener.onResponse(null);
            }

            @Override
            public String toString() {
                return runnable.toString();
            }
        };
    }

    /**
     * Creates a {@link Runnable} that invokes the given listener with the result returned by the supplier.
     * If the supplier throws an exception, the listener's {@link ActionListener#onFailure} is called instead.
     *
     * @param <T> the type of the result
     * @param listener the listener to invoke with the supplied value
     * @param supplier the supplier that provides the value to pass to the listener
     * @return a wrapped {@code Runnable} that handles exceptions
     */
    public static <T> ActionRunnable<T> supply(ActionListener<T> listener, CheckedSupplier<T, Exception> supplier) {
        return ActionRunnable.wrap(listener, new CheckedConsumer<>() {
            @Override
            public void accept(ActionListener<T> l) throws Exception {
                l.onResponse(supplier.get());
            }

            @Override
            public String toString() {
                return supplier.toString();
            }
        });
    }

    /**
     * Similar to {@link #supply(ActionListener, CheckedSupplier)} but specifically for suppliers that return
     * reference-counted objects. The returned object will have its reference count decremented after invoking
     * the listener, ensuring proper resource cleanup.
     *
     * @param <T> the type of the reference-counted result
     * @param listener the listener to invoke with the supplied value
     * @param supplier the supplier that provides the reference-counted value
     * @return a wrapped {@code Runnable} that handles exceptions and decrements the ref count
     */
    public static <T extends RefCounted> ActionRunnable<T> supplyAndDecRef(
        ActionListener<T> listener,
        CheckedSupplier<T, Exception> supplier
    ) {
        return wrap(listener, new CheckedConsumer<>() {
            @Override
            public void accept(ActionListener<T> l) throws Exception {
                ActionListener.respondAndRelease(l, supplier.get());
            }

            @Override
            public String toString() {
                return supplier.toString();
            }
        });
    }

    /**
     * Creates a {@link Runnable} that wraps the given listener and executes a consumer that receives the listener.
     * If the consumer throws an exception, {@link ActionListener#onFailure(Exception)} is invoked on the listener.
     *
     * <p>This is useful for creating runnables that need to perform complex operations and then complete
     * the listener with a result or error.
     *
     * @param <T> the type of the listener's response
     * @param listener the listener to wrap
     * @param consumer the consumer to execute, which receives the wrapped listener
     * @return a wrapped {@code Runnable} that handles exceptions
     */
    public static <T> ActionRunnable<T> wrap(ActionListener<T> listener, CheckedConsumer<ActionListener<T>, Exception> consumer) {
        return new ActionRunnable<>(listener) {
            @Override
            protected void doRun() throws Exception {
                consumer.accept(listener);
            }

            @Override
            public String toString() {
                return "ActionRunnable#wrap[" + consumer + "]";
            }
        };
    }

    /**
     * Similar to {@link #wrap} but also manages a {@link Releasable} resource that is released after
     * executing the consumer, or if the action is rejected by the executor.
     *
     * <p>This is particularly useful for submitting actions holding resources to a threadpool which
     * might have a bounded queue. The resource will be properly released whether the task executes
     * successfully, fails, or is rejected.
     *
     * @param <T> the type of the listener's response
     * @param listener the listener to wrap
     * @param releasable the resource to release after execution
     * @param consumer the consumer to execute, which receives the wrapped listener
     * @return a wrapped {@code Runnable} that handles exceptions and releases resources
     */
    public static <T> ActionRunnable<T> wrapReleasing(
        ActionListener<T> listener,
        Releasable releasable,
        CheckedConsumer<ActionListener<T>, Exception> consumer
    ) {
        return new ActionRunnable<>(listener) {
            @Override
            protected void doRun() {
                try (releasable) {
                    ActionListener.run(listener, consumer);
                }
            }

            @Override
            public void onFailure(Exception e) {
                try (releasable) {
                    super.onFailure(e);
                }
            }

            @Override
            public String toString() {
                return "ActionRunnable#wrapReleasing[" + consumer + "]";
            }
        };
    }

    /**
     * Constructs a new {@code ActionRunnable} with the given listener.
     *
     * @param listener the listener to notify of success or failure
     */
    public ActionRunnable(ActionListener<Response> listener) {
        this.listener = listener;
    }

    /**
     * Invokes the listener's {@link ActionListener#onFailure(Exception)} method with the given exception.
     * This method is automatically called for all exceptions thrown by {@link #doRun()}.
     *
     * @param e the exception that occurred during execution
     */
    @Override
    public void onFailure(Exception e) {
        listener.onFailure(e);
    }

    /**
     * Returns a string representation of this action runnable, including the class name
     * and the listener's string representation.
     *
     * @return a descriptive string for this action runnable
     */
    @Override
    public String toString() {
        return getClass().getName() + "/" + listener;
    }
}
