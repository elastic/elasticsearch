/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action;

import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.CheckedRunnable;
import org.elasticsearch.common.CheckedSupplier;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;

/**
 * Base class for {@link Runnable}s that need to call {@link ActionListener#onFailure(Exception)} in case an uncaught
 * exception or error is thrown while the actual action is run.
 */
public abstract class ActionRunnable<Response> extends AbstractRunnable {

    protected final ActionListener<Response> listener;

    /**
     * Creates a {@link Runnable} that invokes the given listener with {@code null} after the given runnable has executed.
     * @param listener Listener to invoke
     * @param runnable Runnable to execute
     * @return Wrapped {@code Runnable}
     */
    public static <T> ActionRunnable<T> run(ActionListener<T> listener, CheckedRunnable<Exception> runnable) {
        return new ActionRunnable<>(listener) {
            @Override
            protected void doRun() throws Exception {
                runnable.run();
                listener.onResponse(null);
            }
        };
    }

    /**
     * Creates a {@link Runnable} that invokes the given listener with the return of the given supplier.
     * @param listener Listener to invoke
     * @param supplier Supplier that provides value to pass to listener
     * @return Wrapped {@code Runnable}
     */
    public static <T> ActionRunnable<T> supply(ActionListener<T> listener, CheckedSupplier<T, Exception> supplier) {
        return ActionRunnable.wrap(listener, l -> l.onResponse(supplier.get()));
    }

    /**
     * Creates a {@link Runnable} that wraps the given listener and a consumer of it that is executed when the {@link Runnable} is run.
     * Invokes {@link ActionListener#onFailure(Exception)} on it if an exception is thrown on executing the consumer.
     * @param listener ActionListener to wrap
     * @param consumer Consumer of wrapped {@code ActionListener}
     * @param <T> Type of the given {@code ActionListener}
     * @return Wrapped {@code Runnable}
     */
    public static <T> ActionRunnable<T> wrap(ActionListener<T> listener, CheckedConsumer<ActionListener<T>, Exception> consumer) {
        return new ActionRunnable<>(listener) {
            @Override
            protected void doRun() throws Exception {
                consumer.accept(listener);
            }
        };
    }

    public ActionRunnable(ActionListener<Response> listener) {
        this.listener = listener;
    }

    /**
     * Calls the action listeners {@link ActionListener#onFailure(Exception)} method with the given exception.
     * This method is invoked for all exception thrown by {@link #doRun()}
     */
    @Override
    public void onFailure(Exception e) {
        listener.onFailure(e);
    }

    @Override
    public String toString() {
        return getClass().getName() + "/" + listener;
    }
}
