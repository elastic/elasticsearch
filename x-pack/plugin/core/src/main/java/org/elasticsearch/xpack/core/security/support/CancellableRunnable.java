/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.support;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

/**
 * A runnable that allows us to terminate and call the listener. We use this as a runnable can
 * be queued and not executed for a long time or ever and this causes user requests to appear
 * to hang. In these cases at least we can provide a response.
 */
public class CancellableRunnable<T> extends AbstractRunnable {

    private final Runnable in;
    private final ActionListener<T> listener;
    private final Function<Exception, T> defaultValue;
    private final Logger logger;
    private final AtomicReference<RunnableState> state = new AtomicReference<>(RunnableState.AWAITING_EXECUTION);

    public CancellableRunnable(ActionListener<T> listener, Function<Exception, T> defaultValue, Runnable in, Logger logger) {
        this.listener = listener;
        this.defaultValue = Objects.requireNonNull(defaultValue);
        this.in = in;
        this.logger = logger;
    }

    @Override
    public void onFailure(Exception e) {
        logger.error("execution of cancellable runnable failed", e);
        final T result = defaultValue.apply(e);
        listener.onResponse(result);
    }

    @Override
    protected void doRun() throws Exception {
        if (state.compareAndSet(RunnableState.AWAITING_EXECUTION, RunnableState.EXECUTING)) {
            in.run();
        } else {
            logger.trace("skipping execution of cancellable runnable as the current state is [{}]", state.get());
        }
    }

    @Override
    public void onRejection(Exception e) {
        listener.onFailure(e);
    }

    /**
     * If the execution of this runnable has not already started, the runnable is cancelled and we pass an exception to the user
     * listener
     */
    public void maybeTimeout() {
        if (state.compareAndSet(RunnableState.AWAITING_EXECUTION, RunnableState.TIMED_OUT)) {
            logger.warn("skipping execution of cancellable runnable as it has been waiting for execution too long");
            listener.onFailure(new ElasticsearchTimeoutException("timed out waiting for execution of cancellable runnable"));
        }
    }

    private enum RunnableState {
        AWAITING_EXECUTION,
        EXECUTING,
        TIMED_OUT
    }
}
