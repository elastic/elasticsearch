/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action;

import org.elasticsearch.common.util.concurrent.ListenableFuture;
import org.elasticsearch.core.CheckedConsumer;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/**
 * A {@link StepListener} provides a simple way to write a flow consisting of
 * multiple asynchronous steps without having nested callbacks. For example:
 *
 * <pre>{@code
 *  void asyncFlowMethod(... ActionListener<R> flowListener) {
 *    StepListener<R1> step1 = new StepListener<>();
 *    asyncStep1(..., step1);

 *    StepListener<R2> step2 = new StepListener<>();
 *    step1.whenComplete(r1 -> {
 *      asyncStep2(r1, ..., step2);
 *    }, flowListener::onFailure);
 *
 *    step2.whenComplete(r2 -> {
 *      R1 r1 = step1.result();
 *      R r = combine(r1, r2);
 *     flowListener.onResponse(r);
 *    }, flowListener::onFailure);
 *  }
 * }</pre>
 */

public final class StepListener<Response> implements ActionListener<Response> {

    private final AtomicBoolean hasBeenCalled = new AtomicBoolean(false);
    private final ListenableFuture<Response> delegate;

    public StepListener() {
        this.delegate = new ListenableFuture<>();
    }

    @Override
    public void onResponse(Response response) {
        if (hasBeenCalled.compareAndSet(false, true)) {
            delegate.onResponse(response);
        }
    }

    @Override
    public void onFailure(Exception e) {
        if (hasBeenCalled.compareAndSet(false, true)) {
            delegate.onFailure(e);
        }
    }

    /**
     * Registers the given actions which are called when this step is completed. If this step is completed successfully,
     * the {@code onResponse} is called with the result; otherwise the {@code onFailure} is called with the failure.
     *
     * @param onResponse is called when this step is completed successfully
     * @param onFailure  is called when this step is completed with a failure
     */
    public void whenComplete(CheckedConsumer<Response, Exception> onResponse, Consumer<Exception> onFailure) {
        addListener(ActionListener.wrap(onResponse, onFailure));
    }

    /**
     * @return the result of this step, if it has been completed successfully, or throw the exception with which it was completed
     * exceptionally. It is not valid to call this method if the step is incomplete.
     */
    public Response result() {
        return delegate.result();
    }

    /**
     * @return whether this step is complete yet.
     */
    public boolean isDone() {
        return delegate.isDone();
    }

    /**
     * Registers the given listener to be notified with the result of this step.
     */
    public void addListener(ActionListener<Response> listener) {
        delegate.addListener(listener);
    }

}
