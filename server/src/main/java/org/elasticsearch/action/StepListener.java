/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action;

import org.elasticsearch.action.support.GroupedActionListener;
import org.elasticsearch.common.CheckedConsumer;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.FutureUtils;
import org.elasticsearch.common.util.concurrent.ListenableFuture;

import java.util.Optional;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.BinaryOperator;
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

public final class StepListener<Response> extends NotifyOnceListener<Response> {
    private final ListenableFuture<Response> delegate;

    public StepListener() {
        this.delegate = new ListenableFuture<>();
    }

    @Override
    protected void innerOnResponse(Response response) {
        delegate.onResponse(response);
    }

    @Override
    protected void innerOnFailure(Exception e) {
        delegate.onFailure(e);
    }

    /**
     * Registers the given actions which are called when this step is completed. If this step is completed successfully,
     * the {@code onResponse} is called with the result; otherwise the {@code onFailure} is called with the failure.
     *
     * @param onResponse is called when this step is completed successfully
     * @param onFailure  is called when this step is completed with a failure
     */
    public void whenComplete(CheckedConsumer<Response, Exception> onResponse, Consumer<Exception> onFailure) {
        delegate.addListener(ActionListener.wrap(onResponse, onFailure), EsExecutors.newDirectExecutorService(), null);
    }

    /**
     * Combines this listener with another one, waiting for both to successfully complete and combining their results.
     *
     * @param other the other step listener to combine with
     * @param fn    the function that combines the results
     * @return the combined listener
     */
    public StepListener<Response> thenCombine(StepListener<Response> other, BinaryOperator<Response> fn) {
        final StepListener<Response> combined = new StepListener<>();
        final GroupedActionListener<Response> groupedActionListener =
            new GroupedActionListener<>(combined.map(results -> {
                final Optional<Response> response = results.stream().reduce(fn);
                assert response.isPresent();
                return response.get();
            }), 2);
        delegate.addListener(groupedActionListener, EsExecutors.newDirectExecutorService(), null);
        other.delegate.addListener(groupedActionListener, EsExecutors.newDirectExecutorService(), null);
        return combined;
    }

    /**
     * Returns the future associated with the given step listener
     */
    public Future<Response> asFuture() {
        return delegate;
    }

    /**
     * Gets the result of this step. This method will throw {@link IllegalStateException} if this step is not completed yet.
     */
    public Response result() {
        if (delegate.isDone() == false) {
            throw new IllegalStateException("step is not completed yet");
        }
        return FutureUtils.get(delegate, 0L, TimeUnit.NANOSECONDS); // this future is done already - use a non-blocking method.
    }
}
