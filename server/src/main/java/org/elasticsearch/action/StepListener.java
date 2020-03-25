/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.action;

import org.elasticsearch.common.CheckedConsumer;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.FutureUtils;
import org.elasticsearch.common.util.concurrent.ListenableFuture;

import java.util.concurrent.TimeUnit;
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
     * Gets the result of this step. This method will throw {@link IllegalStateException} if this step is not completed yet.
     */
    public Response result() {
        if (delegate.isDone() == false) {
            throw new IllegalStateException("step is not completed yet");
        }
        return FutureUtils.get(delegate, 0L, TimeUnit.NANOSECONDS); // this future is done already - use a non-blocking method.
    }
}
