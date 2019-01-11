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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
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
    private volatile boolean done = false;
    private volatile Response result = null;
    private volatile Exception error = null;
    private final List<ActionListener<Response>> listeners = new ArrayList<>();

    @Override
    public void onResponse(Response response) {
        if (onComplete(response, null)) {
            ActionListener.onResponse(listeners, response);
        }
    }

    @Override
    public void onFailure(Exception e) {
        if (onComplete(null, e)) {
            ActionListener.onFailure(listeners, e);
        }
    }

    /** Returns {@code true} if this method changed the state of this step listener */
    private synchronized boolean onComplete(Response response, Exception e) {
        if (done == false) {
            this.error = e;
            this.result = response;
            this.done = true;
            return true;
        } else {
            return false;
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
        final ActionListener<Response> listener = ActionListener.wrap(onResponse, onFailure);
        final boolean ready;
        synchronized (this) {
            ready = done;
            if (ready == false) {
                listeners.add(listener);
            }
        }
        if (ready) {
            if (error == null) {
                ActionListener.onResponse(Collections.singletonList(listener), result);
            } else {
                ActionListener.onFailure(Collections.singletonList(listener), error);
            }
        }
    }

    /**
     * Gets the result of this step. This method will throw {@link IllegalArgumentException}
     * if this step is not completed yet or completed with a failure.
     */
    public Response result() {
        if (done == false) {
            throw new IllegalStateException("step is not completed yet");
        }
        if (error != null) {
            throw new IllegalStateException("step is completed with a failure", error);
        }
        return result;
    }
}
