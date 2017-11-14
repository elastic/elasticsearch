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

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.CheckedConsumer;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * A listener for action responses or failures.
 */
public interface ActionListener<Response> {
    /**
     * Handle action response. This response may constitute a failure or a
     * success but it is up to the listener to make that decision.
     */
    void onResponse(Response response);

    /**
     * A failure caused by an exception at some phase of the task.
     */
    void onFailure(Exception e);

    /**
     * Creates a listener that listens for a response (or failure) and executes the
     * corresponding consumer when the response (or failure) is received.
     *
     * @param onResponse the checked consumer of the response, when the listener receives one
     * @param onFailure the consumer of the failure, when the listener receives one
     * @param <Response> the type of the response
     * @return a listener that listens for responses and invokes the consumer when received
     */
    static <Response> ActionListener<Response> wrap(CheckedConsumer<Response, ? extends Exception> onResponse,
            Consumer<Exception> onFailure) {
        return new ActionListener<Response>() {
            @Override
            public void onResponse(Response response) {
                try {
                    onResponse.accept(response);
                } catch (Exception e) {
                    onFailure(e);
                }
            }

            @Override
            public void onFailure(Exception e) {
                onFailure.accept(e);
            }
        };
    }

    /**
     * Creates a listener that listens for a response (or failure) and executes the
     * corresponding runnable when the response (or failure) is received.
     *
     * @param runnable the runnable that will be called in event of success or failure
     * @param <Response> the type of the response
     * @return a listener that listens for responses and invokes the runnable when received
     */
    static <Response> ActionListener<Response> wrap(Runnable runnable) {
        return wrap(r -> runnable.run(), e -> runnable.run());
    }

    /**
     * Converts a listener to a {@link BiConsumer} for compatibility with the {@link java.util.concurrent.CompletableFuture}
     * api.
     *
     * @param listener that will be wrapped
     * @param <Response> the type of the response
     * @return a bi consumer that will complete the wrapped listener
     */
    static <Response> BiConsumer<Response, Throwable> toBiConsumer(ActionListener<Response> listener) {
        return (response, throwable) -> {
            if (throwable == null) {
                listener.onResponse(response);
            } else {
                if (throwable instanceof Exception) {
                    listener.onFailure((Exception) throwable);
                } else if (throwable instanceof Error) {
                    throw (Error) throwable;
                } else {
                    throw new AssertionError("Should have been either Error or Exception", throwable);
                }
            }
        };
    }

    /**
     * Notifies every given listener with the response passed to {@link #onResponse(Object)}. If a listener itself throws an exception
     * the exception is forwarded to {@link #onFailure(Exception)}. If in turn {@link #onFailure(Exception)} fails all remaining
     * listeners will be processed and the caught exception will be re-thrown.
     */
    static <Response> void onResponse(Iterable<ActionListener<Response>> listeners, Response response) {
        List<Exception> exceptionList = new ArrayList<>();
        for (ActionListener<Response> listener : listeners) {
            try {
                listener.onResponse(response);
            } catch (Exception ex) {
                try {
                    listener.onFailure(ex);
                } catch (Exception ex1) {
                    exceptionList.add(ex1);
                }
            }
        }
        ExceptionsHelper.maybeThrowRuntimeAndSuppress(exceptionList);
    }

    /**
     * Notifies every given listener with the failure passed to {@link #onFailure(Exception)}. If a listener itself throws an exception
     * all remaining listeners will be processed and the caught exception will be re-thrown.
     */
    static <Response> void onFailure(Iterable<ActionListener<Response>> listeners, Exception failure) {
        List<Exception> exceptionList = new ArrayList<>();
        for (ActionListener<Response> listener : listeners) {
            try {
                listener.onFailure(failure);
            } catch (Exception ex) {
                exceptionList.add(ex);
            }
        }
        ExceptionsHelper.maybeThrowRuntimeAndSuppress(exceptionList);
    }
}
