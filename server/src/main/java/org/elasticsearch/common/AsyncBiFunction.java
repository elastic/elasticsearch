/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.common;

import org.elasticsearch.action.ActionListener;

/**
 * A {@link java.util.function.BiFunction}-like interface designed to be used with asynchronous executions.
 * This functional interface accepts two input parameters and provides the result asynchronously through
 * an {@link ActionListener}.
 *
 * <p><b>Usage Examples:</b></p>
 * <pre>{@code
 * AsyncBiFunction<String, Integer, Result> asyncProcessor = (name, count, listener) -> {
 *     // Perform async operation
 *     executor.execute(() -> {
 *         try {
 *             Result result = processData(name, count);
 *             listener.onResponse(result);
 *         } catch (Exception e) {
 *             listener.onFailure(e);
 *         }
 *     });
 * };
 *
 * asyncProcessor.apply("test", 5, new ActionListener<Result>() {
 *     public void onResponse(Result result) {
 *         // Handle successful result
 *     }
 *     public void onFailure(Exception e) {
 *         // Handle error
 *     }
 * });
 * }</pre>
 *
 * @param <T> the type of the first input parameter
 * @param <U> the type of the second input parameter
 * @param <C> the type of the result provided asynchronously to the listener
 */
public interface AsyncBiFunction<T, U, C> {

    /**
     * Applies this function to the given arguments and provides the result asynchronously through the listener.
     *
     * @param t the first input parameter
     * @param u the second input parameter
     * @param listener the listener to receive the result or failure notification
     */
    void apply(T t, U u, ActionListener<C> listener);
}
