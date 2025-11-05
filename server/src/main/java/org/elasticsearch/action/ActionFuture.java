/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action;

import org.elasticsearch.core.TimeValue;

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * An extension to {@link Future} that provides simplified "get" operations for action execution results.
 * This interface offers alternatives to the standard {@link Future#get()} methods by handling interruption
 * and execution exceptions differently, making them more suitable for Elasticsearch's action framework.
 *
 * <p>The {@code ActionFuture} methods catch {@link InterruptedException} and wrap it in an
 * {@link IllegalStateException}, and unwrap {@link java.util.concurrent.ExecutionException} to throw
 * the actual cause. This behavior simplifies exception handling in the common case where interruption
 * is unexpected and the underlying cause of execution failure is more relevant.
 *
 * <p><b>Usage Examples:</b></p>
 * <pre>{@code
 * // Simple non-blocking get
 * ActionFuture<SearchResponse> future = client.search(request);
 * SearchResponse response = future.actionGet();
 *
 * // Get with timeout using TimeUnit
 * SearchResponse response = future.actionGet(30, TimeUnit.SECONDS);
 *
 * // Get with timeout using TimeValue
 * SearchResponse response = future.actionGet(TimeValue.timeValueSeconds(30));
 * }</pre>
 *
 * @param <T> the type of the response
 */
public interface ActionFuture<T> extends Future<T> {

    /**
     * Gets the result of the action, waiting if necessary for the computation to complete.
     * This method is similar to {@link Future#get()} but with simplified exception handling:
     * <ul>
     *   <li>{@link InterruptedException} is caught and wrapped in an {@link IllegalStateException}</li>
     *   <li>{@link java.util.concurrent.ExecutionException} is caught and its cause is thrown directly</li>
     * </ul>
     *
     * @return the computed result
     * @throws IllegalStateException if the thread is interrupted while waiting
     * @throws RuntimeException if the computation threw an exception (the actual cause is thrown)
     */
    T actionGet();

    /**
     * Gets the result of the action, waiting up to the specified time if necessary for the computation to complete.
     * This method is similar to {@link Future#get(long, TimeUnit)} but with simplified exception handling:
     * <ul>
     *   <li>{@link InterruptedException} is caught and wrapped in an {@link IllegalStateException}</li>
     *   <li>{@link java.util.concurrent.ExecutionException} is caught and its cause is thrown directly</li>
     * </ul>
     *
     * @param timeout the maximum time to wait
     * @param unit the time unit of the timeout argument
     * @return the computed result
     * @throws IllegalStateException if the thread is interrupted while waiting
     * @throws java.util.concurrent.TimeoutException if the wait timed out
     * @throws RuntimeException if the computation threw an exception (the actual cause is thrown)
     */
    T actionGet(long timeout, TimeUnit unit);

    /**
     * Gets the result of the action, waiting up to the specified time if necessary for the computation to complete.
     * This method is similar to {@link Future#get(long, TimeUnit)} but with simplified exception handling
     * and accepts a {@link TimeValue} for timeout specification:
     * <ul>
     *   <li>{@link InterruptedException} is caught and wrapped in an {@link IllegalStateException}</li>
     *   <li>{@link java.util.concurrent.ExecutionException} is caught and its cause is thrown directly</li>
     * </ul>
     *
     * @param timeout the maximum time to wait as a {@link TimeValue}
     * @return the computed result
     * @throws IllegalStateException if the thread is interrupted while waiting
     * @throws java.util.concurrent.TimeoutException if the wait timed out
     * @throws RuntimeException if the computation threw an exception (the actual cause is thrown)
     */
    T actionGet(TimeValue timeout);
}
