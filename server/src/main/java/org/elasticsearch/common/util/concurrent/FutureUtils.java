/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.util.concurrent;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.exception.ElasticsearchTimeoutException;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class FutureUtils {

    /**
     * Cancel execution of this future without interrupting a running thread. See {@link Future#cancel(boolean)} for details.
     *
     * @param toCancel the future to cancel
     * @return false if the future could not be cancelled, otherwise true
     */
    @SuppressForbidden(reason = "Future#cancel()")
    public static boolean cancel(@Nullable final Future<?> toCancel) {
        if (toCancel != null) {
            return toCancel.cancel(false); // this method is a forbidden API since it interrupts threads
        }
        return false;
    }

    /**
     * Calls {@link Future#get()} without the checked exceptions.
     *
     * @param future to dereference
     * @param <T> the type returned
     * @return the value of the future
     */
    public static <T> T get(Future<T> future) {
        try {
            return future.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Future got interrupted", e);
        } catch (ExecutionException e) {
            throw rethrowExecutionException(e);
        }
    }

    /**
     * Calls {@link Future#get(long, TimeUnit)} without the checked exceptions.
     *
     * @param future to dereference
     * @param timeout to wait
     * @param unit for timeout
     * @param <T> the type returned
     * @return the value of the future
     */
    public static <T> T get(Future<T> future, long timeout, TimeUnit unit) {
        try {
            return future.get(timeout, unit);
        } catch (TimeoutException e) {
            throw new ElasticsearchTimeoutException(e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Future got interrupted", e);
        } catch (ExecutionException e) {
            throw FutureUtils.rethrowExecutionException(e);
        }
    }

    public static RuntimeException rethrowExecutionException(ExecutionException e) {
        if (e.getCause() instanceof RuntimeException runtimeException) {
            return runtimeException;
        } else {
            return new UncategorizedExecutionException("Failed execution", e);
        }
    }
}
