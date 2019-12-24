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

package org.elasticsearch.common.util.concurrent;

import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.SuppressForbidden;

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
    public static  <T> T get(Future<T> future, long timeout, TimeUnit unit) {
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
        if (e.getCause() instanceof RuntimeException) {
            return (RuntimeException) e.getCause();
        } else {
            return new UncategorizedExecutionException("Failed execution", e);
        }
    }
}
