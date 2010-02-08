/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.action.support;

import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.ElasticSearchInterruptedException;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.util.Nullable;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * @author kimchy (Shay Banon)
 */
public class PlainActionFuture<T> implements ActionFuture<T>, ActionListener<T> {

    public static <T> PlainActionFuture<T> newFuture() {
        return newFuture(null);
    }

    public static <T> PlainActionFuture<T> newFuture(@Nullable ActionListener<T> listener) {
        return new PlainActionFuture<T>(listener);
    }

    private final ActionListener<T> listener;

    private final CountDownLatch latch;

    private volatile boolean done;
    private volatile boolean canceled;
    private volatile T result;
    private volatile Throwable exp;

    public PlainActionFuture(ActionListener<T> listener) {
        this.listener = listener;
        latch = new CountDownLatch(1);
    }

    @Override public boolean cancel(boolean mayInterruptIfRunning) {
        if (done)
            return true;

        canceled = true;
        latch.countDown();
        return true;
    }

    @Override public boolean isCancelled() {
        return canceled;
    }

    @Override public boolean isDone() {
        return done;
    }

    @Override public T get() throws InterruptedException, ExecutionException {
        latch.await();

        if (!done || canceled) {
            throw new InterruptedException("future was interrupted");
        }

        if (exp != null) {
            throw new ExecutionException(exp.getMessage(), exp);
        }

        return this.result;
    }

    @Override public T get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        latch.await(timeout, unit);

        if (!done || canceled) {
            throw new TimeoutException("response did not arrive");
        }

        if (exp != null) {
            throw new ExecutionException(exp.getMessage(), exp);
        }

        return this.result;
    }

    @Override public T actionGet() throws ElasticSearchException {
        try {
            return get();
        } catch (InterruptedException e) {
            throw new ElasticSearchInterruptedException(e.getMessage());
        } catch (ExecutionException e) {
            if (e.getCause() instanceof ElasticSearchException) {
                throw (ElasticSearchException) e.getCause();
            } else {
                throw new TransportException("Failed execution", e);
            }
        }
    }

    @Override public T actionGet(long timeoutMillis) throws ElasticSearchException, TimeoutException {
        return actionGet(timeoutMillis, TimeUnit.MILLISECONDS);
    }

    @Override public T actionGet(long timeout, TimeUnit unit) throws ElasticSearchException, TimeoutException {
        try {
            return get(timeout, unit);
        } catch (InterruptedException e) {
            throw new ElasticSearchInterruptedException(e.getMessage());
        } catch (ExecutionException e) {
            if (e.getCause() instanceof ElasticSearchException) {
                throw (ElasticSearchException) e.getCause();
            } else {
                throw new ElasticSearchException("Failed execution", e);
            }
        }
    }

    @Override public void onResponse(T result) {
        this.done = true;
        this.result = result;

        if (canceled)
            return;

        if (listener != null) {
            listener.onResponse(result);
        }
        latch.countDown();
    }

    @Override public void onFailure(Throwable e) {
        this.done = true;
        this.exp = e;

        if (canceled)
            return;

        if (listener != null) {
            listener.onFailure(exp);
        }
        latch.countDown();
    }
}
