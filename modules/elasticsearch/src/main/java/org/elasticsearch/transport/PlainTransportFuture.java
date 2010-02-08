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

package org.elasticsearch.transport;

import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.ElasticSearchInterruptedException;
import org.elasticsearch.util.io.Streamable;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * @author kimchy (Shay Banon)
 */
public class PlainTransportFuture<V extends Streamable> implements TransportFuture<V>, TransportResponseHandler<V> {

    private final CountDownLatch latch;
    private final TransportResponseHandler<V> handler;
    private volatile boolean done;
    private volatile boolean canceled;
    private volatile V result;
    private volatile Exception exp;

    public PlainTransportFuture(TransportResponseHandler<V> handler) {
        this.handler = handler;
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

    @Override public V get() throws InterruptedException, ExecutionException {
        latch.await();

        if (!done || canceled) {
            throw new InterruptedException("future was interrupted");
        }

        if (exp != null) {
            throw new ExecutionException(exp.getMessage(), exp);
        }

        return this.result;
    }

    @Override public V get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        latch.await(timeout, unit);

        if (!done || canceled) {
            throw new TimeoutException("response did not arrive");
        }

        if (exp != null) {
            throw new ExecutionException(exp.getMessage(), exp);
        }

        return this.result;
    }

    @Override public V txGet() throws ElasticSearchException {
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

    @Override public V txGet(long timeout, TimeUnit unit) throws ElasticSearchException, TimeoutException {
        try {
            return get(timeout, unit);
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

    @Override public V newInstance() {
        return handler.newInstance();
    }

    @Override public void handleResponse(V response) {
        this.done = true;
        this.result = response;

        if (canceled)
            return;

        handler.handleResponse(response);
        latch.countDown();
    }

    @Override public void handleException(RemoteTransportException exp) {
        this.done = true;
        this.exp = exp;

        if (canceled)
            return;

        handler.handleException(exp);
        latch.countDown();
    }

    @Override public boolean spawn() {
        return handler.spawn();
    }
}
