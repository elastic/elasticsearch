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

package org.elasticsearch.client;

import java.io.IOException;
import java.net.ConnectException;
import java.net.SocketTimeoutException;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import javax.net.ssl.SSLHandshakeException;

import org.apache.http.ConnectionClosedException;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.conn.ConnectTimeoutException;

/**
 * Abstract implementation of {@link RestClientActions} shared by
 * {@link RestClient} and {@link RestClientView}.
 */
abstract class AbstractRestClientActions implements RestClientActions {
    /**
     * Build a {@link SyncResponseListener} to convert requests from
     * asynchronous to synchronous.
     */
    abstract SyncResponseListener syncResponseListener();

    /**
     * Perform the actual request asynchronously, letting any the caller
     * handle all exceptions.
     */
    abstract void performRequestAsyncNoCatch(String method, String endpoint, Map<String, String> params,
        HttpEntity entity, HttpAsyncResponseConsumerFactory httpAsyncResponseConsumerFactory,
        ResponseListener responseListener, Header[] headers) throws IOException;

    @Override
    public final Response performRequest(String method, String endpoint, Header... headers) throws IOException {
        return performRequest(method, endpoint, Collections.<String, String>emptyMap(), null, headers);
    }

    @Override
    public final Response performRequest(String method, String endpoint, Map<String, String> params, Header... headers) throws IOException {
        return performRequest(method, endpoint, params, (HttpEntity)null, headers);
    }

    @Override // TODO this method is not final so the tests for the High Level REST Client don't have to change. We'll fix this soon.
    public Response performRequest(String method, String endpoint, Map<String, String> params,
                                   HttpEntity entity, Header... headers) throws IOException {
        return performRequest(method, endpoint, params, entity, HttpAsyncResponseConsumerFactory.DEFAULT, headers);
    }

    @Override
    public final Response performRequest(String method, String endpoint, Map<String, String> params,
                                   HttpEntity entity, HttpAsyncResponseConsumerFactory httpAsyncResponseConsumerFactory,
                                   Header... headers) throws IOException {
        SyncResponseListener listener = syncResponseListener();
        performRequestAsyncNoCatch(method, endpoint, params, entity, httpAsyncResponseConsumerFactory,
            listener, headers);
        return listener.get();
    }

    public final void performRequestAsync(String method, String endpoint, ResponseListener responseListener, Header... headers) {
        performRequestAsync(method, endpoint, Collections.<String, String>emptyMap(), null, responseListener, headers);
    }

    @Override
    public final void performRequestAsync(String method, String endpoint, Map<String, String> params,
                                    ResponseListener responseListener, Header... headers) {
        performRequestAsync(method, endpoint, params, null, responseListener, headers);
    }

    @Override  // TODO this method is not final so the tests for the High Level REST Client don't have to change. We'll fix this soon.
    public void performRequestAsync(String method, String endpoint, Map<String, String> params,
                                    HttpEntity entity, ResponseListener responseListener, Header... headers) {
        performRequestAsync(method, endpoint, params, entity, HttpAsyncResponseConsumerFactory.DEFAULT, responseListener, headers);
    }

    @Override
    public final void performRequestAsync(String method, String endpoint, Map<String, String> params,
                                    HttpEntity entity, HttpAsyncResponseConsumerFactory httpAsyncResponseConsumerFactory,
                                    ResponseListener responseListener, Header... headers) {
        try {
            performRequestAsyncNoCatch(method, endpoint, params, entity, httpAsyncResponseConsumerFactory,
                responseListener, headers);
        } catch (Exception e) {
            responseListener.onFailure(e);
        }
    }

    /**
     * Listener used in any sync performRequest calls, it waits for a response or an exception back up to a timeout
     */
    static class SyncResponseListener implements ResponseListener {
        private final CountDownLatch latch = new CountDownLatch(1);
        private final AtomicReference<Response> response = new AtomicReference<>();
        private final AtomicReference<Exception> exception = new AtomicReference<>();

        private final long timeout;

        SyncResponseListener(long timeout) {
            assert timeout > 0;
            this.timeout = timeout;
        }

        @Override
        public void onSuccess(Response response) {
            Objects.requireNonNull(response, "response must not be null");
            boolean wasResponseNull = this.response.compareAndSet(null, response);
            if (wasResponseNull == false) {
                throw new IllegalStateException("response is already set");
            }

            latch.countDown();
        }

        @Override
        public void onFailure(Exception exception) {
            Objects.requireNonNull(exception, "exception must not be null");
            boolean wasExceptionNull = this.exception.compareAndSet(null, exception);
            if (wasExceptionNull == false) {
                throw new IllegalStateException("exception is already set");
            }
            latch.countDown();
        }

        /**
         * Waits (up to a timeout) for some result of the request: either a response, or an exception.
         */
        Response get() throws IOException {
            try {
                //providing timeout is just a safety measure to prevent everlasting waits
                //the different client timeouts should already do their jobs
                if (latch.await(timeout, TimeUnit.MILLISECONDS) == false) {
                    throw new IOException("listener timeout after waiting for [" + timeout + "] ms");
                }
            } catch (InterruptedException e) {
                throw new RuntimeException("thread waiting for the response was interrupted", e);
            }

            Exception exception = this.exception.get();
            Response response = this.response.get();
            if (exception != null) {
                if (response != null) {
                    IllegalStateException e = new IllegalStateException("response and exception are unexpectedly set at the same time");
                    e.addSuppressed(exception);
                    throw e;
                }
                /*
                 * Wrap and rethrow whatever exception we received, copying the type
                 * where possible so the synchronous API looks as much as possible
                 * like the asynchronous API. We wrap the exception so that the caller's
                 * signature shows up in any exception we throw.
                 */
                if (exception instanceof ResponseException) {
                    throw new ResponseException((ResponseException) exception);
                }
                if (exception instanceof ConnectException) {
                    ConnectException e = new ConnectException(exception.getMessage());
                    e.initCause(exception);
                    throw e;
                }
                if (exception instanceof ConnectTimeoutException) {
                    ConnectTimeoutException e = new ConnectTimeoutException(exception.getMessage());
                    e.initCause(exception);
                    throw e;
                }
                if (exception instanceof SocketTimeoutException) {
                    SocketTimeoutException e = new SocketTimeoutException(exception.getMessage());
                    e.initCause(exception);
                    throw e;
                }
                if (exception instanceof ConnectionClosedException) {
                    ConnectionClosedException e = new ConnectionClosedException(exception.getMessage());
                    e.initCause(exception);
                    throw e;
                }
                if (exception instanceof SSLHandshakeException) {
                    SSLHandshakeException e = new SSLHandshakeException(exception.getMessage());
                    e.initCause(exception);
                    throw e;
                }
                if (exception instanceof IOException) {
                    throw new IOException(exception.getMessage(), exception);
                }
                if (exception instanceof RuntimeException){
                    throw new RuntimeException(exception.getMessage(), exception);
                }
                throw new RuntimeException("error while performing request", exception);
            }

            if (response == null) {
                throw new IllegalStateException("response not set and no exception caught either");
            }
            return response;
        }
    }
}
