
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

abstract class AbstractRestClientActions implements RestClientActions {
    abstract SyncResponseListener syncResponseListener();

    abstract void performRequestAsyncNoCatch(String method, String endpoint, Map<String, String> params,
        HttpEntity entity, HttpAsyncResponseConsumerFactory httpAsyncResponseConsumerFactory,
        ResponseListener responseListener, Header[] headers) throws IOException;

    /**
     * Sends a request to the Elasticsearch cluster that the client points to and waits for the corresponding response
     * to be returned. Shortcut to {@link #performRequest(String, String, Map, HttpEntity, Header...)} but without parameters
     * and request body.
     *
     * @param method the http method
     * @param endpoint the path of the request (without host and port)
     * @param headers the optional request headers
     * @return the response returned by Elasticsearch
     * @throws IOException in case of a problem or the connection was aborted
     * @throws ClientProtocolException in case of an http protocol error
     * @throws ResponseException in case Elasticsearch responded with a status code that indicated an error
     */
    @Override
    public final Response performRequest(String method, String endpoint, Header... headers) throws IOException {
        return performRequest(method, endpoint, Collections.<String, String>emptyMap(), null, headers);
    }

    /**
     * Sends a request to the Elasticsearch cluster that the client points to and waits for the corresponding response
     * to be returned. Shortcut to {@link #performRequest(String, String, Map, HttpEntity, Header...)} but without request body.
     *
     * @param method the http method
     * @param endpoint the path of the request (without host and port)
     * @param params the query_string parameters
     * @param headers the optional request headers
     * @return the response returned by Elasticsearch
     * @throws IOException in case of a problem or the connection was aborted
     * @throws ClientProtocolException in case of an http protocol error
     * @throws ResponseException in case Elasticsearch responded with a status code that indicated an error
     */
    @Override
    public final Response performRequest(String method, String endpoint, Map<String, String> params, Header... headers) throws IOException {
        return performRequest(method, endpoint, params, (HttpEntity)null, headers);
    }

    /**
     * Sends a request to the Elasticsearch cluster that the client points to and waits for the corresponding response
     * to be returned. Shortcut to {@link #performRequest(String, String, Map, HttpEntity, HttpAsyncResponseConsumerFactory, Header...)}
     * which doesn't require specifying an {@link HttpAsyncResponseConsumerFactory} instance,
     * {@link HttpAsyncResponseConsumerFactory} will be used to create the needed instances of {@link HttpAsyncResponseConsumer}.
     *
     * @param method the http method
     * @param endpoint the path of the request (without host and port)
     * @param params the query_string parameters
     * @param entity the body of the request, null if not applicable
     * @param headers the optional request headers
     * @return the response returned by Elasticsearch
     * @throws IOException in case of a problem or the connection was aborted
     * @throws ClientProtocolException in case of an http protocol error
     * @throws ResponseException in case Elasticsearch responded with a status code that indicated an error
     */
    @Override // TODO this method is not final so the tests for the High Level REST Client don't have to change. We'll fix this soon.
    public Response performRequest(String method, String endpoint, Map<String, String> params,
                                   HttpEntity entity, Header... headers) throws IOException {
        return performRequest(method, endpoint, params, entity, HttpAsyncResponseConsumerFactory.DEFAULT, headers);
    }

    /**
     * Sends a request to the Elasticsearch cluster that the client points to. Blocks until the request is completed and returns
     * its response or fails by throwing an exception. Selects a host out of the provided ones in a round-robin fashion. Failing hosts
     * are marked dead and retried after a certain amount of time (minimum 1 minute, maximum 30 minutes), depending on how many times
     * they previously failed (the more failures, the later they will be retried). In case of failures all of the alive nodes (or dead
     * nodes that deserve a retry) are retried until one responds or none of them does, in which case an {@link IOException} will be thrown.
     *
     * This method works by performing an asynchronous call and waiting
     * for the result. If the asynchronous call throws an exception we wrap
     * it and rethrow it so that the stack trace attached to the exception
     * contains the call site. While we attempt to preserve the original
     * exception this isn't always possible and likely haven't covered all of
     * the cases. You can get the original exception from
     * {@link Exception#getCause()}.
     *
     * @param method the http method
     * @param endpoint the path of the request (without host and port)
     * @param params the query_string parameters
     * @param entity the body of the request, null if not applicable
     * @param httpAsyncResponseConsumerFactory the {@link HttpAsyncResponseConsumerFactory} used to create one
     * {@link HttpAsyncResponseConsumer} callback per retry. Controls how the response body gets streamed from a non-blocking HTTP
     * connection on the client side.
     * @param headers the optional request headers
     * @return the response returned by Elasticsearch
     * @throws IOException in case of a problem or the connection was aborted
     * @throws ClientProtocolException in case of an http protocol error
     * @throws ResponseException in case Elasticsearch responded with a status code that indicated an error
     */
    @Override
    public final Response performRequest(String method, String endpoint, Map<String, String> params,
                                   HttpEntity entity, HttpAsyncResponseConsumerFactory httpAsyncResponseConsumerFactory,
                                   Header... headers) throws IOException {
        SyncResponseListener listener = syncResponseListener();
        performRequestAsyncNoCatch(method, endpoint, params, entity, httpAsyncResponseConsumerFactory,
            listener, headers);
        return listener.get();
    }

    /**
     * Sends a request to the Elasticsearch cluster that the client points to. Doesn't wait for the response, instead
     * the provided {@link ResponseListener} will be notified upon completion or failure. Shortcut to
     * {@link #performRequestAsync(String, String, Map, HttpEntity, ResponseListener, Header...)} but without parameters and  request body.
     *
     * @param method the http method
     * @param endpoint the path of the request (without host and port)
     * @param responseListener the {@link ResponseListener} to notify when the request is completed or fails
     * @param headers the optional request headers
     */
    public final void performRequestAsync(String method, String endpoint, ResponseListener responseListener, Header... headers) {
        performRequestAsync(method, endpoint, Collections.<String, String>emptyMap(), null, responseListener, headers);
    }

    /**
     * Sends a request to the Elasticsearch cluster that the client points to. Doesn't wait for the response, instead
     * the provided {@link ResponseListener} will be notified upon completion or failure. Shortcut to
     * {@link #performRequestAsync(String, String, Map, HttpEntity, ResponseListener, Header...)} but without request body.
     *
     * @param method the http method
     * @param endpoint the path of the request (without host and port)
     * @param params the query_string parameters
     * @param responseListener the {@link ResponseListener} to notify when the request is completed or fails
     * @param headers the optional request headers
     */
    @Override
    public final void performRequestAsync(String method, String endpoint, Map<String, String> params,
                                    ResponseListener responseListener, Header... headers) {
        performRequestAsync(method, endpoint, params, null, responseListener, headers);
    }

    /**
     * Sends a request to the Elasticsearch cluster that the client points to. Doesn't wait for the response, instead
     * the provided {@link ResponseListener} will be notified upon completion or failure.
     * Shortcut to {@link #performRequestAsync(String, String, Map, HttpEntity, HttpAsyncResponseConsumerFactory, ResponseListener,
     * Header...)} which doesn't require specifying an {@link HttpAsyncResponseConsumerFactory} instance,
     * {@link HttpAsyncResponseConsumerFactory} will be used to create the needed instances of {@link HttpAsyncResponseConsumer}.
     *
     * @param method the http method
     * @param endpoint the path of the request (without host and port)
     * @param params the query_string parameters
     * @param entity the body of the request, null if not applicable
     * @param responseListener the {@link ResponseListener} to notify when the request is completed or fails
     * @param headers the optional request headers
     */
    @Override  // TODO this method is not final so the tests for the High Level REST Client don't have to change. We'll fix this soon.
    public void performRequestAsync(String method, String endpoint, Map<String, String> params,
                                    HttpEntity entity, ResponseListener responseListener, Header... headers) {
        performRequestAsync(method, endpoint, params, entity, HttpAsyncResponseConsumerFactory.DEFAULT, responseListener, headers);
    }

    /**
     * Sends a request to the Elasticsearch cluster that the client points to. The request is executed asynchronously
     * and the provided {@link ResponseListener} gets notified upon request completion or failure.
     * Selects a host out of the provided ones in a round-robin fashion. Failing hosts are marked dead and retried after a certain
     * amount of time (minimum 1 minute, maximum 30 minutes), depending on how many times they previously failed (the more failures,
     * the later they will be retried). In case of failures all of the alive nodes (or dead nodes that deserve a retry) are retried
     * until one responds or none of them does, in which case an {@link IOException} will be thrown.
     *
     * @param method the http method
     * @param endpoint the path of the request (without host and port)
     * @param params the query_string parameters
     * @param entity the body of the request, null if not applicable
     * @param httpAsyncResponseConsumerFactory the {@link HttpAsyncResponseConsumerFactory} used to create one
     * {@link HttpAsyncResponseConsumer} callback per retry. Controls how the response body gets streamed from a non-blocking HTTP
     * connection on the client side.
     * @param responseListener the {@link ResponseListener} to notify when the request is completed or fails
     * @param headers the optional request headers
     */
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
