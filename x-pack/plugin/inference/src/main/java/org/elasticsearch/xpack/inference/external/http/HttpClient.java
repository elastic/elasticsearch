/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.http;

import org.apache.http.HttpResponse;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.concurrent.FutureCallback;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.http.impl.nio.conn.PoolingNHttpClientConnectionManager;
import org.apache.http.protocol.HttpContext;
import org.apache.http.util.EntityUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.common.socket.SocketAccess;
import org.elasticsearch.xpack.inference.external.request.HttpRequest;
import org.elasticsearch.xpack.inference.logging.ThrottlerManager;

import java.io.Closeable;
import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.CancellationException;

import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.xpack.inference.InferencePlugin.INFERENCE_RESPONSE_THREAD_POOL_NAME;

/**
 * Provides a wrapper around a {@link CloseableHttpAsyncClient} to move the responses to a separate thread for processing.
 */
public class HttpClient implements Closeable {
    private static final Logger logger = LogManager.getLogger(HttpClient.class);

    private final CloseableHttpAsyncClient client;
    private final ThreadPool threadPool;
    private final HttpSettings settings;
    private final ThrottlerManager throttlerManager;

    public static HttpClient create(
        HttpSettings settings,
        ThreadPool threadPool,
        PoolingNHttpClientConnectionManager connectionManager,
        ThrottlerManager throttlerManager
    ) {
        var client = createAsyncClient(Objects.requireNonNull(connectionManager), Objects.requireNonNull(settings));

        return new HttpClient(settings, client, threadPool, throttlerManager);
    }

    private static CloseableHttpAsyncClient createAsyncClient(
        PoolingNHttpClientConnectionManager connectionManager,
        HttpSettings settings
    ) {
        var requestConfig = RequestConfig.custom().setConnectTimeout(settings.connectionTimeout()).build();

        var clientBuilder = HttpAsyncClientBuilder.create().setConnectionManager(connectionManager).setDefaultRequestConfig(requestConfig);
        // The apache client will be shared across all connections because it can be expensive to create it
        // so we don't want to support cookies to avoid accidental authentication for unauthorized users
        clientBuilder.disableCookieManagement();

        /*
          TODO When we implement multi-project we should ensure this is ok. A cluster will be authenticated to EIS because it is one mTLS
          cert per cluster. So I think we're ok to not need to track the connection state per request. We will need to pass a header
          that contains the project id and organization so EIS can determine if the project is authorized or not.

          See https://stackoverflow.com/questions/13034998/httpclient-is-not-re-using-my-connections-keeps-creating-new-ones for a good
            explanation of why we disable connection state.

          The relevant part is copied below:
          SSL connections established by your applications are likely stateful. That is, the server requested the client to
          authenticate with a private certificate, making them security context specific. HttpClient detects that and prevents
          those connections from being leased to a caller with a different security context. Effectively HttpClient is playing safe
          by forcing a new connection for each request rather than risking leasing persistent SSL connection to the wrong user.

          You can do two things here

          - disable connection state tracking
          - make sure all logically related requests share the same context (recommended)
          For details see this section of the HttpClient tutorial:
          https://hc.apache.org/httpcomponents-client-4.5.x/current/tutorial/html/advanced.html#stateful_conn
         */
        clientBuilder.disableConnectionState();

        /*
          By default, if a keep-alive header is not returned by the server then the connection will be kept alive
          indefinitely. In this situation the default keep alive strategy will return -1. Since we use a connection eviction thread,
          connections that are idle past the max idle time will be closed with the eviction thread executes. If that functionality proves
          not to be sufficient we can add a keep-alive strategy to the builder below.

          In my testing, setting a keep-alive didn't actually influence when the connection would be removed from the pool. Setting a low
          keep alive forced later requests that occurred after the duration to recreate the connection. The stale connections would not be
          removed from the pool until the eviction thread closes expired connections.

          My understanding is that a connection marked as ready to be closed because of an elapsed keep-alive time will only be put into
          expiry status when another request is made.

          For more info see the tutorial here under section keep-alive strategy:
          https://hc.apache.org/httpcomponents-client-4.5.x/current/tutorial/html/connmgmt.html

          And this stackoverflow question:
          https://stackoverflow.com/questions/64676200/understanding-the-lifecycle-of-a-connection-managed-by-poolinghttpclientconnecti
         */
        return clientBuilder.build();
    }

    // Default for testing
    HttpClient(HttpSettings settings, CloseableHttpAsyncClient asyncClient, ThreadPool threadPool, ThrottlerManager throttlerManager) {
        this.settings = Objects.requireNonNull(settings);
        this.threadPool = Objects.requireNonNull(threadPool);
        this.client = Objects.requireNonNull(asyncClient);
        this.throttlerManager = Objects.requireNonNull(throttlerManager);
    }

    public void start() {
        client.start();
    }

    public void send(HttpRequest request, HttpClientContext context, ActionListener<HttpResult> listener) throws IOException {
        SocketAccess.doPrivileged(() -> client.execute(request.httpRequestBase(), context, new FutureCallback<>() {
            @Override
            public void completed(HttpResponse response) {
                respondUsingResponseThread(response, request, listener);
            }

            @Override
            public void failed(Exception ex) {
                throttlerManager.warn(logger, format("Request from inference entity id [%s] failed", request.inferenceEntityId()), ex);
                failUsingResponseThread(getException(ex), listener);
            }

            @Override
            public void cancelled() {
                failUsingResponseThread(
                    new CancellationException(format("Request from inference entity id [%s] was cancelled", request.inferenceEntityId())),
                    listener
                );
            }
        }));
    }

    private void respondUsingResponseThread(HttpResponse response, HttpRequest request, ActionListener<HttpResult> listener) {
        threadPool.executor(INFERENCE_RESPONSE_THREAD_POOL_NAME).execute(() -> {
            try {
                listener.onResponse(HttpResult.create(settings.getMaxResponseSize(), response));
            } catch (Exception e) {
                throttlerManager.warn(
                    logger,
                    format("Failed to create http result from inference entity id [%s]", request.inferenceEntityId()),
                    e
                );
                listener.onFailure(e);
            } finally {
                EntityUtils.consumeQuietly(response.getEntity());
            }
        });
    }

    private void failUsingResponseThread(Exception exception, ActionListener<?> listener) {
        threadPool.executor(INFERENCE_RESPONSE_THREAD_POOL_NAME).execute(() -> listener.onFailure(exception));
    }

    private static Exception getException(Exception e) {
        if (e instanceof CancellationException cancellationException) {
            return createNotRunningException(cancellationException);
        }

        return e;
    }

    private static IllegalStateException createNotRunningException(Exception exception) {
        // If the http client isn't running, it is either not started yet, in which case we have a bug somewhere because
        // it should always be started as part of the inference plugin startup, or it is stopped meaning the node is shutting down.
        // If we're shutting down, the user should retry the request, and hopefully it'll hit a node that isn't shutting down.
        return new IllegalStateException("Http client is not running, please retry the request", exception);
    }

    public void stream(HttpRequest request, HttpContext context, ActionListener<StreamingHttpResult> listener) throws IOException {
        var streamingProcessor = new StreamingHttpResultPublisher(threadPool, settings, listener);

        SocketAccess.doPrivileged(() -> client.execute(request.requestProducer(), streamingProcessor, context, new FutureCallback<>() {
            @Override
            public void completed(Void response) {
                streamingProcessor.close();
            }

            @Override
            public void failed(Exception ex) {
                threadPool.executor(INFERENCE_RESPONSE_THREAD_POOL_NAME).execute(() -> streamingProcessor.failed(getException(ex)));
            }

            @Override
            public void cancelled() {
                threadPool.executor(INFERENCE_RESPONSE_THREAD_POOL_NAME)
                    .execute(
                        () -> streamingProcessor.failed(
                            new CancellationException(
                                format("Request from inference entity id [%s] was cancelled", request.inferenceEntityId())
                            )
                        )
                    );
            }
        }));
    }

    @Override
    public void close() throws IOException {
        client.close();
    }
}
