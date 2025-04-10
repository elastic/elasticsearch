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
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.xpack.inference.InferencePlugin.UTILITY_THREAD_POOL_NAME;

/**
 * Provides a wrapper around a {@link CloseableHttpAsyncClient} to move the responses to a separate thread for processing.
 */
public class HttpClient implements Closeable {
    private static final Logger logger = LogManager.getLogger(HttpClient.class);

    enum Status {
        CREATED,
        STARTED,
        STOPPED
    }

    private final CloseableHttpAsyncClient client;
    private final AtomicReference<Status> status = new AtomicReference<>(Status.CREATED);
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
        if (status.compareAndSet(Status.CREATED, Status.STARTED)) {
            client.start();
        }
    }

    public void send(HttpRequest request, HttpClientContext context, ActionListener<HttpResult> listener) throws IOException {
        // The caller must call start() first before attempting to send a request
        assert status.get() == Status.STARTED : "call start() before attempting to send a request";

        SocketAccess.doPrivileged(() -> client.execute(request.httpRequestBase(), context, new FutureCallback<>() {
            @Override
            public void completed(HttpResponse response) {
                respondUsingUtilityThread(response, request, listener);
            }

            @Override
            public void failed(Exception ex) {
                throttlerManager.warn(logger, format("Request from inference entity id [%s] failed", request.inferenceEntityId()), ex);
                failUsingUtilityThread(ex, listener);
            }

            @Override
            public void cancelled() {
                failUsingUtilityThread(
                    new CancellationException(format("Request from inference entity id [%s] was cancelled", request.inferenceEntityId())),
                    listener
                );
            }
        }));
    }

    private void respondUsingUtilityThread(HttpResponse response, HttpRequest request, ActionListener<HttpResult> listener) {
        threadPool.executor(UTILITY_THREAD_POOL_NAME).execute(() -> {
            try {
                listener.onResponse(HttpResult.create(settings.getMaxResponseSize(), response));
            } catch (Exception e) {
                throttlerManager.warn(
                    logger,
                    format("Failed to create http result from inference entity id [%s]", request.inferenceEntityId()),
                    e
                );
                listener.onFailure(e);
            }
        });
    }

    private void failUsingUtilityThread(Exception exception, ActionListener<?> listener) {
        threadPool.executor(UTILITY_THREAD_POOL_NAME).execute(() -> listener.onFailure(exception));
    }

    public void stream(HttpRequest request, HttpContext context, ActionListener<StreamingHttpResult> listener) throws IOException {
        // The caller must call start() first before attempting to send a request
        assert status.get() == Status.STARTED : "call start() before attempting to send a request";

        var streamingProcessor = new StreamingHttpResultPublisher(threadPool, settings, listener);

        SocketAccess.doPrivileged(() -> client.execute(request.requestProducer(), streamingProcessor, context, new FutureCallback<>() {
            @Override
            public void completed(Void response) {
                streamingProcessor.close();
            }

            @Override
            public void failed(Exception ex) {
                threadPool.executor(UTILITY_THREAD_POOL_NAME).execute(() -> streamingProcessor.failed(ex));
            }

            @Override
            public void cancelled() {
                threadPool.executor(UTILITY_THREAD_POOL_NAME)
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
        status.set(Status.STOPPED);
        client.close();
    }
}
