/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.http;

import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.concurrent.FutureCallback;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.http.impl.nio.conn.PoolingNHttpClientConnectionManager;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.common.socket.SocketAccess;
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
        CloseableHttpAsyncClient client = createAsyncClient(Objects.requireNonNull(connectionManager));

        return new HttpClient(settings, client, threadPool, throttlerManager);
    }

    private static CloseableHttpAsyncClient createAsyncClient(PoolingNHttpClientConnectionManager connectionManager) {
        HttpAsyncClientBuilder clientBuilder = HttpAsyncClientBuilder.create();
        clientBuilder.setConnectionManager(connectionManager);
        // The apache client will be shared across all connections because it can be expensive to create it
        // so we don't want to support cookies to avoid accidental authentication for unauthorized users
        clientBuilder.disableCookieManagement();

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

    public void send(HttpUriRequest request, HttpClientContext context, ActionListener<HttpResult> listener) throws IOException {
        // The caller must call start() first before attempting to send a request
        assert status.get() == Status.STARTED : "call start() before attempting to send a request";

        SocketAccess.doPrivileged(() -> client.execute(request, context, new FutureCallback<>() {
            @Override
            public void completed(HttpResponse response) {
                respondUsingUtilityThread(response, request, listener);
            }

            @Override
            public void failed(Exception ex) {
                throttlerManager.warn(logger, format("Request [%s] failed", request.getRequestLine()), ex);
                failUsingUtilityThread(ex, listener);
            }

            @Override
            public void cancelled() {
                failUsingUtilityThread(new CancellationException(format("Request [%s] was cancelled", request.getRequestLine())), listener);
            }
        }));
    }

    private void respondUsingUtilityThread(HttpResponse response, HttpUriRequest request, ActionListener<HttpResult> listener) {
        threadPool.executor(UTILITY_THREAD_POOL_NAME).execute(() -> {
            try {
                listener.onResponse(HttpResult.create(settings.getMaxResponseSize(), response));
            } catch (Exception e) {
                throttlerManager.warn(logger, format("Failed to create http result for [%s]", request.getRequestLine()), e);
                listener.onFailure(e);
            }
        });
    }

    private void failUsingUtilityThread(Exception exception, ActionListener<HttpResult> listener) {
        threadPool.executor(UTILITY_THREAD_POOL_NAME).execute(() -> listener.onFailure(exception));
    }

    @Override
    public void close() throws IOException {
        status.set(Status.STOPPED);
        client.close();
    }
}
