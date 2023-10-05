/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.http;

import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.concurrent.FutureCallback;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.http.impl.nio.conn.PoolingNHttpClientConnectionManager;
import org.apache.http.impl.nio.reactor.DefaultConnectingIOReactor;
import org.apache.http.nio.reactor.ConnectingIOReactor;
import org.apache.http.nio.reactor.IOReactorException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.common.socket.SocketAccess;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.xpack.inference.InferencePlugin.UTILITY_THREAD_POOL_NAME;

public class HttpClient implements Closeable {
    private static final Logger logger = LogManager.getLogger(HttpClient.class);

    enum Status {
        CREATED,
        STARTED,
        STOPPED
    }

    private final CloseableHttpAsyncClient client;
    private final IdleConnectionEvictor connectionEvictor;
    private final AtomicReference<Status> status = new AtomicReference<>(Status.CREATED);
    private final ThreadPool threadPool;
    private final Settings settings;

    public static HttpClient create(Settings settings, ThreadPool threadPool) {
        PoolingNHttpClientConnectionManager connectionManager = createConnectionManager();
        IdleConnectionEvictor connectionEvictor = new IdleConnectionEvictor(
            threadPool,
            connectionManager,
            HttpSettings.CONNECTION_EVICTION_THREAD_SLEEP_TIME_SETTING.get(settings),
            HttpSettings.CONNECTION_EVICTION_MAX_IDLE_TIME_SETTING.get(settings)
        );

        int maxConnections = HttpSettings.MAX_CONNECTIONS.get(settings);
        CloseableHttpAsyncClient client = createAsyncClient(connectionManager, maxConnections);

        return new HttpClient(settings, client, connectionEvictor, threadPool);
    }

    private static PoolingNHttpClientConnectionManager createConnectionManager() {
        ConnectingIOReactor ioReactor;
        try {
            ioReactor = new DefaultConnectingIOReactor();
        } catch (IOReactorException e) {
            var message = "Failed to initialize the inference http client";
            logger.error(message, e);
            throw new ElasticsearchException(message, e);
        }

        return new PoolingNHttpClientConnectionManager(ioReactor);
    }

    private static CloseableHttpAsyncClient createAsyncClient(PoolingNHttpClientConnectionManager connectionManager, int maxConnections) {
        HttpAsyncClientBuilder clientBuilder = HttpAsyncClientBuilder.create();

        clientBuilder.setConnectionManager(connectionManager);
        clientBuilder.setMaxConnPerRoute(maxConnections);
        clientBuilder.setMaxConnTotal(maxConnections);
        // The apache client will be shared across all connections because it can be expensive to create it
        // so we don't want to support cookies to avoid accidental authentication for unauthorized users
        clientBuilder.disableCookieManagement();

        return clientBuilder.build();
    }

    // Default for testing
    HttpClient(Settings settings, CloseableHttpAsyncClient asyncClient, IdleConnectionEvictor evictor, ThreadPool threadPool) {
        this.settings = settings;
        this.threadPool = threadPool;
        this.client = asyncClient;
        this.connectionEvictor = evictor;
    }

    public void send(HttpUriRequest request, ActionListener<HttpResult> listener) throws IOException {
        // TODO do some testing to see if it's better to start in the inference plugin to avoid the comparison here
        start();

        SocketAccess.doPrivileged(() -> client.execute(request, new FutureCallback<>() {
            @Override
            public void completed(HttpResponse response) {
                respondUsingUtilityThread(response, request, listener);
            }

            @Override
            public void failed(Exception ex) {
                logger.error(format("Request [%s] failed", request.getRequestLine()), ex);
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
                listener.onResponse(HttpResult.create(settings, response));
            } catch (Exception e) {
                logger.error(format("Failed to create http result for [%s]", request.getRequestLine()), e);
                listener.onFailure(e);
            }
        });
    }

    private void failUsingUtilityThread(Exception exception, ActionListener<HttpResult> listener) {
        threadPool.executor(UTILITY_THREAD_POOL_NAME).execute(() -> listener.onFailure(exception));
    }

    private void start() {
        if (status.compareAndSet(Status.CREATED, Status.STARTED)) {
            client.start();
            connectionEvictor.start();
        }
    }

    @Override
    public void close() throws IOException {
        client.close();
        connectionEvictor.shutdownNow();
    }
}
