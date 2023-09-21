/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.http;

import org.apache.http.Header;
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
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.core.Streams;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.common.socket.SocketAccess;
import org.elasticsearch.xpack.inference.common.SizeLimitInputStream;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
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

    private final ByteSizeValue maxResponseSize;
    private final int maxConnections;
    private final CloseableHttpAsyncClient client;
    private final IdleConnectionEvictor connectionEvictor;
    private final AtomicReference<Status> status = new AtomicReference<>(Status.CREATED);
    private final ThreadPool threadPool;
    private final Settings settings;

    public HttpClient(Settings settings, ThreadPool threadPool) {
        this.settings = settings;
        this.threadPool = threadPool;
        this.maxResponseSize = HttpSettings.MAX_HTTP_RESPONSE_SIZE.get(settings);
        this.maxConnections = HttpSettings.MAX_CONNECTIONS.get(settings);

        PoolingNHttpClientConnectionManager connectionManager = createConnectionManager();
        this.connectionEvictor = new IdleConnectionEvictor(
            threadPool,
            connectionManager,
            HttpSettings.CONNECTION_EVICTION_THREAD_SLEEP_TIME_SETTING.get(settings),
            HttpSettings.CONNECTION_EVICTION_MAX_IDLE_TIME_SETTING.get(settings)
        );

        this.client = createAsyncClient(connectionManager);
    }

    private PoolingNHttpClientConnectionManager createConnectionManager() {
        ConnectingIOReactor ioReactor;
        try {
            ioReactor = new DefaultConnectingIOReactor();
        } catch (IOReactorException e) {
            logger.error("Failed to initialize the inference http client", e);
            throw new ElasticsearchException("Failed to initialize the inference http client", e);
        }

        return new PoolingNHttpClientConnectionManager(ioReactor);
    }

    private CloseableHttpAsyncClient createAsyncClient(PoolingNHttpClientConnectionManager connectionManager) {
        HttpAsyncClientBuilder clientBuilder = HttpAsyncClientBuilder.create();

        clientBuilder.setConnectionManager(connectionManager);
        clientBuilder.setMaxConnPerRoute(maxConnections);
        clientBuilder.setMaxConnTotal(maxConnections);
        // The apache client will be shared across all connections because it can be expensive to create it
        // so we don't want to support cookies to avoid accidental authentication for unauthorized users
        clientBuilder.disableCookieManagement();

        return clientBuilder.build();
    }

    public void send(HttpUriRequest request, ActionListener<HttpResult> listener) throws IOException {
        // TODO do some testing to see if it's better to start in the inference plugin to avoid the comparison here
        start();

        SocketAccess.doPrivileged(() -> client.execute(request, new FutureCallback<>() {
            @Override
            public void completed(HttpResponse response) {
                handleResponse(response, listener);
            }

            @Override
            public void failed(Exception ex) {
                listener.onFailure(ex);
            }

            @Override
            public void cancelled() {
                listener.onFailure(new CancellationException(format("Request [%s] was cancelled", request.getRequestLine())));
            }
        }));
    }

    private void handleResponse(HttpResponse response, ActionListener<HttpResult> listener) {
        threadPool.executor(UTILITY_THREAD_POOL_NAME).execute(() -> {
            try {
                listener.onResponse(HttpResult.create(settings, response));
            } catch (Exception e) {
                listener.onFailure(e);
            }
        });
    }

    private void start() {
        if (status.compareAndSet(Status.CREATED, Status.STARTED)) {
            client.start();
            connectionEvictor.start();
        }
    }

    private byte[] body(HttpResponse response) throws IOException {
        if (response.getEntity() == null) {
            return new byte[0];
        }

        final byte[] body;
        try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
            try (InputStream is = new SizeLimitInputStream(maxResponseSize, response.getEntity().getContent())) {
                Streams.copy(is, outputStream);
            }
            body = outputStream.toByteArray();
        }

        return body;
    }

    private Map<String, List<String>> headers(HttpResponse response) {
        Header[] headers = response.getAllHeaders();
        Map<String, List<String>> responseHeaders = Maps.newMapWithExpectedSize(headers.length);

        for (Header header : headers) {
            if (responseHeaders.containsKey(header.getName())) {
                List<String> headerValues = responseHeaders.get(header.getName());
                headerValues.add(header.getValue());
            } else {
                ArrayList<String> values = new ArrayList<>();
                values.add(header.getValue());
                responseHeaders.put(header.getName(), values);
            }
        }

        return responseHeaders;
    }

    @Override
    public void close() throws IOException {
        client.close();
        connectionEvictor.shutdown();

        try {
            connectionEvictor.awaitTermination(3, TimeUnit.SECONDS);
        } catch (TimeoutException e) {
            connectionEvictor.shutdownNow();
        }
    }
}
