/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.http.sender;

import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.client.methods.HttpUriRequest;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.inference.external.http.HttpClientManager;
import org.elasticsearch.xpack.inference.external.http.HttpResult;

import java.io.Closeable;
import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.xpack.inference.InferencePlugin.UTILITY_THREAD_POOL_NAME;

/**
 * A helper class for constructing a {@link HttpRequestSender}.
 */
public class HttpRequestSenderFactory {
    private final ThreadPool threadPool;
    private final HttpClientManager httpClientManager;

    public HttpRequestSenderFactory(ThreadPool threadPool, HttpClientManager httpClientManager) {
        this.threadPool = Objects.requireNonNull(threadPool);
        this.httpClientManager = Objects.requireNonNull(httpClientManager);
    }

    public HttpRequestSender createSender(String serviceName) {
        return new HttpRequestSender(serviceName, threadPool, httpClientManager);
    }

    /**
     * A class for providing a more friendly interface for sending an {@link HttpUriRequest}. This leverages the queuing logic for sending
     * a request.
     */
    public static final class HttpRequestSender implements Closeable {
        private final ThreadPool threadPool;
        private final HttpClientManager manager;
        private final HttpRequestExecutorService service;
        private final AtomicBoolean started = new AtomicBoolean(false);

        private HttpRequestSender(String serviceName, ThreadPool threadPool, HttpClientManager httpClientManager) {
            this.threadPool = Objects.requireNonNull(threadPool);
            this.manager = Objects.requireNonNull(httpClientManager);
            service = new HttpRequestExecutorService(serviceName, manager.getHttpClient());
        }

        /**
         * Start various internal services. This is required before sending requests.
         */
        public void start() {
            if (started.compareAndSet(false, true)) {
                manager.start();
                threadPool.executor(UTILITY_THREAD_POOL_NAME).execute(service::start);
            }
        }

        @Override
        public void close() throws IOException {
            manager.close();
            service.shutdown();
        }

        public void send(HttpRequestBase request, ActionListener<HttpResult> listener) {
            assert started.get() : "call start() before sending a request";
            service.send(request, listener);
        }
    }
}
