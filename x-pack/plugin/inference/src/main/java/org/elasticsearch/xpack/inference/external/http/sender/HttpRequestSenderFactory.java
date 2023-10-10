/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.http.sender;

import org.apache.http.client.methods.HttpUriRequest;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.inference.external.http.HttpClientManager;
import org.elasticsearch.xpack.inference.external.http.HttpResult;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.xpack.inference.InferencePlugin.UTILITY_THREAD_POOL_NAME;

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

    public static final class HttpRequestSender {
        private final ThreadPool threadPool;
        private final HttpClientManager manager;
        private final HttpRequestExecutorService service;
        private final AtomicBoolean started = new AtomicBoolean(false);

        private HttpRequestSender(String serviceName, ThreadPool threadPool, HttpClientManager httpClientManager) {
            this.threadPool = Objects.requireNonNull(threadPool);
            this.manager = Objects.requireNonNull(httpClientManager);
            service = new HttpRequestExecutorService(threadPool.getThreadContext(), serviceName);
        }

        public void start() {
            if (started.compareAndSet(false, true)) {
                manager.start();
                threadPool.executor(UTILITY_THREAD_POOL_NAME).submit(service::start);
            }
        }

        public void send(HttpUriRequest request, ActionListener<HttpResult> listener) {
            assert started.get();
            service.execute(new RequestTask(request, manager.getHttpClient(), listener));
        }
    }
}
