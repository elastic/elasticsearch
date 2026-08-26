/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.http;

import org.apache.hc.client5.http.async.methods.SimpleHttpResponse;
import org.apache.hc.client5.http.async.methods.SimpleRequestProducer;
import org.apache.hc.client5.http.async.methods.SimpleResponseConsumer;
import org.apache.hc.client5.http.config.RequestConfig;
import org.apache.hc.client5.http.impl.async.CloseableHttpAsyncClient;
import org.apache.hc.client5.http.impl.async.HttpAsyncClients;
import org.apache.hc.client5.http.impl.nio.PoolingAsyncClientConnectionManager;
import org.apache.hc.client5.http.protocol.HttpClientContext;
import org.apache.hc.core5.concurrent.FutureCallback;
import org.apache.hc.core5.http.HttpResponse;
import org.apache.hc.core5.http.Message;
import org.apache.hc.core5.io.CloseMode;
import org.apache.hc.core5.reactive.ReactiveResponseConsumer;
import org.apache.hc.core5.reactor.IOReactorConfig;
import org.apache.hc.core5.util.Timeout;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.inference.external.request.HttpRequest;
import org.elasticsearch.xpack.inference.logging.ThrottlerManager;
import org.reactivestreams.Publisher;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
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
    private final CircuitBreaker circuitBreaker;

    public static HttpClient create(
        HttpSettings settings,
        ThreadPool threadPool,
        PoolingAsyncClientConnectionManager connectionManager,
        ThrottlerManager throttlerManager,
        CircuitBreaker circuitBreaker
    ) {
        var client = createAsyncClient(Objects.requireNonNull(connectionManager));

        return new HttpClient(settings, client, threadPool, throttlerManager, circuitBreaker);
    }

    private static CloseableHttpAsyncClient createAsyncClient(PoolingAsyncClientConnectionManager connectionManager) {
        var clientBuilder = HttpAsyncClients.custom()
            .setConnectionManager(connectionManager)
            .setIOReactorConfig(IOReactorConfig.custom().setSoKeepAlive(true).build());
        // The apache client will be shared across all connections because it can be expensive to create it
        // so we don't want to support cookies to avoid accidental authentication for unauthorized users
        clientBuilder.disableCookieManagement();

        /*
          RequestExecutorService is designed around requests queuing until the connection pool can lease a connection, matching
          the 4.x client's unbounded lease wait. The 5.x default request config would instead fail a queued request with a
          non-retryable DeadlineTimeoutException after 3 minutes, so the lease timeout is explicitly disabled.
         */
        clientBuilder.setDefaultRequestConfig(RequestConfig.custom().setConnectionRequestTimeout(Timeout.DISABLED).build());

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
         */
        clientBuilder.disableConnectionState();

        /*
          By default, if a keep-alive header is not returned by the server then the connection will be kept alive
          indefinitely. In this situation the default keep alive strategy will return -1. Since we use a connection eviction thread,
          connections that are idle past the max idle time will be closed when the eviction thread executes. If that functionality proves
          not to be sufficient we can add a keep-alive strategy to the builder below.
         */
        return clientBuilder.build();
    }

    // Default for testing
    HttpClient(
        HttpSettings settings,
        CloseableHttpAsyncClient asyncClient,
        ThreadPool threadPool,
        ThrottlerManager throttlerManager,
        CircuitBreaker circuitBreaker
    ) {
        this.settings = Objects.requireNonNull(settings);
        this.threadPool = Objects.requireNonNull(threadPool);
        this.client = Objects.requireNonNull(asyncClient);
        this.throttlerManager = Objects.requireNonNull(throttlerManager);
        this.circuitBreaker = Objects.requireNonNull(circuitBreaker);
    }

    public void start() {
        client.start();
    }

    public void send(HttpRequest request, HttpClientContext context, ActionListener<HttpResult> listener) {
        client.execute(
            SimpleRequestProducer.create(request.httpRequest()),
            SimpleResponseConsumer.create(),
            context,
            new FutureCallback<>() {
                @Override
                public void completed(SimpleHttpResponse response) {
                    respondUsingResponseThread(response, request, listener);
                }

                @Override
                public void failed(Exception ex) {
                    failRequestUsingResponseThread(request, ex, listener);
                }

                @Override
                public void cancelled() {
                    cancelRequestUsingResponseThread(request, listener);
                }
            }
        );
    }

    private void failRequestUsingResponseThread(HttpRequest request, Exception ex, ActionListener<?> listener) {
        throttlerManager.warn(logger, format("Request from inference entity id [%s] failed", request.inferenceEntityId()), ex);
        failUsingResponseThread(getException(ex), listener);
    }

    private void cancelRequestUsingResponseThread(HttpRequest request, ActionListener<?> listener) {
        failUsingResponseThread(
            new CancellationException(format("Request from inference entity id [%s] was cancelled", request.inferenceEntityId())),
            listener
        );
    }

    private void respondUsingResponseThread(SimpleHttpResponse response, HttpRequest request, ActionListener<HttpResult> listener) {
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

    // TODO (httpclient5 migration): verify streaming end-to-end (internalClusterTest, yamlRestTest, and a live SSE smoke test)
    // before merging.
    public void stream(HttpRequest request, HttpClientContext context, ActionListener<StreamingHttpResult> listener) {
        var notifyOnceListener = ActionListener.notifyOnce(listener);

        // The callback fires as soon as the response head arrives; the body is streamed through the message's publisher afterwards,
        // with backpressure and cancellation handled by the reactive consumer at the channel level. The publisher accounts buffered
        // chunks against the inference circuit breaker and aborts the exchange if the stream is abandoned.
        var reactiveConsumer = new ReactiveResponseConsumer(new FutureCallback<>() {
            @Override
            public void completed(Message<HttpResponse, Publisher<ByteBuffer>> message) {
                threadPool.executor(INFERENCE_RESPONSE_THREAD_POOL_NAME)
                    .execute(
                        () -> notifyOnceListener.onResponse(
                            new StreamingHttpResult(
                                message.getHead(),
                                new ByteArrayFlowPublisher(message.getBody(), threadPool, circuitBreaker, request.inferenceEntityId())
                            )
                        )
                    );
            }

            @Override
            public void failed(Exception ex) {
                failRequestUsingResponseThread(request, ex, notifyOnceListener);
            }

            @Override
            public void cancelled() {
                cancelRequestUsingResponseThread(request, notifyOnceListener);
            }
        });

        client.execute(SimpleRequestProducer.create(request.httpRequest()), reactiveConsumer, context, new FutureCallback<>() {
            @Override
            public void completed(Void response) {
                // the body publisher delivers the terminal signal to the subscriber
            }

            @Override
            public void failed(Exception ex) {
                // only reachable before the response head arrived (e.g. connection failures); afterwards the failure is
                // propagated through the body publisher and the notify-once listener drops this call
                failUsingResponseThread(getException(ex), notifyOnceListener);
            }

            @Override
            public void cancelled() {
                cancelRequestUsingResponseThread(request, notifyOnceListener);
            }
        });
    }

    @Override
    public void close() throws IOException {
        client.close(CloseMode.GRACEFUL);
    }
}
