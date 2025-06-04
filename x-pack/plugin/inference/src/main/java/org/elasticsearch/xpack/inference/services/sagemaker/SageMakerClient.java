/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.sagemaker;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.profiles.ProfileFile;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sagemakerruntime.SageMakerRuntimeAsyncClient;
import software.amazon.awssdk.services.sagemakerruntime.model.InvokeEndpointRequest;
import software.amazon.awssdk.services.sagemakerruntime.model.InvokeEndpointResponse;
import software.amazon.awssdk.services.sagemakerruntime.model.InvokeEndpointWithResponseStreamRequest;
import software.amazon.awssdk.services.sagemakerruntime.model.InvokeEndpointWithResponseStreamResponse;
import software.amazon.awssdk.services.sagemakerruntime.model.InvokeEndpointWithResponseStreamResponseHandler;
import software.amazon.awssdk.services.sagemakerruntime.model.ResponseStream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.exception.ElasticsearchStatusException;
import org.elasticsearch.exception.ExceptionsHelper;
import org.elasticsearch.SpecialPermission;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ContextPreservingActionListener;
import org.elasticsearch.action.support.ListenerTimeouts;
import org.elasticsearch.common.cache.Cache;
import org.elasticsearch.common.cache.CacheBuilder;
import org.elasticsearch.common.cache.CacheLoader;
import org.elasticsearch.common.util.concurrent.FutureUtils;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.inference.common.amazon.AwsSecretSettings;
import org.elasticsearch.xpack.inference.external.http.HttpSettings;
import org.reactivestreams.FlowAdapters;

import java.io.Closeable;
import java.security.AccessController;
import java.security.PrivilegedExceptionAction;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.xpack.inference.InferencePlugin.UTILITY_THREAD_POOL_NAME;

public class SageMakerClient implements Closeable {
    private static final Logger log = LogManager.getLogger(SageMakerClient.class);
    private final Cache<RegionAndSecrets, SageMakerRuntimeAsyncClient> existingClients = CacheBuilder.<
        RegionAndSecrets,
        SageMakerRuntimeAsyncClient>builder()
        .removalListener(removal -> removal.getValue().close())
        .setExpireAfterAccess(TimeValue.timeValueMinutes(15))
        .build();

    private final CacheLoader<RegionAndSecrets, SageMakerRuntimeAsyncClient> clientFactory;
    private final ThreadPool threadPool;

    public SageMakerClient(CacheLoader<RegionAndSecrets, SageMakerRuntimeAsyncClient> clientFactory, ThreadPool threadPool) {
        this.clientFactory = clientFactory;
        this.threadPool = threadPool;
    }

    public void invoke(
        RegionAndSecrets regionAndSecrets,
        InvokeEndpointRequest request,
        TimeValue timeout,
        ActionListener<InvokeEndpointResponse> listener
    ) {
        SageMakerRuntimeAsyncClient asyncClient;
        try {
            asyncClient = existingClients.computeIfAbsent(regionAndSecrets, clientFactory);
        } catch (ExecutionException e) {
            listener.onFailure(clientFailure(regionAndSecrets, e));
            return;
        }

        var contextPreservingListener = new ContextPreservingActionListener<>(
            threadPool.getThreadContext().newRestorableContext(false),
            listener
        );

        var awsFuture = asyncClient.invokeEndpoint(request);
        var timeoutListener = ListenerTimeouts.wrapWithTimeout(
            threadPool,
            timeout,
            threadPool.executor(UTILITY_THREAD_POOL_NAME),
            contextPreservingListener,
            ignored -> {
                FutureUtils.cancel(awsFuture);
                contextPreservingListener.onFailure(
                    new ElasticsearchStatusException("Request timed out after [{}]", RestStatus.REQUEST_TIMEOUT, timeout)
                );
            }
        );
        awsFuture.thenAcceptAsync(timeoutListener::onResponse, threadPool.executor(UTILITY_THREAD_POOL_NAME))
            .exceptionallyAsync(t -> failAndMaybeThrowError(t, timeoutListener), threadPool.executor(UTILITY_THREAD_POOL_NAME));
    }

    private static Exception clientFailure(RegionAndSecrets regionAndSecrets, Exception cause) {
        return new ElasticsearchStatusException(
            "failed to create SageMakerRuntime client for region [{}]",
            RestStatus.INTERNAL_SERVER_ERROR,
            cause,
            regionAndSecrets.region()
        );
    }

    private Void failAndMaybeThrowError(Throwable t, ActionListener<?> listener) {
        if (t instanceof CompletionException ce) {
            t = ce.getCause();
        }
        if (t instanceof Exception e) {
            listener.onFailure(e);
        } else {
            ExceptionsHelper.maybeError(t).ifPresent(ExceptionsHelper::maybeDieOnAnotherThread);
            log.atWarn().withThrowable(t).log("Unknown failure calling SageMaker.");
            listener.onFailure(new RuntimeException("Unknown failure calling SageMaker.", t));
        }
        return null; // Void
    }

    public void invokeStream(
        RegionAndSecrets regionAndSecrets,
        InvokeEndpointWithResponseStreamRequest request,
        TimeValue timeout,
        ActionListener<SageMakerStream> listener
    ) {
        SageMakerRuntimeAsyncClient asyncClient;
        try {
            asyncClient = existingClients.computeIfAbsent(regionAndSecrets, clientFactory);
        } catch (ExecutionException e) {
            listener.onFailure(clientFailure(regionAndSecrets, e));
            return;
        }

        var contextPreservingListener = new ContextPreservingActionListener<>(
            threadPool.getThreadContext().newRestorableContext(false),
            listener
        );

        var responseStreamProcessor = new SageMakerStreamingResponseProcessor();
        var cancelAwsRequestListener = new AtomicReference<CompletableFuture<?>>();
        var timeoutListener = ListenerTimeouts.wrapWithTimeout(
            threadPool,
            timeout,
            threadPool.executor(UTILITY_THREAD_POOL_NAME),
            contextPreservingListener,
            ignored -> {
                FutureUtils.cancel(cancelAwsRequestListener.get());
                contextPreservingListener.onFailure(
                    new ElasticsearchStatusException("Request timed out after [{}]", RestStatus.REQUEST_TIMEOUT, timeout)
                );
            }
        );
        // To stay consistent with HTTP providers, we cancel the TimeoutListener onResponse because we are measuring the time it takes to
        // start receiving bytes.
        var responseStreamListener = InvokeEndpointWithResponseStreamResponseHandler.builder()
            .onResponse(response -> timeoutListener.onResponse(new SageMakerStream(response, responseStreamProcessor)))
            .onEventStream(publisher -> responseStreamProcessor.setPublisher(FlowAdapters.toFlowPublisher(publisher)))
            .build();
        var awsFuture = asyncClient.invokeEndpointWithResponseStream(request, responseStreamListener);
        cancelAwsRequestListener.set(awsFuture);
        awsFuture.exceptionallyAsync(t -> failAndMaybeThrowError(t, timeoutListener), threadPool.executor(UTILITY_THREAD_POOL_NAME));
    }

    @Override
    public void close() {
        existingClients.invalidateAll(); // will close each cached client
    }

    public record RegionAndSecrets(String region, AwsSecretSettings secretSettings) {}

    public static class Factory implements CacheLoader<RegionAndSecrets, SageMakerRuntimeAsyncClient> {
        private final HttpSettings httpSettings;

        public Factory(HttpSettings httpSettings) {
            this.httpSettings = httpSettings;
        }

        @Override
        public SageMakerRuntimeAsyncClient load(RegionAndSecrets key) throws Exception {
            SpecialPermission.check();
            // TODO migrate to entitlements
            return AccessController.doPrivileged((PrivilegedExceptionAction<SageMakerRuntimeAsyncClient>) () -> {
                var credentials = AwsBasicCredentials.create(
                    key.secretSettings().accessKey().toString(),
                    key.secretSettings().secretKey().toString()
                );
                var credentialsProvider = StaticCredentialsProvider.create(credentials);
                var clientConfig = NettyNioAsyncHttpClient.builder().connectionTimeout(httpSettings.connectionTimeoutDuration());
                var override = ClientOverrideConfiguration.builder()
                    // disable profileFile, user credentials will always come from the configured Model Secrets
                    .defaultProfileFileSupplier(ProfileFile.aggregator()::build)
                    .defaultProfileFile(ProfileFile.aggregator().build())
                    .retryPolicy(retryPolicy -> retryPolicy.numRetries(3))
                    .retryStrategy(retryStrategy -> retryStrategy.maxAttempts(3))
                    .build();
                return SageMakerRuntimeAsyncClient.builder()
                    .credentialsProvider(credentialsProvider)
                    .region(Region.of(key.region()))
                    .httpClientBuilder(clientConfig)
                    .overrideConfiguration(override)
                    .build();
            });
        }
    }

    private static class SageMakerStreamingResponseProcessor implements Flow.Publisher<ResponseStream> {
        private static final Logger log = LogManager.getLogger(SageMakerStreamingResponseProcessor.class);
        private final AtomicReference<Tuple<Flow.Publisher<ResponseStream>, Flow.Subscriber<? super ResponseStream>>> holder =
            new AtomicReference<>(null);
        private final AtomicBoolean subscribeCalledOnce = new AtomicBoolean(false);

        @Override
        public void subscribe(Flow.Subscriber<? super ResponseStream> subscriber) {
            if (subscribeCalledOnce.compareAndSet(false, true) == false) {
                subscriber.onError(new IllegalStateException("Subscriber already set."));
                return;
            }
            if (holder.compareAndSet(null, Tuple.tuple(null, subscriber)) == false) {
                log.debug("Subscriber connecting to publisher.");
                var publisher = holder.getAndSet(null).v1();
                publisher.subscribe(subscriber);
            } else {
                log.debug("Subscriber waiting for connection.");
            }
        }

        private void setPublisher(Flow.Publisher<ResponseStream> publisher) {
            if (holder.compareAndSet(null, Tuple.tuple(publisher, null)) == false) {
                log.debug("Publisher connecting to subscriber.");
                var subscriber = holder.getAndSet(null).v2();
                publisher.subscribe(subscriber);
            } else {
                log.debug("Publisher waiting for connection.");
            }
        }
    }

    public record SageMakerStream(InvokeEndpointWithResponseStreamResponse response, Flow.Publisher<ResponseStream> responseStream) {}
}
