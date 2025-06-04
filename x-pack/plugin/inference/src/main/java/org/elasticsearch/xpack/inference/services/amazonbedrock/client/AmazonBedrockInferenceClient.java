/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.amazonbedrock.client;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.profiles.ProfileFile;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.bedrockruntime.BedrockRuntimeAsyncClient;
import software.amazon.awssdk.services.bedrockruntime.model.BedrockRuntimeException;
import software.amazon.awssdk.services.bedrockruntime.model.ConverseRequest;
import software.amazon.awssdk.services.bedrockruntime.model.ConverseResponse;
import software.amazon.awssdk.services.bedrockruntime.model.ConverseStreamRequest;
import software.amazon.awssdk.services.bedrockruntime.model.ConverseStreamResponseHandler;
import software.amazon.awssdk.services.bedrockruntime.model.InvokeModelRequest;
import software.amazon.awssdk.services.bedrockruntime.model.InvokeModelResponse;

import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.exception.ExceptionsHelper;
import org.elasticsearch.SpecialPermission;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.inference.services.amazonbedrock.AmazonBedrockModel;
import org.reactivestreams.FlowAdapters;
import org.slf4j.LoggerFactory;

import java.security.AccessController;
import java.security.PrivilegedExceptionAction;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Flow;

/**
 * Not marking this as "final" so we can subclass it for mocking
 */
public class AmazonBedrockInferenceClient extends AmazonBedrockBaseClient {

    static {
        // we need to load SLF4j on this classloader to pick up the imported SLF4j-2.x
        // otherwise, software.amazon.awssdk:netty-nio-client loads on ExtendedPluginsClassLoader and fails to find the classes
        LoggerFactory.getLogger(AmazonBedrockInferenceClient.class);
    }

    // package-private for testing
    static final int CLIENT_CACHE_EXPIRY_MINUTES = 5;
    private static final Duration DEFAULT_CLIENT_TIMEOUT_MS = Duration.ofMillis(10000);

    private final BedrockRuntimeAsyncClient internalClient;
    private final ThreadPool threadPool;

    public static AmazonBedrockBaseClient create(AmazonBedrockModel model, @Nullable TimeValue timeout, ThreadPool threadPool) {
        try {
            return new AmazonBedrockInferenceClient(model, timeout, threadPool);
        } catch (Exception e) {
            throw new ElasticsearchException("Failed to create Amazon Bedrock Client", e);
        }
    }

    protected AmazonBedrockInferenceClient(AmazonBedrockModel model, @Nullable TimeValue timeout, ThreadPool threadPool) {
        super(model, timeout);
        this.internalClient = createAmazonBedrockClient(model, timeout);
        this.threadPool = Objects.requireNonNull(threadPool);
        setExpiryTimestamp();
    }

    @Override
    public void converse(ConverseRequest converseRequest, ActionListener<ConverseResponse> responseListener) throws ElasticsearchException {
        try {
            var responseFuture = internalClient.converse(converseRequest);
            responseListener.onResponse(responseFuture.get());
        } catch (Exception e) {
            onFailure(responseListener, e, "converse");
        }
    }

    @Override
    public Flow.Publisher<? extends InferenceServiceResults.Result> converseStream(ConverseStreamRequest request)
        throws ElasticsearchException {
        var awsResponseProcessor = new AmazonBedrockStreamingChatProcessor(threadPool);
        internalClient.converseStream(
            request,
            ConverseStreamResponseHandler.builder().subscriber(() -> FlowAdapters.toSubscriber(awsResponseProcessor)).build()
        ).exceptionally(e -> {
            awsResponseProcessor.onError(e);
            return null; // Void
        });
        return awsResponseProcessor;
    }

    private void onFailure(ActionListener<?> listener, Throwable t, String method) {
        ExceptionsHelper.maybeDieOnAnotherThread(t);
        var unwrappedException = t;
        if (t instanceof CompletionException || t instanceof ExecutionException) {
            unwrappedException = t.getCause() != null ? t.getCause() : t;
        }

        if (unwrappedException instanceof BedrockRuntimeException amazonBedrockRuntimeException) {
            listener.onFailure(
                new ElasticsearchException(
                    Strings.format("AmazonBedrock %s failure: [%s]", method, amazonBedrockRuntimeException.getMessage()),
                    amazonBedrockRuntimeException
                )
            );
        } else if (unwrappedException instanceof ElasticsearchException elasticsearchException) {
            listener.onFailure(elasticsearchException);
        } else {
            listener.onFailure(new ElasticsearchException(Strings.format("Amazon Bedrock %s call failed", method), unwrappedException));
        }
    }

    @Override
    public void invokeModel(InvokeModelRequest invokeModelRequest, ActionListener<InvokeModelResponse> responseListener)
        throws ElasticsearchException {
        try {
            var responseFuture = internalClient.invokeModel(invokeModelRequest);
            responseListener.onResponse(responseFuture.get());
        } catch (Exception e) {
            onFailure(responseListener, e, "invoke model");
        }
    }

    // allow this to be overridden for test mocks
    protected BedrockRuntimeAsyncClient createAmazonBedrockClient(AmazonBedrockModel model, @Nullable TimeValue timeout) {
        var secretSettings = model.getSecretSettings();

        var serviceSettings = model.getServiceSettings();

        try {
            SpecialPermission.check();
            return AccessController.doPrivileged((PrivilegedExceptionAction<BedrockRuntimeAsyncClient>) () -> {
                var credentials = AwsBasicCredentials.create(secretSettings.accessKey().toString(), secretSettings.secretKey().toString());
                var credentialsProvider = StaticCredentialsProvider.create(credentials);
                var clientConfig = timeout == null
                    ? NettyNioAsyncHttpClient.builder().connectionTimeout(DEFAULT_CLIENT_TIMEOUT_MS)
                    : NettyNioAsyncHttpClient.builder().connectionTimeout(Duration.ofMillis(timeout.millis()));
                var override = ClientOverrideConfiguration.builder()
                    // disable profileFile, user credentials will always come from the configured Model Secrets
                    .defaultProfileFileSupplier(ProfileFile.aggregator()::build)
                    .defaultProfileFile(ProfileFile.aggregator().build())
                    // each model request retries at most once, limit the impact a request can have on other request's availability
                    .retryPolicy(retryPolicy -> retryPolicy.numRetries(1))
                    .retryStrategy(retryStrategy -> retryStrategy.maxAttempts(1))
                    .build();
                return BedrockRuntimeAsyncClient.builder()
                    .credentialsProvider(credentialsProvider)
                    .region(Region.of(serviceSettings.region()))
                    .httpClientBuilder(clientConfig)
                    .overrideConfiguration(override)
                    .build();
            });
        } catch (BedrockRuntimeException amazonBedrockRuntimeException) {
            throw new ElasticsearchException(
                Strings.format("failed to create AmazonBedrockRuntime client: [%s]", amazonBedrockRuntimeException.getMessage()),
                amazonBedrockRuntimeException
            );
        } catch (Exception e) {
            throw new ElasticsearchException("failed to create AmazonBedrockRuntime client", e);
        }
    }

    private void setExpiryTimestamp() {
        this.expiryTimestamp = clock.instant().plus(Duration.ofMinutes(CLIENT_CACHE_EXPIRY_MINUTES));
    }

    @Override
    public boolean isExpired(Instant currentTimestampMs) {
        Objects.requireNonNull(currentTimestampMs);
        return currentTimestampMs.isAfter(expiryTimestamp);
    }

    public void resetExpiration() {
        setExpiryTimestamp();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AmazonBedrockInferenceClient that = (AmazonBedrockInferenceClient) o;
        return Objects.equals(modelKeysAndRegionHashcode, that.modelKeysAndRegionHashcode);
    }

    @Override
    public int hashCode() {
        return this.modelKeysAndRegionHashcode;
    }

    // make this package-private so only the cache can close it
    @Override
    void close() {
        internalClient.close();
    }
}
