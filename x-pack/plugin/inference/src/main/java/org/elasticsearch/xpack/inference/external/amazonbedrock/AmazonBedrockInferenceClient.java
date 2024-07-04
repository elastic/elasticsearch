/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.amazonbedrock;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.bedrockruntime.AmazonBedrockRuntimeAsync;
import com.amazonaws.services.bedrockruntime.AmazonBedrockRuntimeAsyncClientBuilder;
import com.amazonaws.services.bedrockruntime.model.AmazonBedrockRuntimeException;
import com.amazonaws.services.bedrockruntime.model.ConverseRequest;
import com.amazonaws.services.bedrockruntime.model.ConverseResult;
import com.amazonaws.services.bedrockruntime.model.InvokeModelRequest;
import com.amazonaws.services.bedrockruntime.model.InvokeModelResult;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.SpecialPermission;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xpack.core.common.socket.SocketAccess;
import org.elasticsearch.xpack.inference.services.amazonbedrock.AmazonBedrockModel;

import java.security.AccessController;
import java.security.PrivilegedExceptionAction;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;

/**
 * Not marking this as "final" so we can subclass it for mocking
 */
public class AmazonBedrockInferenceClient extends AmazonBedrockBaseClient {

    // package-private for testing
    static final int CLIENT_CACHE_EXPIRY_MINUTES = 5;
    private static final int DEFAULT_CLIENT_TIMEOUT_MS = 10000;

    private final AmazonBedrockRuntimeAsync internalClient;
    private volatile Instant expiryTimestamp;

    public static AmazonBedrockBaseClient create(AmazonBedrockModel model, @Nullable TimeValue timeout) {
        try {
            return new AmazonBedrockInferenceClient(model, timeout);
        } catch (Exception e) {
            throw new ElasticsearchException("Failed to create Amazon Bedrock Client", e);
        }
    }

    protected AmazonBedrockInferenceClient(AmazonBedrockModel model, @Nullable TimeValue timeout) {
        super(model, timeout);
        this.internalClient = createAmazonBedrockClient(model, timeout);
        setExpiryTimestamp();
    }

    @Override
    public void converse(ConverseRequest converseRequest, ActionListener<ConverseResult> responseListener) throws ElasticsearchException {
        try {
            var responseFuture = internalClient.converseAsync(converseRequest);
            responseListener.onResponse(responseFuture.get());
        } catch (AmazonBedrockRuntimeException amazonBedrockRuntimeException) {
            responseListener.onFailure(
                new ElasticsearchException(
                    Strings.format("AmazonBedrock converse failure: [%s]", amazonBedrockRuntimeException.getMessage()),
                    amazonBedrockRuntimeException
                )
            );
        } catch (ElasticsearchException elasticsearchException) {
            // just throw the exception if we have one
            responseListener.onFailure(elasticsearchException);
        } catch (Exception e) {
            responseListener.onFailure(new ElasticsearchException("Amazon Bedrock client converse call failed", e));
        }
    }

    @Override
    public void invokeModel(InvokeModelRequest invokeModelRequest, ActionListener<InvokeModelResult> responseListener)
        throws ElasticsearchException {
        try {
            var responseFuture = internalClient.invokeModelAsync(invokeModelRequest);
            responseListener.onResponse(responseFuture.get());
        } catch (AmazonBedrockRuntimeException amazonBedrockRuntimeException) {
            responseListener.onFailure(
                new ElasticsearchException(
                    Strings.format("AmazonBedrock invoke model failure: [%s]", amazonBedrockRuntimeException.getMessage()),
                    amazonBedrockRuntimeException
                )
            );
        } catch (ElasticsearchException elasticsearchException) {
            // just throw the exception if we have one
            responseListener.onFailure(elasticsearchException);
        } catch (Exception e) {
            responseListener.onFailure(new ElasticsearchException(e));
        }
    }

    // allow this to be overridden for test mocks
    protected AmazonBedrockRuntimeAsync createAmazonBedrockClient(AmazonBedrockModel model, @Nullable TimeValue timeout) {
        var secretSettings = model.getSecretSettings();
        var credentials = new BasicAWSCredentials(secretSettings.accessKey.toString(), secretSettings.secretKey.toString());
        var credentialsProvider = new AWSStaticCredentialsProvider(credentials);
        var clientConfig = timeout == null
            ? new ClientConfiguration().withConnectionTimeout(DEFAULT_CLIENT_TIMEOUT_MS)
            : new ClientConfiguration().withConnectionTimeout((int) timeout.millis());

        var serviceSettings = model.getServiceSettings();

        try {
            SpecialPermission.check();
            AmazonBedrockRuntimeAsyncClientBuilder builder = AccessController.doPrivileged(
                (PrivilegedExceptionAction<AmazonBedrockRuntimeAsyncClientBuilder>) () -> AmazonBedrockRuntimeAsyncClientBuilder.standard()
                    .withCredentials(credentialsProvider)
                    .withRegion(serviceSettings.region())
                    .withClientConfiguration(clientConfig)
            );

            return SocketAccess.doPrivileged(builder::build);
        } catch (AmazonBedrockRuntimeException amazonBedrockRuntimeException) {
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
        internalClient.shutdown();
    }
}
