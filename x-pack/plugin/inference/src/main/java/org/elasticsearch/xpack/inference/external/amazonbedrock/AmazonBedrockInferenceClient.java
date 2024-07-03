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
import com.amazonaws.services.bedrockruntime.AmazonBedrockRuntime;
import com.amazonaws.services.bedrockruntime.AmazonBedrockRuntimeClientBuilder;
import com.amazonaws.services.bedrockruntime.model.AmazonBedrockRuntimeException;
import com.amazonaws.services.bedrockruntime.model.ConverseRequest;
import com.amazonaws.services.bedrockruntime.model.ConverseResult;
import com.amazonaws.services.bedrockruntime.model.InvokeModelRequest;
import com.amazonaws.services.bedrockruntime.model.InvokeModelResult;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.SpecialPermission;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xpack.core.common.socket.SocketAccess;
import org.elasticsearch.xpack.inference.services.amazonbedrock.AmazonBedrockModel;
import org.joda.time.Instant;

import java.security.AccessController;
import java.security.PrivilegedExceptionAction;
import java.util.Objects;

/**
 * Not marking this as "final" so we can subclass it for mocking
 */
public class AmazonBedrockInferenceClient extends AmazonBedrockBaseClient {

    private static final int CLIENT_CACHE_EXPIRY_MINUTES = 5;
    private static final long CACHE_EXPIRY_ADD_MS = (1000 * 60 * CLIENT_CACHE_EXPIRY_MINUTES);
    private static final int DEFAULT_CLIENT_TIMEOUT_MS = 10000;

    private final AmazonBedrockRuntime internalClient;
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
    public ConverseResult converse(ConverseRequest converseRequest) throws ElasticsearchException {
        try {
            return internalClient.converse(converseRequest);
        } catch (AmazonBedrockRuntimeException amazonBedrockRuntimeException) {
            throw new ElasticsearchException(
                Strings.format("failed to create AmazonBedrockRuntime client: [%s]", amazonBedrockRuntimeException.getMessage()),
                amazonBedrockRuntimeException
            );
        } catch (ElasticsearchException elasticsearchException) {
            // just throw the exception if we have one
            throw elasticsearchException;
        } catch (Exception e) {
            throw new ElasticsearchException("Amazon Bedrock client converse call failed", e);
        }
    }

    @Override
    public InvokeModelResult invokeModel(InvokeModelRequest invokeModelRequest) throws ElasticsearchException {
        try {
            return internalClient.invokeModel(invokeModelRequest);
        } catch (AmazonBedrockRuntimeException amazonBedrockRuntimeException) {
            throw new ElasticsearchException(
                Strings.format("failed to create AmazonBedrockRuntime client: [%s]", amazonBedrockRuntimeException.getMessage()),
                amazonBedrockRuntimeException
            );
        } catch (ElasticsearchException elasticsearchException) {
            // just throw the exception if we have one
            throw elasticsearchException;
        } catch (Exception e) {
            throw new ElasticsearchException("Amazon Bedrock client invokeModel call failed", e);
        }
    }

    // allow this to be overridden for test mocks
    protected AmazonBedrockRuntime createAmazonBedrockClient(AmazonBedrockModel model, @Nullable TimeValue timeout) {
        var secretSettings = model.getSecretSettings();
        var credentials = new BasicAWSCredentials(secretSettings.accessKey.toString(), secretSettings.secretKey.toString());
        var credentialsProvider = new AWSStaticCredentialsProvider(credentials);
        var clientConfig = timeout == null
            ? new ClientConfiguration().withConnectionTimeout(DEFAULT_CLIENT_TIMEOUT_MS)
            : new ClientConfiguration().withConnectionTimeout((int) timeout.millis());

        var serviceSettings = model.getServiceSettings();

        try {
            SpecialPermission.check();
            AmazonBedrockRuntimeClientBuilder builder = AccessController.doPrivileged(
                (PrivilegedExceptionAction<AmazonBedrockRuntimeClientBuilder>) () -> AmazonBedrockRuntimeClientBuilder.standard()
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

    private synchronized void setExpiryTimestamp() {
        this.expiryTimestamp = (new Instant()).withDurationAdded(CACHE_EXPIRY_ADD_MS, 1);
    }

    @Override
    public boolean isExpired(Instant currentTimestampMs) {
        Objects.requireNonNull(currentTimestampMs);
        return (this.expiryTimestamp.compareTo(currentTimestampMs) < 0);
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
