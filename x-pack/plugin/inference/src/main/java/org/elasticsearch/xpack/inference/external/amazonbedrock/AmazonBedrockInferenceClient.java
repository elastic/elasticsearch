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
import com.amazonaws.regions.Regions;
import com.amazonaws.services.bedrockruntime.AmazonBedrockRuntime;
import com.amazonaws.services.bedrockruntime.AmazonBedrockRuntimeClientBuilder;
import com.amazonaws.services.bedrockruntime.model.ConverseRequest;
import com.amazonaws.services.bedrockruntime.model.ConverseResult;
import com.amazonaws.services.bedrockruntime.model.InvokeModelRequest;
import com.amazonaws.services.bedrockruntime.model.InvokeModelResult;

import org.elasticsearch.core.AbstractRefCounted;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.xpack.core.common.socket.SocketAccess;
import org.elasticsearch.xpack.inference.services.amazonbedrock.AmazonBedrockModel;
import org.elasticsearch.xpack.inference.services.amazonbedrock.AmazonBedrockSecretSettings;
import org.elasticsearch.xpack.inference.services.amazonbedrock.AmazonBedrockServiceSettings;

import java.io.IOException;
import java.util.Objects;

public final class AmazonBedrockInferenceClient extends AbstractRefCounted implements AmazonBedrockClient, Releasable {

    public static final int CLIENT_CACHE_EXPIRY_MINUTES = 5;
    private static final long CACHE_EXPIRY_ADD_MS = (1000 * 60 * CLIENT_CACHE_EXPIRY_MINUTES);
    private static final int DEFAULT_CLIENT_TIMEOUT_MS = 10000;

    private final Integer modelKeysAndRegionHashcode;
    private final AmazonBedrockRuntime client;
    private volatile long expiryTimestamp;

    public AmazonBedrockInferenceClient(AmazonBedrockModel model) throws IOException {
        this.modelKeysAndRegionHashcode = getModelKeysAndRegionHashcode(model);
        this.client = createAmazonBedrockClient(model);
        setExpiryTimestamp();
    }

    /*
    public AmazonBedrockRuntime getAmazonBedrockClient() {
        return client;
    }
     */
    @Override
    public ConverseResult converse(ConverseRequest converseRequest) {
        return client.converse(converseRequest);
    }

    @Override
    public InvokeModelResult invokeModel(InvokeModelRequest invokeModelRequest) {
        return client.invokeModel(invokeModelRequest);
    }

    public static Integer getModelKeysAndRegionHashcode(AmazonBedrockModel model) {
        var secretSettings = (AmazonBedrockSecretSettings) model.getSecretSettings();
        var serviceSettings = (AmazonBedrockServiceSettings) model.getServiceSettings();
        return Objects.hash(secretSettings.accessKey, secretSettings.secretKey, serviceSettings.region());
    }

    private AmazonBedrockRuntime createAmazonBedrockClient(AmazonBedrockModel model) throws IOException {
        var secretSettings = (AmazonBedrockSecretSettings) model.getSecretSettings();
        var credentials = new BasicAWSCredentials(secretSettings.accessKey.toString(), secretSettings.secretKey.toString());
        var credentialsProvider = new AWSStaticCredentialsProvider(credentials);

        // TODO - allow modification to this
        var clientConfig = new ClientConfiguration().withConnectionTimeout(DEFAULT_CLIENT_TIMEOUT_MS);

        var serviceSettings = (AmazonBedrockServiceSettings) model.getServiceSettings();
        var builder = AmazonBedrockRuntimeClientBuilder.standard()
            .withClientConfiguration(clientConfig)
            .withCredentials(credentialsProvider)
            .withRegion(Regions.fromName(serviceSettings.region()));

        return SocketAccess.doPrivileged(builder::build);
    }

    private void setExpiryTimestamp() {
        synchronized (this) {
            this.expiryTimestamp = System.currentTimeMillis() + CACHE_EXPIRY_ADD_MS;
        }
    }

    public boolean isExpired(long currentTimestampMs) {
        return this.expiryTimestamp < currentTimestampMs;
    }

    public boolean tryToIncreaseReference() {
        setExpiryTimestamp();
        return this.tryIncRef();
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

    @Override
    public void close() {
        decRef();
    }

    @Override
    protected void closeInternal() {
        client.shutdown();
    }

}
