/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.amazonbedrock.client;

import software.amazon.awssdk.services.bedrockruntime.model.ConverseResponse;
import software.amazon.awssdk.services.bedrockruntime.model.InvokeModelResponse;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xpack.inference.services.amazonbedrock.AmazonBedrockModel;

import java.io.IOException;

public class AmazonBedrockMockClientCache implements AmazonBedrockClientCache {
    private ConverseResponse converseResponse = null;
    private InvokeModelResponse invokeModelResponse = null;
    private ElasticsearchException exceptionToThrow = null;

    public AmazonBedrockMockClientCache() {}

    public AmazonBedrockMockClientCache(
        @Nullable ConverseResponse converseResponse,
        @Nullable InvokeModelResponse invokeModelResponse,
        @Nullable ElasticsearchException exceptionToThrow
    ) {
        this.converseResponse = converseResponse;
        this.invokeModelResponse = invokeModelResponse;
        this.exceptionToThrow = exceptionToThrow;
    }

    @Override
    public AmazonBedrockBaseClient getOrCreateClient(AmazonBedrockModel model, TimeValue timeout) {
        var client = AmazonBedrockMockInferenceClient.create(model, timeout);
        client.setConverseResponse(converseResponse);
        client.setInvokeModelResponse(invokeModelResponse);
        client.setExceptionToThrow(exceptionToThrow);
        return client;
    }

    @Override
    public void close() throws IOException {
        // nothing to do
    }

    public void setConverseResponse(ConverseResponse converseResponse) {
        this.converseResponse = converseResponse;
    }

    public void setInvokeModelResponse(InvokeModelResponse invokeModelResponse) {
        this.invokeModelResponse = invokeModelResponse;
    }

    public void setExceptionToThrow(ElasticsearchException exceptionToThrow) {
        this.exceptionToThrow = exceptionToThrow;
    }
}
