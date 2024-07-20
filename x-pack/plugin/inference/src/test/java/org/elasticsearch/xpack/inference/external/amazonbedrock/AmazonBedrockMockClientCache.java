/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.amazonbedrock;

import com.amazonaws.services.bedrockruntime.model.ConverseResult;
import com.amazonaws.services.bedrockruntime.model.InvokeModelResult;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xpack.inference.services.amazonbedrock.AmazonBedrockModel;

import java.io.IOException;

public class AmazonBedrockMockClientCache implements AmazonBedrockClientCache {
    private ConverseResult converseResult = null;
    private InvokeModelResult invokeModelResult = null;
    private ElasticsearchException exceptionToThrow = null;

    public AmazonBedrockMockClientCache() {}

    public AmazonBedrockMockClientCache(
        @Nullable ConverseResult converseResult,
        @Nullable InvokeModelResult invokeModelResult,
        @Nullable ElasticsearchException exceptionToThrow
    ) {
        this.converseResult = converseResult;
        this.invokeModelResult = invokeModelResult;
        this.exceptionToThrow = exceptionToThrow;
    }

    @Override
    public AmazonBedrockBaseClient getOrCreateClient(AmazonBedrockModel model, TimeValue timeout) {
        var client = (AmazonBedrockMockInferenceClient) AmazonBedrockMockInferenceClient.create(model, timeout);
        client.setConverseResult(converseResult);
        client.setInvokeModelResult(invokeModelResult);
        client.setExceptionToThrow(exceptionToThrow);
        return client;
    }

    @Override
    public void close() throws IOException {
        // nothing to do
    }

    public void setConverseResult(ConverseResult converseResult) {
        this.converseResult = converseResult;
    }

    public void setInvokeModelResult(InvokeModelResult invokeModelResult) {
        this.invokeModelResult = invokeModelResult;
    }

    public void setExceptionToThrow(ElasticsearchException exceptionToThrow) {
        this.exceptionToThrow = exceptionToThrow;
    }
}
