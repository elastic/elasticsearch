/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.amazonbedrock;

import com.amazonaws.services.bedrockruntime.model.ConverseRequest;
import com.amazonaws.services.bedrockruntime.model.ConverseResult;
import com.amazonaws.services.bedrockruntime.model.InvokeModelRequest;
import com.amazonaws.services.bedrockruntime.model.InvokeModelResult;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xpack.inference.services.amazonbedrock.AmazonBedrockModel;

import java.util.Objects;

public class AmazonBedrockMockInferenceClient extends AmazonBedrockBaseClient {
    private ConverseResult converseResult = null;
    private InvokeModelResult invokeModelResult = null;
    private ElasticsearchException exceptionToThrow = null;

    public static AmazonBedrockBaseClient create(AmazonBedrockModel model, @Nullable TimeValue timeout) {
        return new AmazonBedrockMockInferenceClient(model, timeout);
    }

    protected AmazonBedrockMockInferenceClient(AmazonBedrockModel model, @Nullable TimeValue timeout) {
        super(model, timeout);
    }

    public void setExceptionToThrow(ElasticsearchException exceptionToThrow) {
        this.exceptionToThrow = exceptionToThrow;
    }

    public void setConverseResult(ConverseResult result) {
        this.converseResult = result;
    }

    public void setInvokeModelResult(InvokeModelResult result) {
        this.invokeModelResult = result;
    }

    @Override
    protected void closeInternal() {}

    @Override
    public ConverseResult converse(ConverseRequest converseRequest) throws ElasticsearchException {
        if (exceptionToThrow != null) {
            throw exceptionToThrow;
        }
        return converseResult;
    }

    @Override
    public InvokeModelResult invokeModel(InvokeModelRequest invokeModelRequest) throws ElasticsearchException {
        if (exceptionToThrow != null) {
            throw exceptionToThrow;
        }
        return invokeModelResult;
    }

    @Override
    public boolean isExpired(long currentTimestampMs) {
        return false;
    }

    @Override
    public boolean tryToIncreaseReference() {
        return true;
    }

    @Override
    public void close() {}

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AmazonBedrockMockInferenceClient that = (AmazonBedrockMockInferenceClient) o;
        return Objects.equals(modelKeysAndRegionHashcode, that.modelKeysAndRegionHashcode);
    }

    @Override
    public int hashCode() {
        return modelKeysAndRegionHashcode;
    }
}
