/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.amazonbedrock;

import com.amazonaws.services.bedrockruntime.AmazonBedrockRuntime;
import com.amazonaws.services.bedrockruntime.model.ConverseResult;
import com.amazonaws.services.bedrockruntime.model.InvokeModelResult;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xpack.inference.services.amazonbedrock.AmazonBedrockModel;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AmazonBedrockMockInferenceClient extends AmazonBedrockInferenceClient {
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
    protected AmazonBedrockRuntime createAmazonBedrockClient(AmazonBedrockModel model, @Nullable TimeValue timeout) {
        var runtimeClient = mock(AmazonBedrockRuntime.class);
        when(runtimeClient.converse(any())).thenAnswer(new Answer<ConverseResult>() {
            @Override
            public ConverseResult answer(InvocationOnMock invocationOnMock) throws Throwable {
                return internalConverse();
            }
        });
        when(runtimeClient.invokeModel(any())).thenAnswer(new Answer<InvokeModelResult>() {
            @Override
            public InvokeModelResult answer(InvocationOnMock invocationOnMock) throws Throwable {
                return internalInvokeModel();
            }
        });

        return runtimeClient;
    }

    private ConverseResult internalConverse() throws ElasticsearchException {
        if (exceptionToThrow != null) {
            throw exceptionToThrow;
        }
        return converseResult;
    }

    public InvokeModelResult internalInvokeModel() throws ElasticsearchException {
        if (exceptionToThrow != null) {
            throw exceptionToThrow;
        }
        return invokeModelResult;
    }

    @Override
    protected void closeInternal() {}
}
