/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.amazonbedrock;

import software.amazon.awssdk.services.bedrockruntime.BedrockRuntimeAsyncClient;
import software.amazon.awssdk.services.bedrockruntime.model.ConverseRequest;
import software.amazon.awssdk.services.bedrockruntime.model.ConverseResponse;
import software.amazon.awssdk.services.bedrockruntime.model.InvokeModelRequest;
import software.amazon.awssdk.services.bedrockruntime.model.InvokeModelResponse;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.inference.services.amazonbedrock.AmazonBedrockModel;

import java.util.concurrent.CompletableFuture;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AmazonBedrockMockInferenceClient extends AmazonBedrockInferenceClient {
    private CompletableFuture<ConverseResponse> converseResponseFuture = CompletableFuture.completedFuture(null);
    private CompletableFuture<InvokeModelResponse> invokeModelResponseFuture = CompletableFuture.completedFuture(null);

    public static AmazonBedrockMockInferenceClient create(AmazonBedrockModel model, @Nullable TimeValue timeout) {
        return new AmazonBedrockMockInferenceClient(model, timeout);
    }

    protected AmazonBedrockMockInferenceClient(AmazonBedrockModel model, @Nullable TimeValue timeout) {
        super(model, timeout, mockThreadPool());
    }

    private static ThreadPool mockThreadPool() {
        ThreadPool threadPool = mock();
        when(threadPool.executor(anyString())).thenReturn(EsExecutors.DIRECT_EXECUTOR_SERVICE);
        return threadPool;
    }

    public void setExceptionToThrow(ElasticsearchException exceptionToThrow) {
        if (exceptionToThrow != null) {
            this.converseResponseFuture = new CompletableFuture<>();
            this.converseResponseFuture.completeExceptionally(exceptionToThrow);
            this.invokeModelResponseFuture = new CompletableFuture<>();
            this.invokeModelResponseFuture.completeExceptionally(exceptionToThrow);
        }
    }

    public void setConverseResponse(ConverseResponse result) {
        this.converseResponseFuture = CompletableFuture.completedFuture(result);
    }

    public void setInvokeModelResponse(InvokeModelResponse result) {
        this.invokeModelResponseFuture = CompletableFuture.completedFuture(result);
    }

    @Override
    protected BedrockRuntimeAsyncClient createAmazonBedrockClient(AmazonBedrockModel model, @Nullable TimeValue timeout) {
        var runtimeClient = mock(BedrockRuntimeAsyncClient.class);
        doAnswer(invocation -> invokeModelResponseFuture).when(runtimeClient).invokeModel(any(InvokeModelRequest.class));
        doAnswer(invocation -> converseResponseFuture).when(runtimeClient).converse(any(ConverseRequest.class));

        return runtimeClient;
    }

    @Override
    void close() {}
}
