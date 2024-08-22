/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.amazonbedrock;

import com.amazonaws.services.bedrockruntime.AmazonBedrockRuntimeAsync;
import com.amazonaws.services.bedrockruntime.model.ConverseResult;
import com.amazonaws.services.bedrockruntime.model.InvokeModelResult;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xpack.inference.services.amazonbedrock.AmazonBedrockModel;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

public class AmazonBedrockMockInferenceClient extends AmazonBedrockInferenceClient {
    private ConverseResult converseResult = null;
    private InvokeModelResult invokeModelResult = null;
    private ElasticsearchException exceptionToThrow = null;

    private Future<ConverseResult> converseResultFuture = new MockConverseResultFuture();
    private Future<InvokeModelResult> invokeModelResultFuture = new MockInvokeResultFuture();

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
    protected AmazonBedrockRuntimeAsync createAmazonBedrockClient(AmazonBedrockModel model, @Nullable TimeValue timeout) {
        var runtimeClient = mock(AmazonBedrockRuntimeAsync.class);
        doAnswer(invocation -> invokeModelResultFuture).when(runtimeClient).invokeModelAsync(any());
        doAnswer(invocation -> converseResultFuture).when(runtimeClient).converseAsync(any());

        return runtimeClient;
    }

    @Override
    void close() {}

    private class MockConverseResultFuture implements Future<ConverseResult> {
        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            return false;
        }

        @Override
        public boolean isCancelled() {
            return false;
        }

        @Override
        public boolean isDone() {
            return false;
        }

        @Override
        public ConverseResult get() throws InterruptedException, ExecutionException {
            if (exceptionToThrow != null) {
                throw exceptionToThrow;
            }
            return converseResult;
        }

        @Override
        public ConverseResult get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
            if (exceptionToThrow != null) {
                throw exceptionToThrow;
            }
            return converseResult;
        }
    }

    private class MockInvokeResultFuture implements Future<InvokeModelResult> {
        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            return false;
        }

        @Override
        public boolean isCancelled() {
            return false;
        }

        @Override
        public boolean isDone() {
            return false;
        }

        @Override
        public InvokeModelResult get() throws InterruptedException, ExecutionException {
            if (exceptionToThrow != null) {
                throw exceptionToThrow;
            }
            return invokeModelResult;
        }

        @Override
        public InvokeModelResult get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
            if (exceptionToThrow != null) {
                throw exceptionToThrow;
            }
            return invokeModelResult;
        }
    }
}
