/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.amazonbedrock;

import com.amazonaws.services.bedrockruntime.model.ConverseResult;
import com.amazonaws.services.bedrockruntime.model.InvokeModelResult;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.xpack.inference.external.request.amazonbedrock.AmazonBedrockRequest;
import org.elasticsearch.xpack.inference.external.response.amazonbedrock.AmazonBedrockResponseHandler;
import org.elasticsearch.xpack.inference.logging.ThrottlerManager;

import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Supplier;

public class AmazonBedrockMockExecuteRequestSender extends AmazonBedrockExecuteOnlyRequestSender {

    private Queue<Object> results = new ConcurrentLinkedQueue<>();
    private Queue<List<String>> inputs = new ConcurrentLinkedQueue<>();
    private int sendCounter = 0;

    public AmazonBedrockMockExecuteRequestSender(AmazonBedrockClientCache clientCache, ThrottlerManager throttlerManager) {
        super(clientCache, throttlerManager);
    }

    public void enqueue(Object result) {
        results.add(result);
    }

    public int sendCount() {
        return sendCounter;
    }

    public List<String> getInputs() {
        return inputs.remove();
    }

    @Override
    protected AmazonBedrockExecutor createExecutor(
        AmazonBedrockRequest awsRequest,
        AmazonBedrockResponseHandler awsResponse,
        Logger logger,
        Supplier<Boolean> hasRequestTimedOutFunction,
        ActionListener<InferenceServiceResults> listener
    ) {
        setCacheResult();
        return super.createExecutor(awsRequest, awsResponse, logger, hasRequestTimedOutFunction, listener);
    }

    private void setCacheResult() {
        var mockCache = (AmazonBedrockMockClientCache) this.clientCache;
        var result = results.remove();
        if (result instanceof ConverseResult converseResult) {
            mockCache.setConverseResult(converseResult);
            return;
        }

        if (result instanceof InvokeModelResult invokeModelResult) {
            mockCache.setInvokeModelResult(invokeModelResult);
            return;
        }

        if (result instanceof ElasticsearchException exception) {
            mockCache.setExceptionToThrow(exception);
            return;
        }

        throw new RuntimeException("Unknown result type: " + result.getClass());
    }
}
