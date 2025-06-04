/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.amazonbedrock.client;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.inference.ChunkInferenceInput;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.xpack.inference.external.http.retry.ResponseHandler;
import org.elasticsearch.xpack.inference.external.http.sender.ChatCompletionInput;
import org.elasticsearch.xpack.inference.external.http.sender.EmbeddingsInput;
import org.elasticsearch.xpack.inference.external.http.sender.InferenceInputs;
import org.elasticsearch.xpack.inference.external.http.sender.RequestManager;
import org.elasticsearch.xpack.inference.external.http.sender.Sender;
import org.elasticsearch.xpack.inference.external.request.Request;
import org.elasticsearch.xpack.inference.services.ServiceComponents;

import java.io.IOException;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

public class AmazonBedrockMockRequestSender implements Sender {

    public static class Factory extends AmazonBedrockRequestSender.Factory {
        private final Sender sender;

        public Factory(ServiceComponents serviceComponents, ClusterService clusterService) {
            super(serviceComponents, clusterService);
            this.sender = new AmazonBedrockMockRequestSender();
        }

        public Sender createSender() {
            return sender;
        }
    }

    private Queue<Object> results = new ConcurrentLinkedQueue<>();
    private Queue<List<String>> inputs = new ConcurrentLinkedQueue<>();
    private Queue<InputType> inputTypes = new ConcurrentLinkedQueue<>();
    private int sendCounter = 0;

    public void enqueue(Object result) {
        results.add(result);
    }

    public int sendCount() {
        return sendCounter;
    }

    public List<String> getInputs() {
        return inputs.remove();
    }

    public InputType getInputType() {
        return inputTypes.remove();
    }

    @Override
    public void start() {
        // do nothing
    }

    @Override
    public void updateRateLimitDivisor(int rateLimitDivisor) {
        // do nothing
    }

    @Override
    public void send(
        RequestManager requestCreator,
        InferenceInputs inferenceInputs,
        TimeValue timeout,
        ActionListener<InferenceServiceResults> listener
    ) {
        sendCounter++;
        if (inferenceInputs instanceof EmbeddingsInput docsInput) {
            inputs.add(ChunkInferenceInput.inputs(docsInput.getInputs()));
            if (docsInput.getInputType() != null) {
                inputTypes.add(docsInput.getInputType());
            }
        } else if (inferenceInputs instanceof ChatCompletionInput chatCompletionInput) {
            inputs.add(chatCompletionInput.getInputs());
        } else {
            throw new IllegalArgumentException(
                "Invalid inference inputs received in mock sender: " + inferenceInputs.getClass().getSimpleName()
            );
        }

        if (results.isEmpty()) {
            listener.onFailure(new ElasticsearchException("No results found"));
        } else {
            var resultObject = results.remove();
            if (resultObject instanceof InferenceServiceResults inferenceResult) {
                listener.onResponse(inferenceResult);
            } else if (resultObject instanceof Exception e) {
                listener.onFailure(e);
            } else {
                throw new RuntimeException("Unknown result type: " + resultObject.getClass());
            }
        }
    }

    @Override
    public void sendWithoutQueuing(
        Logger logger,
        Request request,
        ResponseHandler responseHandler,
        TimeValue timeout,
        ActionListener<InferenceServiceResults> listener
    ) {
        throw new UnsupportedOperationException("not supported");
    }

    @Override
    public void close() throws IOException {
        // do nothing
    }
}
