/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.amazonbedrock;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.xpack.inference.external.http.sender.DocumentsOnlyInput;
import org.elasticsearch.xpack.inference.external.http.sender.InferenceInputs;
import org.elasticsearch.xpack.inference.external.http.sender.RequestManager;
import org.elasticsearch.xpack.inference.external.http.sender.Sender;
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

    @Override
    public void start() {
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
        var docsInput = (DocumentsOnlyInput) inferenceInputs;
        inputs.add(docsInput.getInputs());

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
    public void close() throws IOException {
        // do nothing
    }
}
