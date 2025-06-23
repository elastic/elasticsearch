/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.amazonbedrock;

import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.inference.external.http.sender.RequestExecutorService;
import org.elasticsearch.xpack.inference.external.http.sender.RequestExecutorServiceSettings;
import org.elasticsearch.xpack.inference.services.amazonbedrock.client.AmazonBedrockExecuteOnlyRequestSender;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

/**
 * Allows this to have a public interface for Amazon Bedrock support
 */
public class AmazonBedrockRequestExecutorService extends RequestExecutorService {

    private final AmazonBedrockExecuteOnlyRequestSender requestSender;

    public AmazonBedrockRequestExecutorService(
        ThreadPool threadPool,
        CountDownLatch startupLatch,
        RequestExecutorServiceSettings settings,
        AmazonBedrockExecuteOnlyRequestSender requestSender
    ) {
        super(threadPool, startupLatch, settings, requestSender);
        this.requestSender = requestSender;
    }

    @Override
    public void shutdown() {
        super.shutdown();
        try {
            requestSender.shutdown();
        } catch (IOException e) {
            // swallow the exception
        }
    }
}
