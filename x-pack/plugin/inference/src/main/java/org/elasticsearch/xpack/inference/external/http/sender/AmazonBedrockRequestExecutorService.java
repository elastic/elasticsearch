/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.http.sender;

import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.inference.external.amazonbedrock.AmazonBedrockExecuteOnlyRequestSender;
import org.elasticsearch.xpack.inference.external.http.retry.RequestSender;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

/**
 * Allows this to have a public interface for Amazon Bedrock support
 */
public class AmazonBedrockRequestExecutorService extends RequestExecutorService {

    private final RequestSender localRequestSender;

    public AmazonBedrockRequestExecutorService(
        ThreadPool threadPool,
        CountDownLatch startupLatch,
        RequestExecutorServiceSettings settings,
        RequestSender requestSender
    ) {
        super(threadPool, startupLatch, settings, requestSender);
        this.localRequestSender = requestSender;
    }

    @Override
    public void shutdown() {
        super.shutdown();
        if (localRequestSender instanceof AmazonBedrockExecuteOnlyRequestSender amazonExecuteSender) {
            try {
                amazonExecuteSender.shutdown();
            } catch (IOException e) {
                // swallow the exception
            }
        }
    }
}
