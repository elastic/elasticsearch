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
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.inference.external.http.retry.ResponseHandler;
import org.elasticsearch.xpack.inference.external.http.sender.InferenceInputs;
import org.elasticsearch.xpack.inference.external.http.sender.RequestExecutorServiceSettings;
import org.elasticsearch.xpack.inference.external.http.sender.RequestManager;
import org.elasticsearch.xpack.inference.external.http.sender.Sender;
import org.elasticsearch.xpack.inference.external.request.Request;
import org.elasticsearch.xpack.inference.services.ServiceComponents;
import org.elasticsearch.xpack.inference.services.amazonbedrock.AmazonBedrockRequestExecutorService;
import org.elasticsearch.xpack.inference.services.amazonbedrock.AmazonBedrockRequestManager;

import java.io.IOException;
import java.time.Clock;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.xpack.inference.InferencePlugin.UTILITY_THREAD_POOL_NAME;

public class AmazonBedrockRequestSender implements Sender {

    public static class Factory {
        private final ServiceComponents serviceComponents;
        private final ClusterService clusterService;

        public Factory(ServiceComponents serviceComponents, ClusterService clusterService) {
            this.serviceComponents = Objects.requireNonNull(serviceComponents);
            this.clusterService = Objects.requireNonNull(clusterService);
        }

        public Sender createSender() {
            var clientCache = new AmazonBedrockInferenceClientCache(
                (model, timeout) -> AmazonBedrockInferenceClient.create(model, timeout, serviceComponents.threadPool()),
                Clock.systemUTC()
            );
            return createSender(new AmazonBedrockExecuteOnlyRequestSender(clientCache, serviceComponents.throttlerManager()));
        }

        Sender createSender(AmazonBedrockExecuteOnlyRequestSender requestSender) {
            var sender = new AmazonBedrockRequestSender(
                serviceComponents.threadPool(),
                clusterService,
                serviceComponents.settings(),
                Objects.requireNonNull(requestSender)
            );
            // ensure this is started
            sender.start();
            return sender;
        }
    }

    private static final TimeValue START_COMPLETED_WAIT_TIME = TimeValue.timeValueSeconds(5);

    private final ThreadPool threadPool;
    private final AmazonBedrockRequestExecutorService executorService;
    private final AtomicBoolean started = new AtomicBoolean(false);
    private final CountDownLatch startCompleted = new CountDownLatch(1);

    protected AmazonBedrockRequestSender(
        ThreadPool threadPool,
        ClusterService clusterService,
        Settings settings,
        AmazonBedrockExecuteOnlyRequestSender requestSender
    ) {
        this.threadPool = Objects.requireNonNull(threadPool);
        executorService = new AmazonBedrockRequestExecutorService(
            threadPool,
            startCompleted,
            new RequestExecutorServiceSettings(settings, clusterService),
            requestSender
        );
    }

    @Override
    public void updateRateLimitDivisor(int rateLimitDivisor) {
        executorService.updateRateLimitDivisor(rateLimitDivisor);
    }

    @Override
    public void start() {
        if (started.compareAndSet(false, true)) {
            // The manager must be started before the executor service. That way we guarantee that the http client
            // is ready prior to the service attempting to use the http client to send a request
            threadPool.executor(UTILITY_THREAD_POOL_NAME).execute(executorService::start);
            waitForStartToComplete();
        }
    }

    private void waitForStartToComplete() {
        try {
            if (startCompleted.await(START_COMPLETED_WAIT_TIME.getSeconds(), TimeUnit.SECONDS) == false) {
                throw new IllegalStateException("Amazon Bedrock sender startup did not complete in time");
            }
        } catch (InterruptedException e) {
            throw new IllegalStateException("Amazon Bedrock sender interrupted while waiting for startup to complete");
        }
    }

    @Override
    public void send(
        RequestManager requestCreator,
        InferenceInputs inferenceInputs,
        TimeValue timeout,
        ActionListener<InferenceServiceResults> listener
    ) {
        assert started.get() : "Amazon Bedrock request sender: call start() before sending a request";
        waitForStartToComplete();

        if (requestCreator instanceof AmazonBedrockRequestManager amazonBedrockRequestManager) {
            executorService.execute(amazonBedrockRequestManager, inferenceInputs, timeout, listener);
            return;
        }

        listener.onFailure(new ElasticsearchException("Amazon Bedrock request sender did not receive a valid request request manager"));
    }

    @Override
    public void sendWithoutQueuing(
        Logger logger,
        Request request,
        ResponseHandler responseHandler,
        TimeValue timeout,
        ActionListener<InferenceServiceResults> listener
    ) {
        throw new UnsupportedOperationException("not implemented");
    }

    @Override
    public void close() throws IOException {
        executorService.shutdown();
    }
}
