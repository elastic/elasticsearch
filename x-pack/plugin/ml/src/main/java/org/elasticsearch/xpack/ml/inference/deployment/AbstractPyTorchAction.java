/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.deployment;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.ml.MachineLearning;

import java.util.concurrent.atomic.AtomicBoolean;

abstract class AbstractPyTorchAction<T> extends AbstractRunnable {

    private final String modelId;
    private final long requestId;
    private final TimeValue timeout;
    private final Scheduler.Cancellable timeoutHandler;
    private final DeploymentManager.ProcessContext processContext;
    private final AtomicBoolean notified = new AtomicBoolean();

    private final ActionListener<T> listener;

    protected AbstractPyTorchAction(
        String modelId,
        long requestId,
        TimeValue timeout,
        DeploymentManager.ProcessContext processContext,
        ThreadPool threadPool,
        ActionListener<T> listener
    ) {
        this.modelId = modelId;
        this.requestId = requestId;
        this.timeout = timeout;
        this.timeoutHandler = threadPool.schedule(
            this::onTimeout,
            ExceptionsHelper.requireNonNull(timeout, "timeout"),
            MachineLearning.UTILITY_THREAD_POOL_NAME
        );
        this.processContext = processContext;
        this.listener = listener;
    }

    void onTimeout() {
        if (notified.compareAndSet(false, true)) {
            processContext.getTimeoutCount().incrementAndGet();
            processContext.getResultProcessor().ignoreResponseWithoutNotifying(String.valueOf(requestId));
            listener.onFailure(
                new ElasticsearchStatusException("timeout [{}] waiting for inference result", RestStatus.REQUEST_TIMEOUT, timeout)
            );
            return;
        }
        getLogger().debug("[{}] request [{}] received timeout after [{}] but listener already alerted", modelId, requestId, timeout);
    }

    void onSuccess(T result) {
        timeoutHandler.cancel();
        if (notified.compareAndSet(false, true)) {
            listener.onResponse(result);
            return;
        }
        getLogger().debug("[{}] request [{}] received inference response but listener already notified", modelId, requestId);
    }

    @Override
    public void onFailure(Exception e) {
        timeoutHandler.cancel();
        if (notified.compareAndSet(false, true)) {
            processContext.getResultProcessor().ignoreResponseWithoutNotifying(String.valueOf(requestId));
            listener.onFailure(e);
            return;
        }
        getLogger().debug(
            () -> new ParameterizedMessage("[{}] request [{}] received failure but listener already notified", modelId, requestId),
            e
        );
    }

    protected void onFailure(String errorMessage) {
        onFailure(new ElasticsearchStatusException("Error in inference process: [" + errorMessage + "]", RestStatus.INTERNAL_SERVER_ERROR));
    }

    boolean isNotified() {
        return notified.get();
    }

    long getRequestId() {
        return requestId;
    }

    String getModelId() {
        return modelId;
    }

    DeploymentManager.ProcessContext getProcessContext() {
        return processContext;
    }

    protected abstract Logger getLogger();
}
