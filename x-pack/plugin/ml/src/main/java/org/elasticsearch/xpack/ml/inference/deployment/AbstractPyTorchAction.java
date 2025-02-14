/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.deployment;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.job.process.AbstractInitializableRunnable;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.core.Strings.format;

abstract class AbstractPyTorchAction<T> extends AbstractInitializableRunnable {

    private final String deploymentId;
    private final long requestId;
    private final TimeValue timeout;
    private Scheduler.Cancellable timeoutHandler;
    private final DeploymentManager.ProcessContext processContext;
    private final AtomicBoolean notified = new AtomicBoolean();
    private final ActionListener<T> listener;
    private final ThreadPool threadPool;

    protected AbstractPyTorchAction(
        String deploymentId,
        long requestId,
        TimeValue timeout,
        DeploymentManager.ProcessContext processContext,
        ThreadPool threadPool,
        ActionListener<T> listener
    ) {
        this.deploymentId = ExceptionsHelper.requireNonNull(deploymentId, "deploymentId");
        this.requestId = requestId;
        this.timeout = ExceptionsHelper.requireNonNull(timeout, "timeout");
        this.processContext = ExceptionsHelper.requireNonNull(processContext, "processContext");
        this.listener = ExceptionsHelper.requireNonNull(listener, "listener");
        this.threadPool = ExceptionsHelper.requireNonNull(threadPool, "threadPool");
    }

    /**
     * Needs to be called after construction. This init starts the timeout handler and needs to be called before added to the executor for
     * scheduled work.
     */
    @Override
    public final void init() {
        if (this.timeoutHandler == null) {
            this.timeoutHandler = threadPool.schedule(
                this::onTimeout,
                timeout,
                threadPool.executor(MachineLearning.UTILITY_THREAD_POOL_NAME)
            );
        }
    }

    void onTimeout() {
        onTimeout(new ElasticsearchStatusException("timeout [{}] waiting for inference result", RestStatus.REQUEST_TIMEOUT, timeout));
    }

    void onCancel() {
        onTimeout(new ElasticsearchStatusException("inference task cancelled", RestStatus.BAD_REQUEST));
    }

    void onTimeout(Exception e) {
        if (notified.compareAndSet(false, true)) {
            processContext.getTimeoutCount().incrementAndGet();
            processContext.getResultProcessor().ignoreResponseWithoutNotifying(String.valueOf(requestId));
            listener.onFailure(e);
            return;
        }
        getLogger().debug("[{}] request [{}] received timeout after [{}] but listener already alerted", deploymentId, requestId, timeout);
    }

    void onSuccess(T result) {
        if (timeoutHandler != null) {
            timeoutHandler.cancel();
        } else {
            assert false : "init() not called, timeout handler unexpectedly null";
        }
        if (notified.compareAndSet(false, true)) {
            listener.onResponse(result);
            return;
        }
        getLogger().debug("[{}] request [{}] received inference response but listener already notified", deploymentId, requestId);
    }

    @Override
    public void onRejection(Exception e) {
        super.onRejection(e);
        processContext.getRejectedExecutionCount().incrementAndGet();
    }

    @Override
    public void onFailure(Exception e) {
        if (timeoutHandler != null) {
            timeoutHandler.cancel();
        } else {
            assert false : "init() not called, timeout handler unexpectedly null";
        }
        if (notified.compareAndSet(false, true)) {
            processContext.getResultProcessor().ignoreResponseWithoutNotifying(String.valueOf(requestId));
            listener.onFailure(e);
            return;
        }
        getLogger().debug(() -> format("[%s] request [%s] received failure but listener already notified", deploymentId, requestId), e);
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

    String getDeploymentId() {
        return deploymentId;
    }

    DeploymentManager.ProcessContext getProcessContext() {
        return processContext;
    }

    TimeValue getTimeout() {
        return timeout;
    }

    protected abstract Logger getLogger();
}
