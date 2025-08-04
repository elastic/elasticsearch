/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.deployment;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.ml.inference.pytorch.results.PyTorchResult;

import java.io.IOException;

import static org.elasticsearch.core.Strings.format;

abstract class AbstractControlMessagePyTorchAction<T> extends AbstractPyTorchAction<T> {

    private static final Logger logger = LogManager.getLogger(AbstractControlMessagePyTorchAction.class);

    enum ControlMessageTypes {
        AllocationThreads,
        ClearCache
    };

    AbstractControlMessagePyTorchAction(
        String deploymentId,
        long requestId,
        TimeValue timeout,
        DeploymentManager.ProcessContext processContext,
        ThreadPool threadPool,
        ActionListener<T> listener
    ) {
        super(deploymentId, requestId, timeout, processContext, threadPool, listener);
    }

    abstract int controlOrdinal();

    abstract void writeMessage(XContentBuilder builder) throws IOException;

    abstract T getResult(PyTorchResult result);

    @Override
    protected void doRun() throws Exception {
        if (isNotified()) {
            // Should not execute request as it has already timed out while waiting in the queue
            logger.debug(
                () -> format("[%s] skipping control message on request [%s] as it has timed out", getDeploymentId(), getRequestId())
            );
            return;
        }

        final String requestIdStr = String.valueOf(getRequestId());
        try {
            var message = buildControlMessage(requestIdStr);

            getProcessContext().getResultProcessor()
                .registerRequest(requestIdStr, ActionListener.wrap(this::processResponse, this::onFailure));

            getProcessContext().getProcess().get().writeInferenceRequest(message);
        } catch (IOException e) {
            logger.error(() -> "[" + getDeploymentId() + "] error writing control message to the inference process", e);
            onFailure(ExceptionsHelper.serverError("Error writing control message to the inference process", e));
        } catch (Exception e) {
            onFailure(e);
        }
    }

    final BytesReference buildControlMessage(String requestId) throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        builder.field("request_id", requestId);
        builder.field("control", controlOrdinal());
        writeMessage(builder);
        builder.endObject();
        // BytesReference.bytes closes the builder
        return BytesReference.bytes(builder);
    }

    private void processResponse(PyTorchResult result) {
        if (result.isError()) {
            onFailure(result.errorResult());
            return;
        }
        onSuccess(getResult(result));
    }

    @Override
    protected Logger getLogger() {
        return logger;
    }

}
