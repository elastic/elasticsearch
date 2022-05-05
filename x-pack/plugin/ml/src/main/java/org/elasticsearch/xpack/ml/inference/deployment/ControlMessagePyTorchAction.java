/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.deployment;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.ml.inference.pytorch.results.PyTorchResult;
import org.elasticsearch.xpack.ml.inference.pytorch.results.ThreadSettings;

import java.io.IOException;

class ControlMessagePyTorchAction extends AbstractPyTorchAction<ThreadSettings> {

    private static final Logger logger = LogManager.getLogger(InferencePyTorchAction.class);

    private final int numAllocationThreads;

    private enum ControlMessageTypes {
        AllocationThreads
    };

    ControlMessagePyTorchAction(
        String modelId,
        long requestId,
        int numAllocationThreads,
        TimeValue timeout,
        DeploymentManager.ProcessContext processContext,
        ThreadPool threadPool,
        ActionListener<ThreadSettings> listener
    ) {
        super(modelId, requestId, timeout, processContext, threadPool, listener);
        this.numAllocationThreads = numAllocationThreads;
    }

    @Override
    protected void doRun() throws Exception {
        if (isNotified()) {
            // Should not execute request as it has already timed out while waiting in the queue
            logger.debug(
                () -> new ParameterizedMessage(
                    "[{}] skipping control message on request [{}] as it has timed out",
                    getModelId(),
                    getRequestId()
                )
            );
            return;
        }

        final String requestIdStr = String.valueOf(getRequestId());
        try {
            var message = buildControlMessage(requestIdStr, numAllocationThreads);

            getProcessContext().getResultProcessor()
                .registerRequest(requestIdStr, ActionListener.wrap(this::processResponse, this::onFailure));

            getProcessContext().getProcess().get().writeInferenceRequest(message);
        } catch (IOException e) {
            logger.error(new ParameterizedMessage("[{}] error writing control message to the inference process", getModelId()), e);
            onFailure(ExceptionsHelper.serverError("Error writing control message to the inference process", e));
        } catch (Exception e) {
            onFailure(e);
        }
    }

    public static BytesReference buildControlMessage(String requestId, int numAllocationThreads) throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        builder.field("request_id", requestId);
        builder.field("control", ControlMessageTypes.AllocationThreads.ordinal());
        builder.field("num_allocations", numAllocationThreads);
        builder.endObject();

        // BytesReference.bytes closes the builder
        return BytesReference.bytes(builder);
    }

    public void processResponse(PyTorchResult result) {
        if (result.isError()) {
            onFailure(result.errorResult().error());
            return;
        }
        onSuccess(result.threadSettings());
    }

    @Override
    protected Logger getLogger() {
        return logger;
    }

}
