/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.deployment;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.ml.inference.pytorch.results.PyTorchResult;

public class ClearCacheControlMessagePytorchAction extends AbstractControlMessagePyTorchAction<Boolean> {

    ClearCacheControlMessagePytorchAction(
        String modelId,
        long requestId,
        TimeValue timeout,
        DeploymentManager.ProcessContext processContext,
        ThreadPool threadPool,
        ActionListener<Boolean> listener
    ) {
        super(modelId, requestId, timeout, processContext, threadPool, listener);
    }

    @Override
    int controlOrdinal() {
        return ControlMessageTypes.ClearCache.ordinal();
    }

    @Override
    void writeMessage(XContentBuilder builder) {
        // Nothing is written
    }

    @Override
    Boolean getResult(PyTorchResult result) {
        return true;
    }
}
