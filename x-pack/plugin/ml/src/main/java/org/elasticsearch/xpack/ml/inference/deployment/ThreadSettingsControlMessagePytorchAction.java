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
import org.elasticsearch.xpack.ml.inference.pytorch.results.ThreadSettings;

import java.io.IOException;

public class ThreadSettingsControlMessagePytorchAction extends AbstractControlMessagePyTorchAction<ThreadSettings> {
    private final int numAllocationThreads;

    ThreadSettingsControlMessagePytorchAction(
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
    int controlOrdinal() {
        return ControlMessageTypes.AllocationThreads.ordinal();
    }

    @Override
    void writeMessage(XContentBuilder builder) throws IOException {
        builder.field("num_allocations", numAllocationThreads);
    }

    @Override
    ThreadSettings getResult(PyTorchResult result) {
        return result.threadSettings();
    }
}
