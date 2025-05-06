/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.inference;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.AsyncOperator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.esql.inference.bulk.BulkInferenceExecutionConfig;
import org.elasticsearch.xpack.esql.inference.bulk.BulkInferenceExecutor;
import org.elasticsearch.xpack.esql.inference.bulk.BulkInferenceOutputBuilder;
import org.elasticsearch.xpack.esql.inference.bulk.BulkInferenceRequestIterator;

public abstract class InferenceOperator<InferenceResult extends InferenceServiceResults> extends AsyncOperator<Page> {

    // Move to a setting.
    private static final int MAX_INFERENCE_WORKER = 10;
    private final String inferenceId;
    private final BlockFactory blockFactory;

    private final BulkInferenceExecutor<InferenceResult, Page> bulkInferenceExecutor;

    @SuppressWarnings("this-escape")
    public InferenceOperator(DriverContext driverContext, InferenceRunner inferenceRunner, ThreadPool threadPool, String inferenceId) {
        super(driverContext, threadPool.getThreadContext(), MAX_INFERENCE_WORKER);
        this.blockFactory = driverContext.blockFactory();
        this.bulkInferenceExecutor = new BulkInferenceExecutor<>(inferenceRunner, threadPool, bulkExecutionConfig());
        this.inferenceId = inferenceId;
    }

    protected BlockFactory blockFactory() {
        return blockFactory;
    }

    protected String inferenceId() {
        return inferenceId;
    }

    @Override
    protected void releaseFetchedOnAnyThread(Page page) {
        releasePageOnAnyThread(page);
    }

    @Override
    public Page getOutput() {
        return fetchFromBuffer();
    }

    @Override
    protected void performAsync(Page input, ActionListener<Page> listener) {
        try {
            BulkInferenceOutputBuilder<InferenceResult, Page> outputBuilder = outputBuilder(input);
            listener = ActionListener.releaseBefore(outputBuilder, listener);

            BulkInferenceRequestIterator requests = requests(input);
            listener = ActionListener.releaseBefore(requests, listener);

            bulkInferenceExecutor.execute(requests, outputBuilder, listener);
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    protected BulkInferenceExecutionConfig bulkExecutionConfig() {
        return BulkInferenceExecutionConfig.DEFAULT;
    }

    protected abstract BulkInferenceRequestIterator requests(Page input);

    protected abstract BulkInferenceOutputBuilder<InferenceResult, Page> outputBuilder(Page input);
}
