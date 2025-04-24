/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.inference;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.AsyncOperator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.esql.inference.bulk.BulkInferenceOperation;
import org.elasticsearch.xpack.esql.inference.bulk.BulkInferenceOutputBuilder;
import org.elasticsearch.xpack.esql.inference.bulk.BulkInferenceRequestIterator;

public abstract class InferenceOperator<InferenceResult extends InferenceServiceResults> extends AsyncOperator<Page> {

    // Move to a setting.
    private static final int MAX_INFERENCE_WORKER = 10;
    private final InferenceRunner inferenceRunner;
    private final String inferenceId;

    public InferenceOperator(DriverContext driverContext, InferenceRunner inferenceRunner, String inferenceId) {
        super(driverContext, inferenceRunner.threadContext(), MAX_INFERENCE_WORKER);
        this.inferenceRunner = inferenceRunner;
        this.inferenceId = inferenceId;
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
        new BulkInferenceOperation<>(bulkInferenceRequestIterator(input), bulkOutputBuilder(input)).execute(inferenceRunner, listener);
    }

    protected InferenceAction.Request.Builder inferenceRequestBuilder() {
        return InferenceAction.Request.builder(inferenceId, taskType());
    }

    protected abstract TaskType taskType();

    protected abstract BulkInferenceRequestIterator bulkInferenceRequestIterator(Page input);

    protected abstract BulkInferenceOutputBuilder<InferenceResult, Page> bulkOutputBuilder(Page input);
}
