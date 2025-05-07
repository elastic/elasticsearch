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
import org.elasticsearch.core.Releasable;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.esql.inference.bulk.BulkInferenceExecutionConfig;
import org.elasticsearch.xpack.esql.inference.bulk.BulkInferenceExecutor;
import org.elasticsearch.xpack.esql.inference.bulk.BulkInferenceRequestIterator;

import java.util.List;

import static org.elasticsearch.common.logging.LoggerMessageFormat.format;

public abstract class InferenceOperator<IR extends InferenceServiceResults> extends AsyncOperator<InferenceOperator.OnGoingInference<IR>> {

    // Move to a setting.
    private static final int MAX_INFERENCE_WORKER = 10;
    private final String inferenceId;
    private final BlockFactory blockFactory;

    private final BulkInferenceExecutor bulkInferenceExecutor;

    @SuppressWarnings("this-escape")
    public InferenceOperator(DriverContext driverContext, InferenceRunner inferenceRunner, ThreadPool threadPool, String inferenceId) {
        super(driverContext, threadPool.getThreadContext(), MAX_INFERENCE_WORKER);
        this.blockFactory = driverContext.blockFactory();
        this.bulkInferenceExecutor = new BulkInferenceExecutor(inferenceRunner, threadPool, bulkExecutionConfig());
        this.inferenceId = inferenceId;
    }

    protected BlockFactory blockFactory() {
        return blockFactory;
    }

    protected String inferenceId() {
        return inferenceId;
    }

    @Override
    protected void releaseFetchedOnAnyThread(OnGoingInference<IR> onGoingInference) {
        releasePageOnAnyThread(onGoingInference.inputPage);
    }

    @Override
    public Page getOutput() {
        OnGoingInference<IR> onGoingInference = fetchFromBuffer();

        if (onGoingInference == null) {
            return null;
        }

        try (OutputBuilder<IR> outputBuilder = outputBuilder(onGoingInference)) {
            onGoingInference.inferenceResponses.forEach(outputBuilder::addInferenceResult);
            return outputBuilder.buildOutput();
        }
    }

    @Override
    protected void performAsync(Page input, ActionListener<OnGoingInference<IR>> listener) {
        try (BulkInferenceRequestIterator requests = requests(input)) {
            bulkInferenceExecutor.execute(requests, listener.map(r -> onGoingInference(input, r)));
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    private OnGoingInference<IR> onGoingInference(Page input, List<InferenceAction.Response> inferenceResponses) {
        return new OnGoingInference<>(input, inferenceResponses.stream().map(this::inferenceResults).toList());
    }

    IR inferenceResults(InferenceAction.Response inferenceResponse) {
        InferenceServiceResults results = inferenceResponse.getResults();
        if (inferenceResultsClass().isInstance(results)) {
            return inferenceResultsClass().cast(results);
        }

        throw new IllegalStateException(
            format(
                "Inference result has wrong type. Got [{}] while expecting [{}]",
                results.getClass().getName(),
                inferenceResultsClass().getName()
            )
        );
    }

    protected abstract Class<IR> inferenceResultsClass();

    protected BulkInferenceExecutionConfig bulkExecutionConfig() {
        return BulkInferenceExecutionConfig.DEFAULT;
    }

    protected abstract BulkInferenceRequestIterator requests(Page input);

    protected abstract OutputBuilder<IR> outputBuilder(OnGoingInference<IR> onGoingInference);

    public abstract static class OutputBuilder<IR extends InferenceServiceResults> implements Releasable {

        public abstract void addInferenceResult(IR inferenceResult);

        public abstract Page buildOutput();

        protected void releasePageOnAnyThread(Page page) {
            InferenceOperator.releasePageOnAnyThread(page);
        }
    }

    public record OnGoingInference<IR extends InferenceServiceResults>(Page inputPage, List<IR> inferenceResponses) {

    }
}
