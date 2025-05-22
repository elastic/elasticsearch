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

import static org.elasticsearch.common.logging.LoggerMessageFormat.format;

public abstract class InferenceOperator<IR extends InferenceServiceResults> extends AsyncOperator<InferenceOperator.OngoingInference> {

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
    protected void releaseFetchedOnAnyThread(OngoingInference result) {
        releasePageOnAnyThread(result.inputPage);
    }

    @Override
    public Page getOutput() {
        OngoingInference ongoingInference = fetchFromBuffer();
        if (ongoingInference == null) {
            return null;
        }

        try (OutputBuilder<IR> outputBuilder = outputBuilder(ongoingInference.inputPage)) {
            for (int i = 0; i < ongoingInference.responses.length; i++) {
                outputBuilder.addInferenceResults(inferenceResults(ongoingInference.responses[i]));
            }
            return outputBuilder.buildOutput();
        } finally {
            releaseFetchedOnAnyThread(ongoingInference);
        }
    }

    @Override
    protected void performAsync(Page input, ActionListener<OngoingInference> listener) {
        try {
            bulkInferenceExecutor.execute(requests(input), listener.map(responses -> new OngoingInference(input, responses)));
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    protected BulkInferenceExecutionConfig bulkExecutionConfig() {
        return BulkInferenceExecutionConfig.DEFAULT;
    }

    private IR inferenceResults(InferenceAction.Response inferenceResponse) {
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

    protected abstract BulkInferenceRequestIterator requests(Page input);

    protected abstract Class<IR> inferenceResultsClass();

    protected abstract OutputBuilder<IR> outputBuilder(Page input);

    public record OngoingInference(Page inputPage, InferenceAction.Response[] responses) {

    }

    public interface OutputBuilder<IR extends InferenceServiceResults> extends Releasable {
        void addInferenceResults(IR inferenceResults);

        Page buildOutput();
    }
}
