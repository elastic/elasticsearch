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

public abstract class InferenceOperator extends AsyncOperator<InferenceOperator.OngoingInference> {
    private final String inferenceId;
    private final BlockFactory blockFactory;
    private final BulkInferenceExecutor bulkInferenceExecutor;

    public InferenceOperator(
        DriverContext driverContext,
        InferenceRunner inferenceRunner,
        BulkInferenceExecutionConfig bulkExecutionConfig,
        ThreadPool threadPool,
        String inferenceId
    ) {
        super(driverContext, threadPool.getThreadContext(), bulkExecutionConfig.workers());
        this.blockFactory = driverContext.blockFactory();
        this.bulkInferenceExecutor = new BulkInferenceExecutor(inferenceRunner, threadPool, bulkExecutionConfig);
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
    protected void performAsync(Page input, ActionListener<OngoingInference> listener) {
        try {
            BulkInferenceRequestIterator requests = requests(input);
            listener = ActionListener.releaseAfter(listener, requests);
            bulkInferenceExecutor.execute(requests, listener.map(responses -> new OngoingInference(input, responses)));
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    @Override
    public Page getOutput() {
        OngoingInference ongoingInference = fetchFromBuffer();
        if (ongoingInference == null) {
            return null;
        }

        try (OutputBuilder outputBuilder = outputBuilder(ongoingInference.inputPage)) {
            ongoingInference.responses.forEach(outputBuilder::addInferenceResponse);
            return outputBuilder.buildOutput();
        } finally {
            releaseFetchedOnAnyThread(ongoingInference);
        }
    }

    protected abstract BulkInferenceRequestIterator requests(Page input);

    protected abstract OutputBuilder outputBuilder(Page input);

    public interface OutputBuilder extends Releasable {
        void addInferenceResponse(InferenceAction.Response inferenceResponse);

        Page buildOutput();

        static <IR extends InferenceServiceResults> IR inferenceResults(InferenceAction.Response inferenceResponse, Class<IR> clazz) {
            InferenceServiceResults results = inferenceResponse.getResults();
            if (clazz.isInstance(results)) {
                return clazz.cast(results);
            }

            throw new IllegalStateException(
                format("Inference result has wrong type. Got [{}] while expecting [{}]", results.getClass().getName(), clazz.getName())
            );
        }
    }

    public record OngoingInference(Page inputPage, List<InferenceAction.Response> responses) {

    }
}
