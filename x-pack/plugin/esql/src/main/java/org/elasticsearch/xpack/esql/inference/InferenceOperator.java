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
import org.elasticsearch.core.Releasables;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.esql.inference.bulk.BulkInferenceExecutionConfig;
import org.elasticsearch.xpack.esql.inference.bulk.BulkInferenceExecutor;
import org.elasticsearch.xpack.esql.inference.bulk.BulkInferenceRequestIterator;

import java.util.List;

import static org.elasticsearch.common.logging.LoggerMessageFormat.format;

/**
 * An abstract asynchronous operator that performs throttled bulk inference execution using an {@link InferenceRunner}.
 * <p>
 * The {@code InferenceOperator} integrates with the compute framework  supports throttled bulk execution of inference requests. It
 * transforms input {@link Page} into inference requests, asynchronously executes them, and converts the responses into a new {@link Page}.
 * </p>
 */
public abstract class InferenceOperator extends AsyncOperator<InferenceOperator.OngoingInferenceResult> {
    private final String inferenceId;
    private final BlockFactory blockFactory;
    private final BulkInferenceExecutor bulkInferenceExecutor;

    /**
     * Constructs a new {@code InferenceOperator}.
     *
     * @param driverContext        The driver context.
     * @param inferenceRunner      The runner used to execute inference requests.
     * @param bulkExecutionConfig  Configuration for inference execution.
     * @param threadPool           The thread pool used for executing async inference.
     * @param inferenceId          The ID of the inference model to use.
     */
    public InferenceOperator(
        DriverContext driverContext,
        InferenceRunner inferenceRunner,
        BulkInferenceExecutionConfig bulkExecutionConfig,
        ThreadPool threadPool,
        String inferenceId
    ) {
        super(driverContext, bulkExecutionConfig.workers());
        this.blockFactory = driverContext.blockFactory();
        this.bulkInferenceExecutor = new BulkInferenceExecutor(inferenceRunner, threadPool, bulkExecutionConfig);
        this.inferenceId = inferenceId;
    }

    /**
     * Returns the {@link BlockFactory} used to create output data blocks.
     */
    protected BlockFactory blockFactory() {
        return blockFactory;
    }

    /**
     * Returns the inference model ID used for this operator.
     */
    protected String inferenceId() {
        return inferenceId;
    }

    /**
     * Initiates asynchronous inferences for the given input page.
     */
    @Override
    protected void performAsync(Page input, ActionListener<OngoingInferenceResult> listener) {
        try {
            BulkInferenceRequestIterator requests = requests(input);
            listener = ActionListener.releaseAfter(listener, requests);
            bulkInferenceExecutor.execute(requests, listener.map(responses -> new OngoingInferenceResult(input, responses)));
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    /**
     * Releases resources associated with an ongoing inference.
     */
    @Override
    protected void releaseFetchedOnAnyThread(OngoingInferenceResult ongoingInferenceResult) {
        Releasables.close(ongoingInferenceResult);
    }

    /**
     * Returns the next available output page constructed from completed inference results.
     */
    @Override
    public Page getOutput() {
        OngoingInferenceResult ongoingInferenceResult = fetchFromBuffer();
        if (ongoingInferenceResult == null) {
            return null;
        }

        try (OutputBuilder outputBuilder = outputBuilder(ongoingInferenceResult.inputPage)) {
            for (InferenceAction.Response response : ongoingInferenceResult.responses) {
                outputBuilder.addInferenceResponse(response);
            }
            return outputBuilder.buildOutput();

        } finally {
            releaseFetchedOnAnyThread(ongoingInferenceResult);
        }
    }

    /**
     * Converts the given input page into a sequence of inference requests.
     *
     * @param input The input page to process.
     */
    protected abstract BulkInferenceRequestIterator requests(Page input);

    /**
     * Creates a new {@link OutputBuilder} instance used to build the output page.
     *
     * @param input The corresponding input page used to generate the inference requests.
     */
    protected abstract OutputBuilder outputBuilder(Page input);

    /**
     * An interface for accumulating inference responses and constructing a result {@link Page}.
     */
    public interface OutputBuilder extends Releasable {

        /**
         * Adds an inference response to the output.
         * <p>
         * The responses must be added in the same order as the corresponding inference requests were generated.
         * Failing to preserve order may lead to incorrect or misaligned output rows.
         * </p>
         *
         * @param inferenceResponse The inference response to include.
         */
        void addInferenceResponse(InferenceAction.Response inferenceResponse);

        /**
         * Builds the final output page from accumulated inference responses.
         *
         * @return The constructed output page.
         */
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

        default void releasePageOnAnyThread(Page page) {
            InferenceOperator.releasePageOnAnyThread(page);
        }
    }

    /**
     * Represents the result of an ongoing inference operation, including the original input page
     * and the list of inference responses.
     *
     * @param inputPage The input page used to generate inference requests.
     * @param responses The inference responses returned by the inference service.
     */
    public record OngoingInferenceResult(Page inputPage, List<InferenceAction.Response> responses) implements Releasable {

        @Override
        public void close() {
            releasePageOnAnyThread(inputPage);
        }
    }
}
