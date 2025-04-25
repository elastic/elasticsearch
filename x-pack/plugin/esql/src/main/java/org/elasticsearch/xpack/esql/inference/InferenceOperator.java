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
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;

import java.util.Iterator;

import static org.elasticsearch.common.logging.LoggerMessageFormat.format;

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
        final RequestIterator requests = requests(input);
        final OutputBuilder<InferenceResult> outputBuilder = outputBuilder(input);

        new BulkInferenceOperation(requests, outputBuilder).execute(
            inferenceExecutionContext(),
            listener.delegateFailureIgnoreResponseAndWrap(l -> {
                l.onResponse(outputBuilder.buildOutput());
                Releasables.closeExpectNoException(requests, outputBuilder);
            })
        );
    }

    protected InferenceExecutionContext inferenceExecutionContext() {
        return inferenceRunner.executionContextBuilder().build();
    }

    protected abstract RequestIterator requests(Page input);

    protected abstract OutputBuilder<InferenceResult> outputBuilder(Page input);

    public abstract static class OutputBuilder<InferenceResults extends InferenceServiceResults>
        implements
            CheckedConsumer<InferenceAction.Response, Exception>,
            Releasable {
        protected abstract Class<InferenceResults> inferenceResultsClass();

        public abstract Page buildOutput();

        public abstract void onInferenceResults(InferenceResults results);

        @Override
        public void accept(InferenceAction.Response response) throws Exception {
            InferenceServiceResults results = response.getResults();
            if (inferenceResultsClass().isInstance(response.getResults()) == false) {
                throw new IllegalStateException(
                    format(
                        "Inference result has wrong type. Got [{}] while expecting [{}]",
                        results.getClass().getName(),
                        inferenceResultsClass().getName()
                    )
                );
            }

            onInferenceResults(inferenceResultsClass().cast(results));
        }
    }

    public interface RequestIterator extends Iterator<InferenceAction.Request>, Releasable {}
}
