/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.inference;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.compute.operator.AsyncOperator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;

import static org.elasticsearch.common.logging.LoggerMessageFormat.format;

abstract public class InferenceOperator<Fetched, InferenceResult extends InferenceServiceResults> extends AsyncOperator<Fetched> {

    // Move to a setting.
    private static final int MAX_INFERENCE_WORKER = 10;
    private final InferenceRunner inferenceRunner;
    private final String inferenceId;
    private final Class<InferenceResult> inferenceResultClass;

    public InferenceOperator(
        DriverContext driverContext,
        ThreadContext threadContext,
        InferenceRunner inferenceRunner,
        String inferenceId,
        Class<InferenceResult> inferenceResultClass
    ) {
        super(driverContext, threadContext, MAX_INFERENCE_WORKER);
        this.inferenceRunner = inferenceRunner;
        this.inferenceId = inferenceId;
        this.inferenceResultClass = inferenceResultClass;

        assert inferenceRunner.getThreadContext() != null;
    }

    protected final void doInference(InferenceAction.Request inferenceRequest, ActionListener<InferenceResult> listener) {
        inferenceRunner.doInference(inferenceRequest, listener.map(this::checkedInferenceResults));
    }

    protected String inferenceId() {
        return inferenceId;
    }

    private InferenceResult checkedInferenceResults(InferenceAction.Response inferenceResponse) {
        if (inferenceResultClass.isInstance(inferenceResponse.getResults())) {
            return inferenceResultClass.cast(inferenceResponse.getResults());
        }
        throw new IllegalStateException(
            format(
                "Inference result has wrong type. Got [{}] while expecting [{}]",
                inferenceResponse.getResults().getClass().getName(),
                inferenceResultClass.getName()
            )
        );
    }
}
