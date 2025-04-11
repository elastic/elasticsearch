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
import org.elasticsearch.xpack.core.inference.action.InferenceAction;

abstract public class InferenceOperator<Fetched> extends AsyncOperator<Fetched> {

    // Move to a setting.
    private static final int MAX_INFERENCE_WORKER = 10;

    private final InferenceRunner inferenceRunner;
    private final String inferenceId;

    public InferenceOperator(
        DriverContext driverContext,
        ThreadContext threadContext,
        InferenceRunner inferenceRunner,
        String inferenceId
    ) {
        super(driverContext, threadContext, MAX_INFERENCE_WORKER);
        this.inferenceRunner = inferenceRunner;
        this.inferenceId = inferenceId;

        assert inferenceRunner.getThreadContext() != null;
    }

    protected void doInference(InferenceAction.Request inferenceRequest, ActionListener<InferenceAction.Response> listener) {
        inferenceRunner.doInference(inferenceRequest, listener);
    }

    protected String inferenceId() {
        return inferenceId;
    }
}
