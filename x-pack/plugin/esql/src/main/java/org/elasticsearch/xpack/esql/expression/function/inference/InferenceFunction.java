/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.inference;

import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.function.Function;
import org.elasticsearch.xpack.esql.core.tree.Source;

import java.util.List;

/**
 * A function is a function using an inference model.
 */
public abstract class InferenceFunction<PlanType extends InferenceFunction<PlanType>> extends Function {

    public static final String INFERENCE_ID_PARAMETER_NAME = "inference_id";

    protected InferenceFunction(Source source, List<Expression> children) {
        super(source, children);
    }

    /**
     * Returns the inference model ID expression.
     */
    public abstract Expression inferenceId();

    /**
     * Returns the task type of the inference model.
     */
    public abstract TaskType taskType();

    public abstract PlanType withInferenceResolutionError(String inferenceId, String error);

    @Override
    public boolean foldable() {
        // Inference functions are not foldable and need to be evaluated using an async inference call.
        return false;
    }
}
