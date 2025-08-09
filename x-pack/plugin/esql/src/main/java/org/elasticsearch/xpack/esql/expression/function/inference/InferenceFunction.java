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
import org.elasticsearch.xpack.esql.inference.InferenceFunctionEvaluator;

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

    /**
     * Returns a new instance of the function with the specified inference resolution error.
     */
    public abstract PlanType withInferenceResolutionError(String inferenceId, String error);

    /**
     * Returns the inference function evaluator factory.
     */
    public abstract InferenceFunctionEvaluator.Factory inferenceEvaluatorFactory();

    /**
     * Returns true if the function has a nested inference function.
     */
    public boolean hasNestedInferenceFunction() {
        return anyMatch(e -> e instanceof InferenceFunction && e != this);
    }
}
