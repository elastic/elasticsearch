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
 * Base class for ESQL functions that use inference endpoints (e.g., TEXT_EMBEDDING).
 */
public abstract class InferenceFunction<PlanType extends InferenceFunction<PlanType>> extends Function {

    public static final String INFERENCE_ID_PARAMETER_NAME = "inference_id";

    protected InferenceFunction(Source source, List<Expression> children) {
        super(source, children);
    }

    /** The inference endpoint identifier expression. */
    public abstract Expression inferenceId();

    /** The task type required by this function (e.g., TEXT_EMBEDDING). */
    public abstract TaskType taskType();

    /** Returns a copy with inference resolution error for display to user. */
    public abstract PlanType withInferenceResolutionError(String inferenceId, String error);

    /** True if this function contains nested inference function calls. */
    public boolean hasNestedInferenceFunction() {
        return anyMatch(e -> e instanceof InferenceFunction && e != this);
    }
}
