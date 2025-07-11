/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.inference;

import org.elasticsearch.xpack.esql.core.expression.Expression;

/**
 * A function is a function using an inference model.
 */
public interface InferenceFunction {

    String INFERENCE_ID_PARAMETER_NAME = "inference_id";

    /**
     * Returns the inference model ID expression.
     */
    Expression inferenceId();
}
