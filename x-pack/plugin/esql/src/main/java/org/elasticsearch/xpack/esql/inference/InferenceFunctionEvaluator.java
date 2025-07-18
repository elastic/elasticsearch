/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.inference;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;

public interface InferenceFunctionEvaluator {

    void eval(FoldContext foldContext, ActionListener<Expression> listener);

    interface Factory {
        InferenceFunctionEvaluator get(InferenceRunner inferenceRunner);
    }
}
