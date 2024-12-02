/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.inference;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.AsyncOperator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.compute.operator.Operator;

public class CompletionInferenceOperator extends AsyncOperator {

    // Move to a setting.
    private static final int MAX_INFERENCE_WORKER = 1;

    public record Factory(EvalOperator.ExpressionEvaluator.Factory promptEvaluatorFactory) implements OperatorFactory {
        public String describe() {
            return "CompletionInferenceOperator[]";
        }


        @Override
        public Operator get(DriverContext driverContext) {
            return new CompletionInferenceOperator(driverContext, promptEvaluatorFactory.get(driverContext));
        }
    }

    private final EvalOperator.ExpressionEvaluator promptEvaluator;

    public CompletionInferenceOperator(DriverContext driverContext, EvalOperator.ExpressionEvaluator promptEvaluator) {
        super(driverContext, MAX_INFERENCE_WORKER);
        this.promptEvaluator = promptEvaluator;
    }


    @Override
    protected void performAsync(Page inputPage, ActionListener<Page> listener) {
        Block promptBlock = promptEvaluator.eval(inputPage);
        listener.onResponse(inputPage.appendBlock(promptBlock));
    }

    @Override
    protected void doClose() {

    }

    @Override
    public String toString() {
        return "CompletionInferenceOperator[]";
    }
}
