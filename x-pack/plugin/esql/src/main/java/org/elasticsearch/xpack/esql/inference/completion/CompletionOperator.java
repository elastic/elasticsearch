/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.inference.completion;

import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator.ExpressionEvaluator;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.xpack.esql.inference.InferenceOperator;
import org.elasticsearch.xpack.esql.inference.InferenceService;

import java.util.Map;

/**
 * {@link CompletionOperator} is an {@link InferenceOperator} that performs inference using prompt-based model (e.g., text completion).
 * It evaluates a prompt expression for each input row, constructs inference requests, and emits the model responses as output.
 */
public class CompletionOperator extends InferenceOperator {
    private final Map<String, Object> taskSettings;

    /**
     * Constructs a new {@code CompletionOperator}.
     *
     * @param driverContext     The driver context.
     * @param inferenceService  The inference service to use for executing inference requests.
     * @param inferenceId       The ID of the inference model to invoke.
     * @param promptEvaluator   Evaluator for computing prompts from input rows.
     */
    CompletionOperator(
        DriverContext driverContext,
        InferenceService inferenceService,
        String inferenceId,
        ExpressionEvaluator promptEvaluator,
        Map<String, Object> taskSettings
    ) {
        super(
            driverContext,
            inferenceService,
            new CompletionRequestIterator.Factory(inferenceId, promptEvaluator, taskSettings),
            new CompletionOutputBuilder(driverContext.blockFactory())
        );
        this.taskSettings = taskSettings;
    }

    @Override
    public String toString() {
        return "CompletionOperator[inference_id=[" + inferenceId() + "]]";
    }

    /**
     * Factory for creating {@link CompletionOperator} instances.
     */
    public record Factory(
        InferenceService inferenceService,
        String inferenceId,
        ExpressionEvaluator.Factory promptEvaluatorFactory,
        Map<String, Object> taskSettings
    ) implements OperatorFactory {

        @Override
        public String describe() {
            return "CompletionOperator[inference_id=[" + inferenceId + "]]";
        }

        @Override
        public Operator get(DriverContext driverContext) {
            return new CompletionOperator(
                driverContext,
                inferenceService,
                inferenceId,
                promptEvaluatorFactory.get(driverContext),
                taskSettings
            );
        }
    }
}
