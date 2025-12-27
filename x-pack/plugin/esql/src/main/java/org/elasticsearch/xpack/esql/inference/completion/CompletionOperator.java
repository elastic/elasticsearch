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

/**
 * {@link CompletionOperator} is an {@link InferenceOperator} that performs inference using prompt-based model (e.g., text completion).
 * It evaluates a prompt expression for each input row, constructs inference requests, and emits the model responses as output.
 */
public class CompletionOperator extends InferenceOperator {
    /**
     * Constructs a new {@code CompletionOperator}.
     *
     * @param driverContext                   The driver context.
     * @param inferenceService                The inference service to use for executing inference requests.
     * @param requestItemIteratorFactory      Factory for creating request iterators from input pages.
     * @param outputBuilder                   Builder for converting inference responses into output pages.
     * @param maxOutstandingPages             The maximum number of pages processed in parallel.
     * @param maxOutstandingInferenceRequests The maximum number of inference requests to be run in parallel.
     */
    CompletionOperator(
        DriverContext driverContext,
        InferenceService inferenceService,
        CompletionRequestIterator.Factory requestItemIteratorFactory,
        CompletionOutputBuilder outputBuilder,
        int maxOutstandingPages,
        int maxOutstandingInferenceRequests
    ) {
        super(
            driverContext,
            inferenceService,
            requestItemIteratorFactory,
            outputBuilder,
            maxOutstandingPages,
            maxOutstandingInferenceRequests
        );
    }

    /**
     * Factory for creating {@link CompletionOperator} instances.
     */
    public record Factory(InferenceService inferenceService, String inferenceId, ExpressionEvaluator.Factory promptEvaluatorFactory)
        implements
            OperatorFactory {
        @Override
        public String describe() {
            return "CompletionOperator[]";
        }

        @Override
        public Operator get(DriverContext driverContext) {
            return new CompletionOperator(
                driverContext,
                inferenceService,
                new CompletionRequestIterator.Factory(inferenceId, promptEvaluatorFactory.get(driverContext)),
                new CompletionOutputBuilder(driverContext.blockFactory()),
                DEFAULT_MAX_OUTSTANDING_PAGES,
                DEFAULT_MAX_OUTSTANDING_REQUESTS
            );
        }
    }

}
