/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.inference;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BlockUtils;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator.ExpressionEvaluator;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.indices.breaker.AllCircuitBreakerStats;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.indices.breaker.CircuitBreakerStats;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.evaluator.EvalMapper;
import org.elasticsearch.xpack.esql.expression.function.inference.InferenceFunction;
import org.elasticsearch.xpack.esql.expression.function.inference.TextEmbedding;
import org.elasticsearch.xpack.esql.inference.textembedding.TextEmbeddingOperator;

/**
 * Evaluator for inference functions that performs constant folding by executing inference operations
 * at optimization time and replacing them with their computed results.
 * <p>
 * This class is responsible for:
 * <ul>
 * <li>Setting up the necessary execution context (DriverContext, CircuitBreaker, etc.)</li>
 * <li>Creating and configuring appropriate inference operators for different function types</li>
 * <li>Executing inference operations asynchronously</li>
 * <li>Converting operator results back to ESQL expressions</li>
 * </ul>
 */
public class InferenceFunctionEvaluator {

    private final FoldContext foldContext;
    private final InferenceService inferenceService;
    private final InferenceOperatorProvider inferenceOperatorProvider;

    /**
     * Creates a new inference function evaluator with the default operator provider.
     *
     * @param foldContext      the fold context containing circuit breakers and evaluation settings
     * @param inferenceService the inference service for executing inference operations
     */
    public InferenceFunctionEvaluator(FoldContext foldContext, InferenceService inferenceService) {
        this.foldContext = foldContext;
        this.inferenceService = inferenceService;
        this.inferenceOperatorProvider = this::createInferenceOperator;
    }

    /**
     * Creates a new inference function evaluator with a custom operator provider.
     * This constructor is primarily used for testing to inject mock operator providers.
     *
     * @param foldContext               the fold context containing circuit breakers and evaluation settings
     * @param inferenceService          the inference service for executing inference operations
     * @param inferenceOperatorProvider custom provider for creating inference operators
     */
    InferenceFunctionEvaluator(
        FoldContext foldContext,
        InferenceService inferenceService,
        InferenceOperatorProvider inferenceOperatorProvider
    ) {
        this.foldContext = foldContext;
        this.inferenceService = inferenceService;
        this.inferenceOperatorProvider = inferenceOperatorProvider;
    }

    /**
     * Folds an inference function by executing it and replacing it with its computed result.
     * <p>
     * This method performs the following steps:
     * <ol>
     * <li>Validates that the function is foldable (has constant parameters)</li>
     * <li>Sets up a minimal execution context with appropriate circuit breakers</li>
     * <li>Creates and configures the appropriate inference operator</li>
     * <li>Executes the inference operation asynchronously</li>
     * <li>Converts the result to a {@link Literal} expression</li>
     * </ol>
     *
     * @param f        the inference function to fold - must be foldable (have constant parameters)
     * @param listener the listener to notify when folding completes successfully or fails
     * @throws IllegalArgumentException if the function is not foldable
     */
    public void fold(InferenceFunction<?> f, ActionListener<Expression> listener) {
        if (f.foldable() == false) {
            listener.onFailure(new IllegalArgumentException("Inference function must be foldable"));
            return;
        }

        // Set up a DriverContext for executing the inference operator.
        // This follows the same pattern as EvaluatorMapper but in a simplified context
        // suitable for constant folding during optimization.
        CircuitBreaker breaker = foldContext.circuitBreakerView(f.source());
        BigArrays bigArrays = new BigArrays(null, new CircuitBreakerService() {
            @Override
            public CircuitBreaker getBreaker(String name) {
                if (name.equals(CircuitBreaker.REQUEST) == false) {
                    throw new UnsupportedOperationException("Only REQUEST circuit breaker is supported");
                }
                return breaker;
            }

            @Override
            public AllCircuitBreakerStats stats() {
                throw new UnsupportedOperationException("Circuit breaker stats not supported in fold context");
            }

            @Override
            public CircuitBreakerStats stats(String name) {
                throw new UnsupportedOperationException("Circuit breaker stats not supported in fold context");
            }
        }, CircuitBreaker.REQUEST).withCircuitBreaking();

        DriverContext driverContext = new DriverContext(bigArrays, new BlockFactory(breaker, bigArrays));

        // Create the inference operator for the specific function type using the provider
        Operator inferenceOperator = inferenceOperatorProvider.getOperator(f, driverContext);

        // Execute the inference operation asynchronously and handle the result
        // The operator will perform the actual inference call and return a page with the result
        driverContext.waitForAsyncActions(listener.delegateFailureIgnoreResponseAndWrap(l -> {
            Page output = inferenceOperator.getOutput();

            if (output == null) {
                l.onFailure(new IllegalStateException("Expected output page from inference operator"));
                return;
            }

            if (output.getPositionCount() != 1 || output.getBlockCount() != 1) {
                l.onFailure(new IllegalStateException("Expected a single block with a single value from inference operator"));
                return;
            }

            // Convert the operator result back to an ESQL expression (Literal)
            l.onResponse(Literal.of(f, BlockUtils.toJavaObject(output.getBlock(0), 0)));
        }));

        // Feed the operator with a single page to trigger execution
        // The actual input data is already bound in the operator through expression evaluators
        inferenceOperator.addInput(new Page(1));

        driverContext.finish();
    }

    /**
     * Creates an inference operator for the given function type and driver context.
     * <p>
     * This method uses pattern matching to determine the correct operator factory based on
     * the inference function type, creates the factory, and then instantiates the operator
     * with the provided driver context. Each supported inference function type has its own
     * specialized operator implementation.
     *
     * @param f             the inference function to create an operator for
     * @param driverContext the driver context to use for operator creation
     * @return an operator instance configured for the given function type
     * @throws IllegalArgumentException if the function type is not supported
     */
    private Operator createInferenceOperator(InferenceFunction<?> f, DriverContext driverContext) {
        Operator.OperatorFactory factory = switch (f) {
            case TextEmbedding textEmbedding -> new TextEmbeddingOperator.Factory(
                inferenceService,
                inferenceId(f),
                expressionEvaluatorFactory(textEmbedding.inputText())
            );
            default -> throw new IllegalArgumentException("Unknown inference function: " + f.getClass().getName());
        };

        return factory.get(driverContext);
    }

    /**
     * Extracts the inference endpoint ID from an inference function.
     *
     * @param f the inference function containing the inference ID
     * @return the inference endpoint ID as a string
     */
    private String inferenceId(InferenceFunction<?> f) {
        return BytesRefs.toString(f.inferenceId().fold(foldContext));
    }

    /**
     * Creates an expression evaluator factory for a foldable expression.
     * <p>
     * This method converts a foldable expression into an evaluator factory that can be used by inference
     * operators. The expression is first folded to its constant value and then wrapped in a literal.
     *
     * @param e the foldable expression to create an evaluator factory for
     * @return an expression evaluator factory for the given expression
     * @throws AssertionError if the expression is not foldable (in debug builds)
     */
    private ExpressionEvaluator.Factory expressionEvaluatorFactory(Expression e) {
        assert e.foldable() : "Input expression must be foldable";
        return EvalMapper.toEvaluator(foldContext, Literal.of(foldContext, e), null);
    }

    /**
     * Functional interface for providing inference operators.
     * <p>
     * This interface abstracts the creation of inference operators for different function types,
     * allowing for easier testing and potential future extensibility. The provider is responsible
     * for creating an appropriate operator instance given an inference function and driver context.
     */
    @FunctionalInterface
    interface InferenceOperatorProvider {
        /**
         * Creates an inference operator for the given function and driver context.
         *
         * @param f             the inference function to create an operator for
         * @param driverContext the driver context to use for operator creation
         * @return an operator instance configured for the given function
         */
        Operator getOperator(InferenceFunction<?> f, DriverContext driverContext);
    }
}
