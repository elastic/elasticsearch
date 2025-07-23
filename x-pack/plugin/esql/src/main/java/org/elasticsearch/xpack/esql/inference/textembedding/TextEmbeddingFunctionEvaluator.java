/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.inference.textembedding;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.indices.breaker.AllCircuitBreakerStats;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.indices.breaker.CircuitBreakerStats;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.evaluator.EvalMapper;
import org.elasticsearch.xpack.esql.expression.function.inference.TextEmbedding;
import org.elasticsearch.xpack.esql.inference.BulkInferenceRunner;
import org.elasticsearch.xpack.esql.inference.InferenceFunctionEvaluator;
import org.elasticsearch.xpack.esql.planner.Layout;

import static org.elasticsearch.compute.data.BlockUtils.fromArrayRow;
import static org.elasticsearch.compute.data.BlockUtils.toJavaObject;

/**
 * Evaluates text embedding functions by executing them through the inference service.
 * <p>
 * This class handles the execution of text embedding operations, managing resources
 * like circuit breakers and memory allocation during the inference process.
 */
public class TextEmbeddingFunctionEvaluator implements InferenceFunctionEvaluator {

    private final BulkInferenceRunner.Factory inferenceRunnerFactory;

    private final TextEmbedding f;

    /**
     * Creates a new text embedding function evaluator.
     *
     * @param f The text embedding function to evaluate
     * @param inferenceRunnerFactory Factory for creating inference runners
     */
    public TextEmbeddingFunctionEvaluator(TextEmbedding f, BulkInferenceRunner.Factory inferenceRunnerFactory) {
        this.inferenceRunnerFactory = inferenceRunnerFactory;
        this.f = f;
    }

    @Override
    public void eval(FoldContext foldContext, ActionListener<Expression> listener) {

        // Set up circuit breaker and big arrays for memory management
        // The circuit breaker prevents out of memory errors by tracking memory usage
        CircuitBreaker breaker = foldContext.circuitBreakerView(f.source());
        BigArrays bigArrays = new BigArrays(null, new CircuitBreakerService() {
            @Override
            public CircuitBreaker getBreaker(String name) {
                if (name.equals(CircuitBreaker.REQUEST) == false) {
                    throw new UnsupportedOperationException();
                }
                return breaker;
            }

            @Override
            public AllCircuitBreakerStats stats() {
                throw new UnsupportedOperationException();
            }

            @Override
            public CircuitBreakerStats stats(String name) {
                throw new UnsupportedOperationException();
            }
        }, CircuitBreaker.REQUEST).withCircuitBreaking();
        DriverContext driverCtx = new DriverContext(bigArrays, new BlockFactory(breaker, bigArrays));

        Layout layout = new Layout.Builder().append(new Alias(f.inputText().source(), "inputText", f.inputText())).build();

        // Create and configure the text embedding operator
        // This operator will handle the actual execution of the embedding model
        Operator textEmbeddingOperator = new TextEmbeddingOperator.Factory(
            inferenceRunnerFactory,
            BytesRefs.toString(f.inferenceId().fold(foldContext)),
            EvalMapper.toEvaluator(foldContext, f.inputText(), layout)
        ).get(driverCtx);

        try {
            driverCtx.waitForAsyncActions(listener.delegateFailureIgnoreResponseAndWrap(l -> {
                Page outputPage = textEmbeddingOperator.getOutput();
                if (outputPage != null) {
                    l.onResponse(new Literal(f.source(), toJavaObject(outputPage.getBlock(0), 0), f.dataType()));
                }
            }));
            textEmbeddingOperator.addInput(new Page(fromArrayRow(driverCtx.blockFactory(), f.inputText().fold(foldContext))));
            driverCtx.finish();

        } catch (Exception e) {
            listener.onFailure(e);
        }
    }
}
