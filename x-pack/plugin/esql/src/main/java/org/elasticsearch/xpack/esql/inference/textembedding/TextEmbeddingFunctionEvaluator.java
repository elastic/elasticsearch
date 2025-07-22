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
import org.elasticsearch.xpack.esql.inference.InferenceFunctionEvaluator;
import org.elasticsearch.xpack.esql.inference.InferenceRunner;
import org.elasticsearch.xpack.esql.planner.Layout;

import static org.elasticsearch.compute.data.BlockUtils.fromArrayRow;
import static org.elasticsearch.compute.data.BlockUtils.toJavaObject;

public class TextEmbeddingFunctionEvaluator implements InferenceFunctionEvaluator {

    private final InferenceRunner.Factory inferenceRunnerFactory;

    private final TextEmbedding f;

    public TextEmbeddingFunctionEvaluator(TextEmbedding f, InferenceRunner.Factory inferenceRunnerFactory) {
        this.inferenceRunnerFactory = inferenceRunnerFactory;
        this.f = f;
    }

    @Override
    public void eval(FoldContext foldContext, ActionListener<Expression> listener) {

        // Create a driver context to track the inference function execution
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

        // Create an inference operator to execute the inference function
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
