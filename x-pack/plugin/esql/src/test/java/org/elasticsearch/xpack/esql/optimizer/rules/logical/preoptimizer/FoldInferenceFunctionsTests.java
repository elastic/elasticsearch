/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical.preoptimizer;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.function.inference.InferenceFunction;
import org.elasticsearch.xpack.esql.expression.function.inference.TextEmbedding;
import org.elasticsearch.xpack.esql.expression.function.vector.Knn;
import org.elasticsearch.xpack.esql.inference.InferenceFunctionEvaluator;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

import java.util.List;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getFieldAttribute;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.of;
import static org.elasticsearch.xpack.esql.core.type.DataType.DENSE_VECTOR;
import static org.elasticsearch.xpack.esql.optimizer.LocalLogicalPlanOptimizerTests.relation;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

public class FoldInferenceFunctionsTests extends ESTestCase {
    /**
     * Tests that the rule correctly evaluates TEXT_EMBEDDING functions in Eval nodes.
     */
    public void testEvalFunctionEmbedding() throws Exception {
        for (int round = 0; round < 100; round++) {
            // Setup: Create a plan with an Eval node containing a TEXT_EMBEDDING function
            String inferenceId = randomUUID();
            String query = String.join(" ", randomArray(1, String[]::new, () -> randomAlphaOfLength(randomIntBetween(1, 10))));
            int dimensions = randomIntBetween(1, 2048);
            String fieldName = randomIdentifier();

            logger.info("query sent: {}", query);

            EsRelation relation = relation();
            Eval eval = new Eval(
                Source.EMPTY,
                relation,
                List.of(new Alias(Source.EMPTY, fieldName, new TextEmbedding(Source.EMPTY, of(query), of(inferenceId))))
            );
            eval.setAnalyzed();

            SetOnce<LogicalPlan> resultHolder = new SetOnce<>();
            createFoldInferenceFunctionRule(dimensions).apply(eval, ActionListener.wrap(resultHolder::set, ESTestCase::fail));

            assertBusy(() -> {
                Eval preOptimizedEval = as(resultHolder.get(), Eval.class);
                assertThat(preOptimizedEval.fields(), hasSize(1));
                assertThat(preOptimizedEval.fields().get(0).name(), equalTo(fieldName));
                Literal preOptimizedQuery = as(preOptimizedEval.fields().get(0).child(), Literal.class);
                assertThat(preOptimizedQuery.dataType(), equalTo(DENSE_VECTOR));
                assertThat(preOptimizedQuery.value(), equalTo(generateTestEmbedding(query, dimensions)));
            });

        }
    }

    /**
     * Tests that the rule correctly evaluates TEXT_EMBEDDING functions as KNN query.
     */
    public void testKnnFunctionEmbedding() throws Exception {
        for (int round = 0; round < 100; round++) {
            // Setup: Create a plan with a Filter node containing a KNN predicate with a TEXT_EMBEDDING function
            String inferenceId = randomUUID();
            String query = String.join(" ", randomArray(1, String[]::new, () -> randomAlphaOfLength(randomIntBetween(1, 10))));
            int dimensions = randomIntBetween(1, 2048);

            EsRelation relation = relation();
            Filter filter = new Filter(
                Source.EMPTY,
                relation,
                new Knn(Source.EMPTY, getFieldAttribute("a"), new TextEmbedding(Source.EMPTY, of(query), of(inferenceId)), null)
            );
            Knn knn = as(filter.condition(), Knn.class);

            SetOnce<LogicalPlan> resultHolder = new SetOnce<>();
            createFoldInferenceFunctionRule(dimensions).apply(filter, ActionListener.wrap(resultHolder::set, ESTestCase::fail));

            assertBusy(() -> {
                Filter preOptimizedFilter = as(resultHolder.get(), Filter.class);

                Knn preOptimizedKnn = as(preOptimizedFilter.condition(), Knn.class);
                assertThat(preOptimizedKnn.field(), equalTo(knn.field()));
                assertThat(preOptimizedKnn.k(), equalTo(knn.k()));
                assertThat(preOptimizedKnn.options(), equalTo(knn.options()));

                Literal preOptimizedQuery = as(preOptimizedKnn.query(), Literal.class);
                assertThat(preOptimizedQuery.dataType(), equalTo(DENSE_VECTOR));
                assertThat(preOptimizedQuery.value(), equalTo(generateTestEmbedding(query, dimensions)));
            });
        }
    }

    @SuppressWarnings("unchecked")
    private LogicalPlanPreOptimizerRule createFoldInferenceFunctionRule(int dimensions) {
        InferenceFunctionEvaluator functionEvaluator = mock(InferenceFunctionEvaluator.class);

        doAnswer((i) -> {
            ActionListener<Expression> l = i.getArgument(1, ActionListener.class);
            InferenceFunction<?> function = i.getArgument(0, InferenceFunction.class);
            if (function instanceof TextEmbedding textEmbedding) {
                BytesRef bytesRef = (BytesRef) as(textEmbedding.inputText(), Literal.class).value();
                float[] embedding = generateTestEmbedding(bytesRef.utf8ToString(), dimensions);
                l.onResponse(Literal.of(function, embedding));
            } else {
                fail("Unexpected function type: " + function.getClass().getSimpleName());
            }
            return null;
        }).when(functionEvaluator).fold(any(InferenceFunction.class), any(ActionListener.class));
        return new FoldInferenceFunctions(functionEvaluator);
    }

    /**
     * Generates a deterministic mock embedding vector based on the input text.
     * This ensures our tests are repeatable and verifiable.
     */
    private float[] generateTestEmbedding(String inputText, int dimensions) {
        float[] embedding = new float[dimensions];

        for (int i = 0; i < dimensions; i++) {
            embedding[i] = generateMockFloatEmbeddingValue(inputText, i);
        }

        return embedding;
    }

    /**
     * Generates a single embedding value for a specific dimension based on input text.
     * Uses a deterministic function so tests are repeatable.
     */
    private float generateMockFloatEmbeddingValue(String inputText, int dimension) {
        // Create a deterministic value based on input text and dimension
        int hash = (inputText.hashCode() + dimension * 31) % 10000;
        return hash / 10000.0f; // Normalize to [0, 1) range
    }
}
