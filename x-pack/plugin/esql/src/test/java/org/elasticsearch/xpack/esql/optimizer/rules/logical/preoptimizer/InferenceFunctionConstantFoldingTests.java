/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical.preoptimizer;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.function.inference.TextEmbedding;
import org.elasticsearch.xpack.esql.expression.function.vector.Knn;
import org.elasticsearch.xpack.esql.optimizer.AbstractLogicalPlanPreOptimizerTests;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

import java.util.Arrays;
import java.util.List;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getFieldAttribute;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.of;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.relation;
import static org.elasticsearch.xpack.esql.core.type.DataType.DENSE_VECTOR;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class InferenceFunctionConstantFoldingTests extends AbstractLogicalPlanPreOptimizerTests {

    private final TestEmbeddingModel embeddingModel;

    public InferenceFunctionConstantFoldingTests(TestEmbeddingModel embeddingModel) {
        this.embeddingModel = embeddingModel;
    }

    @ParametersFactory(argumentFormatting = "textEmbeddingType=%1$s")
    public static Iterable<Object[]> parameters() {
        return Arrays.stream(TestEmbeddingModel.values()).map(textEmbeddingModel -> new Object[] { textEmbeddingModel }).toList();
    }

    /**
     * Tests that the rule correctly evaluates TEXT_EMBEDDING functions in Eval nodes.
     */
    public void testEvalFunctionEmbedding() throws Exception {
        // Setup: Create a plan with an Eval node containing a TEXT_EMBEDDING function
        String inferenceId = randomUUID();
        String query = String.join(" ", randomArray(1, String[]::new, () -> randomAlphaOfLength(randomIntBetween(1, 10))));
        String fieldName = randomIdentifier();

        EsRelation relation = relation();
        Eval eval = new Eval(
            Source.EMPTY,
            relation,
            List.of(new Alias(Source.EMPTY, fieldName, new TextEmbedding(Source.EMPTY, of(query), of(inferenceId))))
        );
        eval.setAnalyzed();

        Eval preOptimizedEval = as(inferenceFunctionConstantFolding(eval), Eval.class);

        assertThat(preOptimizedEval.fields(), hasSize(1));
        assertThat(preOptimizedEval.fields().get(0).name(), equalTo(fieldName));
        Literal preOptimizedQuery = as(preOptimizedEval.fields().get(0).child(), Literal.class);
        assertThat(preOptimizedQuery.dataType(), equalTo(DENSE_VECTOR));
        assertThat(preOptimizedQuery.value(), equalTo(embedding(embeddingModel, query)));
    }

    /**
     * Tests that the rule correctly evaluates TEXT_EMBEDDING functions as KNN query.
     */
    public void testKnnFunctionEmbedding() throws Exception {
        // Setup: Create a plan with a Filter node containing a KNN predicate with a TEXT_EMBEDDING function
        String inferenceId = randomUUID();
        String query = String.join(" ", randomArray(1, String[]::new, () -> randomAlphaOfLength(randomIntBetween(1, 10))));

        EsRelation relation = relation();
        Filter filter = new Filter(
            Source.EMPTY,
            relation,
            new Knn(Source.EMPTY, getFieldAttribute("a"), new TextEmbedding(Source.EMPTY, of(query), of(inferenceId)), of(10), null)
        );
        Knn knn = as(filter.condition(), Knn.class);

        Filter preOptimizedFilter = as(inferenceFunctionConstantFolding(filter), Filter.class);

        Knn preOptimizedKnn = as(preOptimizedFilter.condition(), Knn.class);
        assertThat(preOptimizedKnn.field(), equalTo(knn.field()));
        assertThat(preOptimizedKnn.k(), equalTo(knn.k()));
        assertThat(preOptimizedKnn.options(), equalTo(knn.options()));

        Literal preOptimizedQuery = as(preOptimizedKnn.query(), Literal.class);
        assertThat(preOptimizedQuery.dataType(), equalTo(DENSE_VECTOR));
        assertThat(preOptimizedQuery.value(), equalTo(embedding(embeddingModel, query)));
    }

    /**
     * Helper method to apply the InferenceFunctionConstantFolding rule to a logical plan.
     */
    private LogicalPlan inferenceFunctionConstantFolding(LogicalPlan plan) {
        PlainActionFuture<LogicalPlan> preOptimized = new PlainActionFuture<>();
        new InferenceFunctionConstantFolding(mockBulkInferenceRunner(embeddingModel), FoldContext.small()).apply(plan, preOptimized);
        return preOptimized.actionGet();
    }
}
