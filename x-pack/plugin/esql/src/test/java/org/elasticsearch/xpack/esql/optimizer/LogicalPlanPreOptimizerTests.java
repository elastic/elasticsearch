/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

import java.util.Arrays;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

public class LogicalPlanPreOptimizerTests extends AbstractLogicalPlanPreOptimizerTests {

    private final TestEmbeddingModel embeddingModel;

    public LogicalPlanPreOptimizerTests(TestEmbeddingModel embeddingModel) {
        this.embeddingModel = embeddingModel;
    }

    @ParametersFactory(argumentFormatting = "%1$s")
    public static Iterable<Object[]> parameters() {
        return Arrays.stream(TestEmbeddingModel.values()).map(textEmbeddingModel -> new Object[] { textEmbeddingModel }).toList();
    }

    /**
     * Tests that the pre-optimizer correctly marks plans as pre-optimized.
     */
    public void testPlanIsMarkedAsPreOptimized() throws Exception {
        for (int round = 0; round < 100; round++) {
            // Create a random plan for testing
            LogicalPlan plan = randomPlan();
            plan.setAnalyzed();

            // Apply pre-optimization
            preOptimizedPlan(preOptimizer(embeddingModel), plan);
        }
    }

    /**
     * Tests that the pre-optimizer fails when given a plan that is not analyzed.
     */
    public void testPreOptimizeFailsPlanIsNotAnalyzed() throws Exception {
        // Create a plan that is not marked as analyzed
        LogicalPlan plan = EsqlTestUtils.relation();
        SetOnce<Exception> exceptionHolder = new SetOnce<>();

        // Apply pre-optimization and expect failure
        preOptimizer(embeddingModel).preOptimize(plan, ActionListener.wrap(r -> fail("Should have failed"), exceptionHolder::set));
        assertBusy(() -> {
            assertThat(exceptionHolder.get(), notNullValue());
            IllegalStateException e = as(exceptionHolder.get(), IllegalStateException.class);
            assertThat(e.getMessage(), equalTo("Expected analyzed plan"));
        });
    }

    /**
     * Executes pre-optimization on the given plan and returns the result.
     */
    protected LogicalPlan preOptimizedPlan(LogicalPlanPreOptimizer preOptimizer, LogicalPlan plan) throws Exception {
        // set plan as analyzed to meet pre-optimizer requirements
        plan.setPreOptimized();

        PlainActionFuture<LogicalPlan> logicalPlanFuture = new PlainActionFuture<>();
        preOptimizer.preOptimize(plan, logicalPlanFuture);

        LogicalPlan preOptimized = logicalPlanFuture.get();

        assertThat(preOptimized, notNullValue());
        assertThat(preOptimized.preOptimized(), equalTo(true));

        return preOptimized;
    }
}
