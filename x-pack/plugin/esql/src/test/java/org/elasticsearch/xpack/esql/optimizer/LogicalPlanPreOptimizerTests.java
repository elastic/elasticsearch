/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.Concat;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Add;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.preoptimizer.LogicalPlanPreOptimizerRule;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.junit.After;
import org.junit.Before;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.fieldAttribute;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.of;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

public class LogicalPlanPreOptimizerTests extends ESTestCase {

    private ThreadPool threadPool;

    @Before
    public void setUpThreadPool() {
        threadPool = createThreadPool();
    }

    @After
    public void tearDownThreadPool() {
        terminate(threadPool);
    }

    public void testPlanIsMarkedAsPreOptimized() throws Exception {
        for (int round = 0; round < 100; round++) {
            // We want to make sure that the pre-optimizer works for a wide range of plans
            preOptimizedPlan(randomPlan());
        }
    }

    public void testPreOptimizeFailsIfPlanIsNotAnalyzed() throws Exception {
        LogicalPlan plan = EsqlTestUtils.relation();
        SetOnce<Exception> exceptionHolder = new SetOnce<>();

        preOptimizer().preOptimize(plan, ActionListener.wrap(r -> fail("Should have failed"), exceptionHolder::set));
        assertBusy(() -> {
            assertThat(exceptionHolder.get(), notNullValue());
            IllegalStateException e = as(exceptionHolder.get(), IllegalStateException.class);
            assertThat(e.getMessage(), equalTo("Expected analyzed plan"));
        });
    }

    public void testPreOptimizerRulesAreAppliedInOrder() throws Exception {
        LogicalPlan plan = EsqlTestUtils.relation();
        plan.setPreOptimized();

        StringBuilder executionOrder = new StringBuilder();

        // Create mock rules that track execution order
        LogicalPlanPreOptimizerRule rule1 = createOrderTrackingRule("A", executionOrder);
        LogicalPlanPreOptimizerRule rule2 = createOrderTrackingRule("B", executionOrder);
        LogicalPlanPreOptimizerRule rule3 = createOrderTrackingRule("C", executionOrder);

        LogicalPlanPreOptimizer preOptimizer = new LogicalPlanPreOptimizer(List.of(rule1, rule2, rule3));

        SetOnce<LogicalPlan> resultHolder = new SetOnce<>();

        preOptimizer.preOptimize(plan, ActionListener.wrap(resultHolder::set, ESTestCase::fail));

        assertBusy(() -> {
            assertThat(resultHolder.get(), notNullValue());
            // Rules should be applied in the order they were provided
            assertThat(executionOrder.toString(), equalTo("ABC"));
            assertThat(resultHolder.get().preOptimized(), equalTo(true));
        });
    }

    public void testPreOptimizerWithEmptyRulesList() throws Exception {
        LogicalPlan plan = EsqlTestUtils.relation();
        plan.setPreOptimized();

        LogicalPlanPreOptimizer preOptimizer = new LogicalPlanPreOptimizer(List.of());

        SetOnce<LogicalPlan> resultHolder = new SetOnce<>();

        preOptimizer.preOptimize(plan, ActionListener.wrap(resultHolder::set, ESTestCase::fail));

        assertBusy(() -> {
            assertThat(resultHolder.get(), notNullValue());
            assertThat(resultHolder.get().preOptimized(), equalTo(true));
            // The plan should be the same as the original (no modifications)
            assertThat(resultHolder.get(), equalTo(plan));
        });
    }

    public void testPreOptimizerRuleFailurePropagatesError() throws Exception {
        LogicalPlan plan = EsqlTestUtils.relation();
        plan.setPreOptimized();

        RuntimeException expectedError = new RuntimeException("Mock rule failure");

        AtomicInteger ruleACounter = new AtomicInteger();
        LogicalPlanPreOptimizerRule ruleA = createMockRule(ruleACounter);
        LogicalPlanPreOptimizerRule ruleB = createFailingRule(expectedError);
        AtomicInteger ruleCCounter = new AtomicInteger();
        LogicalPlanPreOptimizerRule ruleC = createMockRule(ruleCCounter);

        LogicalPlanPreOptimizer preOptimizer = new LogicalPlanPreOptimizer(List.of(ruleA, ruleB, ruleC));

        SetOnce<Exception> exceptionHolder = new SetOnce<>();

        preOptimizer.preOptimize(plan, ActionListener.wrap(r -> fail("Should have failed"), exceptionHolder::set));

        assertBusy(() -> {
            assertThat(exceptionHolder.get(), notNullValue());
            assertThat(exceptionHolder.get(), equalTo(expectedError));
            assertThat(ruleACounter.get(), equalTo(1));
            assertThat(ruleCCounter.get(), equalTo(0));
        });
    }

    public LogicalPlan preOptimizedPlan(LogicalPlan plan) throws Exception {
        // set plan as analyzed
        plan.setPreOptimized();

        SetOnce<LogicalPlan> resultHolder = new SetOnce<>();
        SetOnce<Exception> exceptionHolder = new SetOnce<>();

        preOptimizer().preOptimize(plan, ActionListener.wrap(resultHolder::set, exceptionHolder::set));

        if (exceptionHolder.get() != null) {
            throw exceptionHolder.get();
        }

        assertThat(resultHolder.get(), notNullValue());
        assertThat(resultHolder.get().preOptimized(), equalTo(true));

        return resultHolder.get();
    }

    private LogicalPlanPreOptimizer preOptimizer() {
        LogicalPreOptimizerContext preOptimizerContext = new LogicalPreOptimizerContext(FoldContext.small(), null);
        return new LogicalPlanPreOptimizer(preOptimizerContext);
    }

    private LogicalPlan randomPlan() {
        LogicalPlan plan = EsqlTestUtils.relation();
        int numCommands = between(0, 100);

        for (int i = 0; i < numCommands; i++) {
            plan = switch (randomInt(3)) {
                case 0 -> new Eval(Source.EMPTY, plan, List.of(new Alias(Source.EMPTY, randomIdentifier(), randomExpression())));
                case 1 -> new Limit(Source.EMPTY, of(randomInt()), plan);
                case 2 -> new Filter(Source.EMPTY, plan, randomCondition());
                default -> new Project(Source.EMPTY, plan, List.of(new Alias(Source.EMPTY, randomIdentifier(), fieldAttribute())));
            };
        }
        return plan;
    }

    private Expression randomExpression() {
        return switch (randomInt(3)) {
            case 0 -> of(randomInt());
            case 1 -> of(randomIdentifier());
            case 2 -> new Add(Source.EMPTY, of(randomInt()), of(randomDouble()));
            default -> new Concat(Source.EMPTY, of(randomIdentifier()), randomList(1, 10, () -> of(randomIdentifier())));
        };
    }

    private Expression randomCondition() {
        if (randomBoolean()) {
            return EsqlTestUtils.equalsOf(randomExpression(), randomExpression());
        }

        return EsqlTestUtils.greaterThanOf(randomExpression(), randomExpression());
    }

    // Helper methods for creating mock rules

    private LogicalPlanPreOptimizerRule createMockRule(AtomicInteger executionCounter) {
        return (plan, listener) -> {
            threadPool.schedule(() -> {
                executionCounter.incrementAndGet();
                listener.onResponse(plan); // Return the plan unchanged
            }, randomTimeValue(1, 100, TimeUnit.MILLISECONDS), threadPool.executor(ThreadPool.Names.GENERIC));
        };
    }

    private LogicalPlanPreOptimizerRule createOrderTrackingRule(String ruleId, StringBuilder executionOrder) {
        return (plan, listener) -> {
            threadPool.schedule(() -> {
                executionOrder.append(ruleId);
                listener.onResponse(plan); // Return the plan unchanged
            }, randomTimeValue(1, 100, TimeUnit.MILLISECONDS), threadPool.executor(ThreadPool.Names.GENERIC));
        };
    }

    private LogicalPlanPreOptimizerRule createFailingRule(Exception error) {
        return (plan, listener) -> listener.onFailure(error);
    }
}
