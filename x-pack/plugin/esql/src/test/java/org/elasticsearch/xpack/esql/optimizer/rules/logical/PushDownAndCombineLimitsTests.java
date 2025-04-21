/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.expression.Order;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToInteger;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.OrderBy;
import org.elasticsearch.xpack.esql.plan.logical.UnaryPlan;
import org.elasticsearch.xpack.esql.plan.logical.inference.Completion;

import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getFieldAttribute;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.randomLiteral;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.unboundLogicalOptimizerContext;
import static org.elasticsearch.xpack.esql.core.tree.Source.EMPTY;
import static org.elasticsearch.xpack.esql.core.type.DataType.INTEGER;
import static org.elasticsearch.xpack.esql.core.type.DataType.TEXT;
import static org.elasticsearch.xpack.esql.optimizer.LocalLogicalPlanOptimizerTests.relation;

public class PushDownAndCombineLimitsTests extends ESTestCase {

    private static class PushDownLimitTestCase<PlanType extends UnaryPlan> {
        private final Class<PlanType> clazz;
        private final BiFunction<LogicalPlan, Attribute, PlanType> planBuilder;
        private final BiConsumer<PlanType, PlanType> planChecker;

        PushDownLimitTestCase(
            Class<PlanType> clazz,
            BiFunction<LogicalPlan, Attribute, PlanType> planBuilder,
            BiConsumer<PlanType, PlanType> planChecker
        ) {
            this.clazz = clazz;
            this.planBuilder = planBuilder;
            this.planChecker = planChecker;
        }

        public PlanType buildPlan(LogicalPlan child, Attribute attr) {
            return planBuilder.apply(child, attr);
        }

        public void checkOptimizedPlan(LogicalPlan basePlan, LogicalPlan optimizedPlan) {
            planChecker.accept(as(basePlan, clazz), as(optimizedPlan, clazz));
        }
    }

    private static final List<PushDownLimitTestCase<? extends UnaryPlan>> PUSHABLE_LIMIT_TEST_CASES = List.of(
        new PushDownLimitTestCase<>(
            Eval.class,
            (plan, attr) -> new Eval(EMPTY, plan, List.of(new Alias(EMPTY, "y", new ToInteger(EMPTY, attr)))),
            (basePlan, optimizedPlan) -> {
                assertEquals(basePlan.source(), optimizedPlan.source());
                assertEquals(basePlan.fields(), optimizedPlan.fields());
            }
        ),
        new PushDownLimitTestCase<>(
            Completion.class,
            (plan, attr) -> new Completion(EMPTY, plan, randomLiteral(TEXT), randomLiteral(TEXT), attr),
            (basePlan, optimizedPlan) -> {
                assertEquals(basePlan.source(), optimizedPlan.source());
                assertEquals(basePlan.inferenceId(), optimizedPlan.inferenceId());
                assertEquals(basePlan.prompt(), optimizedPlan.prompt());
                assertEquals(basePlan.targetField(), optimizedPlan.targetField());
            }
        )
    );

    private static final List<PushDownLimitTestCase<? extends UnaryPlan>> NON_PUSHABLE_LIMIT_TEST_CASES = List.of(
        new PushDownLimitTestCase<>(
            Filter.class,
            (plan, attr) -> new Filter(EMPTY, plan, new Equals(EMPTY, attr, new Literal(EMPTY, "right", TEXT))),
            (basePlan, optimizedPlan) -> {
                assertEquals(basePlan.source(), optimizedPlan.source());
                assertEquals(basePlan.condition(), optimizedPlan.condition());
            }
        ),
        new PushDownLimitTestCase<>(
            OrderBy.class,
            (plan, attr) -> new OrderBy(EMPTY, plan, List.of(new Order(EMPTY, attr, Order.OrderDirection.DESC, null))),
            (basePlan, optimizedPlan) -> {
                assertEquals(basePlan.source(), optimizedPlan.source());
                assertEquals(basePlan.order(), optimizedPlan.order());
            }
        )
    );

    public void testPushableLimit() {
        FieldAttribute a = getFieldAttribute("a");
        FieldAttribute b = getFieldAttribute("b");
        EsRelation relation = relation().withAttributes(List.of(a, b));

        for (PushDownLimitTestCase<? extends UnaryPlan> pushableLimitTestCase : PUSHABLE_LIMIT_TEST_CASES) {
            int precedingLimitValue = randomIntBetween(1, 10_000);
            Limit precedingLimit = new Limit(EMPTY, new Literal(EMPTY, precedingLimitValue, INTEGER), relation);

            LogicalPlan pushableLimitTestPlan = pushableLimitTestCase.buildPlan(precedingLimit, a);

            int pushableLimitValue = randomIntBetween(1, 10_000);
            Limit pushableLimit = new Limit(EMPTY, new Literal(EMPTY, pushableLimitValue, INTEGER), pushableLimitTestPlan);

            LogicalPlan optimizedPlan = optimizePlan(pushableLimit);

            pushableLimitTestCase.checkOptimizedPlan(pushableLimitTestPlan, optimizedPlan);

            assertEquals(
                as(optimizedPlan, UnaryPlan.class).child(),
                new Limit(EMPTY, new Literal(EMPTY, Math.min(pushableLimitValue, precedingLimitValue), INTEGER), relation)
            );
        }
    }

    public void testNonPushableLimit() {
        FieldAttribute a = getFieldAttribute("a");
        FieldAttribute b = getFieldAttribute("b");
        EsRelation relation = relation().withAttributes(List.of(a, b));

        for (PushDownLimitTestCase<? extends UnaryPlan> nonPushableLimitTestCase : NON_PUSHABLE_LIMIT_TEST_CASES) {
            int precedingLimitValue = randomIntBetween(1, 10_000);
            Limit precedingLimit = new Limit(EMPTY, new Literal(EMPTY, precedingLimitValue, INTEGER), relation);
            UnaryPlan nonPushableLimitTestPlan = nonPushableLimitTestCase.buildPlan(precedingLimit, a);
            int nonPushableLimitValue = randomIntBetween(1, 10_000);
            Limit nonPushableLimit = new Limit(EMPTY, new Literal(EMPTY, nonPushableLimitValue, INTEGER), nonPushableLimitTestPlan);
            Limit optimizedPlan = as(optimizePlan(nonPushableLimit), Limit.class);
            nonPushableLimitTestCase.checkOptimizedPlan(nonPushableLimitTestPlan, optimizedPlan.child());
            assertEquals(
                optimizedPlan,
                new Limit(
                    EMPTY,
                    new Literal(EMPTY, Math.min(nonPushableLimitValue, precedingLimitValue), INTEGER),
                    nonPushableLimitTestPlan
                )
            );
            assertEquals(as(optimizedPlan.child(), UnaryPlan.class).child(), nonPushableLimitTestPlan.child());
        }
    }

    private LogicalPlan optimizePlan(LogicalPlan plan) {
        return new PushDownAndCombineLimits().apply(plan, unboundLogicalOptimizerContext());
    }
}
