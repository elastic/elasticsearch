/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.expression.Order;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToInteger;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.esql.plan.logical.Enrich;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.MvExpand;
import org.elasticsearch.xpack.esql.plan.logical.OrderBy;
import org.elasticsearch.xpack.esql.plan.logical.UnaryPlan;
import org.elasticsearch.xpack.esql.plan.logical.inference.Completion;
import org.elasticsearch.xpack.esql.plan.logical.inference.Rerank;
import org.elasticsearch.xpack.esql.plan.logical.join.Join;
import org.elasticsearch.xpack.esql.plan.logical.join.JoinConfig;
import org.elasticsearch.xpack.esql.plan.logical.join.JoinTypes;

import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getFieldAttribute;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.randomLiteral;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.unboundLogicalOptimizerContext;
import static org.elasticsearch.xpack.esql.core.tree.Source.EMPTY;
import static org.elasticsearch.xpack.esql.core.type.DataType.INTEGER;
import static org.elasticsearch.xpack.esql.core.type.DataType.KEYWORD;
import static org.elasticsearch.xpack.esql.core.type.DataType.TEXT;
import static org.elasticsearch.xpack.esql.optimizer.LocalLogicalPlanOptimizerTests.relation;

public class PushDownAndCombineLimitsTests extends ESTestCase {

    private static class PushDownLimitTestCase<PlanType extends LogicalPlan> {
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
            (plan, attr) -> new Completion(EMPTY, plan, randomLiteral(KEYWORD), randomLiteral(KEYWORD), attr),
            (basePlan, optimizedPlan) -> {
                assertEquals(basePlan.source(), optimizedPlan.source());
                assertEquals(basePlan.inferenceId(), optimizedPlan.inferenceId());
                assertEquals(basePlan.prompt(), optimizedPlan.prompt());
                assertEquals(basePlan.targetField(), optimizedPlan.targetField());
            }
        ),
        new PushDownLimitTestCase<>(
            Rerank.class,
            (plan, attr) -> new Rerank(
                EMPTY,
                plan,
                randomLiteral(KEYWORD),
                randomLiteral(KEYWORD),
                randomList(1, 10, () -> new Alias(EMPTY, randomIdentifier(), randomLiteral(KEYWORD))),
                attr
            ),
            (basePlan, optimizedPlan) -> {
                assertEquals(basePlan.source(), optimizedPlan.source());
                assertEquals(basePlan.inferenceId(), optimizedPlan.inferenceId());
                assertEquals(basePlan.queryText(), optimizedPlan.queryText());
                assertEquals(basePlan.rerankFields(), optimizedPlan.rerankFields());
                assertEquals(basePlan.scoreAttribute(), optimizedPlan.scoreAttribute());
            }
        ),
        new PushDownLimitTestCase<>(
            Enrich.class,
            (plan, attr) -> new Enrich(
                EMPTY,
                plan,
                randomFrom(Enrich.Mode.ANY, Enrich.Mode.COORDINATOR),
                randomLiteral(KEYWORD),
                attr,
                null,
                Map.of(),
                List.of()
            ),
            (basePlan, optimizedPlan) -> {
                assertEquals(basePlan.source(), optimizedPlan.source());
                assertEquals(basePlan.mode(), optimizedPlan.mode());
                assertEquals(basePlan.policyName(), optimizedPlan.policyName());
                assertEquals(basePlan.matchField(), optimizedPlan.matchField());
            }
        )
    );

    private static final List<PushDownLimitTestCase<? extends UnaryPlan>> NON_PUSHABLE_LIMIT_TEST_CASES = List.of(
        new PushDownLimitTestCase<>(
            Filter.class,
            (plan, attr) -> new Filter(EMPTY, plan, new Equals(EMPTY, attr, new Literal(EMPTY, BytesRefs.toBytesRef("right"), TEXT))),
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

    private static final List<PushDownLimitTestCase<? extends LogicalPlan>> DUPLICATING_TEST_CASES = List.of(
        new PushDownLimitTestCase<>(
            Enrich.class,
            (plan, attr) -> new Enrich(EMPTY, plan, Enrich.Mode.REMOTE, randomLiteral(KEYWORD), attr, null, Map.of(), List.of()),
            (basePlan, optimizedPlan) -> {
                assertEquals(basePlan.source(), optimizedPlan.source());
                assertEquals(basePlan.mode(), optimizedPlan.mode());
                assertEquals(basePlan.policyName(), optimizedPlan.policyName());
                assertEquals(basePlan.matchField(), optimizedPlan.matchField());
                var limit = as(optimizedPlan.child(), Limit.class);
                assertTrue(limit.local());
                assertFalse(limit.duplicated());
            }
        ),
        new PushDownLimitTestCase<>(MvExpand.class, (plan, attr) -> new MvExpand(EMPTY, plan, attr, attr), (basePlan, optimizedPlan) -> {
            assertEquals(basePlan.source(), optimizedPlan.source());
            assertEquals(basePlan.expanded(), optimizedPlan.expanded());
            var limit = as(optimizedPlan.child(), Limit.class);
            assertFalse(limit.local());
            assertFalse(limit.duplicated());
        }),
        new PushDownLimitTestCase<>(
            Join.class,
            (plan, attr) -> new Join(EMPTY, plan, plan, new JoinConfig(JoinTypes.LEFT, List.of(), List.of(), attr)),
            (basePlan, optimizedPlan) -> {
                assertEquals(basePlan.source(), optimizedPlan.source());
                var limit = as(optimizedPlan.left(), Limit.class);
                assertFalse(limit.local());
                assertFalse(limit.duplicated());
            }
        )

    );

    public void testPushableLimitDuplicate() {
        FieldAttribute a = getFieldAttribute("a");
        FieldAttribute b = getFieldAttribute("b");
        EsRelation relation = relation().withAttributes(List.of(a, b));

        for (PushDownLimitTestCase<? extends LogicalPlan> duplicatingTestCase : DUPLICATING_TEST_CASES) {
            int precedingLimitValue = randomIntBetween(1, 10_000);
            Limit precedingLimit = new Limit(EMPTY, new Literal(EMPTY, precedingLimitValue, INTEGER), relation);
            LogicalPlan duplicatingLimitTestPlan = duplicatingTestCase.buildPlan(precedingLimit, a);
            // Explicitly raise the probability of equal limits, to test for https://github.com/elastic/elasticsearch/issues/139250
            int upperLimitValue = randomBoolean() ? precedingLimitValue : randomIntBetween(1, precedingLimitValue);
            Limit upperLimit = new Limit(EMPTY, new Literal(EMPTY, upperLimitValue, INTEGER), duplicatingLimitTestPlan);
            Limit optimizedPlan = as(optimizePlan(upperLimit), Limit.class);
            duplicatingTestCase.checkOptimizedPlan(duplicatingLimitTestPlan, optimizedPlan.child());
            assertTrue(optimizedPlan.duplicated());
            assertFalse(optimizedPlan.local());
        }
    }

    private LogicalPlan optimizePlan(LogicalPlan plan) {
        return new PushDownAndCombineLimits().apply(plan, unboundLogicalOptimizerContext());
    }
}
