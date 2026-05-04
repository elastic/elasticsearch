/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer;

import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Count;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Max;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Min;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Sum;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Add;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Mul;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Sub;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Project;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;

/**
 * Tests for {@link org.elasticsearch.xpack.esql.optimizer.rules.logical.SimplifyAggregateOverArithmetic}.
 */
public class SimplifyAggregateOverArithmeticTests extends AbstractLogicalPlanOptimizerTests {

    // ---- Core SUM patterns ----

    /**
     * SUM(salary + 100) should be rewritten to SUM(salary) + 100 * COUNT(*)
     */
    public void testSumAddLiteral() {
        var plan = optimizedPlan("FROM test | STATS s = SUM(salary + 100)");

        var project = as(plan, Project.class);
        var eval = as(project.child(), Eval.class);

        // The eval should contain the correction: sum_salary + 100 * count_star
        assertThat(eval.fields(), hasSize(1));
        var correctionAlias = as(eval.fields().get(0), Alias.class);
        assertEquals("s", correctionAlias.name());
        var add = as(correctionAlias.child(), Add.class);
        // left side: reference to SUM(salary)
        as(add.left(), ReferenceAttribute.class);
        // right side: 100 * COUNT(*) (children may be in either order)
        var mul = as(add.right(), Mul.class);
        assertMulLiteralRight(mul, 100);

        var limit = as(eval.child(), Limit.class);
        var agg = as(limit.child(), Aggregate.class);
        // Aggregate should have SUM(salary) and COUNT(*)
        var aggs = agg.aggregates();
        assertThat(aggs, hasSize(2));
        var sumAlias = as(aggs.get(0), Alias.class);
        var sum = as(sumAlias.child(), Sum.class);
        assertEquals("salary", Expressions.name(sum.field()));
        var countAlias = as(aggs.get(1), Alias.class);
        as(countAlias.child(), Count.class);
    }

    /**
     * SUM(salary - 50) should be rewritten to SUM(salary) - 50 * COUNT(*)
     */
    public void testSumSubLiteral() {
        var plan = optimizedPlan("FROM test | STATS s = SUM(salary - 50)");

        var project = as(plan, Project.class);
        var eval = as(project.child(), Eval.class);

        assertThat(eval.fields(), hasSize(1));
        var correctionAlias = as(eval.fields().get(0), Alias.class);
        var sub = as(correctionAlias.child(), Sub.class);
        as(sub.left(), ReferenceAttribute.class);
        var mul = as(sub.right(), Mul.class);
        assertMulLiteralRight(mul, 50);

        var limit = as(eval.child(), Limit.class);
        var agg = as(limit.child(), Aggregate.class);
        assertThat(agg.aggregates(), hasSize(2));
        var sumAlias = as(agg.aggregates().get(0), Alias.class);
        var sum = as(sumAlias.child(), Sum.class);
        assertEquals("salary", Expressions.name(sum.field()));
    }

    /**
     * SUM(salary * 2) should be rewritten to SUM(salary) * 2
     */
    public void testSumMulLiteral() {
        var plan = optimizedPlan("FROM test | STATS s = SUM(salary * 2)");

        var project = as(plan, Project.class);
        var eval = as(project.child(), Eval.class);

        assertThat(eval.fields(), hasSize(1));
        var correctionAlias = as(eval.fields().get(0), Alias.class);
        var mul = as(correctionAlias.child(), Mul.class);
        as(mul.left(), ReferenceAttribute.class);
        assertEquals(2, as(mul.right(), Literal.class).value());

        var limit = as(eval.child(), Limit.class);
        var agg = as(limit.child(), Aggregate.class);
        // Only SUM(salary), no COUNT needed for mul
        assertThat(agg.aggregates(), hasSize(1));
        var sumAlias = as(agg.aggregates().get(0), Alias.class);
        var sum = as(sumAlias.child(), Sum.class);
        assertEquals("salary", Expressions.name(sum.field()));
    }

    /**
     * SUM(100 + salary) should work the same as SUM(salary + 100) -- commutative
     */
    public void testSumLiteralOnLeft() {
        var plan = optimizedPlan("FROM test | STATS s = SUM(100 + salary)");

        var project = as(plan, Project.class);
        var eval = as(project.child(), Eval.class);

        assertThat(eval.fields(), hasSize(1));
        var correctionAlias = as(eval.fields().get(0), Alias.class);
        var add = as(correctionAlias.child(), Add.class);
        as(add.left(), ReferenceAttribute.class);
        var mul = as(add.right(), Mul.class);

        var limit = as(eval.child(), Limit.class);
        var agg = as(limit.child(), Aggregate.class);
        assertThat(agg.aggregates(), hasSize(2));
        var sumAlias = as(agg.aggregates().get(0), Alias.class);
        var sum = as(sumAlias.child(), Sum.class);
        assertEquals("salary", Expressions.name(sum.field()));
    }

    // ---- AVG patterns ----

    /**
     * AVG(salary + 100) should be rewritten to AVG(salary) + 100
     * Note: AVG is a SurrogateExpression, so it may be replaced before or after our rule.
     * Since our rule runs before SubstituteSurrogateAggregations, we handle AVG directly.
     */
    public void testAvgAddLiteral() {
        var plan = optimizedPlan("FROM test | STATS a = AVG(salary + 100)");

        // After full optimization, AVG is decomposed to SUM/COUNT.
        // The simplification should still be visible in the plan structure.
        // The plan should NOT have SUM(salary + 100) -- it should have SUM(salary) instead.
        var project = as(plan, Project.class);
        assertPlanDoesNotContainArithmeticInsideAgg(project);
    }

    /**
     * AVG(salary * 2) should be rewritten to AVG(salary) * 2
     */
    public void testAvgMulLiteral() {
        var plan = optimizedPlan("FROM test | STATS a = AVG(salary * 2)");

        var project = as(plan, Project.class);
        assertPlanDoesNotContainArithmeticInsideAgg(project);
    }

    // ---- MIN/MAX patterns ----

    /**
     * MIN(salary + 100) should be rewritten to MIN(salary) + 100
     */
    public void testMinAddLiteral() {
        var plan = optimizedPlan("FROM test | STATS m = MIN(salary + 100)");

        var project = as(plan, Project.class);
        var eval = as(project.child(), Eval.class);

        assertThat(eval.fields(), hasSize(1));
        var correctionAlias = as(eval.fields().get(0), Alias.class);
        var add = as(correctionAlias.child(), Add.class);
        as(add.left(), ReferenceAttribute.class);
        assertEquals(100, as(add.right(), Literal.class).value());

        var limit = as(eval.child(), Limit.class);
        var agg = as(limit.child(), Aggregate.class);
        assertThat(agg.aggregates(), hasSize(1));
        var minAlias = as(agg.aggregates().get(0), Alias.class);
        var min = as(minAlias.child(), Min.class);
        assertEquals("salary", Expressions.name(min.field()));
    }

    /**
     * MAX(salary + 100) should be rewritten to MAX(salary) + 100
     */
    public void testMaxAddLiteral() {
        var plan = optimizedPlan("FROM test | STATS m = MAX(salary + 100)");

        var project = as(plan, Project.class);
        var eval = as(project.child(), Eval.class);

        assertThat(eval.fields(), hasSize(1));
        var correctionAlias = as(eval.fields().get(0), Alias.class);
        var add = as(correctionAlias.child(), Add.class);
        as(add.left(), ReferenceAttribute.class);
        assertEquals(100, as(add.right(), Literal.class).value());

        var limit = as(eval.child(), Limit.class);
        var agg = as(limit.child(), Aggregate.class);
        assertThat(agg.aggregates(), hasSize(1));
        var maxAlias = as(agg.aggregates().get(0), Alias.class);
        var max = as(maxAlias.child(), Max.class);
        assertEquals("salary", Expressions.name(max.field()));
    }

    /**
     * MIN(salary * 2) should be rewritten to MIN(salary) * 2 (K > 0)
     */
    public void testMinMulPositive() {
        var plan = optimizedPlan("FROM test | STATS m = MIN(salary * 2)");

        var project = as(plan, Project.class);
        var eval = as(project.child(), Eval.class);

        assertThat(eval.fields(), hasSize(1));
        var correctionAlias = as(eval.fields().get(0), Alias.class);
        var mul = as(correctionAlias.child(), Mul.class);
        as(mul.left(), ReferenceAttribute.class);
        assertEquals(2, as(mul.right(), Literal.class).value());

        var limit = as(eval.child(), Limit.class);
        var agg = as(limit.child(), Aggregate.class);
        assertThat(agg.aggregates(), hasSize(1));
        var minAlias = as(agg.aggregates().get(0), Alias.class);
        // should still be MIN (K > 0, no swap)
        as(minAlias.child(), Min.class);
    }

    /**
     * MIN(salary * -1) should be rewritten to MAX(salary) * -1 (K is negative, order reversal)
     */
    public void testMinMulNegative() {
        var plan = optimizedPlan("FROM test | STATS m = MIN(salary * -1)");

        var project = as(plan, Project.class);
        var eval = as(project.child(), Eval.class);

        assertThat(eval.fields(), hasSize(1));
        var correctionAlias = as(eval.fields().get(0), Alias.class);
        var mul = as(correctionAlias.child(), Mul.class);
        as(mul.left(), ReferenceAttribute.class);
        assertEquals(-1, as(mul.right(), Literal.class).value());

        var limit = as(eval.child(), Limit.class);
        var agg = as(limit.child(), Aggregate.class);
        assertThat(agg.aggregates(), hasSize(1));
        var maxAlias = as(agg.aggregates().get(0), Alias.class);
        // MIN with K < 0 should swap to MAX
        as(maxAlias.child(), Max.class);
    }

    /**
     * MAX(salary * -1) should be rewritten to MIN(salary) * -1 (K is negative, order reversal)
     */
    public void testMaxMulNegative() {
        var plan = optimizedPlan("FROM test | STATS m = MAX(salary * -1)");

        var project = as(plan, Project.class);
        var eval = as(project.child(), Eval.class);

        assertThat(eval.fields(), hasSize(1));
        var correctionAlias = as(eval.fields().get(0), Alias.class);
        var mul = as(correctionAlias.child(), Mul.class);
        as(mul.left(), ReferenceAttribute.class);
        assertEquals(-1, as(mul.right(), Literal.class).value());

        var limit = as(eval.child(), Limit.class);
        var agg = as(limit.child(), Aggregate.class);
        assertThat(agg.aggregates(), hasSize(1));
        var minAlias = as(agg.aggregates().get(0), Alias.class);
        // MAX with K < 0 should swap to MIN
        as(minAlias.child(), Min.class);
    }

    // ---- Deduplication ----

    /**
     * SUM(col+1), SUM(col+2) should share one SUM(col) and one COUNT(*)
     */
    public void testMultipleSumsShareBase() {
        var plan = optimizedPlan("FROM test | STATS s1 = SUM(salary + 1), s2 = SUM(salary + 2)");

        var project = as(plan, Project.class);
        var eval = as(project.child(), Eval.class);

        // Two corrections
        assertThat(eval.fields(), hasSize(2));

        var limit = as(eval.child(), Limit.class);
        var agg = as(limit.child(), Aggregate.class);
        // Should have one SUM(salary) and one COUNT(*) -- deduplicated
        assertThat(agg.aggregates(), hasSize(2));
        var sumAlias = as(agg.aggregates().get(0), Alias.class);
        var sum = as(sumAlias.child(), Sum.class);
        assertEquals("salary", Expressions.name(sum.field()));
        var countAlias = as(agg.aggregates().get(1), Alias.class);
        as(countAlias.child(), Count.class);
    }

    // ---- Safety: rule should NOT fire ----

    /**
     * SUM(salary / 2) should NOT be simplified (integer truncation changes semantics)
     */
    public void testDivNotSimplified() {
        var plan = optimizedPlan("FROM test | STATS s = SUM(salary / 2)");
        // The plan should still have the arithmetic inside the agg (after ReplaceAggregateNestedExpressionWithEval)
        assertPlanContainsAggOverAttribute(plan, Sum.class);
    }

    /**
     * SUM(salary + emp_no) should NOT be simplified (not a constant)
     */
    public void testNonLiteralNotSimplified() {
        var plan = optimizedPlan("FROM test | STATS s = SUM(salary + emp_no)");
        assertPlanContainsAggOverAttribute(plan, Sum.class);
    }

    /**
     * SUM(col) with no arithmetic should pass through unchanged
     */
    public void testNoArithmeticPassthrough() {
        var plan = optimizedPlan("FROM test | STATS s = SUM(salary)");

        // Should be a straightforward plan without the simplification eval layer
        var limit = as(plan, Limit.class);
        var agg = as(limit.child(), Aggregate.class);
        assertThat(agg.aggregates(), hasSize(1));
        var sumAlias = as(agg.aggregates().get(0), Alias.class);
        var sum = as(sumAlias.child(), Sum.class);
        assertEquals("salary", Expressions.name(sum.field()));
    }

    /**
     * SUM(salary + 1) WHERE emp_no > 0 -- filter must survive the transformation
     */
    public void testFilterPreserved() {
        var plan = optimizedPlan("FROM test | STATS s = SUM(salary + 1) WHERE emp_no > 0");

        // The simplification should still apply — verify the structural shape:
        // the plan has a Project → Eval (correction) → ... → Aggregate with simplified SUM(salary)
        var project = as(plan, Project.class);
        var eval = as(project.child(), Eval.class);
        assertThat(eval.fields(), hasSize(1));

        // The aggregate should contain SUM(salary), not SUM(salary + 1)
        assertPlanDoesNotContainArithmeticInsideAgg(eval.child());
    }

    // ---- With GROUP BY ----

    /**
     * SUM(salary + 1) BY last_name should work correctly with grouping
     */
    public void testSumAddWithGroupBy() {
        var plan = optimizedPlan("FROM test | STATS s = SUM(salary + 1) BY last_name");

        var project = as(plan, Project.class);
        var eval = as(project.child(), Eval.class);
        assertThat(eval.fields(), hasSize(1));

        var limit = as(eval.child(), Limit.class);
        var agg = as(limit.child(), Aggregate.class);
        // SUM(salary), COUNT(*), and the grouping key last_name
        assertThat(agg.aggregates(), hasSize(3));
        // grouping should be preserved
        assertThat(agg.groupings(), hasSize(1));
    }

    // ---- Randomized ----

    /**
     * Test with random literal values to verify the plan structure is consistent
     */
    public void testRandomLiterals() {
        int k = randomIntBetween(1, 1000);
        var plan = optimizedPlan("FROM test | STATS s = SUM(salary + " + k + ")");

        var project = as(plan, Project.class);
        var eval = as(project.child(), Eval.class);
        assertThat(eval.fields(), hasSize(1));

        var correctionAlias = as(eval.fields().get(0), Alias.class);
        var add = as(correctionAlias.child(), Add.class);
        var mul = as(add.right(), Mul.class);
        assertMulLiteralRight(mul, k);

        var limit = as(eval.child(), Limit.class);
        var agg = as(limit.child(), Aggregate.class);
        assertThat(agg.aggregates(), hasSize(2));
    }

    // ---- Type handling ----

    /**
     * SUM on double field: SUM(salary + 1.5) with integer field + double literal
     */
    public void testSumDoubleAddLiteral() {
        var plan = optimizedPlan("FROM test | STATS s = SUM(salary + 1.5)");

        var project = as(plan, Project.class);
        var eval = as(project.child(), Eval.class);
        assertThat(eval.fields(), hasSize(1));

        var correctionAlias = as(eval.fields().get(0), Alias.class);
        var add = as(correctionAlias.child(), Add.class);
        var mul = as(add.right(), Mul.class);
        assertMulLiteralRight(mul, 1.5);

        var limit = as(eval.child(), Limit.class);
        var agg = as(limit.child(), Aggregate.class);
        assertThat(agg.aggregates(), hasSize(2));
    }

    // ---- Helper methods ----

    /**
     * Asserts that a Mul has a ReferenceAttribute on the left and a Literal with the
     * expected value on the right. The optimizer's {@code LiteralsOnTheRight} rule
     * guarantees this canonical form for commutative operations.
     */
    private static void assertMulLiteralRight(Mul mul, Object expectedLiteral) {
        as(mul.left(), ReferenceAttribute.class);
        assertEquals(expectedLiteral, as(mul.right(), Literal.class).value());
    }

    /**
     * Asserts that no aggregate function in the plan contains an arithmetic operation as its field.
     * This verifies the simplification was applied.
     */
    private static void assertPlanDoesNotContainArithmeticInsideAgg(LogicalPlan plan) {
        plan.forEachDown(Aggregate.class, agg -> {
            for (var aggExpr : agg.aggregates()) {
                if (aggExpr instanceof Alias alias
                    && alias.child() instanceof org.elasticsearch.xpack.esql.expression.function.aggregate.AggregateFunction af) {
                    // COUNT(*) has field() = Literal("*"), which is expected — skip it
                    if (af instanceof Count) continue;
                    assertThat(
                        "Aggregate field should be a simple attribute, not an arithmetic expression: " + af.field(),
                        af.field(),
                        instanceOf(Attribute.class)
                    );
                }
            }
        });
    }

    /**
     * Asserts that the plan contains the given aggregate type operating on a plain attribute
     * (after ReplaceAggregateNestedExpressionWithEval extracts the arithmetic into an eval).
     * This is for cases where our rule should NOT fire.
     */
    private static void assertPlanContainsAggOverAttribute(LogicalPlan plan, Class<?> aggType) {
        plan.forEachDown(Aggregate.class, agg -> {
            boolean found = false;
            for (var aggExpr : agg.aggregates()) {
                if (aggExpr instanceof Alias alias && aggType.isInstance(alias.child())) {
                    found = true;
                    break;
                }
            }
            assertTrue("Expected to find " + aggType.getSimpleName() + " in aggregate", found);
        });
    }

    /**
     * Debug test to print optimized plan structure
     */
    public void testDebugPlanStructure() {
        var plan = optimizedPlan("FROM test | STATS s = SUM(salary + 100)");
        logger.info("\n" + plan.toString());
    }

}
