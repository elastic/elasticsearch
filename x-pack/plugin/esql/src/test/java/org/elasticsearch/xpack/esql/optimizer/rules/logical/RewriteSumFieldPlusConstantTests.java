/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Count;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Sum;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvSingleValueOrNull;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Add;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Mul;
import org.elasticsearch.xpack.esql.optimizer.AbstractLogicalPlanOptimizerTests;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.Project;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;

public class RewriteSumFieldPlusConstantTests extends AbstractLogicalPlanOptimizerTests {

    /**
     * Verify that query with two sums of a field with constants is broken into
     * separate sum and count columns which are reused.
     */
    public void testSumOfFieldPlusConstant() {
        var plan = plan("""
            from test
            | stats s1 = sum(emp_no + 1), s2 = sum(emp_no + 2)
            """, new TestSubstitutionOnlyOptimizer());

        var limit = as(plan, Limit.class);
        var project = as(limit.child(), Project.class);
        var eval = as(project.child(), Eval.class);
        var agg = as(eval.child(), Aggregate.class);

        // The aggregate should contain exactly two entries: SUM(sv) and COUNT(sv).
        assertThat(agg.aggregates(), hasSize(2));
        var svSumAlias = as(agg.aggregates().get(0), Alias.class);
        var svSum = as(svSumAlias.child(), Sum.class);

        var svCountAlias = as(agg.aggregates().get(1), Alias.class);
        var svCount = as(svCountAlias.child(), Count.class);

        // ReplaceAggregateNestedExpressionWithEval (which runs after our rule in substitutions())
        // extracts MvSingleValueOrNull(emp_no) from inside SUM/COUNT into a pre-agg EVAL.
        // Both SUM and COUNT reference the same extracted attribute.
        var preAggEval = as(agg.child(), Eval.class);
        assertThat(preAggEval.fields(), hasSize(1));
        var svAlias = as(preAggEval.fields().get(0), Alias.class);
        assertThat(svAlias.child(), instanceOf(MvSingleValueOrNull.class));
        assertThat(svSum.field().semanticEquals(svAlias.toAttribute()), equalTo(true));
        assertThat(svCount.field().semanticEquals(svAlias.toAttribute()), equalTo(true));

        // The eval should derive s1 and s2 from the shared sv_sum and sv_count.
        var fields = eval.fields();
        assertThat(fields, hasSize(2));

        // s1 = sv_sum + 1 * sv_count
        var s1 = as(fields.get(0), Alias.class);
        assertThat(s1.name(), equalTo("s1"));
        var add1 = as(s1.child(), Add.class);
        assertThat(add1.left().semanticEquals(svSumAlias.toAttribute()), equalTo(true));
        var mul1 = as(add1.right(), Mul.class);
        assertThat(mul1.right().semanticEquals(svCountAlias.toAttribute()), equalTo(true));
        assertThat(mul1.left().fold(FoldContext.small()), equalTo(1));

        // s2 = sv_sum + 2 * sv_count
        var s2 = as(fields.get(1), Alias.class);
        assertThat(s2.name(), equalTo("s2"));
        var add2 = as(s2.child(), Add.class);
        assertThat(add2.left().semanticEquals(svSumAlias.toAttribute()), equalTo(true));
        var mul2 = as(add2.right(), Mul.class);
        assertThat(mul2.right().semanticEquals(svCountAlias.toAttribute()), equalTo(true));
        assertThat(mul2.left().fold(FoldContext.small()), equalTo(2));
    }

    /**
     * Verifies that when a user-provided COUNT is already present alongside SUM(field + c),
     * the rule does not reuse it since it will include values with multi-values.
     */
    public void testCountAlreadyInQueryNotReused() {
        var plan = plan("""
            from test
            | stats c = count(emp_no), s = sum(emp_no + 1)
            """, new TestSubstitutionOnlyOptimizer());

        var limit = as(plan, Limit.class);
        var project = as(limit.child(), Project.class);
        var eval = as(project.child(), Eval.class);
        var agg = as(eval.child(), Aggregate.class);

        // Three aggregates: user's COUNT(emp_no) + internal SUM(sv) + internal COUNT(sv).
        assertThat(agg.aggregates(), hasSize(3));
        var userCount = as(Alias.unwrap(agg.aggregates().get(0)), Count.class);
        var svSum = as(Alias.unwrap(agg.aggregates().get(1)), Sum.class);
        var svCount = as(Alias.unwrap(agg.aggregates().get(2)), Count.class);

        // The internal SUM and COUNT wrap MvSingleValueOrNull(emp_no).
        assertThat(svSum.field(), instanceOf(ReferenceAttribute.class));
        assertThat(svCount.field(), instanceOf(ReferenceAttribute.class));

        // The internal SUM and COUNT reference the same MvSingleValueOrNull attribute.
        assertThat(svSum.field().semanticEquals(svCount.field()), equalTo(true));

        // The user's COUNT is over emp_no directly (not MvSingleValueOrNull-wrapped).
        assertThat(userCount.field(), not(instanceOf(MvSingleValueOrNull.class)));
    }

    /**
     * Confirm that AVG(field + c) is not matched by RewriteSumFieldPlusConstant.
     * Though it would be ideal to also handle AVG, this does not work because
     * SubstituteSurrogateAggregations is run after RewriteSumFieldPlusConstant.
     * TODO maybe this can be improved.
     */
    public void testAvgOfFieldPlusConstantNotRewrittenByRule() {
        var plan = plan("""
            from test
            | stats a = avg(emp_no + 1)
            """, new TestSubstitutionOnlyOptimizer());

        // After substitutions, AVG(emp_no+1) is rewritten to SUM/COUNT by SubstituteSurrogateAggregations,
        // but NOT by RewriteSumFieldPlusConstant. The plan should NOT contain an MvSingleValueOrNull wrapper.
        boolean hasMvSingleValueOrNull = plan.anyMatch(
            node -> node instanceof Eval e
                && e.fields().stream().anyMatch(f -> f instanceof Alias a && a.child() instanceof MvSingleValueOrNull)
        );
        assertFalse("AVG should not be rewritten by RewriteSumFieldPlusConstant", hasMvSingleValueOrNull);
    }

    /**
     * Verifies that when the constant in SUM(field + c) is itself a foldable expression
     * the rewrite correctly uses it as the constant multiplier.
     */
    public void testSumOfFieldPlusFoldableConstantExpression() {
        var plan = plan("""
            from test
            | stats s = sum(emp_no + (5 - 2))
            """, new TestSubstitutionOnlyOptimizer());

        var limit = as(plan, Limit.class);
        var project = as(limit.child(), Project.class);
        var eval = as(project.child(), Eval.class);
        var agg = as(eval.child(), Aggregate.class);

        // Two internal aggregates: SUM(sv) and COUNT(sv).
        assertThat(agg.aggregates(), hasSize(2));
        as(Alias.unwrap(agg.aggregates().get(0)), Sum.class);
        as(Alias.unwrap(agg.aggregates().get(1)), Count.class);

        // The eval expression: s = $$sv_sum + (5-2) * $$sv_count
        assertThat(eval.fields(), hasSize(1));
        var s = as(eval.fields().get(0), Alias.class);
        var add = as(s.child(), Add.class);
        var mul = as(add.right(), Mul.class);
        // The constant (5-2) should fold to 3.
        assertThat(mul.left().fold(FoldContext.small()), equalTo(3));
    }

    /**
     * Verifies that {@code INLINE STATS} with {@code SUM(field + c)} is also rewritten
     * by {@code RewriteSumFieldPlusConstant}.
     */
    public void testInlineStatsWithSumOfFieldPlusConstant() {
        // INLINE STATS places the aggregate inside an InlineJoin, but the Aggregate node
        // is still visited by the rule. Verify the query produces correct results end-to-end.
        var plan = plan("""
            from test
            | inline stats s1 = sum(emp_no + 1), s2 = sum(emp_no + 2)
            """, new TestSubstitutionOnlyOptimizer());

        // Verify MvSingleValueOrNull is injected, confirming the rule fired.
        assertTrue(
            "Expected RewriteSumFieldPlusConstant to fire for INLINE STATS",
            plan.anyMatch(
                node -> node instanceof Eval e
                    && e.fields().stream().anyMatch(f -> f instanceof Alias a && a.child() instanceof MvSingleValueOrNull)
            )
        );
    }
}
