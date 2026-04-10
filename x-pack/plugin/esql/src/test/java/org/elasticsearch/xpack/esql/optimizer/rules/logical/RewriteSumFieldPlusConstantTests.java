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
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Sub;
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

    public void testSumOfFieldPlusConstant() {
        var plan = plan("""
            from test
            | stats s1 = sum(emp_no + 1), s2 = sum(emp_no + 2)
            """, new TestSubstitutionOnlyOptimizer());

        var limit = as(plan, Limit.class);
        var project = as(limit.child(), Project.class);
        var eval = as(project.child(), Eval.class);
        var agg = as(eval.child(), Aggregate.class);

        assertThat(agg.aggregates(), hasSize(2));
        var svSumAlias = as(agg.aggregates().get(0), Alias.class);
        var svSum = as(svSumAlias.child(), Sum.class);

        var svCountAlias = as(agg.aggregates().get(1), Alias.class);
        var svCount = as(svCountAlias.child(), Count.class);

        // ReplaceAggregateNestedExpressionWithEval extracts MvSingleValueOrNull(emp_no)
        // into a pre-agg EVAL; both SUM and COUNT then reference the same attribute.
        var preAggEval = as(agg.child(), Eval.class);
        assertThat(preAggEval.fields(), hasSize(1));
        var svAlias = as(preAggEval.fields().get(0), Alias.class);
        assertThat(svAlias.child(), instanceOf(MvSingleValueOrNull.class));
        assertThat(svSum.field().semanticEquals(svAlias.toAttribute()), equalTo(true));
        assertThat(svCount.field().semanticEquals(svAlias.toAttribute()), equalTo(true));

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

    public void testSumOfFieldMinusConstant() {
        var plan = plan("""
            from test
            | stats s1 = sum(emp_no - 2), s2 = sum(3 - emp_no)
            """, new TestSubstitutionOnlyOptimizer());

        var limit = as(plan, Limit.class);
        var project = as(limit.child(), Project.class);
        var eval = as(project.child(), Eval.class);
        var agg = as(eval.child(), Aggregate.class);

        assertThat(agg.aggregates(), hasSize(2));
        var svSumAlias = as(agg.aggregates().get(0), Alias.class);
        var svCountAlias = as(agg.aggregates().get(1), Alias.class);

        var fields = eval.fields();
        assertThat(fields, hasSize(2));

        // s1 = sv_sum - 2 * sv_count (field - c)
        var s1 = as(fields.get(0), Alias.class);
        assertThat(s1.name(), equalTo("s1"));
        var sub1 = as(s1.child(), Sub.class);
        assertThat(sub1.left().semanticEquals(svSumAlias.toAttribute()), equalTo(true));
        var mul1 = as(sub1.right(), Mul.class);
        assertThat(mul1.right().semanticEquals(svCountAlias.toAttribute()), equalTo(true));
        assertThat(mul1.left().fold(FoldContext.small()), equalTo(2));

        // s2 = 3 * sv_count - sv_sum (c - field)
        var s2 = as(fields.get(1), Alias.class);
        assertThat(s2.name(), equalTo("s2"));
        var sub2 = as(s2.child(), Sub.class);
        var mul2 = as(sub2.left(), Mul.class);
        assertThat(mul2.right().semanticEquals(svCountAlias.toAttribute()), equalTo(true));
        assertThat(mul2.left().fold(FoldContext.small()), equalTo(3));
        assertThat(sub2.right().semanticEquals(svSumAlias.toAttribute()), equalTo(true));
    }

    public void testSumOfFieldPlusConstantWithGroupBy() {
        var plan = plan("""
            from test
            | stats s1 = sum(emp_no + 1), s2 = sum(emp_no + 2) by languages
            """, new TestSubstitutionOnlyOptimizer());

        var limit = as(plan, Limit.class);
        var project = as(limit.child(), Project.class);
        var eval = as(project.child(), Eval.class);
        var agg = as(eval.child(), Aggregate.class);

        assertThat(agg.groupings(), hasSize(1));
        assertThat(agg.aggregates(), hasSize(3)); // SUM(sv), COUNT(sv), grouping attribute
        var svSumAlias = as(agg.aggregates().get(0), Alias.class);
        var svCountAlias = as(agg.aggregates().get(1), Alias.class);
        as(svSumAlias.child(), Sum.class);
        as(svCountAlias.child(), Count.class);

        assertThat(eval.fields(), hasSize(2));
        assertThat(as(eval.fields().get(0), Alias.class).name(), equalTo("s1"));
        assertThat(as(eval.fields().get(1), Alias.class).name(), equalTo("s2"));
    }

    public void testBareSumOfSameFieldNotSharedWithRewrite() {
        var plan = plan("""
            from test
            | stats s1 = sum(emp_no + 1), bare = sum(emp_no), s2 = sum(emp_no + 2)
            """, new TestSubstitutionOnlyOptimizer());

        var limit = as(plan, Limit.class);
        var project = as(limit.child(), Project.class);
        var eval = as(project.child(), Eval.class);
        var agg = as(eval.child(), Aggregate.class);

        // SUM(sv) and COUNT(sv) are inserted first; the bare SUM is appended unchanged.
        assertThat(agg.aggregates(), hasSize(3));
        var svSumAlias = as(agg.aggregates().get(0), Alias.class);
        var svCountAlias = as(agg.aggregates().get(1), Alias.class);
        var bareSumAlias = as(agg.aggregates().get(2), Alias.class);

        as(svSumAlias.child(), Sum.class);
        as(svCountAlias.child(), Count.class);
        var bareSum = as(bareSumAlias.child(), Sum.class);

        assertThat(bareSum.field(), not(instanceOf(MvSingleValueOrNull.class)));
        assertThat(bareSum.field(), not(instanceOf(ReferenceAttribute.class)));
    }

    public void testDuplicateAliasNotRewritten() {
        var plan = plan("""
            from test
            | stats s = sum(emp_no + 1), s = sum(emp_no + 2)
            | limit 10
            """, new TestSubstitutionOnlyOptimizer());

        // After shadowing, only one SUM remains — below the 2-match threshold.
        boolean hasMvSingleValueOrNull = plan.anyMatch(
            node -> node instanceof Eval e
                && e.fields().stream().anyMatch(f -> f instanceof Alias a && a.child() instanceof MvSingleValueOrNull)
        );
        assertFalse("Duplicate alias should not be rewritten by RewriteSumFieldPlusConstant", hasMvSingleValueOrNull);
        assertWarnings("Line 2:9: Field 's' shadowed by field at line 2:30");
    }

    public void testCountAlreadyInQueryNotReused() {
        var plan = plan("""
            from test
            | stats c = count(emp_no), s1 = sum(emp_no + 1), s2 = sum(emp_no + 2)
            """, new TestSubstitutionOnlyOptimizer());

        var limit = as(plan, Limit.class);
        var project = as(limit.child(), Project.class);
        var eval = as(project.child(), Eval.class);
        var agg = as(eval.child(), Aggregate.class);

        assertThat(agg.aggregates(), hasSize(3));
        var userCount = as(Alias.unwrap(agg.aggregates().get(0)), Count.class);
        var svSum = as(Alias.unwrap(agg.aggregates().get(1)), Sum.class);
        var svCount = as(Alias.unwrap(agg.aggregates().get(2)), Count.class);

        // Internal SUM and COUNT share the same MvSingleValueOrNull attribute.
        assertThat(svSum.field(), instanceOf(ReferenceAttribute.class));
        assertThat(svCount.field(), instanceOf(ReferenceAttribute.class));
        assertThat(svSum.field().semanticEquals(svCount.field()), equalTo(true));

        // User's COUNT is over emp_no directly, not the sv wrapper.
        assertThat(userCount.field(), not(instanceOf(MvSingleValueOrNull.class)));
    }

    public void testAvgOfFieldPlusConstantNotRewrittenByRule() {
        var plan = plan("""
            from test
            | stats a = avg(emp_no + 1)
            """, new TestSubstitutionOnlyOptimizer());

        boolean hasMvSingleValueOrNull = plan.anyMatch(
            node -> node instanceof Eval e
                && e.fields().stream().anyMatch(f -> f instanceof Alias a && a.child() instanceof MvSingleValueOrNull)
        );
        assertFalse("AVG should not be rewritten by RewriteSumFieldPlusConstant", hasMvSingleValueOrNull);
    }

    public void testSumOfFieldPlusFoldableConstantExpression() {
        var plan = plan("""
            from test
            | stats s1 = sum(emp_no + (5 - 2)), s2 = sum(emp_no + 1)
            """, new TestSubstitutionOnlyOptimizer());

        var limit = as(plan, Limit.class);
        var project = as(limit.child(), Project.class);
        var eval = as(project.child(), Eval.class);
        var agg = as(eval.child(), Aggregate.class);

        assertThat(agg.aggregates(), hasSize(2));
        as(Alias.unwrap(agg.aggregates().get(0)), Sum.class);
        as(Alias.unwrap(agg.aggregates().get(1)), Count.class);

        assertThat(eval.fields(), hasSize(2));
        var s1 = as(eval.fields().get(0), Alias.class);
        var add = as(s1.child(), Add.class);
        var mul = as(add.right(), Mul.class);
        assertThat(mul.left().fold(FoldContext.small()), equalTo(3));
    }

    // length("abc") is foldable, so it is treated as the constant — folds to 3.
    public void testSumOfFieldPlusFoldableFunctionCallConstant() {
        var plan = plan("""
            from test
            | stats s1 = sum(emp_no + length("abc")), s2 = sum(emp_no + 1)
            """, new TestSubstitutionOnlyOptimizer());

        var limit = as(plan, Limit.class);
        var project = as(limit.child(), Project.class);
        var eval = as(project.child(), Eval.class);
        var agg = as(eval.child(), Aggregate.class);

        assertThat(agg.aggregates(), hasSize(2));
        as(Alias.unwrap(agg.aggregates().get(0)), Sum.class);
        as(Alias.unwrap(agg.aggregates().get(1)), Count.class);

        var s1 = as(eval.fields().get(0), Alias.class);
        var add = as(s1.child(), Add.class);
        var mul = as(add.right(), Mul.class);
        assertThat(mul.left().fold(FoldContext.small()), equalTo(3));
    }

    // Both expressions have the same inner expression (emp_no + 1), so they share one sv pair.
    public void testSumOfNestedArithmeticSharesSvPairOnSameBase() {
        var plan = plan("""
            from test
            | stats s1 = sum(emp_no + 1 + 2), s2 = sum(emp_no + 1 + 3)
            """, new TestSubstitutionOnlyOptimizer());

        var limit = as(plan, Limit.class);
        var project = as(limit.child(), Project.class);
        var eval = as(project.child(), Eval.class);
        var agg = as(eval.child(), Aggregate.class);

        assertThat(agg.aggregates(), hasSize(2));
        as(Alias.unwrap(agg.aggregates().get(0)), Sum.class);
        as(Alias.unwrap(agg.aggregates().get(1)), Count.class);

        assertThat(eval.fields(), hasSize(2));
    }

    public void testThreeSumsShareOneSvPair() {
        var plan = plan("""
            from test
            | stats s1 = sum(emp_no + 1), s2 = sum(emp_no + 2), s3 = sum(emp_no + 3)
            """, new TestSubstitutionOnlyOptimizer());

        var limit = as(plan, Limit.class);
        var project = as(limit.child(), Project.class);
        var eval = as(project.child(), Eval.class);
        var agg = as(eval.child(), Aggregate.class);

        // All three share one SUM(sv)/COUNT(sv) pair.
        assertThat(agg.aggregates(), hasSize(2));
        as(Alias.unwrap(agg.aggregates().get(0)), Sum.class);
        as(Alias.unwrap(agg.aggregates().get(1)), Count.class);

        assertThat(eval.fields(), hasSize(3));
    }

    public void testTwoFieldsGetIndependentSvPairs() {
        var plan = plan("""
            from test
            | stats s1 = sum(emp_no + 1), s2 = sum(emp_no + 2), s3 = sum(salary + 1), s4 = sum(salary + 2)
            """, new TestSubstitutionOnlyOptimizer());

        var limit = as(plan, Limit.class);
        var project = as(limit.child(), Project.class);
        var eval = as(project.child(), Eval.class);
        var agg = as(eval.child(), Aggregate.class);

        // Two fields → two independent SUM(sv)/COUNT(sv) pairs.
        assertThat(agg.aggregates(), hasSize(4));
        as(Alias.unwrap(agg.aggregates().get(0)), Sum.class);
        as(Alias.unwrap(agg.aggregates().get(1)), Count.class);
        as(Alias.unwrap(agg.aggregates().get(2)), Sum.class);
        as(Alias.unwrap(agg.aggregates().get(3)), Count.class);

        assertThat(eval.fields(), hasSize(4));
    }

    // null is foldable, so both operands of the Add are foldable — tryMatch returns null.
    public void testBothFoldableOperandsNotRewritten() {
        var plan = plan("""
            from test
            | stats s1 = sum(null + 1), s2 = sum(null + 2)
            """, new TestSubstitutionOnlyOptimizer());

        boolean hasMvSingleValueOrNull = plan.anyMatch(
            node -> node instanceof Eval e
                && e.fields().stream().anyMatch(f -> f instanceof Alias a && a.child() instanceof MvSingleValueOrNull)
        );
        assertFalse("SUM(null + c) should not be rewritten because null is foldable", hasMvSingleValueOrNull);
    }

    public void testSumWithGroupingExpressionAlias() {
        var plan = plan("""
            from test
            | stats s1 = sum(emp_no + 1), s2 = sum(emp_no + 2) by l = languages
            """, new TestSubstitutionOnlyOptimizer());

        var limit = as(plan, Limit.class);
        var project = as(limit.child(), Project.class);
        var eval = as(project.child(), Eval.class);
        var agg = as(eval.child(), Aggregate.class);

        assertThat(agg.groupings(), hasSize(1));
        assertThat(agg.aggregates(), hasSize(3)); // SUM(sv), COUNT(sv), grouping attribute
        as(Alias.unwrap(agg.aggregates().get(0)), Sum.class);
        as(Alias.unwrap(agg.aggregates().get(1)), Count.class);

        assertThat(eval.fields(), hasSize(2));
        assertThat(as(eval.fields().get(0), Alias.class).name(), equalTo("s1"));
        assertThat(as(eval.fields().get(1), Alias.class).name(), equalTo("s2"));
    }

    public void testShadowedNonMatchingExprRuleFires() {
        var plan = plan("""
            from test
            | stats s1 = sum(emp_no + 1), c = sum(emp_no), c = count(emp_no), s2 = sum(emp_no + 2)
            | limit 10
            """, new TestSubstitutionOnlyOptimizer());

        // After RemoveStatsOverride drops c = sum(emp_no), s1 and s2 still satisfy the 2-match threshold.
        assertTrue(
            "Expected RewriteSumFieldPlusConstant to fire after shadowing removes c = sum(emp_no)",
            plan.anyMatch(
                node -> node instanceof Eval e
                    && e.fields().stream().anyMatch(f -> f instanceof Alias a && a.child() instanceof MvSingleValueOrNull)
            )
        );
        // Aggregate has internal SUM(sv), internal COUNT(sv), and user's COUNT(emp_no).
        assertTrue(
            "Expected 3 aggregates after rewrite",
            plan.anyMatch(node -> node instanceof Aggregate agg && agg.aggregates().size() == 3)
        );
        assertWarnings("Line 2:31: Field 'c' shadowed by field at line 2:48");
    }

    // INLINE STATS wraps the Aggregate in an InlineJoin, but the rule still visits it.
    public void testInlineStatsWithSumOfFieldPlusConstant() {
        var plan = plan("""
            from test
            | inline stats s1 = sum(emp_no + 1), s2 = sum(emp_no + 2)
            """, new TestSubstitutionOnlyOptimizer());

        assertTrue(
            "Expected RewriteSumFieldPlusConstant to fire for INLINE STATS",
            plan.anyMatch(
                node -> node instanceof Eval e
                    && e.fields().stream().anyMatch(f -> f instanceof Alias a && a.child() instanceof MvSingleValueOrNull)
            )
        );
    }
}
