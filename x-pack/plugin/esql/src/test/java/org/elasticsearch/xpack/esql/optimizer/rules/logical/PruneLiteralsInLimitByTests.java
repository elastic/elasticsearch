/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.optimizer.AbstractLogicalPlanOptimizerTests;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LimitBy;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.junit.BeforeClass;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.assertEvalFields;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;

public class PruneLiteralsInLimitByTests extends AbstractLogicalPlanOptimizerTests {

    @BeforeClass
    public static void checkLimitByCapability() {
        assumeTrue("LIMIT BY requires snapshot builds", EsqlCapabilities.Cap.ESQL_LIMIT_BY.isEnabled());
    }

    /**
     * A foldable eval alias used in LIMIT BY should be propagated by {@code PropagateEvalFoldables}
     * and then pruned by {@code PruneLiteralsInLimitBy}, degenerating the LIMIT BY into a plain LIMIT.
     * The two plain limits are then combined. The Eval remains because {@code x} is still in the output.
     * <pre>{@code
     * Eval[[5[INTEGER] AS x]]
     * \_Limit[1[INTEGER],[],false,false]
     *   \_EsRelation[test][...]
     * }</pre>
     */
    public void testLimitByFoldableEvalAlias() {
        var plan = plan("""
            FROM test
            | EVAL x = 5
            | LIMIT 1 BY x
            """);

        var eval = assertEvalFields(as(plan, Eval.class), new String[] { "x" }, new Object[] { 5 });
        var limit = as(eval.child(), Limit.class);
        assertThat(((Literal) limit.limit()).value(), equalTo(1));
        as(limit.child(), EsRelation.class);
    }

    /**
     * <pre>{@code
     * Eval[[5[INTEGER] AS x#4, 7[INTEGER] AS y#7]]
     * \_Limit[1[INTEGER],[],false,false]
     *   \_EsRelation[test][_meta_field{f}#15, emp_no{f}#9, first_name{f}#10, g..]
     * }</pre>
     */
    public void testLimitByFoldableContiguousEvalAlias() {
        var plan = plan("""
            FROM test
            | EVAL x = 5
            | EVAL y = x + 2
            | LIMIT 1 BY y
            """);

        var eval = assertEvalFields(as(plan, Eval.class), new String[] { "x", "y" }, new Object[] { 5, 7 });

        var limit = as(eval.child(), Limit.class);
        assertThat(((Literal) limit.limit()).value(), equalTo(1));
        as(limit.child(), EsRelation.class);
    }

    /**
     * <pre>{@code
     * Eval[[5[INTEGER] AS x#4, 7[INTEGER] AS y#7]]
     * \_Limit[1[INTEGER],[],false,false]
     *   \_EsRelation[test][_meta_field{f}#15, emp_no{f}#9, first_name{f}#10, g..]
     * }</pre>
     */
    public void testLimitByFoldableNonContiguousEvalAlias() {
        var plan = plan("""
            FROM test
            | EVAL x = 5
            | LIMIT 50
            | EVAL y = x + 2
            | LIMIT 1 BY y
            """);

        var eval = assertEvalFields(as(plan, Eval.class), new String[] { "x", "y" }, new Object[] { 5, 7 });

        var limit = as(eval.child(), Limit.class);
        assertThat(((Literal) limit.limit()).value(), equalTo(1));
        as(limit.child(), EsRelation.class);
    }

    /**
     * A foldable eval alias mixed with a non-foldable attribute: the alias is propagated and pruned,
     * the attribute grouping survives.
     * <pre>{@code
     * Eval[[5[INTEGER] AS x]]
     * \_Limit[10000[INTEGER],[],false,false]
     *   \_Limit[1[INTEGER],[emp_no{f}#N],false,false]
     *     \_EsRelation[test][...]
     * }</pre>
     */
    public void testFoldableEvalAliasAndAttributeMixed() {
        var plan = plan("""
            FROM test
            | EVAL x = 5
            | LIMIT 1 BY x, emp_no
            """);

        var eval = assertEvalFields(as(plan, Eval.class), new String[] { "x" }, new Object[] { 5 });
        var defaultLimit = as(eval.child(), Limit.class);
        var limit = as(defaultLimit.child(), LimitBy.class);
        assertThat(((Literal) limit.limitPerGroup()).value(), equalTo(1));
        assertThat(limit.groupings().size(), equalTo(1));
        assertThat(Expressions.names(limit.groupings()), contains("emp_no"));
        as(limit.child(), EsRelation.class);
    }

    /**
     * A foldable eval alias mixed with a foldable literal: both are pruned, degenerating to a plain LIMIT.
     * <pre>{@code
     * Eval[[5[INTEGER] AS x]]
     * \_Limit[1[INTEGER],[],false,false]
     *   \_EsRelation[test][...]
     * }</pre>
     */
    public void testFoldableEvalAliasAndLiteralBothPruned() {
        var plan = plan("""
            FROM test
            | EVAL x = 5
            | LIMIT 1 BY x, 3
            """);

        var eval = assertEvalFields(as(plan, Eval.class), new String[] { "x" }, new Object[] { 5 });
        var limit = as(eval.child(), Limit.class);
        assertThat(((Literal) limit.limit()).value(), equalTo(1));
        as(limit.child(), EsRelation.class);
    }

    /**
     * An expression composed of propagated foldable eval aliases should itself become foldable and be pruned.
     * {@code x + x + 2} becomes {@code 5 + 5 + 2 = 12} after propagation. The expression grouping causes
     * {@code ReplaceLimitByExpressionWithEval} to wrap in a Project to preserve the output schema.
     * <pre>{@code
     * Project[[..., x{r}#N]]
     * \_Eval[[5[INTEGER] AS x]]
     *   \_Limit[1[INTEGER],[],false,false]
     *     \_EsRelation[test][...]
     * }</pre>
     */
    public void testFoldableExpressionFromPropagatedEval() {
        var plan = plan("""
            FROM test
            | EVAL x = 5
            | LIMIT 1 BY x + x + 2
            """);

        var project = as(plan, Project.class);
        var eval = assertEvalFields(as(project.child(), Eval.class), new String[] { "x" }, new Object[] { 5 });
        var limit = as(eval.child(), Limit.class);
        assertThat(((Literal) limit.limit()).value(), equalTo(1));
        as(limit.child(), EsRelation.class);
    }

    /**
     * Contiguous LIMIT BY with a foldable eval alias: both degenerate to plain limits and get combined.
     * <pre>{@code
     * Eval[[5[INTEGER] AS x]]
     * \_Limit[1[INTEGER],[],false,false]
     *   \_EsRelation[test][...]
     * }</pre>
     */
    public void testContiguousLimitByWithFoldableEval() {
        var plan = plan("""
            FROM test
            | EVAL x = 5
            | LIMIT 2 BY x
            | LIMIT 1 BY x
            """);

        var eval = assertEvalFields(as(plan, Eval.class), new String[] { "x" }, new Object[] { 5 });
        var limit = as(eval.child(), Limit.class);
        assertThat(((Literal) limit.limit()).value(), equalTo(1));
        as(limit.child(), EsRelation.class);
    }

    /**
     * Mixing evals pointing to the same foldable and LIMIT BYs with the same canonical groupings
     * should produce a LIMIT without groupings
     * <pre>{@code
     * Eval[[5[INTEGER] AS x#4, 5[INTEGER] AS y#8]]
     * \_Limit[1[INTEGER],[],false,false]
     *   \_EsRelation[test][_meta_field{f}#16, emp_no{f}#10, first_name{f}#11, ..]
     * }</pre>
     */
    public void testNonContiguousLimitByWithFoldableEval() {
        var plan = plan("""
            FROM test
            | EVAL x = 5 + 24
            | LIMIT 2 BY x
            | EVAL y = x
            | LIMIT 1 BY y
            """);

        var eval = assertEvalFields(as(plan, Eval.class), new String[] { "x", "y" }, new Object[] { 29, 29 });
        var limit = as(eval.child(), Limit.class);
        assertThat(((Literal) limit.limit()).value(), equalTo(1));
        as(limit.child(), EsRelation.class);
    }

    /**
     * A foldable eval alias used in LIMIT BY that is not present in the output should be pruned
     * <pre>{@code
     * Project[[emp_no{f}#7]]
     * \_Limit[1[INTEGER],[],false,false]
     *   \_EsRelation[test][_meta_field{f}#13, emp_no{f}#7, first_name{f}#8, ge..]
     * }</pre>
     */
    public void testEvalShouldBePruned() {
        var plan = plan("""
            FROM test
            | EVAL x = 5
            | LIMIT 1 BY x
            | KEEP emp_no
            """);

        var project = as(plan, Project.class);
        var limit = as(project.child(), Limit.class);
        assertThat(((Literal) limit.limit()).value(), equalTo(1));
        as(limit.child(), EsRelation.class);
    }
}
