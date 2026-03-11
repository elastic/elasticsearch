/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.optimizer.AbstractLogicalPlanOptimizerTests;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.Project;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;

public class PruneLiteralsInLimitByTests extends AbstractLogicalPlanOptimizerTests {

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

        var eval = as(plan, Eval.class);
        var limit = as(eval.child(), Limit.class);
        assertThat(((Literal) limit.limit()).value(), equalTo(1));
        assertThat(limit.groupings(), empty());
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

        var eval = as(plan, Eval.class);
        var evalFields = eval.fields();
        assertThat(evalFields.size(), equalTo(2));
        var x = evalFields.get(0);
        var y = evalFields.get(1);
        assertThat(((Literal) x.child()).value(), equalTo(5));
        assertThat(((Literal) y.child()).value(), equalTo(7));
        assertThat(x.name(), equalTo("x"));
        assertThat(y.name(), equalTo("y"));

        var limit = as(eval.child(), Limit.class);
        assertThat(((Literal) limit.limit()).value(), equalTo(1));
        assertThat(limit.groupings(), empty());
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

        var eval = as(plan, Eval.class);
        var evalFields = eval.fields();
        assertThat(evalFields.size(), equalTo(2));
        var x = evalFields.get(0);
        var y = evalFields.get(1);
        assertThat(((Literal) x.child()).value(), equalTo(5));
        assertThat(((Literal) y.child()).value(), equalTo(7));
        assertThat(x.name(), equalTo("x"));
        assertThat(y.name(), equalTo("y"));

        var limit = as(eval.child(), Limit.class);
        assertThat(((Literal) limit.limit()).value(), equalTo(1));
        assertThat(limit.groupings(), empty());
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

        var eval = as(plan, Eval.class);
        var defaultLimit = as(eval.child(), Limit.class);
        assertThat(defaultLimit.groupings(), empty());
        var limit = as(defaultLimit.child(), Limit.class);
        assertThat(((Literal) limit.limit()).value(), equalTo(1));
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

        var eval = as(plan, Eval.class);
        var limit = as(eval.child(), Limit.class);
        assertThat(((Literal) limit.limit()).value(), equalTo(1));
        assertThat(limit.groupings(), empty());
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
        assertThat(limit.groupings(), empty());
        as(limit.child(), EsRelation.class);
    }
}
