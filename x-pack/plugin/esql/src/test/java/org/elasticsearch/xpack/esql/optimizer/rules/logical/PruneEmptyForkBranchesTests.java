/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.optimizer.AbstractLogicalPlanOptimizerTests;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Fork;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;

import java.util.List;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class PruneEmptyForkBranchesTests extends AbstractLogicalPlanOptimizerTests {

    /**
     * <pre>{@code
     * Limit[10[INTEGER],false,false]
     * \_Fork[[_meta_field{r}#29, emp_no{r}#30, first_name{r}#31, gender{r}#32, hire_date{r}#33, job{r}#34, job.raw{r}#35, l
     * anguages{r}#36, last_name{r}#37, long_noidx{r}#38, salary{r}#39, x{r}#40, _fork{r}#41]]
     *   \_Project[[_meta_field{f}#12, emp_no{f}#6, first_name{f}#7, gender{f}#8, hire_date{f}#13, job{f}#14, job.raw{f}#15, lang
     * uages{f}#9, last_name{f}#10, long_noidx{f}#16, salary{f}#11, x{r}#5, _fork{r}#3]]
     *     \_Eval[[1[INTEGER] AS x#5, fork1[KEYWORD] AS _fork#3]]
     *       \_Limit[10[INTEGER],false,false]
     *         \_EsRelation[test][_meta_field{f}#12, emp_no{f}#6, first_name{f}#7, ge..]
     * }</pre>
     */
    public void testEmptyForkBranches() {
        var plan = plan("""
            FROM test
            | FORK (EVAL x = 1 ) (WHERE false)
            | LIMIT 10
            """);

        var limit = as(plan, Limit.class);
        var fork = as(limit.child(), Fork.class);

        // the second fork branch is removed
        assertEquals(1, fork.children().size());

        var firstBranch = as(fork.children().get(0), Project.class);
        var eval = as(firstBranch.child(), Eval.class);
        var branchLimit = as(eval.child(), Limit.class);
        assertThat(((Literal) branchLimit.limit()).value(), equalTo(10));

        assertThat(branchLimit.child(), instanceOf(EsRelation.class));
    }

    public void testAllEmptyForkBranches() {
        var plan = plan("""
            FROM test
            | KEEP salary
            | FORK (EVAL x = 1 | WHERE CONTAINS(CONCAT("something", "else"), "nothing"))
                   (WHERE false)
            | LIMIT 10
            """);

        var localRelation = as(plan, LocalRelation.class);
        assertTrue(localRelation.hasEmptySupplier());

        var attributeNames = localRelation.output().stream().map(NamedExpression::name).toList();
        assertEquals(List.of("salary", "x", "_fork"), attributeNames);
    }
}
