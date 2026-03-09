/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Add;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Mul;
import org.elasticsearch.xpack.esql.optimizer.AbstractLogicalPlanOptimizerTests;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.Project;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class ReplaceLimitByExpressionWithEvalTests extends AbstractLogicalPlanOptimizerTests {

    /**
     * Grouping on a plain attribute needs no rewrite.
     * <pre>{@code
     * Limit[1000[INTEGER],[],false,false]
     * \_Limit[1[INTEGER],[emp_no{f}#N],false,false]
     *   \_EsRelation[test][...]
     * }</pre>
     */
    public void testAttributeGroupingUnchanged() {
        var plan = plan("""
            FROM test
            | LIMIT 1 BY emp_no
            """);

        var defaultLimit = as(plan, Limit.class);
        assertThat(defaultLimit.groupings(), empty());
        var limit = as(defaultLimit.child(), Limit.class);
        assertThat(((Literal) limit.limit()).value(), equalTo(1));
        assertThat(Expressions.names(limit.groupings()), contains("emp_no"));
        as(limit.child(), EsRelation.class);
    }

    /**
     * Multiple plain-attribute groupings need no rewrite.
     * <pre>{@code
     * Limit[1000[INTEGER],[],false,false]
     * \_Limit[2[INTEGER],[emp_no{f}#N, salary{f}#M],false,false]
     *   \_EsRelation[test][...]
     * }</pre>
     */
    public void testMultipleAttributeGroupingsUnchanged() {
        var plan = plan("""
            FROM test
            | LIMIT 2 BY emp_no, salary
            """);

        var defaultLimit = as(plan, Limit.class);
        assertThat(defaultLimit.groupings(), empty());
        var limit = as(defaultLimit.child(), Limit.class);
        assertThat(((Literal) limit.limit()).value(), equalTo(2));
        assertThat(Expressions.names(limit.groupings()), contains("emp_no", "salary"));
        as(limit.child(), EsRelation.class);
    }

    /**
     * A constant (foldable) grouping has no grouping effect and should be pruned,
     * degenerating the LIMIT BY into a plain LIMIT. The two limits then get combined.
     * <pre>{@code
     * Limit[1[INTEGER],[],false,false]
     * \_EsRelation[test][...]
     * }</pre>
     */
    public void testAllFoldableGroupingsDegenerateToPlainLimit() {
        var plan = plan("""
            FROM test
            | LIMIT 1 BY 1
            """);

        var limit = as(plan, Limit.class);
        assertThat(((Literal) limit.limit()).value(), equalTo(1));
        assertThat(limit.groupings(), empty());
        as(limit.child(), EsRelation.class);
    }

    /**
     * Multiple foldable groupings are all pruned, degenerating to a plain LIMIT.
     * <pre>{@code
     * Limit[3[INTEGER],[],false,false]
     * \_EsRelation[test][...]
     * }</pre>
     */
    public void testMultipleFoldableGroupingsDegenerateToPlainLimit() {
        var plan = plan("""
            FROM test
            | LIMIT 3 BY 1, "constant", false
            """);

        var limit = as(plan, Limit.class);
        assertThat(((Literal) limit.limit()).value(), equalTo(3));
        assertThat(limit.groupings(), empty());
        as(limit.child(), EsRelation.class);
    }

    /**
     * Only foldable groupings are pruned; attribute groupings survive.
     * <pre>{@code
     * Limit[1000[INTEGER],[],false,false]
     * \_Limit[1[INTEGER],[emp_no{f}#N],false,false]
     *   \_EsRelation[test][...]
     * }</pre>
     */
    public void testMixedFoldableAndAttributeGroupingsPruneFoldable() {
        var plan = plan("""
            FROM test
            | LIMIT 1 BY emp_no, 1
            """);

        var defaultLimit = as(plan, Limit.class);
        assertThat(defaultLimit.groupings(), empty());
        var limit = as(defaultLimit.child(), Limit.class);
        assertThat(((Literal) limit.limit()).value(), equalTo(1));
        assertThat(Expressions.names(limit.groupings()), contains("emp_no"));
        as(limit.child(), EsRelation.class);
    }

    /**
     * A non-attribute expression grouping is extracted into an Eval and the output schema is
     * preserved with a wrapping Project.
     * <pre>{@code
     * Project[[emp_no{f}#N]]
     * \_Limit[1000[INTEGER],[],false,false]
     *   \_Limit[1[INTEGER],[emp_no + 5{r}#M],false,false]
     *     \_Eval[[emp_no{f}#N + 5[INTEGER] AS emp_no + 5#M]]
     *       \_EsRelation[test][...]
     * }</pre>
     */
    public void testSingleExpressionMovedToEval() {
        var plan = plan("""
            FROM test
            | KEEP emp_no
            | LIMIT 1 BY emp_no + 5
            """);

        var project = as(plan, Project.class);
        assertThat(Expressions.names(project.projections()), contains("emp_no"));
        var defaultLimit = as(project.child(), Limit.class);
        assertThat(defaultLimit.groupings(), empty());
        var limit = as(defaultLimit.child(), Limit.class);
        assertThat(((Literal) limit.limit()).value(), equalTo(1));
        assertThat(Expressions.names(limit.groupings()), contains("emp_no + 5"));
        var eval = as(limit.child(), Eval.class);
        assertThat(eval.fields(), hasSize(1));
        var alias = as(eval.fields().getFirst(), Alias.class);
        assertThat(alias.name(), equalTo("emp_no + 5"));
        as(alias.child(), Add.class);
        as(eval.child(), EsRelation.class);
    }

    /**
     * Multiple non-attribute expression groupings are all extracted into a single Eval.
     * <pre>{@code
     * Project[[emp_no{f}#N, salary{f}#M]]
     * \_Limit[1000[INTEGER],[],false,false]
     *   \_Limit[1[INTEGER],[emp_no + 5{r}#A, salary * 2{r}#B],false,false]
     *     \_Eval[[emp_no{f}#N + 5[INTEGER] AS emp_no + 5#A, salary{f}#M * 2[INTEGER] AS salary * 2#B]]
     *       \_EsRelation[test][...]
     * }</pre>
     */
    public void testMultipleExpressionsMovedToEval() {
        var plan = plan("""
            FROM test
            | KEEP emp_no, salary
            | LIMIT 1 BY emp_no + 5, salary * 2
            """);

        var project = as(plan, Project.class);
        assertThat(Expressions.names(project.projections()), contains("emp_no", "salary"));
        var defaultLimit = as(project.child(), Limit.class);
        assertThat(defaultLimit.groupings(), empty());
        var limit = as(defaultLimit.child(), Limit.class);
        assertThat(((Literal) limit.limit()).value(), equalTo(1));
        assertThat(Expressions.names(limit.groupings()), contains("emp_no + 5", "salary * 2"));
        var eval = as(limit.child(), Eval.class);
        assertThat(eval.fields(), hasSize(2));
        var alias0 = as(eval.fields().get(0), Alias.class);
        assertThat(alias0.name(), equalTo("emp_no + 5"));
        as(alias0.child(), Add.class);
        var alias1 = as(eval.fields().get(1), Alias.class);
        assertThat(alias1.name(), equalTo("salary * 2"));
        as(alias1.child(), Mul.class);
        as(eval.child(), EsRelation.class);
    }

    /**
     * When groupings mix plain attributes and expressions, only the expressions are extracted to Eval.
     * <pre>{@code
     * Project[[emp_no{f}#N, salary{f}#M]]
     * \_Limit[1000[INTEGER],[],false,false]
     *   \_Limit[1[INTEGER],[emp_no{f}#N, salary * 2{r}#A],false,false]
     *     \_Eval[[salary{f}#M * 2[INTEGER] AS salary * 2#A]]
     *       \_EsRelation[test][...]
     * }</pre>
     */
    public void testMixedAttributeAndExpressionGroupings() {
        var plan = plan("""
            FROM test
            | KEEP emp_no, salary
            | LIMIT 1 BY emp_no, salary * 2
            """);

        var project = as(plan, Project.class);
        assertThat(Expressions.names(project.projections()), contains("emp_no", "salary"));
        var defaultLimit = as(project.child(), Limit.class);
        assertThat(defaultLimit.groupings(), empty());
        var limit = as(defaultLimit.child(), Limit.class);
        assertThat(((Literal) limit.limit()).value(), equalTo(1));
        assertThat(Expressions.names(limit.groupings()), contains("emp_no", "salary * 2"));
        var eval = as(limit.child(), Eval.class);
        assertThat(eval.fields(), hasSize(1));
        var alias = as(eval.fields().getFirst(), Alias.class);
        assertThat(alias.name(), equalTo("salary * 2"));
        as(alias.child(), Mul.class);
        as(eval.child(), EsRelation.class);
    }

    /**
     * Foldable groupings are pruned and the surviving expression grouping is extracted to Eval.
     * <pre>{@code
     * Project[[emp_no{f}#N]]
     * \_Limit[1000[INTEGER],[],false,false]
     *   \_Limit[1[INTEGER],[emp_no + 5{r}#M],false,false]
     *     \_Eval[[emp_no{f}#N + 5[INTEGER] AS emp_no + 5#M]]
     *       \_EsRelation[test][...]
     * }</pre>
     */
    public void testFoldableGroupingPrunedAndExpressionExtracted() {
        var plan = plan("""
            FROM test
            | KEEP emp_no
            | LIMIT 1 BY emp_no + 5, 1
            """);

        var project = as(plan, Project.class);
        assertThat(Expressions.names(project.projections()), contains("emp_no"));
        var defaultLimit = as(project.child(), Limit.class);
        assertThat(defaultLimit.groupings(), empty());
        var limit = as(defaultLimit.child(), Limit.class);
        assertThat(((Literal) limit.limit()).value(), equalTo(1));
        assertThat(Expressions.names(limit.groupings()), contains("emp_no + 5"));
        var eval = as(limit.child(), Eval.class);
        assertThat(eval.fields(), hasSize(1));
        var alias = as(eval.fields().getFirst(), Alias.class);
        assertThat(alias.name(), equalTo("emp_no + 5"));
        as(alias.child(), Add.class);
        as(eval.child(), EsRelation.class);
    }

    /**
     * All foldable groupings pruned, only attribute grouping survives. No Eval needed.
     * <pre>{@code
     * Limit[1000[INTEGER],[],false,false]
     * \_Limit[1[INTEGER],[emp_no{f}#N],false,false]
     *   \_EsRelation[test][...]
     * }</pre>
     */
    public void testFoldableGroupingPrunedAttributeSurvives() {
        var plan = plan("""
            FROM test
            | LIMIT 1 BY emp_no, 1, false
            """);

        var defaultLimit = as(plan, Limit.class);
        assertThat(defaultLimit.groupings(), empty());
        var limit = as(defaultLimit.child(), Limit.class);
        assertThat(((Literal) limit.limit()).value(), equalTo(1));
        assertThat(Expressions.names(limit.groupings()), contains("emp_no"));
        as(limit.child(), EsRelation.class);
    }

    /**
     * Foldable grouping pruned, attribute and expression groupings survive.
     * <pre>{@code
     * Project[[emp_no{f}#N, salary{f}#M]]
     * \_Limit[1000[INTEGER],[],false,false]
     *   \_Limit[1[INTEGER],[emp_no{f}#N, salary * 2{r}#A],false,false]
     *     \_Eval[[salary{f}#M * 2[INTEGER] AS salary * 2#A]]
     *       \_EsRelation[test][...]
     * }</pre>
     */
    public void testFoldableAttributeAndExpressionGroupingsMixed() {
        var plan = plan("""
            FROM test
            | KEEP emp_no, salary
            | LIMIT 1 BY emp_no, salary * 2, 1
            """);

        var project = as(plan, Project.class);
        assertThat(Expressions.names(project.projections()), contains("emp_no", "salary"));
        var defaultLimit = as(project.child(), Limit.class);
        assertThat(defaultLimit.groupings(), empty());
        var limit = as(defaultLimit.child(), Limit.class);
        assertThat(((Literal) limit.limit()).value(), equalTo(1));
        assertThat(Expressions.names(limit.groupings()), contains("emp_no", "salary * 2"));
        var eval = as(limit.child(), Eval.class);
        assertThat(eval.fields(), hasSize(1));
        var alias = as(eval.fields().getFirst(), Alias.class);
        assertThat(alias.name(), equalTo("salary * 2"));
        as(alias.child(), Mul.class);
        as(eval.child(), EsRelation.class);
    }
}
