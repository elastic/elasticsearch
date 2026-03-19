/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.expression.Order;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Add;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Mul;
import org.elasticsearch.xpack.esql.optimizer.AbstractLogicalPlanOptimizerTests;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LimitBy;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.TopN;
import org.elasticsearch.xpack.esql.plan.logical.TopNBy;
import org.junit.BeforeClass;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.startsWith;

public class ReplaceLimitByExpressionWithEvalTests extends AbstractLogicalPlanOptimizerTests {

    @BeforeClass
    public static void checkLimitByCapability() {
        assumeTrue("LIMIT BY requires snapshot builds", EsqlCapabilities.Cap.ESQL_LIMIT_BY.isEnabled());
    }

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
        var limit = as(defaultLimit.child(), LimitBy.class);
        assertThat(((Literal) limit.limitPerGroup()).value(), equalTo(1));
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
        var limit = as(defaultLimit.child(), LimitBy.class);
        assertThat(((Literal) limit.limitPerGroup()).value(), equalTo(2));
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
        var limit = as(defaultLimit.child(), LimitBy.class);
        assertThat(((Literal) limit.limitPerGroup()).value(), equalTo(1));
        assertThat(Expressions.names(limit.groupings()), contains("emp_no"));
        as(limit.child(), EsRelation.class);
    }

    /**
     * A non-attribute expression grouping is extracted into an Eval and the output schema is
     * preserved with a wrapping Project.
     * <pre>{@code
     * Project[[emp_no{f}#N]]
     * \_Limit[1000[INTEGER],[],false,false]
     *   \_LimitBy[1[INTEGER],[$$limit_by$0$N{r}#M],false,false]
     *     \_Eval[[emp_no{f}#N + 5[INTEGER] AS $$limit_by$0$N]]
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
        var limit = as(defaultLimit.child(), LimitBy.class);
        assertThat(((Literal) limit.limitPerGroup()).value(), equalTo(1));
        var eval = as(limit.child(), Eval.class);
        assertThat(eval.fields(), hasSize(1));
        var alias = as(eval.fields().getFirst(), Alias.class);
        assertThat(alias.name(), startsWith("$$limit_by$0$"));
        as(alias.child(), Add.class);
        as(eval.child(), EsRelation.class);
    }

    /**
     * Multiple non-attribute expression groupings are all extracted into a single Eval.
     * <pre>{@code
     * Project[[emp_no{f}#N, salary{f}#M]]
     * \_Limit[1000[INTEGER],[],false,false]
     *   \_LimitBy[1[INTEGER],[$$limit_by$0$A{r}#X, $$limit_by$1$B{r}#Y],false,false]
     *     \_Eval[[emp_no{f}#N + 5[INTEGER] AS $$limit_by$0$A, salary{f}#M * 2[INTEGER] AS $$limit_by$1$B]]
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
        var limit = as(defaultLimit.child(), LimitBy.class);
        assertThat(((Literal) limit.limitPerGroup()).value(), equalTo(1));
        var eval = as(limit.child(), Eval.class);
        assertThat(eval.fields(), hasSize(2));
        var alias0 = as(eval.fields().get(0), Alias.class);
        assertThat(alias0.name(), startsWith("$$limit_by$0$"));
        as(alias0.child(), Add.class);
        var alias1 = as(eval.fields().get(1), Alias.class);
        assertThat(alias1.name(), startsWith("$$limit_by$1$"));
        as(alias1.child(), Mul.class);
        as(eval.child(), EsRelation.class);
    }

    /**
     * When groupings mix plain attributes and expressions, only the expressions are extracted to Eval.
     * <pre>{@code
     * Project[[emp_no{f}#N, salary{f}#M]]
     * \_Limit[1000[INTEGER],[],false,false]
     *   \_LimitBy[1[INTEGER],[emp_no{f}#N, $$limit_by$1$A{r}#X],false,false]
     *     \_Eval[[salary{f}#M * 2[INTEGER] AS $$limit_by$1$A]]
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
        var limit = as(defaultLimit.child(), LimitBy.class);
        assertThat(((Literal) limit.limitPerGroup()).value(), equalTo(1));
        assertThat(limit.groupings(), hasSize(2));
        assertThat(Expressions.name(limit.groupings().get(0)), equalTo("emp_no"));
        assertThat(Expressions.name(limit.groupings().get(1)), startsWith("$$limit_by$1$"));
        var eval = as(limit.child(), Eval.class);
        assertThat(eval.fields(), hasSize(1));
        var alias = as(eval.fields().getFirst(), Alias.class);
        assertThat(alias.name(), startsWith("$$limit_by$1$"));
        as(alias.child(), Mul.class);
        as(eval.child(), EsRelation.class);
    }

    /**
     * Foldable groupings are pruned and the surviving expression grouping is extracted to Eval.
     * <pre>{@code
     * Project[[emp_no{f}#N]]
     * \_Limit[1000[INTEGER],[],false,false]
     *   \_LimitBy[1[INTEGER],[$$limit_by$0$N{r}#M],false,false]
     *     \_Eval[[emp_no{f}#N + 5[INTEGER] AS $$limit_by$0$N]]
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
        var limit = as(defaultLimit.child(), LimitBy.class);
        assertThat(((Literal) limit.limitPerGroup()).value(), equalTo(1));
        var eval = as(limit.child(), Eval.class);
        assertThat(eval.fields(), hasSize(1));
        var alias = as(eval.fields().getFirst(), Alias.class);
        assertThat(alias.name(), startsWith("$$limit_by$0$"));
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
        var limit = as(defaultLimit.child(), LimitBy.class);
        assertThat(((Literal) limit.limitPerGroup()).value(), equalTo(1));
        assertThat(Expressions.names(limit.groupings()), contains("emp_no"));
        as(limit.child(), EsRelation.class);
    }

    /**
     * An eval alias used alongside an expression in LIMIT BY: {@code x} is already an attribute
     * and left alone, while {@code salary * 2} is extracted into a synthetic Eval.
     * The optimizer merges the user Eval and the synthetic Eval into a single node.
     * <pre>{@code
     * Project[[_meta_field{f}#N, emp_no{f}#M, ..., x{r}#A]]
     * \_Limit[10000[INTEGER],[],false,false]
     *   \_LimitBy[1[INTEGER],[x{r}#A, $$limit_by$1$B{r}#Y],false,false]
     *     \_Eval[[emp_no{f}#M + 5[INTEGER] AS x, salary{f}#M * 2[INTEGER] AS $$limit_by$1$B]]
     *       \_EsRelation[test][...]
     * }</pre>
     */
    public void testEvalAliasAndExpressionMixedInLimitBy() {
        var plan = plan("""
            FROM test
            | EVAL x = emp_no + 5
            | LIMIT 1 BY x, salary * 2
            """);

        var project = as(plan, Project.class);
        var defaultLimit = as(project.child(), Limit.class);
        var limit = as(defaultLimit.child(), LimitBy.class);
        assertThat(((Literal) limit.limitPerGroup()).value(), equalTo(1));
        assertThat(limit.groupings(), hasSize(2));
        assertThat(Expressions.name(limit.groupings().get(0)), equalTo("x"));
        assertThat(Expressions.name(limit.groupings().get(1)), startsWith("$$limit_by$1$"));
        var eval = as(limit.child(), Eval.class);
        assertThat(eval.fields(), hasSize(2));
        var xAlias = as(eval.fields().get(0), Alias.class);
        assertThat(xAlias.name(), equalTo("x"));
        assertThat(xAlias.child(), instanceOf(Add.class));
        var salaryAlias = as(eval.fields().get(1), Alias.class);
        assertThat(salaryAlias.name(), startsWith("$$limit_by$1$"));
        assertThat(salaryAlias.child(), instanceOf(Mul.class));
        as(eval.child(), EsRelation.class);
    }

    /**
     * Foldable grouping pruned, attribute and expression groupings survive.
     * <pre>{@code
     * Project[[emp_no{f}#N, salary{f}#M]]
     * \_Limit[1000[INTEGER],[],false,false]
     *   \_LimitBy[1[INTEGER],[emp_no{f}#N, $$limit_by$1$A{r}#X],false,false]
     *     \_Eval[[salary{f}#M * 2[INTEGER] AS $$limit_by$1$A]]
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
        var limit = as(defaultLimit.child(), LimitBy.class);
        assertThat(((Literal) limit.limitPerGroup()).value(), equalTo(1));
        assertThat(limit.groupings(), hasSize(2));
        assertThat(Expressions.name(limit.groupings().get(0)), equalTo("emp_no"));
        assertThat(Expressions.name(limit.groupings().get(1)), startsWith("$$limit_by$1$"));
        var eval = as(limit.child(), Eval.class);
        assertThat(eval.fields(), hasSize(1));
        var alias = as(eval.fields().getFirst(), Alias.class);
        assertThat(alias.name(), startsWith("$$limit_by$1$"));
        as(alias.child(), Mul.class);
        as(eval.child(), EsRelation.class);
    }

    /**
     * SORT | LIMIT BY with a plain attribute should not introduce any synthetic EVAL.
     * The plan should be the same as the existing testTopNWithGroupingsPlan.
     */
    public void testTopNByAttributeDoesNotIntroduceEval() {
        assumeTrue("SORT | LIMIT BY requires snapshot builds", EsqlCapabilities.Cap.ESQL_TOPN_BY.isEnabled());
        var query = """
            FROM employees
            | SORT salary DESC
            | LIMIT 5 BY languages
            """;

        var plan = optimizedPlan(query);

        var defaultLimit = as(plan, Limit.class);
        assertThat(((Literal) defaultLimit.limit()).value(), equalTo(1000));
        var topNBy = as(defaultLimit.child(), TopNBy.class);
        var limit = as(topNBy.limitPerGroup(), Literal.class);
        assertThat(limit.value(), equalTo(5));
        assertThat(topNBy.order().size(), equalTo(1));

        assertThat(topNBy.groupings(), everyItem(instanceOf(FieldAttribute.class)));
        assertThat(topNBy.groupings().size(), equalTo(1));
        var groupKey = as(topNBy.groupings().getFirst(), FieldAttribute.class);
        assertThat(groupKey.synthetic(), equalTo(false));
        assertThat(groupKey.name(), equalTo("languages"));

        var order = as(topNBy.order().getFirst(), Order.class);
        var orderAttr = as(order.child(), FieldAttribute.class);
        assertThat(orderAttr.name(), equalTo("salary"));
        assertThat(order.direction(), equalTo(Order.OrderDirection.DESC));

        var esRelation = as(topNBy.child(), EsRelation.class);
        assertThat(esRelation.indexPattern(), equalTo("employees"));
    }

    /**
     * SORT | LIMIT BY with an expression should extract the expression into a synthetic EVAL.
     * {@code FROM employees | SORT salary | LIMIT 2 BY languages * 2}
     * becomes
     * {@code Project | TopNBy[groupings=[$$limit_by_ref]] | Eval[$$limit_by = languages * 2] | EsRelation}
     */
    public void testTopNByExpressionIntroducesEval() {
        assumeTrue("SORT | LIMIT BY requires snapshot builds", EsqlCapabilities.Cap.ESQL_TOPN_BY.isEnabled());
        var query = """
            FROM employees
            | SORT salary DESC
            | LIMIT 5 BY languages * 2
            """;

        var plan = optimizedPlan(query);

        // Project hides the synthetic eval attribute
        var project = as(plan, Project.class);

        var defaultLimit = as(project.child(), Limit.class);
        assertThat(((Literal) defaultLimit.limit()).value(), equalTo(1000));
        // TopNBy has the synthetic attribute as grouping
        var topNBy = as(defaultLimit.child(), TopNBy.class);
        var limit = as(topNBy.limitPerGroup(), Literal.class);
        assertThat(limit.value(), equalTo(5));
        assertThat(topNBy.order().size(), equalTo(1));

        assertThat(topNBy.groupings().size(), equalTo(1));
        var groupKey = as(topNBy.groupings().getFirst(), ReferenceAttribute.class);

        var order = as(topNBy.order().getFirst(), Order.class);
        var orderAttr = as(order.child(), FieldAttribute.class);
        assertThat(orderAttr.name(), equalTo("salary"));
        assertThat(order.direction(), equalTo(Order.OrderDirection.DESC));

        // Eval computes the expression
        var eval = as(topNBy.child(), Eval.class);
        assertThat(eval.fields().size(), equalTo(1));
        var alias = as(eval.fields().getFirst(), Alias.class);
        assertThat(alias.toAttribute().id(), equalTo(groupKey.id()));
        assertThat(alias.child(), instanceOf(Mul.class));

        as(eval.child(), EsRelation.class);
    }

    /**
     * SORT | LIMIT BY with a mix of attributes and expressions should only extract the expression.
     * {@code FROM employees | SORT salary | LIMIT 2 BY languages, salary * 2}
     */
    public void testTopNByMixedAttrAndExpression() {
        assumeTrue("SORT | LIMIT BY requires snapshot builds", EsqlCapabilities.Cap.ESQL_TOPN_BY.isEnabled());
        var query = """
            FROM employees
            | SORT salary DESC
            | LIMIT 5 BY languages, salary * 2
            """;

        var plan = optimizedPlan(query);

        var project = as(plan, Project.class);
        var defaultLimit = as(project.child(), Limit.class);
        assertThat(((Literal) defaultLimit.limit()).value(), equalTo(1000));
        var topnNBy = as(defaultLimit.child(), TopNBy.class);
        assertThat(topnNBy.groupings().size(), equalTo(2));

        // First grouping is a plain attribute -- no extraction needed
        var firstGroupKey = as(topnNBy.groupings().get(0), FieldAttribute.class);
        assertThat(firstGroupKey.name(), equalTo("languages"));

        // Second grouping is a synthetic reference attribute
        var secondGroupKey = as(topnNBy.groupings().get(1), ReferenceAttribute.class);

        // Eval only computes the expression for the second grouping
        var eval = as(topnNBy.child(), Eval.class);
        assertThat(eval.fields().size(), equalTo(1));
        var alias = as(eval.fields().getFirst(), Alias.class);
        assertThat(alias.toAttribute().id(), equalTo(secondGroupKey.id()));
        assertThat(alias.child(), instanceOf(Mul.class));

        as(eval.child(), EsRelation.class);
    }

    /**
     * All groupings are foldable -- the LIMIT BY degenerates to a plain LIMIT (TopN with no groupings).
     */
    public void testTopNByFoldableExprs() {
        assumeTrue("SORT | LIMIT BY requires snapshot builds", EsqlCapabilities.Cap.ESQL_TOPN_BY.isEnabled());
        var query = """
            FROM employees
            | SORT salary DESC
            | LIMIT 5 BY 20 * 5, 10
            """;

        var plan = optimizedPlan(query);

        // No Project wrapper needed -- no synthetic eval was created
        var topN = as(plan, TopN.class);
        var limit = as(topN.limit(), Literal.class);
        assertThat(limit.value(), equalTo(5));
        as(topN.child(), EsRelation.class);
    }

    /**
     * Mixed foldable and attribute groupings: the foldable one (42) is pruned, the attribute (languages) remains.
     */
    public void testTopNByMixedAttrAndFoldable() {
        assumeTrue("SORT | LIMIT BY requires snapshot builds", EsqlCapabilities.Cap.ESQL_TOPN_BY.isEnabled());
        var query = """
            FROM employees
            | SORT salary DESC
            | LIMIT 5 BY languages, 42
            """;

        var plan = optimizedPlan(query);

        // No Project wrapper -- the remaining grouping is a plain attribute, no eval needed
        var defaultLimit = as(plan, Limit.class);
        assertThat(((Literal) defaultLimit.limit()).value(), equalTo(1000));
        var topNBy = as(defaultLimit.child(), TopNBy.class);
        assertThat(topNBy.groupings().size(), equalTo(1));
        var groupKey = as(topNBy.groupings().getFirst(), FieldAttribute.class);
        assertThat(groupKey.name(), equalTo("languages"));
        var limit = as(topNBy.limitPerGroup(), Literal.class);
        assertThat(limit.value(), equalTo(5));
        as(topNBy.child(), EsRelation.class);
    }

    /**
     * Mixed foldable and expression groupings: the foldable (42) is pruned, the expression (languages * 2) is extracted to eval.
     */
    public void testTopNByMixedExprsAttr() {
        assumeTrue("SORT | LIMIT BY requires snapshot builds", EsqlCapabilities.Cap.ESQL_TOPN_BY.isEnabled());
        var query = """
            FROM employees
            | SORT salary DESC
            | LIMIT 5 BY languages * 2, 42
            """;

        var plan = optimizedPlan(query);

        // Project hides the synthetic eval attribute
        var project = as(plan, Project.class);
        var defaultLimit = as(project.child(), Limit.class);
        assertThat(((Literal) defaultLimit.limit()).value(), equalTo(1000));
        var topNBy = as(defaultLimit.child(), TopNBy.class);
        assertThat(topNBy.groupings().size(), equalTo(1));
        var groupKey = as(topNBy.groupings().getFirst(), ReferenceAttribute.class);

        // Eval computes the expression
        var eval = as(topNBy.child(), Eval.class);
        assertThat(eval.fields().size(), equalTo(1));
        var alias = as(eval.fields().getFirst(), Alias.class);
        assertThat(alias.toAttribute().id(), equalTo(groupKey.id()));

        as(eval.child(), EsRelation.class);
    }
}
