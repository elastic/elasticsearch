/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.optimizer.AbstractLogicalPlanOptimizerTests;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Fork;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.Row;
import org.elasticsearch.xpack.esql.plan.logical.Subquery;
import org.elasticsearch.xpack.esql.plan.logical.UnionAll;
import org.elasticsearch.xpack.esql.plan.logical.ViewUnionAll;
import org.elasticsearch.xpack.esql.plan.logical.local.EmptyLocalSupplier;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;

import java.util.LinkedHashMap;
import java.util.List;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class PruneEmptyForkBranchesTests extends AbstractLogicalPlanOptimizerTests {

    /**
     * {@snippet lang="text":
     * Limit[10[INTEGER],false,false]
     * \_Fork[[_meta_field{r}#29, emp_no{r}#30, first_name{r}#31, gender{r}#32, hire_date{r}#33, job{r}#34, job.raw{r}#35, l
     * anguages{r}#36, last_name{r}#37, long_noidx{r}#38, salary{r}#39, x{r}#40, _fork{r}#41]]
     *   \_Project[[_meta_field{f}#12, emp_no{f}#6, first_name{f}#7, gender{f}#8, hire_date{f}#13, job{f}#14, job.raw{f}#15, lang
     * uages{f}#9, last_name{f}#10, long_noidx{f}#16, salary{f}#11, x{r}#5, _fork{r}#3]]
     *     \_Eval[[1[INTEGER] AS x#5, fork1[KEYWORD] AS _fork#3]]
     *       \_Limit[10[INTEGER],false,false]
     *         \_EsRelation[test][_meta_field{f}#12, emp_no{f}#6, first_name{f}#7, ge..]
     * }
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

    /**
     * {@snippet lang="text":
     * Limit[10[INTEGER],false,false]
     * \_UnionAll[[_meta_field{r}#28, emp_no{r}#29, first_name{r}#30, gender{r}#31, hire_date{r}#32, job{r}#33, job.raw{r}#34, l
     * anguages{r}#35, last_name{r}#36, long_noidx{r}#37, salary{r}#38, x{r}#39]]
     *   \_Project[[_meta_field{f}#11, emp_no{f}#5, first_name{f}#6, gender{f}#7, hire_date{f}#12, job{f}#13, job.raw{f}#14, lang
     * uages{f}#8, last_name{f}#9, long_noidx{f}#15, salary{f}#10, x{r}#4]]
     *     \_Subquery[]
     *       \_Eval[[1[INTEGER] AS x#4]]
     *         \_EsRelation[test][_meta_field{f}#11, emp_no{f}#5, first_name{f}#6, ge..]
     * }
     */
    public void testOneEmptySubquery() {
        checkSubquerySupport();
        var plan = plan("""
            FROM (FROM test | EVAL x = 1),
                 (FROM test | WHERE false)
            | LIMIT 10
            """);

        var limit = as(plan, Limit.class);
        var unionAll = as(limit.child(), UnionAll.class);

        // the second subquery is removed
        assertEquals(1, unionAll.children().size());

        var project = as(unionAll.children().get(0), Project.class);
        var subquery = as(project.child(), Subquery.class);
        var eval = as(subquery.child(), Eval.class);
        assertThat(eval.child(), instanceOf(EsRelation.class));
    }

    public void testAllEmptySubqueries() {
        checkSubquerySupport();
        var plan = plan("""
            FROM (FROM test | KEEP salary | EVAL x = 1 | WHERE CONTAINS(CONCAT("something", "else"), "nothing")),
                 (FROM test | KEEP salary | WHERE false)
            | LIMIT 10
            """);

        var localRelation = as(plan, LocalRelation.class);
        assertTrue(localRelation.hasEmptySupplier());

        var attributeNames = localRelation.output().stream().map(NamedExpression::name).toList();
        assertEquals(List.of("salary", "x"), attributeNames);
    }

    private void checkSubquerySupport() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        assumeTrue(
            "Requires subquery in FROM command support",
            EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND_WITHOUT_IMPLICIT_LIMIT.isEnabled()
        );
    }

    /**
     * Regression: a {@link ViewUnionAll} with an empty {@link LocalRelation} branch used to trip
     * the {@code asSubqueryMap} assertion when this rule called {@code fork.replaceChildren} with
     * a shorter list — {@code ViewUnionAll}'s positional 1:1 invariant doesn't tolerate
     * count changes. The fix routes the prune through {@code Fork.pruneEmptyBranches},
     * polymorphically dispatched to {@code ViewUnionAll}'s name-aware override which preserves
     * the named-subqueries map for surviving children.
     * <p>
     * Built directly rather than via {@code plan(...)} since the failing scenario in serverless
     * needs a CPS-emitted {@link ViewUnionAll} that the local test fixture doesn't construct.
     */
    public void testPrunesEmptyLocalRelationFromViewUnionAll() {
        LocalRelation emptyBranch = new LocalRelation(Source.EMPTY, List.of(), EmptyLocalSupplier.EMPTY);
        Row keptA = new Row(Source.EMPTY, List.of());
        Row keptB = new Row(Source.EMPTY, List.of());

        LinkedHashMap<String, LogicalPlan> children = new LinkedHashMap<>();
        children.put("name_a", keptA);
        children.put("name_empty", emptyBranch);
        children.put("name_b", keptB);
        ViewUnionAll vua = new ViewUnionAll(Source.EMPTY, children, List.of());

        LogicalPlan result = new PruneEmptyForkBranches().apply(vua);

        ViewUnionAll pruned = as(result, ViewUnionAll.class);
        assertEquals(2, pruned.children().size());
        assertEquals(List.of("name_a", "name_b"), List.copyOf(pruned.namedSubqueries().keySet()));
        assertSame(keptA, pruned.namedSubqueries().get("name_a"));
        assertSame(keptB, pruned.namedSubqueries().get("name_b"));
    }

    /**
     * All branches pruned: the prune primitive produces a zero-child wrapper. The verifier's
     * {@code Fork.checkBranchCount} is responsible for surfacing this as a clear failure
     * ({@code "ViewUnionAll requires at least one branch"}); rules that want to handle the
     * all-empty case successfully (like {@link PruneEmptyForkBranches} replacing it with a
     * {@code LocalRelation}) must short-circuit BEFORE delegating to {@code pruneEmptyBranches}.
     */
    public void testAllEmptyProducesZeroChildViewUnionAllForVerifierToCatch() {
        LocalRelation a = new LocalRelation(Source.EMPTY, List.of(), EmptyLocalSupplier.EMPTY);
        LocalRelation b = new LocalRelation(Source.EMPTY, List.of(), EmptyLocalSupplier.EMPTY);

        LinkedHashMap<String, LogicalPlan> children = new LinkedHashMap<>();
        children.put("name_a", a);
        children.put("name_b", b);
        ViewUnionAll vua = new ViewUnionAll(Source.EMPTY, children, List.of());

        // Bypass PruneEmptyForkBranches's all-empty pre-check by calling pruneEmptyBranches
        // directly — this is the contract the analyzer's PruneEmptyUnionAllBranch and
        // ViewCompaction.stripViewShadowRelations rely on.
        LogicalPlan result = vua.pruneEmptyBranches(c -> c instanceof LocalRelation lr && lr.hasEmptySupplier());

        ViewUnionAll empty = as(result, ViewUnionAll.class);
        assertEquals(0, empty.children().size());
        assertEquals(0, empty.namedSubqueries().size());
    }

    /**
     * Single survivor: {@link PruneEmptyForkBranches} preserves the {@link ViewUnionAll} wrapper
     * even when only one branch is left (the existing UnionAll-based tests in this file rely on
     * the same no-collapse semantics — single-survivor collapse lives in
     * {@code ViewCompaction.stripViewShadowRelations}, not in the prune primitive).
     */
    public void testKeepsViewUnionAllWrapperEvenWithSingleSurvivor() {
        LocalRelation emptyBranch = new LocalRelation(Source.EMPTY, List.of(), EmptyLocalSupplier.EMPTY);
        Row kept = new Row(Source.EMPTY, List.of());

        LinkedHashMap<String, LogicalPlan> children = new LinkedHashMap<>();
        children.put("name_kept", kept);
        children.put("name_empty", emptyBranch);
        ViewUnionAll vua = new ViewUnionAll(Source.EMPTY, children, List.of());

        LogicalPlan result = new PruneEmptyForkBranches().apply(vua);

        ViewUnionAll pruned = as(result, ViewUnionAll.class);
        assertEquals(1, pruned.children().size());
        assertSame(kept, pruned.children().getFirst());
        assertEquals(List.of("name_kept"), List.copyOf(pruned.namedSubqueries().keySet()));
    }
}
