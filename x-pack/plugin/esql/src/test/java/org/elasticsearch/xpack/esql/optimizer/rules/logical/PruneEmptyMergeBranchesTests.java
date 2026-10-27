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
import org.elasticsearch.xpack.esql.plan.logical.BranchIdentity;
import org.elasticsearch.xpack.esql.plan.logical.BranchPlan;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.Row;
import org.elasticsearch.xpack.esql.plan.logical.local.EmptyLocalSupplier;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.hamcrest.Matchers.instanceOf;

public class PruneEmptyMergeBranchesTests extends AbstractLogicalPlanOptimizerTests {

    public PruneEmptyMergeBranchesTests(VersionMode versionMode) {
        super(versionMode);
    }

    /**
     * {@snippet lang="text":
     * Project[[...]]
     * \_Eval[[1[INTEGER] AS x, fork1[KEYWORD] AS _fork]]
     *   \_Limit[10[INTEGER],false,false]
     *     \_EsRelation[test][...]
     * }
     */
    public void testEmptyForkBranches() {
        var plan = plan("""
            FROM test
            | FORK (EVAL x = 1 ) (WHERE false)
            | LIMIT 10
            """);

        Project project = as(plan, Project.class);
        Eval eval = as(project.child(), Eval.class);
        Limit branchLimit = as(eval.child(), Limit.class);
        Literal limitLiteral = as(branchLimit.limit(), Literal.class);
        assertEquals(10, limitLiteral.value());
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
     * Project[[...]]
     * \_Eval[[1[INTEGER] AS x]]
     *   \_Limit[10[INTEGER],false,false]
     *     \_EsRelation[test][...]
     * }
     */
    public void testOneEmptySubquery() {
        checkSubquerySupport();
        var plan = plan("""
            FROM (FROM test | EVAL x = 1),
                 (FROM test | WHERE false)
            | LIMIT 10
            """);

        Project project = as(plan, Project.class);
        Eval eval = as(project.child(), Eval.class);
        Limit limit = as(eval.child(), Limit.class);
        Literal limitLiteral = as(limit.limit(), Literal.class);
        assertEquals(10, limitLiteral.value());
        assertThat(limit.child(), instanceOf(EsRelation.class));
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
     * Regression: a {@link BranchPlan} with an empty {@link LocalRelation} branch used to trip
     * the {@code asSubqueryMap} assertion when this rule called {@code replaceChildren} with
     * a shorter list — {@code BranchPlan}'s positional 1:1 invariant doesn't tolerate
     * count changes. The fix routes the prune through {@code MergePlan.pruneEmptyBranches},
     * polymorphically dispatched to {@code BranchPlan}'s name-aware override which preserves
     * the named-subqueries map for surviving children.
     * <p>
     * Built directly rather than via {@code plan(...)} since the failing scenario in serverless
     * needs a CPS-emitted {@link BranchPlan} that the local test fixture doesn't construct.
     */
    public void testPrunesEmptyLocalRelationFromBranchPlan() {
        LocalRelation emptyBranch = new LocalRelation(Source.EMPTY, List.of(), EmptyLocalSupplier.EMPTY);
        Row keptA = new Row(Source.EMPTY, List.of());
        Row keptB = new Row(Source.EMPTY, List.of());

        LinkedHashMap<String, LogicalPlan> children = new LinkedHashMap<>();
        children.put("name_a", keptA);
        children.put("name_empty", emptyBranch);
        children.put("name_b", keptB);
        BranchPlan vua = new BranchPlan(Source.EMPTY, children, allViewIdentities(children.keySet()), List.of());

        LogicalPlan result = new PruneEmptyMergeBranches().apply(vua);

        BranchPlan pruned = as(result, BranchPlan.class);
        assertEquals(2, pruned.children().size());
        assertEquals(List.of("name_a", "name_b"), List.copyOf(pruned.namedSubqueries().keySet()));
        assertSame(keptA, pruned.namedSubqueries().get("name_a"));
        assertSame(keptB, pruned.namedSubqueries().get("name_b"));
    }

    /**
     * All branches pruned: the prune primitive produces a zero-child wrapper. The verifier's
     * {@code MergePlan.checkBranchCount} is responsible for surfacing this as a clear failure
     * ({@code "BranchPlan requires at least one branch"}); rules that want to handle the
     * all-empty case successfully (like {@link PruneEmptyMergeBranches} replacing it with a
     * {@code LocalRelation}) must short-circuit BEFORE delegating to {@code pruneEmptyBranches}.
     */
    public void testAllEmptyProducesZeroChildBranchPlanForVerifierToCatch() {
        LocalRelation a = new LocalRelation(Source.EMPTY, List.of(), EmptyLocalSupplier.EMPTY);
        LocalRelation b = new LocalRelation(Source.EMPTY, List.of(), EmptyLocalSupplier.EMPTY);

        LinkedHashMap<String, LogicalPlan> children = new LinkedHashMap<>();
        children.put("name_a", a);
        children.put("name_b", b);
        BranchPlan vua = new BranchPlan(Source.EMPTY, children, allViewIdentities(children.keySet()), List.of());

        // Bypass PruneEmptyMergeBranches's all-empty pre-check by calling pruneEmptyBranches
        // directly — this is the contract the analyzer's PruneEmptyUnionAllBranch and
        // ViewCompaction.stripViewShadowRelations rely on.
        LogicalPlan result = vua.pruneEmptyBranches(c -> c instanceof LocalRelation lr && lr.hasEmptySupplier());

        BranchPlan empty = as(result, BranchPlan.class);
        assertEquals(0, empty.children().size());
        assertEquals(0, empty.namedSubqueries().size());
    }

    /**
     * Single survivor: {@link PruneEmptyMergeBranches} preserves the {@link BranchPlan} wrapper
     * even when only one branch is left (the existing UnionAll-based tests in this file rely on
     * the same no-collapse semantics — single-survivor collapse lives in
     * {@code ViewCompaction.stripViewShadowRelations}, not in the prune primitive).
     */
    public void testKeepsBranchPlanWrapperEvenWithSingleSurvivor() {
        LocalRelation emptyBranch = new LocalRelation(Source.EMPTY, List.of(), EmptyLocalSupplier.EMPTY);
        Row kept = new Row(Source.EMPTY, List.of());

        LinkedHashMap<String, LogicalPlan> children = new LinkedHashMap<>();
        children.put("name_kept", kept);
        children.put("name_empty", emptyBranch);
        BranchPlan vua = new BranchPlan(Source.EMPTY, children, allViewIdentities(children.keySet()), List.of());

        LogicalPlan result = new PruneEmptyMergeBranches().apply(vua);

        BranchPlan pruned = as(result, BranchPlan.class);
        assertEquals(1, pruned.children().size());
        assertSame(kept, pruned.children().getFirst());
        assertEquals(List.of("name_kept"), List.copyOf(pruned.namedSubqueries().keySet()));
    }

    /** Every key is a view branch named after itself, matching how these tests used to pass {@code children.keySet()}. */
    private static Map<String, BranchIdentity> allViewIdentities(Set<String> keys) {
        Map<String, BranchIdentity> identities = new LinkedHashMap<>();
        for (String key : keys) {
            identities.put(key, BranchIdentity.view(key));
        }
        return identities;
    }
}
