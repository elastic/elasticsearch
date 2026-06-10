/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.UnresolvedAttribute;
import org.elasticsearch.xpack.esql.core.expression.UnresolvedStar;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.UnresolvedNamePattern;

import java.util.List;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.of;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.relation;
import static org.elasticsearch.xpack.esql.core.tree.Source.EMPTY;
import static org.elasticsearch.xpack.esql.plan.logical.UnmappedFieldsPattern.ALL;
import static org.hamcrest.Matchers.equalTo;

/**
 * Unit tests for {@link LogicalPlan#unmappedFieldsToKeep()}.
 *
 * <p>These tests describe the expected behavior BEFORE the implementation is complete.
 * Tests involving {@link Keep}, {@link Drop}, {@link Eval}, and {@link Rename} will
 * fail until those classes override {@code unmappedFieldsToKeep()}, because the
 * inherited {@link UnaryPlan} implementation just delegates to the child (returning
 * {@link UnmappedFieldsPattern#ALL} for all currently-written overrides).
 *
 * <p>The method must be called on plans where {@link Keep}/{@link Drop}/{@link Rename}
 * still hold their original wildcard patterns (i.e., before the analyzer's
 * {@code ResolveRefs} rule converts them to {@link Project} nodes).
 */
public class UnmappedFieldsToKeepTests extends ESTestCase {

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    /**
     * Creates an {@link UnresolvedNamePattern} for the given wildcard string.
     * The automaton is left {@code null} because {@code unmappedFieldsToKeep} reads
     * only {@link UnresolvedNamePattern#pattern()}, never calling
     * {@link UnresolvedNamePattern#match(String)}.
     */
    private static UnresolvedNamePattern namePattern(String pattern) {
        return new UnresolvedNamePattern(EMPTY, null, pattern, pattern);
    }

    private static UnresolvedAttribute attr(String name) {
        return new UnresolvedAttribute(EMPTY, name);
    }

    /** A harmless literal used as a condition or expression in plan nodes. */
    private static Literal trueLiteral() {
        return new Literal(EMPTY, Boolean.TRUE, DataType.BOOLEAN);
    }

    // -------------------------------------------------------------------------
    // EsRelation — base case
    // -------------------------------------------------------------------------

    public void testEsRelationAlone() {
        assertThat(relation().unmappedFieldsToKeep(), equalTo(ALL));
    }

    // -------------------------------------------------------------------------
    // Pass-through unary nodes
    // These nodes do not affect which field names reach the output, so they
    // delegate to their child and must return the same pattern.
    // -------------------------------------------------------------------------

    public void testFilterIsTransparent() {
        Filter filter = new Filter(EMPTY, relation(), trueLiteral());
        assertThat(filter.unmappedFieldsToKeep(), equalTo(ALL));
    }

    public void testLimitIsTransparent() {
        Limit limit = new Limit(EMPTY, of(5), relation());
        assertThat(limit.unmappedFieldsToKeep(), equalTo(ALL));
    }

    public void testOrderByIsTransparent() {
        OrderBy orderBy = new OrderBy(EMPTY, relation(), List.of());
        assertThat(orderBy.unmappedFieldsToKeep(), equalTo(ALL));
    }

    // -------------------------------------------------------------------------
    // KEEP — restricts the set of fields to those matching the projection patterns
    // -------------------------------------------------------------------------

    /**
     * {@code FROM idx | KEEP *}
     * KEEP * keeps everything, so the pattern is ALL.
     * NOTE: Before implementation this passes coincidentally because UnaryPlan
     * delegates to the child (EsRelation), which also returns ALL. The test is
     * still included for documentation purposes and because correctness must be
     * explicitly verified after implementation.
     */
    public void testKeepStar() {
        Keep keep = new Keep(EMPTY, relation(), List.of(new UnresolvedStar(EMPTY, null)));
        assertThat(keep.unmappedFieldsToKeep(), equalTo(ALL));
    }

    /**
     * {@code FROM idx | KEEP emp_no*}
     * Only unmapped fields whose name matches {@code emp_no*} are kept.
     * FAILS before implementation (UnaryPlan returns ALL via child delegation).
     */
    public void testKeepSingleWildcard() {
        Keep keep = new Keep(EMPTY, relation(), List.of(namePattern("emp_no*")));
        assertThat(keep.unmappedFieldsToKeep(), equalTo(new UnmappedFieldsPattern(List.of("emp_no*"), List.of())));
    }

    /**
     * {@code FROM idx | KEEP salary}
     * Only an unmapped field named exactly {@code salary} is kept.
     * FAILS before implementation.
     */
    public void testKeepSingleExactName() {
        Keep keep = new Keep(EMPTY, relation(), List.of(attr("salary")));
        assertThat(keep.unmappedFieldsToKeep(), equalTo(new UnmappedFieldsPattern(List.of("salary"), List.of())));
    }

    /**
     * {@code FROM idx | KEEP emp_no*, salary}
     * Unmapped fields matching {@code emp_no*} OR named {@code salary} are kept.
     * FAILS before implementation.
     */
    public void testKeepMultiplePatterns() {
        List<NamedExpression> projections = List.of(namePattern("emp_no*"), attr("salary"));
        Keep keep = new Keep(EMPTY, relation(), projections);
        assertThat(keep.unmappedFieldsToKeep(), equalTo(new UnmappedFieldsPattern(List.of("emp_no*", "salary"), List.of())));
    }

    /**
     * {@code FROM idx | KEEP foo*, bar*}
     * Unmapped fields matching {@code foo*} OR {@code bar*} are kept.
     * FAILS before implementation.
     */
    public void testKeepMultipleWildcards() {
        List<NamedExpression> projections = List.of(namePattern("foo*"), namePattern("bar*"));
        Keep keep = new Keep(EMPTY, relation(), projections);
        assertThat(keep.unmappedFieldsToKeep(), equalTo(new UnmappedFieldsPattern(List.of("foo*", "bar*"), List.of())));
    }

    // -------------------------------------------------------------------------
    // DROP — excludes fields matching the removal patterns
    // -------------------------------------------------------------------------

    /**
     * {@code FROM idx | DROP salary*}
     * Every unmapped field is kept EXCEPT those matching {@code salary*}.
     * FAILS before implementation.
     */
    public void testDropSingleWildcard() {
        Drop drop = new Drop(EMPTY, relation(), List.of(namePattern("salary*")));
        assertThat(drop.unmappedFieldsToKeep(), equalTo(new UnmappedFieldsPattern(List.of("*"), List.of("salary*"))));
    }

    /**
     * {@code FROM idx | DROP salary}
     * Every unmapped field is kept EXCEPT the one named exactly {@code salary}.
     * FAILS before implementation.
     */
    public void testDropSingleExactName() {
        Drop drop = new Drop(EMPTY, relation(), List.of(attr("salary")));
        assertThat(drop.unmappedFieldsToKeep(), equalTo(new UnmappedFieldsPattern(List.of("*"), List.of("salary"))));
    }

    /**
     * {@code FROM idx | DROP salary*, manager_id}
     * FAILS before implementation.
     */
    public void testDropMultiple() {
        Drop drop = new Drop(EMPTY, relation(), List.of(namePattern("salary*"), attr("manager_id")));
        assertThat(drop.unmappedFieldsToKeep(), equalTo(new UnmappedFieldsPattern(List.of("*"), List.of("salary*", "manager_id"))));
    }

    // -------------------------------------------------------------------------
    // EVAL — computed fields shadow any source field with the same name
    // -------------------------------------------------------------------------

    /**
     * {@code FROM idx | EVAL x = 1}
     * Every unmapped field passes through EXCEPT {@code x}, which is shadowed
     * by the computed value.
     * FAILS before implementation.
     */
    public void testEvalSingleField() {
        Alias xAlias = new Alias(EMPTY, "x", trueLiteral());
        Eval eval = new Eval(EMPTY, relation(), List.of(xAlias));
        assertThat(eval.unmappedFieldsToKeep(), equalTo(new UnmappedFieldsPattern(List.of("*"), List.of("x"))));
    }

    /**
     * {@code FROM idx | EVAL x = 1, y = 2}
     * Both {@code x} and {@code y} are shadowed.
     * FAILS before implementation.
     */
    public void testEvalMultipleFields() {
        List<Alias> aliases = List.of(new Alias(EMPTY, "x", trueLiteral()), new Alias(EMPTY, "y", trueLiteral()));
        Eval eval = new Eval(EMPTY, relation(), aliases);
        assertThat(eval.unmappedFieldsToKeep(), equalTo(new UnmappedFieldsPattern(List.of("*"), List.of("x", "y"))));
    }

    // -------------------------------------------------------------------------
    // RENAME — target names shadow any source field with the same name
    // (source names are already in EsRelation and thus excluded by definition)
    // -------------------------------------------------------------------------

    /**
     * {@code FROM idx | RENAME y AS x}
     * The output column {@code x} now refers to the mapped field {@code y}.
     * Any unmapped field named {@code x} is shadowed and cannot appear.
     * FAILS before implementation.
     */
    public void testRenameSingleField() {
        Alias renaming = new Alias(EMPTY, "x", attr("y"));
        Rename rename = new Rename(EMPTY, relation(), List.of(renaming));
        assertThat(rename.unmappedFieldsToKeep(), equalTo(new UnmappedFieldsPattern(List.of("*"), List.of("x"))));
    }

    /**
     * {@code FROM idx | RENAME y AS x, z AS w}
     * Both {@code x} and {@code w} are excluded.
     * FAILS before implementation.
     */
    public void testRenameMultipleFields() {
        List<Alias> renamings = List.of(new Alias(EMPTY, "x", attr("y")), new Alias(EMPTY, "w", attr("z")));
        Rename rename = new Rename(EMPTY, relation(), renamings);
        assertThat(rename.unmappedFieldsToKeep(), equalTo(new UnmappedFieldsPattern(List.of("*"), List.of("x", "w"))));
    }

    // -------------------------------------------------------------------------
    // Combinations
    // -------------------------------------------------------------------------

    /**
     * {@code FROM idx | KEEP emp_no* | EVAL x = 1}
     * Only unmapped fields matching {@code emp_no*} pass the KEEP, and of those,
     * any field named {@code x} is then shadowed by the EVAL.
     * Plan tree: Eval(fields=[x], child=Keep([emp_no*], EsRelation))
     * FAILS before implementation.
     */
    public void testEvalOnTopOfKeep() {
        Keep keep = new Keep(EMPTY, relation(), List.of(namePattern("emp_no*")));
        Eval eval = new Eval(EMPTY, keep, List.of(new Alias(EMPTY, "x", trueLiteral())));
        assertThat(eval.unmappedFieldsToKeep(), equalTo(new UnmappedFieldsPattern(List.of("emp_no*"), List.of("x"))));
    }

    /**
     * {@code FROM idx | DROP salary | EVAL x = 1}
     * Unmapped fields pass through except salary (dropped) and x (shadowed by EVAL).
     * Plan tree: Eval(fields=[x], child=Drop([salary], EsRelation))
     * FAILS before implementation.
     */
    public void testEvalOnTopOfDrop() {
        Drop drop = new Drop(EMPTY, relation(), List.of(attr("salary")));
        Eval eval = new Eval(EMPTY, drop, List.of(new Alias(EMPTY, "x", trueLiteral())));
        assertThat(eval.unmappedFieldsToKeep(), equalTo(new UnmappedFieldsPattern(List.of("*"), List.of("salary", "x"))));
    }

    /**
     * {@code FROM idx | EVAL x = 1 | KEEP emp_no*}
     * EVAL x shadows unmapped x; then KEEP emp_no* restricts to emp_no*.
     * Plan tree: Keep([emp_no*], child=Eval([x], EsRelation))
     * FAILS before implementation.
     */
    public void testKeepOnTopOfEval() {
        Eval eval = new Eval(EMPTY, relation(), List.of(new Alias(EMPTY, "x", trueLiteral())));
        Keep keep = new Keep(EMPTY, eval, List.of(namePattern("emp_no*")));
        assertThat(keep.unmappedFieldsToKeep(), equalTo(new UnmappedFieldsPattern(List.of("emp_no*"), List.of("x"))));
    }

    /**
     * {@code FROM idx | LIMIT 10 | EVAL x = 1 | KEEP emp_no*}
     * LIMIT is transparent; EVAL x shadows unmapped x; KEEP emp_no* restricts.
     * Plan tree: Keep([emp_no*], child=Eval([x], child=Limit(EsRelation)))
     * FAILS before implementation.
     */
    public void testKeepOnTopOfEvalOnTopOfLimit() {
        Limit limit = new Limit(EMPTY, of(10), relation());
        Eval eval = new Eval(EMPTY, limit, List.of(new Alias(EMPTY, "x", trueLiteral())));
        Keep keep = new Keep(EMPTY, eval, List.of(namePattern("emp_no*")));
        assertThat(keep.unmappedFieldsToKeep(), equalTo(new UnmappedFieldsPattern(List.of("emp_no*"), List.of("x"))));
    }

    /**
     * {@code FROM idx | RENAME y AS x | DROP salary*}
     * Rename excludes x (target); Drop excludes salary*.
     * Plan tree: Drop([salary*], child=Rename([y→x], EsRelation))
     * FAILS before implementation.
     */
    public void testDropOnTopOfRename() {
        Rename rename = new Rename(EMPTY, relation(), List.of(new Alias(EMPTY, "x", attr("y"))));
        Drop drop = new Drop(EMPTY, rename, List.of(namePattern("salary*")));
        assertThat(drop.unmappedFieldsToKeep(), equalTo(new UnmappedFieldsPattern(List.of("*"), List.of("x", "salary*"))));
    }
}
