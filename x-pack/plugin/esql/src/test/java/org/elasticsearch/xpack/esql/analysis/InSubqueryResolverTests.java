/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.UnresolvedAttribute;
import org.elasticsearch.xpack.esql.expression.predicate.logical.And;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Not;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThan;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.InSubquery;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThan;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.Keep;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.UnionAllFromDisjunctiveInSubquery;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedRelation;
import org.elasticsearch.xpack.esql.plan.logical.join.AntiJoin;
import org.elasticsearch.xpack.esql.plan.logical.join.JoinTypes;
import org.elasticsearch.xpack.esql.plan.logical.join.SemiJoin;
import org.junit.Before;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_PARSER;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.hamcrest.Matchers.containsString;

/**
 * Tests for {@link InSubqueryResolver}, which converts {@link InSubquery} expressions in
 * {@link Filter} conditions into {@link SemiJoin}/{@link AntiJoin} nodes, and rejects
 * {@link InSubquery} in unsupported positions (EVAL, SORT, STATS BY, etc.).
 */
public class InSubqueryResolverTests extends ESTestCase {

    @Before
    public void checkCapability() {
        assumeTrue("Requires IN subquery support", EsqlCapabilities.Cap.WHERE_IN_SUBQUERY_WITH_VIEW.isEnabled());
    }

    // ---- positive: WHERE IN subquery → SemiJoin ----

    /**
     * SemiJoin[left=UnresolvedRelation[main], right=UnresolvedRelation[sub]]
     */
    public void testBasicInSubquery() {
        LogicalPlan plan = resolve("FROM main | WHERE x IN (FROM sub)");
        SemiJoin semiJoin = as(plan, SemiJoin.class);
        assertEquals(JoinTypes.SEMI, semiJoin.config().type());
        assertEquals(1, semiJoin.config().leftFields().size());
        assertEquals("x", semiJoin.config().leftFields().get(0).name());
        assertTrue(semiJoin.config().rightFields().isEmpty());
        UnresolvedRelation main = as(semiJoin.left(), UnresolvedRelation.class);
        assertEquals("main", main.indexPattern().indexPattern());
        UnresolvedRelation sub = as(semiJoin.right(), UnresolvedRelation.class);
        assertEquals("sub", sub.indexPattern().indexPattern());
    }

    // ---- positive: WHERE NOT IN subquery → AntiJoin ----

    /**
     * AntiJoin[left=UnresolvedRelation[main], right=UnresolvedRelation[sub]]
     */
    public void testBasicNotInSubquery() {
        LogicalPlan plan = resolve("FROM main | WHERE x NOT IN (FROM sub)");
        AntiJoin antiJoin = as(plan, AntiJoin.class);
        assertEquals(JoinTypes.ANTI, antiJoin.config().type());
        assertEquals(1, antiJoin.config().leftFields().size());
        assertEquals("x", antiJoin.config().leftFields().get(0).name());
        assertTrue(antiJoin.config().rightFields().isEmpty());
        UnresolvedRelation main = as(antiJoin.left(), UnresolvedRelation.class);
        assertEquals("main", main.indexPattern().indexPattern());
        UnresolvedRelation sub = as(antiJoin.right(), UnresolvedRelation.class);
        assertEquals("sub", sub.indexPattern().indexPattern());
    }

    // ---- positive: subquery with processing commands ----

    /**
     * SemiJoin[left=UnresolvedRelation[main], right=Keep[y][UnresolvedRelation[sub]]]
     */
    public void testInSubqueryWithKeep() {
        LogicalPlan plan = resolve("FROM main | WHERE x IN (FROM sub | KEEP y)");
        SemiJoin semiJoin = as(plan, SemiJoin.class);
        assertEquals(JoinTypes.SEMI, semiJoin.config().type());
        assertEquals("x", semiJoin.config().leftFields().get(0).name());
        UnresolvedRelation main = as(semiJoin.left(), UnresolvedRelation.class);
        assertEquals("main", main.indexPattern().indexPattern());
        Keep keep = as(semiJoin.right(), Keep.class);
        UnresolvedRelation sub = as(keep.child(), UnresolvedRelation.class);
        assertEquals("sub", sub.indexPattern().indexPattern());
    }

    // ---- positive: subquery with STATS ----

    /**
     * SemiJoin[left=UnresolvedRelation[main], right=Aggregate[UnresolvedRelation[sub]]]
     */
    public void testInSubqueryWithStats() {
        LogicalPlan plan = resolve("FROM main | WHERE x IN (FROM sub | STATS MAX(y))");
        SemiJoin semiJoin = as(plan, SemiJoin.class);
        assertEquals(JoinTypes.SEMI, semiJoin.config().type());
        assertEquals("x", semiJoin.config().leftFields().get(0).name());
        UnresolvedRelation main = as(semiJoin.left(), UnresolvedRelation.class);
        assertEquals("main", main.indexPattern().indexPattern());
        Aggregate agg = as(semiJoin.right(), Aggregate.class);
        UnresolvedRelation sub = as(agg.child(), UnresolvedRelation.class);
        assertEquals("sub", sub.indexPattern().indexPattern());
    }

    // ---- positive: constant left-hand side ----

    /**
     * SemiJoin[left=Eval[$$in_subquery_const$...=42][UnresolvedRelation[main]], right=Keep[y][UnresolvedRelation[sub]]]
     */
    public void testConstantLeftSide() {
        LogicalPlan plan = resolve("FROM main | WHERE 42 IN (FROM sub | KEEP y)");
        SemiJoin semiJoin = as(plan, SemiJoin.class);
        assertEquals(JoinTypes.SEMI, semiJoin.config().type());
        Attribute leftField = semiJoin.config().leftFields().get(0);
        assertThat(leftField.name(), containsString("$$in_subquery_const$"));
        Eval eval = as(semiJoin.left(), Eval.class);
        assertEquals(1, eval.fields().size());
        Alias alias = eval.fields().get(0);
        Literal literal = as(alias.child(), Literal.class);
        assertEquals(42, literal.value());
        UnresolvedRelation main = as(eval.child(), UnresolvedRelation.class);
        assertEquals("main", main.indexPattern().indexPattern());
        Keep keep = as(semiJoin.right(), Keep.class);
        UnresolvedRelation sub = as(keep.child(), UnresolvedRelation.class);
        assertEquals("sub", sub.indexPattern().indexPattern());
    }

    // ---- positive: IN subquery combined with other conditions ----

    /**
     * SemiJoin[left=Filter[GreaterThan[?a, 5]][UnresolvedRelation[main]], right=Keep[y][UnresolvedRelation[sub]]]
     */
    public void testConjunctiveInSubqueryWithOtherPredicates() {
        LogicalPlan plan = resolve("FROM main | WHERE a > 5 AND x IN (FROM sub | KEEP y)");
        SemiJoin semiJoin = as(plan, SemiJoin.class);
        assertEquals(JoinTypes.SEMI, semiJoin.config().type());
        assertEquals("x", semiJoin.config().leftFields().get(0).name());
        Filter filter = as(semiJoin.left(), Filter.class);
        as(filter.condition(), GreaterThan.class);
        UnresolvedRelation main = as(filter.child(), UnresolvedRelation.class);
        assertEquals("main", main.indexPattern().indexPattern());
        Keep keep = as(semiJoin.right(), Keep.class);
        UnresolvedRelation sub = as(keep.child(), UnresolvedRelation.class);
        assertEquals("sub", sub.indexPattern().indexPattern());
    }

    // ---- positive: predicate between two IN subqueries ----

    /**
     * SemiJoin[?y, left=SemiJoin[?x, left=Filter[GreaterThan][UnresolvedRelation[main]]]]
     */
    public void testConjunctiveInSubqueriesWithOtherPredicates() {
        LogicalPlan plan = resolve("""
            FROM main | WHERE x IN (FROM sub1) AND a > 5 AND y IN (FROM sub2)
            """);
        SemiJoin outer = as(plan, SemiJoin.class);
        assertEquals(JoinTypes.SEMI, outer.config().type());
        assertEquals("y", outer.config().leftFields().get(0).name());
        UnresolvedRelation outerRight = as(outer.right(), UnresolvedRelation.class);
        assertEquals("sub2", outerRight.indexPattern().indexPattern());
        SemiJoin inner = as(outer.left(), SemiJoin.class);
        assertEquals(JoinTypes.SEMI, inner.config().type());
        assertEquals("x", inner.config().leftFields().get(0).name());
        UnresolvedRelation innerRight = as(inner.right(), UnresolvedRelation.class);
        assertEquals("sub1", innerRight.indexPattern().indexPattern());
        Filter filter = as(inner.left(), Filter.class);
        as(filter.condition(), GreaterThan.class);
        UnresolvedRelation main = as(filter.child(), UnresolvedRelation.class);
        assertEquals("main", main.indexPattern().indexPattern());
    }

    // ---- positive: IN subquery AND regular predicate AND NOT IN subquery ----

    /**
     * AntiJoin[?y, left=SemiJoin[?x, left=Filter[GreaterThan][UnresolvedRelation[main]]]]
     */
    public void testConjunctiveInAndNotInSubqueryWithOtherPredicates() {
        LogicalPlan plan = resolve("""
            FROM main | WHERE x IN (FROM sub1) AND a > 5 AND y NOT IN (FROM sub2)
            """);
        AntiJoin antiJoin = as(plan, AntiJoin.class);
        assertEquals(JoinTypes.ANTI, antiJoin.config().type());
        assertEquals("y", antiJoin.config().leftFields().get(0).name());
        UnresolvedRelation antiRight = as(antiJoin.right(), UnresolvedRelation.class);
        assertEquals("sub2", antiRight.indexPattern().indexPattern());
        SemiJoin semiJoin = as(antiJoin.left(), SemiJoin.class);
        assertEquals(JoinTypes.SEMI, semiJoin.config().type());
        assertEquals("x", semiJoin.config().leftFields().get(0).name());
        UnresolvedRelation semiRight = as(semiJoin.right(), UnresolvedRelation.class);
        assertEquals("sub1", semiRight.indexPattern().indexPattern());
        Filter filter = as(semiJoin.left(), Filter.class);
        as(filter.condition(), GreaterThan.class);
        UnresolvedRelation main = as(filter.child(), UnresolvedRelation.class);
        assertEquals("main", main.indexPattern().indexPattern());
    }

    // ---- positive: multiple IN subqueries (stacked joins) ----

    /**
     * SemiJoin[?y, left=SemiJoin[?x, left=UnresolvedRelation[main]]]
     */
    public void testConjunctiveInSubqueriesOnly() {
        LogicalPlan plan = resolve("FROM main | WHERE x IN (FROM sub1) AND y IN (FROM sub2)");
        SemiJoin outer = as(plan, SemiJoin.class);
        assertEquals(JoinTypes.SEMI, outer.config().type());
        assertEquals("y", outer.config().leftFields().get(0).name());
        UnresolvedRelation outerRight = as(outer.right(), UnresolvedRelation.class);
        assertEquals("sub2", outerRight.indexPattern().indexPattern());
        SemiJoin inner = as(outer.left(), SemiJoin.class);
        assertEquals(JoinTypes.SEMI, inner.config().type());
        assertEquals("x", inner.config().leftFields().get(0).name());
        UnresolvedRelation innerRight = as(inner.right(), UnresolvedRelation.class);
        assertEquals("sub1", innerRight.indexPattern().indexPattern());
        UnresolvedRelation main = as(inner.left(), UnresolvedRelation.class);
        assertEquals("main", main.indexPattern().indexPattern());
    }

    // ---- positive: mixed IN and NOT IN ----

    /**
     * AntiJoin[?y, left=SemiJoin[?x, left=UnresolvedRelation[main]]]
     */
    public void testConjunctiveInSubqueryAndNotInSubquery() {
        LogicalPlan plan = resolve("FROM main | WHERE x IN (FROM sub1) AND y NOT IN (FROM sub2)");
        AntiJoin antiJoin = as(plan, AntiJoin.class);
        assertEquals(JoinTypes.ANTI, antiJoin.config().type());
        assertEquals("y", antiJoin.config().leftFields().get(0).name());
        UnresolvedRelation antiRight = as(antiJoin.right(), UnresolvedRelation.class);
        assertEquals("sub2", antiRight.indexPattern().indexPattern());
        SemiJoin semiJoin = as(antiJoin.left(), SemiJoin.class);
        assertEquals(JoinTypes.SEMI, semiJoin.config().type());
        assertEquals("x", semiJoin.config().leftFields().get(0).name());
        UnresolvedRelation semiRight = as(semiJoin.right(), UnresolvedRelation.class);
        assertEquals("sub1", semiRight.indexPattern().indexPattern());
        UnresolvedRelation main = as(semiJoin.left(), UnresolvedRelation.class);
        assertEquals("main", main.indexPattern().indexPattern());
    }

    // ---- positive: multiple NOT IN subqueries ----

    /**
     * AntiJoin[?y, left=AntiJoin[?x, left=UnresolvedRelation[main]]]
     */
    public void testConjunctiveNotInSubqueriesOnly() {
        LogicalPlan plan = resolve("FROM main | WHERE x NOT IN (FROM sub1) AND y NOT IN (FROM sub2)");
        AntiJoin outer = as(plan, AntiJoin.class);
        assertEquals(JoinTypes.ANTI, outer.config().type());
        assertEquals("y", outer.config().leftFields().get(0).name());
        UnresolvedRelation outerRight = as(outer.right(), UnresolvedRelation.class);
        assertEquals("sub2", outerRight.indexPattern().indexPattern());
        AntiJoin inner = as(outer.left(), AntiJoin.class);
        assertEquals(JoinTypes.ANTI, inner.config().type());
        assertEquals("x", inner.config().leftFields().get(0).name());
        UnresolvedRelation innerRight = as(inner.right(), UnresolvedRelation.class);
        assertEquals("sub1", innerRight.indexPattern().indexPattern());
        UnresolvedRelation main = as(inner.left(), UnresolvedRelation.class);
        assertEquals("main", main.indexPattern().indexPattern());
    }

    // ---- positive: double NOT produces SemiJoin ----

    public void testDoubleNotInSubquery() {
        LogicalPlan plan = resolve("FROM main | WHERE NOT (x NOT IN (FROM sub))");
        SemiJoin semiJoin = as(plan, SemiJoin.class);
        assertEquals(JoinTypes.SEMI, semiJoin.config().type());
        assertEquals("x", semiJoin.config().leftFields().get(0).name());
        UnresolvedRelation main = as(semiJoin.left(), UnresolvedRelation.class);
        assertEquals("main", main.indexPattern().indexPattern());
    }

    // ---- positive: triple NOT produces AntiJoin ----

    public void testTripleNestedNotInSubquery() {
        LogicalPlan plan = resolve("FROM main | WHERE NOT (NOT (x NOT IN (FROM sub)))");
        AntiJoin antiJoin = as(plan, AntiJoin.class);
        assertEquals(JoinTypes.ANTI, antiJoin.config().type());
        assertEquals("x", antiJoin.config().leftFields().get(0).name());
        UnresolvedRelation main = as(antiJoin.left(), UnresolvedRelation.class);
        assertEquals("main", main.indexPattern().indexPattern());
    }

    // ---- positive: nested IN subquery inside the subquery ----

    /**
     * SemiJoin[?x, left=UnresolvedRelation[main],
     *          right=Keep[SemiJoin[?y, left=UnresolvedRelation[sub1], right=Keep[UnresolvedRelation[sub2]]]]]
     */
    public void testNestedInSubquery() {
        LogicalPlan plan = resolve("FROM main | WHERE x IN (FROM sub1 | WHERE y IN (FROM sub2 | KEEP b) | KEEP a)");
        SemiJoin outer = as(plan, SemiJoin.class);
        assertEquals(JoinTypes.SEMI, outer.config().type());
        assertEquals("x", outer.config().leftFields().get(0).name());
        UnresolvedRelation main = as(outer.left(), UnresolvedRelation.class);
        assertEquals("main", main.indexPattern().indexPattern());
        Keep outerKeep = as(outer.right(), Keep.class);
        SemiJoin inner = as(outerKeep.child(), SemiJoin.class);
        assertEquals(JoinTypes.SEMI, inner.config().type());
        assertEquals("y", inner.config().leftFields().get(0).name());
        UnresolvedRelation sub1 = as(inner.left(), UnresolvedRelation.class);
        assertEquals("sub1", sub1.indexPattern().indexPattern());
        Keep innerKeep = as(inner.right(), Keep.class);
        UnresolvedRelation sub2 = as(innerKeep.child(), UnresolvedRelation.class);
        assertEquals("sub2", sub2.indexPattern().indexPattern());
    }

    // ---- positive: nested NOT IN subquery inside a NOT IN subquery ----

    /**
     * AntiJoin[?x, left=UnresolvedRelation[main],
     *          right=Keep[AntiJoin[?y, left=UnresolvedRelation[sub1], right=Keep[UnresolvedRelation[sub2]]]]]
     */
    public void testNestedNotInSubquery() {
        LogicalPlan plan = resolve("""
            FROM main | WHERE x NOT IN (FROM sub1 | WHERE y NOT IN (FROM sub2 | KEEP b) | KEEP a)
            """);
        AntiJoin outer = as(plan, AntiJoin.class);
        assertEquals(JoinTypes.ANTI, outer.config().type());
        assertEquals("x", outer.config().leftFields().get(0).name());
        UnresolvedRelation main = as(outer.left(), UnresolvedRelation.class);
        assertEquals("main", main.indexPattern().indexPattern());
        Keep outerKeep = as(outer.right(), Keep.class);
        AntiJoin inner = as(outerKeep.child(), AntiJoin.class);
        assertEquals(JoinTypes.ANTI, inner.config().type());
        assertEquals("y", inner.config().leftFields().get(0).name());
        UnresolvedRelation sub1 = as(inner.left(), UnresolvedRelation.class);
        assertEquals("sub1", sub1.indexPattern().indexPattern());
        Keep innerKeep = as(inner.right(), Keep.class);
        UnresolvedRelation sub2 = as(innerKeep.child(), UnresolvedRelation.class);
        assertEquals("sub2", sub2.indexPattern().indexPattern());
    }

    // ---- positive: nested IN inside NOT IN ----

    /**
     * AntiJoin[?x, left=UnresolvedRelation[main],
     *          right=Keep[SemiJoin[?y, left=UnresolvedRelation[sub1], right=Keep[UnresolvedRelation[sub2]]]]]
     */
    public void testNestedInSubqueryAndNotInSubquery() {
        LogicalPlan plan = resolve("""
            FROM main | WHERE x NOT IN (FROM sub1 | WHERE y IN (FROM sub2 | KEEP b) | KEEP a)
            """);
        AntiJoin outer = as(plan, AntiJoin.class);
        assertEquals(JoinTypes.ANTI, outer.config().type());
        assertEquals("x", outer.config().leftFields().get(0).name());
        UnresolvedRelation main = as(outer.left(), UnresolvedRelation.class);
        assertEquals("main", main.indexPattern().indexPattern());
        Keep outerKeep = as(outer.right(), Keep.class);
        SemiJoin inner = as(outerKeep.child(), SemiJoin.class);
        assertEquals(JoinTypes.SEMI, inner.config().type());
        assertEquals("y", inner.config().leftFields().get(0).name());
        UnresolvedRelation sub1 = as(inner.left(), UnresolvedRelation.class);
        assertEquals("sub1", sub1.indexPattern().indexPattern());
    }

    // ---- positive: nested NOT IN inside IN ----

    /**
     * SemiJoin[?x, left=UnresolvedRelation[main],
     *          right=Keep[AntiJoin[?y, left=UnresolvedRelation[sub1], right=Keep[UnresolvedRelation[sub2]]]]]
     */
    public void testNestedNotInInsideIn() {
        LogicalPlan plan = resolve("""
            FROM main | WHERE x IN (FROM sub1 | WHERE y NOT IN (FROM sub2 | KEEP b) | KEEP a)
            """);
        SemiJoin outer = as(plan, SemiJoin.class);
        assertEquals(JoinTypes.SEMI, outer.config().type());
        assertEquals("x", outer.config().leftFields().get(0).name());
        UnresolvedRelation main = as(outer.left(), UnresolvedRelation.class);
        assertEquals("main", main.indexPattern().indexPattern());
        Keep outerKeep = as(outer.right(), Keep.class);
        AntiJoin inner = as(outerKeep.child(), AntiJoin.class);
        assertEquals(JoinTypes.ANTI, inner.config().type());
        assertEquals("y", inner.config().leftFields().get(0).name());
        UnresolvedRelation sub1 = as(inner.left(), UnresolvedRelation.class);
        assertEquals("sub1", sub1.indexPattern().indexPattern());
    }

    // ---- positive: two separate WHERE clauses each with IN subquery ----

    /**
     * SemiJoin[?y, left=SemiJoin[?x, left=UnresolvedRelation[main]]]
     */
    public void testTwoWhereClausesWithInSubqueries() {
        LogicalPlan plan = resolve("""
            FROM main | WHERE x IN (FROM sub1 | KEEP a) | WHERE y IN (FROM sub2 | KEEP b)
            """);
        SemiJoin outer = as(plan, SemiJoin.class);
        assertEquals(JoinTypes.SEMI, outer.config().type());
        assertEquals("y", outer.config().leftFields().get(0).name());
        Keep outerRight = as(outer.right(), Keep.class);
        UnresolvedRelation sub2 = as(outerRight.child(), UnresolvedRelation.class);
        assertEquals("sub2", sub2.indexPattern().indexPattern());
        SemiJoin inner = as(outer.left(), SemiJoin.class);
        assertEquals(JoinTypes.SEMI, inner.config().type());
        assertEquals("x", inner.config().leftFields().get(0).name());
        Keep innerRight = as(inner.right(), Keep.class);
        UnresolvedRelation sub1 = as(innerRight.child(), UnresolvedRelation.class);
        assertEquals("sub1", sub1.indexPattern().indexPattern());
        UnresolvedRelation main = as(inner.left(), UnresolvedRelation.class);
        assertEquals("main", main.indexPattern().indexPattern());
    }

    // ---- positive: OR disjuncts with IN subquery → UnionAllFromDisjunctiveInSubquery ----

    public void testDisjunctiveInSubquery() {
        LogicalPlan plan = resolve("FROM main | WHERE x IN (FROM sub1) OR y IN (FROM sub2)");
        UnionAllFromDisjunctiveInSubquery unionAll = as(plan, UnionAllFromDisjunctiveInSubquery.class);
        assertEquals(2, unionAll.children().size());
        SemiJoin first = as(unionAll.children().get(0), SemiJoin.class);
        assertEquals(JoinTypes.SEMI, first.config().type());
        assertEquals("x", first.config().leftFields().get(0).name());
        UnresolvedRelation firstRight = as(first.right(), UnresolvedRelation.class);
        assertEquals("sub1", firstRight.indexPattern().indexPattern());
        UnresolvedRelation left = as(first.left(), UnresolvedRelation.class);
        assertEquals("main", left.indexPattern().indexPattern());
        SemiJoin second = as(unionAll.children().get(1), SemiJoin.class);
        assertEquals(JoinTypes.SEMI, second.config().type());
        assertEquals("y", second.config().leftFields().get(0).name());
        UnresolvedRelation secondRight = as(second.right(), UnresolvedRelation.class);
        assertEquals("sub2", secondRight.indexPattern().indexPattern());
        left = as(first.left(), UnresolvedRelation.class);
        assertEquals("main", left.indexPattern().indexPattern());
    }

    // ---- positive: OR with NOT IN subquery ----

    public void testDisjunctiveNotInSubquery() {
        LogicalPlan plan = resolve("FROM main | WHERE x NOT IN (FROM sub1) OR y NOT IN (FROM sub2)");
        UnionAllFromDisjunctiveInSubquery unionAll = as(plan, UnionAllFromDisjunctiveInSubquery.class);
        assertEquals(2, unionAll.children().size());
        AntiJoin first = as(unionAll.children().get(0), AntiJoin.class);
        assertEquals(JoinTypes.ANTI, first.config().type());
        assertEquals("x", first.config().leftFields().get(0).name());
        UnresolvedRelation firstRight = as(first.right(), UnresolvedRelation.class);
        assertEquals("sub1", firstRight.indexPattern().indexPattern());
        UnresolvedRelation left = as(first.left(), UnresolvedRelation.class);
        assertEquals("main", left.indexPattern().indexPattern());
        AntiJoin second = as(unionAll.children().get(1), AntiJoin.class);
        assertEquals(JoinTypes.ANTI, second.config().type());
        assertEquals("y", second.config().leftFields().get(0).name());
        UnresolvedRelation secondRight = as(second.right(), UnresolvedRelation.class);
        assertEquals("sub2", secondRight.indexPattern().indexPattern());
        left = as(first.left(), UnresolvedRelation.class);
        assertEquals("main", left.indexPattern().indexPattern());
    }

    // ---- positive: OR mixing IN and NOT IN ----

    public void testDisjunctiveInAndNotInSubquery() {
        LogicalPlan plan = resolve("FROM main | WHERE x IN (FROM sub1) OR y NOT IN (FROM sub2)");
        UnionAllFromDisjunctiveInSubquery unionAll = as(plan, UnionAllFromDisjunctiveInSubquery.class);
        assertEquals(2, unionAll.children().size());
        SemiJoin first = as(unionAll.children().get(0), SemiJoin.class);
        assertEquals(JoinTypes.SEMI, first.config().type());
        assertEquals("x", first.config().leftFields().get(0).name());
        UnresolvedRelation firstRight = as(first.right(), UnresolvedRelation.class);
        assertEquals("sub1", firstRight.indexPattern().indexPattern());
        UnresolvedRelation left = as(first.left(), UnresolvedRelation.class);
        assertEquals("main", left.indexPattern().indexPattern());
        AntiJoin second = as(unionAll.children().get(1), AntiJoin.class);
        assertEquals(JoinTypes.ANTI, second.config().type());
        assertEquals("y", second.config().leftFields().get(0).name());
        UnresolvedRelation secondRight = as(second.right(), UnresolvedRelation.class);
        assertEquals("sub2", secondRight.indexPattern().indexPattern());
        left = as(first.left(), UnresolvedRelation.class);
        assertEquals("main", left.indexPattern().indexPattern());
    }

    // ---- positive: OR with IN subquery and regular predicate ----

    /**
     * UnionAllFromDisjunctiveInSubquery[
     *   SemiJoin[?x, left=UnresolvedRelation[main], right=UnresolvedRelation[sub]],
     *   AntiJoin[?x, left=Filter[GreaterThan][UnresolvedRelation[main]], right=UnresolvedRelation[sub]]
     * ]
     * The second branch is NOT(x IN sub) AND a > 5, which becomes AntiJoin on top of Filter[a > 5].
     */
    public void testDisjunctiveInSubqueryWithOtherPredicate() {
        // Reordering puts "a > 5" (no InSubquery, complexity 0) before "x IN sub" (complexity 1)
        LogicalPlan plan = resolve("FROM main | WHERE x IN (FROM sub) OR a > 5");
        UnionAllFromDisjunctiveInSubquery unionAll = as(plan, UnionAllFromDisjunctiveInSubquery.class);
        assertEquals(2, unionAll.children().size());
        // Branch 1: a > 5 → Filter
        Filter first = as(unionAll.children().get(0), Filter.class);
        as(first.condition(), GreaterThan.class);
        UnresolvedRelation firstLeft = as(first.child(), UnresolvedRelation.class);
        assertEquals("main", firstLeft.indexPattern().indexPattern());
        // Branch 2: NOT(a > 5) AND x IN sub → SemiJoin with exclusion filter below
        SemiJoin second = as(unionAll.children().get(1), SemiJoin.class);
        assertEquals(JoinTypes.SEMI, second.config().type());
        assertEquals("x", second.config().leftFields().get(0).name());
        UnresolvedRelation secondRight = as(second.right(), UnresolvedRelation.class);
        assertEquals("sub", secondRight.indexPattern().indexPattern());
        Filter secondFilter = as(second.left(), Filter.class);
        UnresolvedRelation secondLeft = as(secondFilter.child(), UnresolvedRelation.class);
        assertEquals("main", secondLeft.indexPattern().indexPattern());
    }

    // ---- positive: three-way OR with IN subqueries ----

    public void testMultipleDisjunctiveInSubqueries() {
        LogicalPlan plan = resolve("FROM main | WHERE x IN (FROM sub1) OR y IN (FROM sub2) OR z IN (FROM sub3)");
        UnionAllFromDisjunctiveInSubquery unionAll = as(plan, UnionAllFromDisjunctiveInSubquery.class);
        assertEquals(3, unionAll.children().size());
        SemiJoin first = as(unionAll.children().get(0), SemiJoin.class);
        assertEquals(JoinTypes.SEMI, first.config().type());
        assertEquals("x", first.config().leftFields().get(0).name());
        UnresolvedRelation firstRight = as(first.right(), UnresolvedRelation.class);
        assertEquals("sub1", firstRight.indexPattern().indexPattern());
        UnresolvedRelation main = as(first.left(), UnresolvedRelation.class);
        assertEquals("main", main.indexPattern().indexPattern());
        SemiJoin second = as(unionAll.children().get(1), SemiJoin.class);
        assertEquals(JoinTypes.SEMI, second.config().type());
        assertEquals("y", second.config().leftFields().get(0).name());
        UnresolvedRelation secondRight = as(second.right(), UnresolvedRelation.class);
        assertEquals("sub2", secondRight.indexPattern().indexPattern());
        AntiJoin antiJoin = as(second.left(), AntiJoin.class);
        assertEquals(JoinTypes.ANTI, antiJoin.config().type());
        assertEquals("x", antiJoin.config().leftFields().get(0).name());
        firstRight = as(antiJoin.right(), UnresolvedRelation.class);
        assertEquals("sub1", firstRight.indexPattern().indexPattern());
        main = as(antiJoin.left(), UnresolvedRelation.class);
        assertEquals("main", main.indexPattern().indexPattern());
        SemiJoin third = as(unionAll.children().get(2), SemiJoin.class);
        assertEquals(JoinTypes.SEMI, third.config().type());
        assertEquals("z", third.config().leftFields().get(0).name());
        UnresolvedRelation thirdRight = as(third.right(), UnresolvedRelation.class);
        assertEquals("sub3", thirdRight.indexPattern().indexPattern());
        antiJoin = as(third.left(), AntiJoin.class);
        assertEquals(JoinTypes.ANTI, antiJoin.config().type());
        assertEquals("y", antiJoin.config().leftFields().get(0).name());
        secondRight = as(antiJoin.right(), UnresolvedRelation.class);
        assertEquals("sub2", secondRight.indexPattern().indexPattern());
        antiJoin = as(antiJoin.left(), AntiJoin.class);
        assertEquals(JoinTypes.ANTI, antiJoin.config().type());
        assertEquals("x", antiJoin.config().leftFields().get(0).name());
        firstRight = as(antiJoin.right(), UnresolvedRelation.class);
        assertEquals("sub1", firstRight.indexPattern().indexPattern());
        main = as(antiJoin.left(), UnresolvedRelation.class);
        assertEquals("main", main.indexPattern().indexPattern());
    }

    // ---- positive: constant NOT IN subquery ----

    /**
     * AntiJoin[left=Eval[$$in_subquery_const$...=42][UnresolvedRelation[main]], right=Keep[y][UnresolvedRelation[sub]]]
     */
    public void testConstantNotInSubquery() {
        LogicalPlan plan = resolve("FROM main | WHERE 42 NOT IN (FROM sub | KEEP y)");
        AntiJoin antiJoin = as(plan, AntiJoin.class);
        assertEquals(JoinTypes.ANTI, antiJoin.config().type());
        Attribute leftField = antiJoin.config().leftFields().get(0);
        assertThat(leftField.name(), containsString("$$in_subquery_const$"));
        Eval eval = as(antiJoin.left(), Eval.class);
        assertEquals(1, eval.fields().size());
        as(eval.fields().get(0).child(), Literal.class);
        UnresolvedRelation main = as(eval.child(), UnresolvedRelation.class);
        assertEquals("main", main.indexPattern().indexPattern());
        Keep keep = as(antiJoin.right(), Keep.class);
        UnresolvedRelation sub = as(keep.child(), UnresolvedRelation.class);
        assertEquals("sub", sub.indexPattern().indexPattern());
    }

    // ---- positive: fully disjunctive OR chain with NOT IN subquery ----

    /**
     * {@code WHERE x IN (FROM sub1) OR (y == 1 OR (z < 0 OR w NOT IN (FROM sub2)))}
     * <p>
     * The nested ORs flatten into four disjuncts: {@code x IN sub1}, {@code y == 1}, {@code z < 0},
     * {@code w NOT IN sub2}. Each becomes a branch in the UnionAll. Branches containing InSubquery
     * are resolved to SemiJoin/AntiJoin; the others stay as Filters.
     */
    public void testDisjunctiveOrChainWithInSubquery() {
        // Reordering puts plain predicates (y==1, z<0) before InSubquery predicates (x IN sub1, w NOT IN sub2)
        LogicalPlan plan = resolve("FROM main | WHERE x IN (FROM sub1) OR (y == 1 OR (z < 0 OR w NOT IN (FROM sub2)))");
        UnionAllFromDisjunctiveInSubquery unionAll = as(plan, UnionAllFromDisjunctiveInSubquery.class);
        assertEquals(4, unionAll.children().size());

        // Branch 1: y == 1 → Filter[main]
        Filter branch1 = as(unionAll.children().get(0), Filter.class);
        Equals equals = as(branch1.condition(), Equals.class);
        as(equals.left(), UnresolvedAttribute.class);
        UnresolvedRelation branch1Left = as(branch1.child(), UnresolvedRelation.class);
        assertEquals("main", branch1Left.indexPattern().indexPattern());

        // Branch 2: NOT(y==1) AND z < 0 → Filter[...][main]
        Filter branch2 = as(unionAll.children().get(1), Filter.class);
        And and = as(branch2.condition(), And.class);
        LessThan lessThan = as(and.right(), LessThan.class);
        as(lessThan.left(), UnresolvedAttribute.class);
        Not not = as(and.left(), Not.class);
        equals = as(not.field(), Equals.class);
        as(equals.left(), UnresolvedAttribute.class);
        UnresolvedRelation branch2Left = as(branch2.child(), UnresolvedRelation.class);
        assertEquals("main", branch2Left.indexPattern().indexPattern());

        // Branch 3: NOT(y==1) AND NOT(z<0) AND x IN sub1 → SemiJoin[x, left=Filter[...][main], right=sub1]
        SemiJoin branch3 = as(unionAll.children().get(2), SemiJoin.class);
        assertEquals(JoinTypes.SEMI, branch3.config().type());
        assertEquals("x", branch3.config().leftFields().get(0).name());
        Filter branch3Filter = as(branch3.left(), Filter.class);
        and = as(branch3Filter.condition(), And.class);
        not = as(and.right(), Not.class);
        lessThan = as(not.field(), LessThan.class);
        as(lessThan.left(), UnresolvedAttribute.class);
        not = as(and.left(), Not.class);
        equals = as(not.field(), Equals.class);
        as(equals.left(), UnresolvedAttribute.class);
        UnresolvedRelation branch3Left = as(branch3Filter.child(), UnresolvedRelation.class);
        assertEquals("main", branch3Left.indexPattern().indexPattern());
        UnresolvedRelation branch3Right = as(branch3.right(), UnresolvedRelation.class);
        assertEquals("sub1", branch3Right.indexPattern().indexPattern());

        // Branch 4: ...AND NOT(x IN sub1) AND w NOT IN sub2
        // → AntiJoin[w, left=AntiJoin[x, left=Filter[...][main], right=sub1], right=sub2]
        AntiJoin branch4 = as(unionAll.children().get(3), AntiJoin.class);
        assertEquals(JoinTypes.ANTI, branch4.config().type());
        assertEquals("w", branch4.config().leftFields().get(0).name());
        UnresolvedRelation branch4Right = as(branch4.right(), UnresolvedRelation.class);
        assertEquals("sub2", branch4Right.indexPattern().indexPattern());
        AntiJoin branch4Exclusion = as(branch4.left(), AntiJoin.class);
        assertEquals("x", branch4Exclusion.config().leftFields().get(0).name());
        UnresolvedRelation branch4ExclRight = as(branch4Exclusion.right(), UnresolvedRelation.class);
        assertEquals("sub1", branch4ExclRight.indexPattern().indexPattern());
        Filter branch4Filter = as(branch4Exclusion.left(), Filter.class);
        and = as(branch4Filter.condition(), And.class);
        not = as(and.right(), Not.class);
        lessThan = as(not.field(), LessThan.class);
        as(lessThan.left(), UnresolvedAttribute.class);
        not = as(and.left(), Not.class);
        equals = as(not.field(), Equals.class);
        as(equals.left(), UnresolvedAttribute.class);
    }

    // ---- positive: mixed OR chain with AND containing NOT IN subquery ----

    /**
     * {@code WHERE x IN (FROM sub1) OR (y == 1 OR (z < 0 AND w NOT IN (FROM sub2)))}
     * Reordered: [y == 1, x IN sub1, z &lt; 0 AND w NOT IN sub2] (complexity 0, 1, 2)
     */
    public void testDisjunctiveOrChainWithConjunctiveInSubquery() {
        LogicalPlan plan = resolve("FROM main | WHERE x IN (FROM sub1) OR (y == 1 OR (z < 0 AND w NOT IN (FROM sub2)))");
        UnionAllFromDisjunctiveInSubquery unionAll = as(plan, UnionAllFromDisjunctiveInSubquery.class);
        assertEquals(3, unionAll.children().size());

        // Branch 1: y == 1 → Filter[main]
        Filter branch1 = as(unionAll.children().get(0), Filter.class);
        Equals equals = as(branch1.condition(), Equals.class);
        as(equals.left(), UnresolvedAttribute.class);
        UnresolvedRelation branch1Left = as(branch1.child(), UnresolvedRelation.class);
        assertEquals("main", branch1Left.indexPattern().indexPattern());

        // Branch 2: NOT(y==1) AND x IN sub1 → SemiJoin[x, left=Filter[...][main], right=sub1]
        SemiJoin branch2 = as(unionAll.children().get(1), SemiJoin.class);
        assertEquals(JoinTypes.SEMI, branch2.config().type());
        assertEquals("x", branch2.config().leftFields().get(0).name());
        Filter branch2Filter = as(branch2.left(), Filter.class);
        Not not = as(branch2Filter.condition(), Not.class);
        equals = as(not.field(), Equals.class);
        as(equals.left(), UnresolvedAttribute.class);
        UnresolvedRelation branch2Right = as(branch2.right(), UnresolvedRelation.class);
        assertEquals("sub1", branch2Right.indexPattern().indexPattern());

        // Branch 3: NOT(y==1) AND NOT(x IN sub1) AND z < 0 AND w NOT IN sub2
        // → AntiJoin[w, left=AntiJoin[x, left=Filter[...][main], right=sub1], right=sub2]
        AntiJoin branch3 = as(unionAll.children().get(2), AntiJoin.class);
        assertEquals(JoinTypes.ANTI, branch3.config().type());
        assertEquals("w", branch3.config().leftFields().get(0).name());
        UnresolvedRelation branch3Right = as(branch3.right(), UnresolvedRelation.class);
        assertEquals("sub2", branch3Right.indexPattern().indexPattern());
        AntiJoin branch3AntiJoin = as(branch3.left(), AntiJoin.class);
        assertEquals(JoinTypes.ANTI, branch3AntiJoin.config().type());
        assertEquals("x", branch3AntiJoin.config().leftFields().get(0).name());
        branch3Right = as(branch3AntiJoin.right(), UnresolvedRelation.class);
        assertEquals("sub1", branch3Right.indexPattern().indexPattern());
        Filter branch3Filter = as(branch3AntiJoin.left(), Filter.class);
        And and = as(branch3Filter.condition(), And.class);
        not = as(and.left(), Not.class);
        equals = as(not.field(), Equals.class);
        as(equals.left(), UnresolvedAttribute.class);
        LessThan lessThan = as(and.right(), LessThan.class);
        as(lessThan.left(), UnresolvedAttribute.class);
    }

    // ---- positive: OR chain with NOT IN subquery in the middle ----

    /**
     * {@code WHERE x IN (FROM sub1) OR (y == 1 OR (w NOT IN (FROM sub2)) OR z < 0)}
     * Reordered: [y == 1, z &lt; 0, x IN sub1, w NOT IN sub2] (complexity 0, 0, 1, 1)
     */
    public void testDisjunctiveOrChainWithNotInSubqueryInMiddle() {
        LogicalPlan plan = resolve("FROM main | WHERE x IN (FROM sub1) OR (y == 1 OR (w NOT IN (FROM sub2)) OR z < 0)");
        UnionAllFromDisjunctiveInSubquery unionAll = as(plan, UnionAllFromDisjunctiveInSubquery.class);
        assertEquals(4, unionAll.children().size());

        // Branch 1: y == 1 → Filter[main]
        Filter branch1 = as(unionAll.children().get(0), Filter.class);
        Equals equals = as(branch1.condition(), Equals.class);
        as(equals.left(), UnresolvedAttribute.class);
        UnresolvedRelation branch1Left = as(branch1.child(), UnresolvedRelation.class);
        assertEquals("main", branch1Left.indexPattern().indexPattern());

        // Branch 2: NOT(y==1) AND z < 0 → Filter[...][main]
        Filter branch2 = as(unionAll.children().get(1), Filter.class);
        And and = as(branch2.condition(), And.class);
        Not not = as(and.left(), Not.class);
        equals = as(not.field(), Equals.class);
        as(equals.left(), UnresolvedAttribute.class);
        LessThan lessThan = as(and.right(), LessThan.class);
        as(lessThan.left(), UnresolvedAttribute.class);
        UnresolvedRelation branch2Left = as(branch2.child(), UnresolvedRelation.class);
        assertEquals("main", branch2Left.indexPattern().indexPattern());

        // Branch 3: ...AND x IN sub1 → SemiJoin[x, left=Filter[...][main], right=sub1]
        SemiJoin branch3 = as(unionAll.children().get(2), SemiJoin.class);
        assertEquals(JoinTypes.SEMI, branch3.config().type());
        assertEquals("x", branch3.config().leftFields().get(0).name());
        UnresolvedRelation branch3Right = as(branch3.right(), UnresolvedRelation.class);
        assertEquals("sub1", branch3Right.indexPattern().indexPattern());
        Filter branch3Filter = as(branch3.left(), Filter.class);
        and = as(branch3Filter.condition(), And.class);
        not = as(and.left(), Not.class);
        equals = as(not.field(), Equals.class);
        as(equals.left(), UnresolvedAttribute.class);
        not = as(and.right(), Not.class);
        lessThan = as(not.field(), LessThan.class);
        as(lessThan.left(), UnresolvedAttribute.class);
        UnresolvedRelation branch3Left = as(branch3Filter.child(), UnresolvedRelation.class);
        assertEquals("main", branch3Left.indexPattern().indexPattern());

        // Branch 4: ...AND NOT(x IN sub1) AND w NOT IN sub2
        // → AntiJoin[w, left=AntiJoin[x, ...], right=sub2]
        AntiJoin branch4 = as(unionAll.children().get(3), AntiJoin.class);
        assertEquals(JoinTypes.ANTI, branch4.config().type());
        assertEquals("w", branch4.config().leftFields().get(0).name());
        UnresolvedRelation branch4Right = as(branch4.right(), UnresolvedRelation.class);
        assertEquals("sub2", branch4Right.indexPattern().indexPattern());
        AntiJoin branch4AntiJoin = as(branch4.left(), AntiJoin.class);
        assertEquals(JoinTypes.ANTI, branch4AntiJoin.config().type());
        assertEquals("x", branch4AntiJoin.config().leftFields().get(0).name());
        Filter branch4Filter = as(branch4AntiJoin.left(), Filter.class);
        and = as(branch4Filter.condition(), And.class);
        not = as(and.left(), Not.class);
        equals = as(not.field(), Equals.class);
        as(equals.left(), UnresolvedAttribute.class);
        not = as(and.right(), Not.class);
        lessThan = as(not.field(), LessThan.class);
        as(lessThan.left(), UnresolvedAttribute.class);
        UnresolvedRelation branch4Left = as(branch3Filter.child(), UnresolvedRelation.class);
        assertEquals("main", branch4Left.indexPattern().indexPattern());
    }

    // ---- negative: nested OR with NOT IN subquery inside AND ----

    /**
     * {@code WHERE x IN (FROM sub1) OR (y == 1 AND (z < 0 OR w NOT IN (FROM sub2)))}
     * <p>
     * The top-level OR triggers a disjunctive rewrite into two branches. The second branch's condition
     * is {@code NOT(x IN sub1) AND y == 1 AND (z < 0 OR w NOT IN sub2)}. The inner OR
     * ({@code z < 0 OR w NOT IN sub2}) is an AND-conjunct that is not a bare InSubquery, so it stays
     * as a remaining Filter condition. The {@code w NOT IN sub2} inside it is not resolvable and is
     * rejected by the post-resolution validation. Nested {@code UnionAll} is not supported yet.
     */
    public void testRejectsNestedConjunctiveAndDisjunctiveInSubquery() {
        assertResolveError(
            "FROM main | WHERE x IN (FROM sub1) OR (y == 1 AND (z < 0 OR w NOT IN (FROM sub2)))",
            "line 1:61: Complicated IN subquery is not yet supported in the WHERE command "
                + "[WHERE x IN (FROM sub1) OR (y == 1 AND (z < 0 OR w NOT IN (FROM sub2)))]"
        );
    }

    // ---- negative: IN subquery in EVAL ----

    public void testRejectsInSubqueryInEval() {
        assertResolveError("FROM main | EVAL z = x IN (FROM sub)", "line 1:22: IN subquery is not supported in [EVAL z = x IN (FROM sub)]");
    }

    public void testRejectsNotInSubqueryInEval() {
        assertResolveError(
            "FROM main | EVAL z = x NOT IN (FROM sub)",
            "line 1:22: IN subquery is not supported in [EVAL z = x NOT IN (FROM sub)]"
        );
    }

    // ---- negative: IN subquery in SORT ----

    public void testRejectsInSubqueryInSort() {
        assertResolveError("FROM main | SORT x IN (FROM sub)", "line 1:18: IN subquery is not supported in [SORT x IN (FROM sub)]");
    }

    // ---- negative: IN subquery in STATS BY ----

    public void testRejectsInSubqueryInStatsBy() {
        assertResolveError(
            "FROM main | STATS c = COUNT(*) BY x IN (FROM sub)",
            "line 1:35: IN subquery is not supported in [STATS c = COUNT(*) BY x IN (FROM sub)]"
        );
    }

    // ---- negative: IN subquery in STATS WHERE filter ----

    public void testRejectsInSubqueryInStatsWhereFilter() {
        assertResolveError(
            "FROM main | STATS c = COUNT(*) WHERE x IN (FROM sub)",
            "line 1:38: IN subquery is not supported in [STATS c = COUNT(*) WHERE x IN (FROM sub)]"
        );
    }

    // ---- negative: IN subquery in LIMIT BY ----

    public void testRejectsInSubqueryInLimitBy() {
        assertResolveError(
            "FROM main | SORT a | LIMIT 10 BY x IN (FROM sub)",
            "line 1:34: IN subquery is not supported in [LIMIT 10 BY x IN (FROM sub)]"
        );
    }

    // ---- negative: IN subquery as function argument in WHERE ----

    public void testRejectsInSubqueryAsFunctionArgInWhere() {
        assertResolveError(
            "FROM main | WHERE CASE(x IN (FROM sub), true, false)",
            "line 1:24: IN subquery is not supported within other expressions [CASE(x IN (FROM sub), true, false)]"
        );
    }

    // ---- negative: IN subquery with IS NOT NULL in WHERE ----

    public void testRejectsInSubqueryWithIsNotNullInWhere() {
        assertResolveError(
            "FROM main | WHERE (x IN (FROM sub)) IS NOT NULL",
            "line 1:20: IN subquery is not supported within other expressions [(x IN (FROM sub)) IS NOT NULL]"
        );
    }

    // ---- negative: IN and NOT IN subqueries inside CASE function in WHERE ----

    /**
     * {@code WHERE CASE(x IN (FROM sub1) OR (y == 1 OR (w NOT IN (FROM sub2)) OR z < 0), true, false)}
     * <p>
     * The CASE wraps the entire boolean expression as a function argument. InSubqueryResolver only
     * extracts InSubquery at the top level of AND/OR conjuncts in a Filter condition, not from inside
     * function arguments. Both {@code x IN sub1} and {@code w NOT IN sub2} remain unresolved.
     */
    public void testRejectsInSubqueryInCaseFunctionWithDisjunctiveChain() {
        var e = expectThrows(
            VerificationException.class,
            () -> resolve("FROM main | WHERE CASE(x IN (FROM sub1) OR (y == 1 OR (w NOT IN (FROM sub2)) OR z < 0), true, false)")
        );
        assertThat(e.getMessage(), containsString("Found 2 problem"));
        assertThat(
            e.getMessage(),
            containsString(
                "IN subquery is not supported within other expressions"
                    + " [CASE(x IN (FROM sub1) OR (y == 1 OR (w NOT IN (FROM sub2)) OR z < 0), true, false)]"
            )
        );
    }

    // ---- helpers ----

    private static LogicalPlan resolve(String query) {
        return InSubqueryResolver.resolve(TEST_PARSER.parseQuery(query));
    }

    private static void assertResolveError(String query, String expectedError) {
        var e = expectThrows(VerificationException.class, () -> resolve(query));
        assertEquals("Found 1 problem\n" + expectedError, e.getMessage());
    }
}
