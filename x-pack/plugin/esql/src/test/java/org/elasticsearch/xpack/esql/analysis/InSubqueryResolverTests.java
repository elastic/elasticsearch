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
import org.elasticsearch.xpack.esql.expression.predicate.logical.And;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Not;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Or;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThan;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.InSubquery;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThan;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.Keep;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedRelation;
import org.elasticsearch.xpack.esql.plan.logical.join.AntiJoin;
import org.elasticsearch.xpack.esql.plan.logical.join.JoinTypes;
import org.elasticsearch.xpack.esql.plan.logical.join.MarkJoin;
import org.elasticsearch.xpack.esql.plan.logical.join.SemiJoin;
import org.junit.Before;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_PARSER;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.hamcrest.Matchers.containsString;

/**
 * Tests for {@link InSubqueryResolver}, which converts {@link InSubquery} expressions in
 * {@link Filter} conditions into:
 * <ul>
 *   <li>{@link SemiJoin}/{@link AntiJoin} nodes for AND-conjunct {@code IN}/{@code NOT IN}
 *       (the row-filtering shape, most efficient for the common case);</li>
 *   <li>{@link MarkJoin} nodes for {@code InSubquery} occurrences embedded in {@code OR}
 *       (or under {@code NOT}/{@code AND} below {@code OR}); each emits a synthetic boolean
 *       mark attribute that the rewritten {@code WHERE} condition references, so SQL
 *       three-valued logic flows through the surrounding boolean expression naturally.</li>
 * </ul>
 * The resolver also rejects {@link InSubquery} in unsupported positions (EVAL, SORT, STATS BY,
 * function arguments, IS NOT NULL, etc.).
 */
public class InSubqueryResolverTests extends ESTestCase {

    @Before
    public void checkCapability() {
        assumeTrue("Requires IN subquery support", EsqlCapabilities.Cap.WHERE_IN_SUBQUERY_WITHOUT_VIEW.isEnabled());
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

    // ---- positive: OR disjuncts with IN subquery → MarkJoin per InSubquery + Filter on marks ----

    /**
     * {@code WHERE x IN (FROM sub1) OR y IN (FROM sub2)} rewrites to (from top down):
     * <pre>
     * Project[main.output]
     *   Filter[$m1 OR $m2]
     *     MarkJoin[?y → $m2, left=MarkJoin[?x → $m1, left=main, right=sub1], right=sub2]
     * </pre>
     */
    public void testDisjunctiveInSubquery() {
        LogicalPlan plan = resolve("FROM main | WHERE x IN (FROM sub1) OR y IN (FROM sub2)");
        Filter filter = as(plan, Filter.class);
        Or or = as(filter.condition(), Or.class);
        Attribute leftMark = as(or.left(), Attribute.class);
        Attribute rightMark = as(or.right(), Attribute.class);
        // Outer (latest stacked) join is sub2 ($m2 is the right operand of OR)
        MarkJoin outer = as(filter.child(), MarkJoin.class);
        assertEquals(JoinTypes.MARK, outer.config().type());
        assertEquals("y", outer.config().leftFields().get(0).name());
        assertEquals(rightMark.id(), outer.markAttribute().id());
        UnresolvedRelation outerRight = as(outer.right(), UnresolvedRelation.class);
        assertEquals("sub2", outerRight.indexPattern().indexPattern());
        // Inner (first stacked) join is sub1
        MarkJoin inner = as(outer.left(), MarkJoin.class);
        assertEquals(JoinTypes.MARK, inner.config().type());
        assertEquals("x", inner.config().leftFields().get(0).name());
        assertEquals(leftMark.id(), inner.markAttribute().id());
        UnresolvedRelation innerRight = as(inner.right(), UnresolvedRelation.class);
        assertEquals("sub1", innerRight.indexPattern().indexPattern());
        UnresolvedRelation main = as(inner.left(), UnresolvedRelation.class);
        assertEquals("main", main.indexPattern().indexPattern());
    }

    // ---- positive: OR with NOT IN subquery ----

    /**
     * {@code WHERE x NOT IN (FROM sub1) OR y NOT IN (FROM sub2)}: each {@code NOT IN} is rewritten as
     * {@code NOT $mN}, where {@code $mN} is the mark from a {@link MarkJoin}. The {@code NOT}s and
     * the {@code OR} are evaluated by the standard expression machinery — three-valued logic falls
     * out for free.
     */
    public void testDisjunctiveNotInSubquery() {
        LogicalPlan plan = resolve("FROM main | WHERE x NOT IN (FROM sub1) OR y NOT IN (FROM sub2)");
        Filter filter = as(plan, Filter.class);
        Or or = as(filter.condition(), Or.class);
        Not leftNot = as(or.left(), Not.class);
        Attribute leftMark = as(leftNot.field(), Attribute.class);
        Not rightNot = as(or.right(), Not.class);
        Attribute rightMark = as(rightNot.field(), Attribute.class);
        MarkJoin outer = as(filter.child(), MarkJoin.class);
        assertEquals("y", outer.config().leftFields().get(0).name());
        assertEquals(rightMark.id(), outer.markAttribute().id());
        MarkJoin inner = as(outer.left(), MarkJoin.class);
        assertEquals("x", inner.config().leftFields().get(0).name());
        assertEquals(leftMark.id(), inner.markAttribute().id());
        UnresolvedRelation main = as(inner.left(), UnresolvedRelation.class);
        assertEquals("main", main.indexPattern().indexPattern());
    }

    // ---- positive: OR mixing IN and NOT IN ----

    public void testDisjunctiveInAndNotInSubquery() {
        LogicalPlan plan = resolve("FROM main | WHERE x IN (FROM sub1) OR y NOT IN (FROM sub2)");
        Filter filter = as(plan, Filter.class);
        Or or = as(filter.condition(), Or.class);
        Attribute leftMark = as(or.left(), Attribute.class);
        Not rightNot = as(or.right(), Not.class);
        Attribute rightMark = as(rightNot.field(), Attribute.class);
        MarkJoin outer = as(filter.child(), MarkJoin.class);
        assertEquals("y", outer.config().leftFields().get(0).name());
        assertEquals(rightMark.id(), outer.markAttribute().id());
        MarkJoin inner = as(outer.left(), MarkJoin.class);
        assertEquals("x", inner.config().leftFields().get(0).name());
        assertEquals(leftMark.id(), inner.markAttribute().id());
        UnresolvedRelation main = as(inner.left(), UnresolvedRelation.class);
        assertEquals("main", main.indexPattern().indexPattern());
    }

    // ---- positive: OR with IN subquery and regular predicate ----

    /**
     * {@code WHERE x IN (FROM sub) OR a > 5} rewrites to:
     * <pre>
     * Project[main.output]
     *   Filter[$m OR a > 5]
     *     MarkJoin[?x → $m, left=main, right=sub]
     * </pre>
     */
    public void testDisjunctiveInSubqueryWithOtherPredicate() {
        LogicalPlan plan = resolve("FROM main | WHERE x IN (FROM sub) OR a > 5");
        Filter filter = as(plan, Filter.class);
        Or or = as(filter.condition(), Or.class);
        Attribute mark = as(or.left(), Attribute.class);
        as(or.right(), GreaterThan.class);
        MarkJoin mj = as(filter.child(), MarkJoin.class);
        assertEquals(JoinTypes.MARK, mj.config().type());
        assertEquals("x", mj.config().leftFields().get(0).name());
        assertEquals(mark.id(), mj.markAttribute().id());
        UnresolvedRelation right = as(mj.right(), UnresolvedRelation.class);
        assertEquals("sub", right.indexPattern().indexPattern());
        UnresolvedRelation main = as(mj.left(), UnresolvedRelation.class);
        assertEquals("main", main.indexPattern().indexPattern());
    }

    // ---- positive: three-way OR with IN subqueries ----

    public void testMultipleDisjunctiveInSubqueries() {
        LogicalPlan plan = resolve("FROM main | WHERE x IN (FROM sub1) OR y IN (FROM sub2) OR z IN (FROM sub3)");
        Filter filter = as(plan, Filter.class);
        // The original parser produces left-associative ORs: ((x IN sub1) OR (y IN sub2)) OR (z IN sub3)
        Or topOr = as(filter.condition(), Or.class);
        Attribute zMark = as(topOr.right(), Attribute.class);
        Or innerOr = as(topOr.left(), Or.class);
        Attribute xMark = as(innerOr.left(), Attribute.class);
        Attribute yMark = as(innerOr.right(), Attribute.class);
        // Stacking order matches expression-tree traversal order: x → y → z (z is outermost).
        MarkJoin zJoin = as(filter.child(), MarkJoin.class);
        assertEquals("z", zJoin.config().leftFields().get(0).name());
        assertEquals(zMark.id(), zJoin.markAttribute().id());
        MarkJoin yJoin = as(zJoin.left(), MarkJoin.class);
        assertEquals("y", yJoin.config().leftFields().get(0).name());
        assertEquals(yMark.id(), yJoin.markAttribute().id());
        MarkJoin xJoin = as(yJoin.left(), MarkJoin.class);
        assertEquals("x", xJoin.config().leftFields().get(0).name());
        assertEquals(xMark.id(), xJoin.markAttribute().id());
        UnresolvedRelation main = as(xJoin.left(), UnresolvedRelation.class);
        assertEquals("main", main.indexPattern().indexPattern());
    }

    // ---- positive: constant NOT IN subquery ----

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

    // ---- positive: fully disjunctive OR chain mixing IN/NOT IN with regular predicates ----

    /**
     * {@code WHERE x IN (FROM sub1) OR (y == 1 OR (z < 0 OR w NOT IN (FROM sub2)))}
     * <p>
     * The boolean expression has every {@link InSubquery} replaced by a fresh mark attribute, and
     * each rewrite stacks one {@link MarkJoin} below the {@link Filter}. The plain comparison
     * predicates ({@code y == 1}, {@code z < 0}) survive untouched in the boolean tree.
     */
    public void testDisjunctiveOrChainWithInSubquery() {
        LogicalPlan plan = resolve("FROM main | WHERE x IN (FROM sub1) OR (y == 1 OR (z < 0 OR w NOT IN (FROM sub2)))");
        Filter filter = as(plan, Filter.class);

        // The explicit parens force right-associative ORs:
        // (x IN sub1) OR ((y == 1) OR ((z < 0) OR (w NOT IN sub2)))
        Or topOr = as(filter.condition(), Or.class);
        Attribute xMark = as(topOr.left(), Attribute.class);
        Or or2 = as(topOr.right(), Or.class);
        as(or2.left(), Equals.class);
        Or or3 = as(or2.right(), Or.class);
        as(or3.left(), LessThan.class);
        Not wNot = as(or3.right(), Not.class);
        Attribute wMark = as(wNot.field(), Attribute.class);

        // Two MarkJoins, stacked in declaration order: x first, w on top.
        MarkJoin wJoin = as(filter.child(), MarkJoin.class);
        assertEquals("w", wJoin.config().leftFields().get(0).name());
        assertEquals(wMark.id(), wJoin.markAttribute().id());
        UnresolvedRelation wRight = as(wJoin.right(), UnresolvedRelation.class);
        assertEquals("sub2", wRight.indexPattern().indexPattern());
        MarkJoin xJoin = as(wJoin.left(), MarkJoin.class);
        assertEquals("x", xJoin.config().leftFields().get(0).name());
        assertEquals(xMark.id(), xJoin.markAttribute().id());
        UnresolvedRelation xRight = as(xJoin.right(), UnresolvedRelation.class);
        assertEquals("sub1", xRight.indexPattern().indexPattern());
        UnresolvedRelation main = as(xJoin.left(), UnresolvedRelation.class);
        assertEquals("main", main.indexPattern().indexPattern());
    }

    // ---- positive: mixed OR chain with AND containing NOT IN subquery ----

    /**
     * {@code WHERE x IN (FROM sub1) OR (y == 1 OR (z < 0 AND w NOT IN (FROM sub2)))}
     * <p>
     * Both {@link InSubquery}s sit under {@code OR} (the inner {@code AND} is itself a child of
     * {@code OR}), so each becomes a {@link MarkJoin} feeding a single {@link Filter}.
     */
    public void testDisjunctiveOrChainWithConjunctiveInSubquery() {
        LogicalPlan plan = resolve("FROM main | WHERE x IN (FROM sub1) OR (y == 1 OR (z < 0 AND w NOT IN (FROM sub2)))");
        Filter filter = as(plan, Filter.class);

        // Right-associative due to parens:
        // (x IN sub1) OR ((y == 1) OR (z < 0 AND w NOT IN sub2))
        Or topOr = as(filter.condition(), Or.class);
        Attribute xMark = as(topOr.left(), Attribute.class);
        Or innerOr = as(topOr.right(), Or.class);
        as(innerOr.left(), Equals.class);
        And rightAnd = as(innerOr.right(), And.class);
        as(rightAnd.left(), LessThan.class);
        Not wNot = as(rightAnd.right(), Not.class);
        Attribute wMark = as(wNot.field(), Attribute.class);

        MarkJoin wJoin = as(filter.child(), MarkJoin.class);
        assertEquals("w", wJoin.config().leftFields().get(0).name());
        assertEquals(wMark.id(), wJoin.markAttribute().id());
        MarkJoin xJoin = as(wJoin.left(), MarkJoin.class);
        assertEquals("x", xJoin.config().leftFields().get(0).name());
        assertEquals(xMark.id(), xJoin.markAttribute().id());
    }

    // ---- positive: AND-conjunct with disjunctive IN subquery in the OR sub-expression ----

    /**
     * {@code WHERE a > 0 AND (x IN (FROM sub1) OR y == 1)}: the first conjunct is a plain predicate
     * and survives in the final {@link Filter}; the second conjunct contains an {@code OR} with an
     * {@link InSubquery} inside, so the {@code IN} is replaced by a mark attribute and a
     * {@link MarkJoin} is stacked below the filter.
     */
    public void testConjunctOfPredicateAndDisjunctiveInSubquery() {
        LogicalPlan plan = resolve("FROM main | WHERE a > 0 AND (x IN (FROM sub1) OR y == 1)");
        Filter filter = as(plan, Filter.class);

        And and = as(filter.condition(), And.class);
        as(and.left(), GreaterThan.class);
        Or or = as(and.right(), Or.class);
        Attribute xMark = as(or.left(), Attribute.class);
        as(or.right(), Equals.class);

        MarkJoin xJoin = as(filter.child(), MarkJoin.class);
        assertEquals("x", xJoin.config().leftFields().get(0).name());
        assertEquals(xMark.id(), xJoin.markAttribute().id());
        UnresolvedRelation main = as(xJoin.left(), UnresolvedRelation.class);
        assertEquals("main", main.indexPattern().indexPattern());
    }

    // ---- positive: mixing AND-conjunct IN subquery (SemiJoin) with OR-context IN subquery (MarkJoin) ----

    /**
     * {@code WHERE x IN (FROM sub1) AND (y IN (FROM sub2) OR a > 0)} mixes both rewrite paths:
     * <ul>
     *   <li>{@code x IN (FROM sub1)} is a top-level AND conjunct → {@link SemiJoin} stacked on top</li>
     *   <li>{@code y IN (FROM sub2)} is inside an {@code OR} → {@link MarkJoin} stacked below the
     *       remaining filter</li>
     * </ul>
     */
    public void testMixedSemiJoinAndMarkJoin() {
        LogicalPlan plan = resolve("FROM main | WHERE x IN (FROM sub1) AND (y IN (FROM sub2) OR a > 0)");
        // SemiJoin for x is stacked on TOP of Filter on TOP of MarkJoin for y.
        // The synthetic mark attribute introduced by the MarkJoin is stripped by the analyzer's
        // planWithoutSyntheticAttributes (post-resolution); the resolver itself does not add a Project.
        SemiJoin xJoin = as(plan, SemiJoin.class);
        assertEquals(JoinTypes.SEMI, xJoin.config().type());
        assertEquals("x", xJoin.config().leftFields().get(0).name());
        UnresolvedRelation xRight = as(xJoin.right(), UnresolvedRelation.class);
        assertEquals("sub1", xRight.indexPattern().indexPattern());

        Filter filter = as(xJoin.left(), Filter.class);
        Or or = as(filter.condition(), Or.class);
        Attribute yMark = as(or.left(), Attribute.class);
        as(or.right(), GreaterThan.class);

        MarkJoin yJoin = as(filter.child(), MarkJoin.class);
        assertEquals(JoinTypes.MARK, yJoin.config().type());
        assertEquals("y", yJoin.config().leftFields().get(0).name());
        assertEquals(yMark.id(), yJoin.markAttribute().id());
        UnresolvedRelation yRight = as(yJoin.right(), UnresolvedRelation.class);
        assertEquals("sub2", yRight.indexPattern().indexPattern());
        UnresolvedRelation main = as(yJoin.left(), UnresolvedRelation.class);
        assertEquals("main", main.indexPattern().indexPattern());
    }

    // ---- positive: nested OR within a top-level AND conjunct (compound non-bare conjunct) ----

    /**
     * {@code WHERE x IN (FROM sub1) OR (y == 1 AND (z < 0 OR w NOT IN (FROM sub2)))}
     * <p>
     * The whole expression is a single AND-conjunct (top-level OR). Both {@link InSubquery}s reach
     * boolean position through {@code OR}/{@code AND}/{@code NOT} only, so both become
     * {@link MarkJoin}s. The previous resolver rejected this query as "Complicated IN subquery";
     * with the MARK rewrite it is supported.
     */
    public void testNestedConjunctiveAndDisjunctiveInSubquery() {
        LogicalPlan plan = resolve("FROM main | WHERE x IN (FROM sub1) OR (y == 1 AND (z < 0 OR w NOT IN (FROM sub2)))");
        Filter filter = as(plan, Filter.class);
        Or topOr = as(filter.condition(), Or.class);
        Attribute xMark = as(topOr.left(), Attribute.class);
        And rightAnd = as(topOr.right(), And.class);
        as(rightAnd.left(), Equals.class);
        Or innerOr = as(rightAnd.right(), Or.class);
        as(innerOr.left(), LessThan.class);
        Not wNot = as(innerOr.right(), Not.class);
        Attribute wMark = as(wNot.field(), Attribute.class);

        MarkJoin wJoin = as(filter.child(), MarkJoin.class);
        assertEquals("w", wJoin.config().leftFields().get(0).name());
        assertEquals(wMark.id(), wJoin.markAttribute().id());
        MarkJoin xJoin = as(wJoin.left(), MarkJoin.class);
        assertEquals("x", xJoin.config().leftFields().get(0).name());
        assertEquals(xMark.id(), xJoin.markAttribute().id());
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

    public void testRejectsInSubqueryWithExpressionOnLHS() {
        assertResolveError(
            "FROM main | WHERE a + b IN (FROM sub)",
            "line 1:19: Complicated IN subquery is not yet supported in the WHERE command [WHERE a + b IN (FROM sub)]"
        );

        assertResolveError(
            "FROM main | WHERE abs(a) IN (FROM sub)",
            "line 1:19: Complicated IN subquery is not yet supported in the WHERE command [WHERE abs(a) IN (FROM sub)]"
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
