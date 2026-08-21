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
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.expression.function.UnresolvedFunction;
import org.elasticsearch.xpack.esql.expression.function.aggregate.FilteredExpression;
import org.elasticsearch.xpack.esql.expression.predicate.logical.And;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Not;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Or;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNotNull;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNull;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThan;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.InSubquery;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThan;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.InlineStats;
import org.elasticsearch.xpack.esql.plan.logical.Keep;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Row;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedRelation;
import org.elasticsearch.xpack.esql.plan.logical.join.AntiJoin;
import org.elasticsearch.xpack.esql.plan.logical.join.JoinTypes;
import org.elasticsearch.xpack.esql.plan.logical.join.MarkJoin;
import org.elasticsearch.xpack.esql.plan.logical.join.SemiJoin;

import java.util.HashSet;
import java.util.Set;

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
 *   <li>In an {@link Eval} field definition, only {@link MarkJoin} is ever created — EVAL
 *       preserves every row and produces a value, so SemiJoin/AntiJoin don't apply. The rewrite
 *       allowlist is identical to the WHERE case.</li>
 * </ul>
 * The resolver also rejects {@link InSubquery} in unsupported positions (SORT, STATS BY,
 * arithmetic operators, etc.).
 */
public class InSubqueryResolverTests extends ESTestCase {

    private static void checkMultiColumnInSubquery() {
        assumeTrue("multi-column IN subquery", EsqlCapabilities.Cap.WHERE_IN_MULTI_COLUMN_SUBQUERY.isEnabled());
    }

    private static void requireLambda() {
        assumeTrue("Requires Lambda syntax support", EsqlCapabilities.Cap.LAMBDA_SYNTAX.isEnabled());
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

    // ---- negative: IN subquery in LIMIT BY ----

    public void testRejectsInSubqueryInLimitBy() {
        assertResolveError(
            "FROM main | SORT a | LIMIT 10 BY x IN (FROM sub)",
            "line 1:34: IN subquery is not supported in [LIMIT 10 BY x IN (FROM sub)]"
        );
    }

    /**
     * {@code WHERE CASE(x IN (FROM sub), true, false)}: the WHEN condition contains an IN subquery.
     * The resolver recurses into the CASE arguments and rewrites the InSubquery into a MarkJoin:
     * <pre>
     * Filter[CASE($$mark, true, false)]
     *   MarkJoin[x → $$mark, left=main, right=sub]
     *     UnresolvedRelation[main]
     * </pre>
     */
    public void testCaseWhenConditionInSubquery() {
        LogicalPlan plan = resolve("FROM main | WHERE CASE(x IN (FROM sub), true, false)");
        Filter filter = as(plan, Filter.class);
        UnresolvedFunction caseExpr = as(filter.condition(), UnresolvedFunction.class);
        assertEquals("CASE", caseExpr.name());
        Attribute mark = as(caseExpr.children().get(0), Attribute.class);
        MarkJoin mj = as(filter.child(), MarkJoin.class);
        assertEquals(JoinTypes.MARK, mj.config().type());
        assertEquals("x", mj.config().leftFields().get(0).name());
        assertEquals(mark.id(), mj.markAttribute().id());
        assertEquals("sub", as(mj.right(), UnresolvedRelation.class).indexPattern().indexPattern());
        assertEquals("main", as(mj.left(), UnresolvedRelation.class).indexPattern().indexPattern());
    }

    /**
     * {@code WHERE CASE(a == 1, x IN (FROM sub), false)}: the THEN value arm contains an IN subquery.
     */
    public void testCaseThenArmInSubquery() {
        LogicalPlan plan = resolve("FROM main | WHERE CASE(a == 1, x IN (FROM sub), false)");
        Filter filter = as(plan, Filter.class);
        UnresolvedFunction caseExpr = as(filter.condition(), UnresolvedFunction.class);
        Attribute mark = as(caseExpr.children().get(1), Attribute.class);
        MarkJoin mj = as(filter.child(), MarkJoin.class);
        assertEquals("x", mj.config().leftFields().get(0).name());
        assertEquals(mark.id(), mj.markAttribute().id());
    }

    /**
     * {@code WHERE CASE(a == 1, false, x IN (FROM sub))}: the ELSE arm contains an IN subquery.
     */
    public void testCaseElseArmInSubquery() {
        LogicalPlan plan = resolve("FROM main | WHERE CASE(a == 1, false, x IN (FROM sub))");
        Filter filter = as(plan, Filter.class);
        UnresolvedFunction caseExpr = as(filter.condition(), UnresolvedFunction.class);
        Attribute mark = as(caseExpr.children().get(2), Attribute.class);
        MarkJoin mj = as(filter.child(), MarkJoin.class);
        assertEquals("x", mj.config().leftFields().get(0).name());
        assertEquals(mark.id(), mj.markAttribute().id());
    }

    /**
     * {@code WHERE CASE(x NOT IN (FROM sub), true, false)}: NOT IN inside CASE condition.
     */
    public void testCaseNotInSubquery() {
        LogicalPlan plan = resolve("FROM main | WHERE CASE(x NOT IN (FROM sub), true, false)");
        Filter filter = as(plan, Filter.class);
        UnresolvedFunction caseExpr = as(filter.condition(), UnresolvedFunction.class);
        Not notMark = as(caseExpr.children().get(0), Not.class);
        Attribute mark = as(notMark.field(), Attribute.class);
        MarkJoin mj = as(filter.child(), MarkJoin.class);
        assertEquals("x", mj.config().leftFields().get(0).name());
        assertEquals(mark.id(), mj.markAttribute().id());
    }

    /**
     * {@code WHERE CASE(x IN (FROM sub1) OR (y == 1 OR (w NOT IN (FROM sub2)) OR z < 0), true, false)}:
     * CASE WHEN condition contains a disjunctive chain with two IN subqueries. Both are rewritten.
     * <pre>
     * Filter[CASE($$m1 OR (y == 1 OR (NOT $$m2 OR z &lt; 0)), true, false)]
     *   MarkJoin[w → $$m2, left=innerJoin, right=sub2]
     *     MarkJoin[x → $$m1, left=main, right=sub1]
     *       UnresolvedRelation[main]
     * </pre>
     */
    public void testCaseWithDisjunctiveInSubqueries() {
        LogicalPlan plan = resolve("FROM main | WHERE CASE(x IN (FROM sub1) OR (y == 1 OR (w NOT IN (FROM sub2)) OR z < 0), true, false)");
        Filter filter = as(plan, Filter.class);
        UnresolvedFunction caseExpr = as(filter.condition(), UnresolvedFunction.class);
        // Outer MarkJoin is for w/sub2 (second subquery encountered in traversal)
        MarkJoin outer = as(filter.child(), MarkJoin.class);
        assertEquals("w", outer.config().leftFields().get(0).name());
        assertEquals("sub2", as(outer.right(), UnresolvedRelation.class).indexPattern().indexPattern());
        Attribute markM2 = outer.markAttribute();
        // Inner MarkJoin is for x/sub1 (first subquery encountered)
        MarkJoin inner = as(outer.left(), MarkJoin.class);
        assertEquals("x", inner.config().leftFields().get(0).name());
        assertEquals("sub1", as(inner.right(), UnresolvedRelation.class).indexPattern().indexPattern());
        assertEquals("main", as(inner.left(), UnresolvedRelation.class).indexPattern().indexPattern());
        Attribute markM1 = inner.markAttribute();
        // Both mark attributes must appear in the CASE condition; no InSubquery should survive
        Set<NameId> expectedMarks = Set.of(markM1.id(), markM2.id());
        HashSet<NameId> markAttributes = new HashSet<>();
        caseExpr.forEachDown(Attribute.class, a -> {
            if (expectedMarks.contains(a.id())) {
                markAttributes.add(a.id());
            }
        });
        assertEquals(expectedMarks, markAttributes);
        filter.forEachExpression(InSubquery.class, inSub -> fail("InSubquery survived: " + inSub));
    }

    /**
     * {@code WHERE CASE(x IN (FROM sub1) AND w IN (FROM sub2), true, false)}:
     * CASE WHEN condition contains two conjunctive IN subqueries. Unlike bare top-level
     * {@code AND}-conjuncts (which become {@link SemiJoin}s), the enclosing CASE forces both into
     * {@link MarkJoin}s so their marks can flow into the CASE expression:
     * <pre>
     * Filter[CASE($$m1 AND $$m2, true, false)]
     *   MarkJoin[w → $$m2, left=innerJoin, right=sub2]
     *     MarkJoin[x → $$m1, left=main, right=sub1]
     *       UnresolvedRelation[main]
     * </pre>
     */
    public void testCaseWithConjunctiveInSubqueries() {
        LogicalPlan plan = resolve("FROM main | WHERE CASE(x IN (FROM sub1) AND w IN (FROM sub2), true, false)");
        Filter filter = as(plan, Filter.class);
        UnresolvedFunction caseExpr = as(filter.condition(), UnresolvedFunction.class);
        // Outer MarkJoin is for w/sub2 (second subquery encountered in traversal)
        MarkJoin outer = as(filter.child(), MarkJoin.class);
        assertEquals("w", outer.config().leftFields().get(0).name());
        assertEquals("sub2", as(outer.right(), UnresolvedRelation.class).indexPattern().indexPattern());
        Attribute markM2 = outer.markAttribute();
        // Inner MarkJoin is for x/sub1 (first subquery encountered)
        MarkJoin inner = as(outer.left(), MarkJoin.class);
        assertEquals("x", inner.config().leftFields().get(0).name());
        assertEquals("sub1", as(inner.right(), UnresolvedRelation.class).indexPattern().indexPattern());
        assertEquals("main", as(inner.left(), UnresolvedRelation.class).indexPattern().indexPattern());
        Attribute markM1 = inner.markAttribute();
        // Both mark attributes must appear in the CASE condition; no InSubquery should survive
        HashSet<NameId> markAttributes = new HashSet<>();
        caseExpr.forEachDown(Attribute.class, a -> markAttributes.add(a.id()));
        assertEquals(2, markAttributes.size());
        assertTrue("$$m1 mark not referenced in CASE condition", markAttributes.contains(markM1.id()));
        assertTrue("$$m2 mark not referenced in CASE condition", markAttributes.contains(markM2.id()));
        filter.forEachExpression(InSubquery.class, inSub -> fail("InSubquery survived: " + inSub));
    }

    /**
     * {@code WHERE CASE(x IN (FROM sub1) AND (y > 0 OR w IN (FROM sub2)), true, false)}:
     * CASE WHEN condition mixes a conjunctive and a disjunctive IN subquery. Both are rewritten
     * into {@link MarkJoin}s:
     * <pre>
     * Filter[CASE($$m1 AND (y &gt; 0 OR $$m2), true, false)]
     *   MarkJoin[w → $$m2, left=innerJoin, right=sub2]
     *     MarkJoin[x → $$m1, left=main, right=sub1]
     *       UnresolvedRelation[main]
     * </pre>
     */
    public void testCaseWithMixedConjunctiveDisjunctiveInSubqueries() {
        LogicalPlan plan = resolve("FROM main | WHERE CASE(x IN (FROM sub1) AND (y > 0 OR w IN (FROM sub2)), true, false)");
        Filter filter = as(plan, Filter.class);
        UnresolvedFunction caseExpr = as(filter.condition(), UnresolvedFunction.class);
        // Outer MarkJoin is for w/sub2 (second subquery encountered in traversal)
        MarkJoin outer = as(filter.child(), MarkJoin.class);
        assertEquals("w", outer.config().leftFields().get(0).name());
        assertEquals("sub2", as(outer.right(), UnresolvedRelation.class).indexPattern().indexPattern());
        Attribute markM2 = outer.markAttribute();
        // Inner MarkJoin is for x/sub1 (first subquery encountered)
        MarkJoin inner = as(outer.left(), MarkJoin.class);
        assertEquals("x", inner.config().leftFields().get(0).name());
        assertEquals("sub1", as(inner.right(), UnresolvedRelation.class).indexPattern().indexPattern());
        assertEquals("main", as(inner.left(), UnresolvedRelation.class).indexPattern().indexPattern());
        Attribute markM1 = inner.markAttribute();
        // Both mark attributes must appear in the CASE condition; no InSubquery should survive
        Set<NameId> expectedMarks = Set.of(markM1.id(), markM2.id());
        HashSet<NameId> markAttributes = new HashSet<>();
        caseExpr.forEachDown(Attribute.class, a -> {
            if (expectedMarks.contains(a.id())) {
                markAttributes.add(a.id());
            }
        });
        assertEquals(expectedMarks, markAttributes);
        filter.forEachExpression(InSubquery.class, inSub -> fail("InSubquery survived: " + inSub));
    }

    // ---- positive: IN subquery inside IS [NOT] NULL ----

    /**
     * {@code WHERE (x IN (FROM sub)) IS NOT NULL}: the IsNotNull operand is an IN subquery.
     * The resolver rewrites it into a MarkJoin:
     * <pre>
     * Filter[IsNotNull($$mark)]
     *   MarkJoin[x → $$mark, left=main, right=sub]
     *     UnresolvedRelation[main]
     * </pre>
     */
    public void testIsNotNullInSubquery() {
        LogicalPlan plan = resolve("FROM main | WHERE (x IN (FROM sub)) IS NOT NULL");
        Filter filter = as(plan, Filter.class);
        IsNotNull isNotNull = as(filter.condition(), IsNotNull.class);
        Attribute mark = as(isNotNull.field(), Attribute.class);
        MarkJoin mj = as(filter.child(), MarkJoin.class);
        assertEquals(JoinTypes.MARK, mj.config().type());
        assertEquals("x", mj.config().leftFields().get(0).name());
        assertEquals(mark.id(), mj.markAttribute().id());
        assertEquals("sub", as(mj.right(), UnresolvedRelation.class).indexPattern().indexPattern());
        assertEquals("main", as(mj.left(), UnresolvedRelation.class).indexPattern().indexPattern());
    }

    /**
     * {@code WHERE (x IN (FROM sub)) IS NULL}: IS NULL with IN subquery operand.
     */
    public void testIsNullInSubquery() {
        LogicalPlan plan = resolve("FROM main | WHERE (x IN (FROM sub)) IS NULL");
        Filter filter = as(plan, Filter.class);
        IsNull isNull = as(filter.condition(), IsNull.class);
        Attribute mark = as(isNull.field(), Attribute.class);
        MarkJoin mj = as(filter.child(), MarkJoin.class);
        assertEquals("x", mj.config().leftFields().get(0).name());
        assertEquals(mark.id(), mj.markAttribute().id());
    }

    /**
     * {@code WHERE (x NOT IN (FROM sub)) IS NULL}: NOT IN operand inside IS NULL.
     */
    public void testIsNullNotInSubquery() {
        LogicalPlan plan = resolve("FROM main | WHERE (x NOT IN (FROM sub)) IS NULL");
        Filter filter = as(plan, Filter.class);
        IsNull isNull = as(filter.condition(), IsNull.class);
        Not notMark = as(isNull.field(), Not.class);
        Attribute mark = as(notMark.field(), Attribute.class);
        MarkJoin mj = as(filter.child(), MarkJoin.class);
        assertEquals("x", mj.config().leftFields().get(0).name());
        assertEquals(mark.id(), mj.markAttribute().id());
    }

    /**
     * {@code WHERE (x IN (FROM sub1) OR y IN (FROM sub2)) IS NOT NULL}: IS NOT NULL wraps a
     * disjunction of two IN subqueries. Both are rewritten into MarkJoins:
     * <pre>
     * Filter[IsNotNull($$m1 OR $$m2)]
     *   MarkJoin[y → $$m2, left=innerJoin, right=sub2]
     *     MarkJoin[x → $$m1, left=main, right=sub1]
     *       UnresolvedRelation[main]
     * </pre>
     */
    public void testIsNotNullWithDisjunctiveInSubqueries() {
        LogicalPlan plan = resolve("FROM main | WHERE (x IN (FROM sub1) OR y IN (FROM sub2)) IS NOT NULL");
        Filter filter = as(plan, Filter.class);
        IsNotNull isNotNull = as(filter.condition(), IsNotNull.class);
        MarkJoin outer = as(filter.child(), MarkJoin.class);
        assertEquals("y", outer.config().leftFields().get(0).name());
        assertEquals("sub2", as(outer.right(), UnresolvedRelation.class).indexPattern().indexPattern());
        Attribute markM2 = outer.markAttribute();
        MarkJoin inner = as(outer.left(), MarkJoin.class);
        assertEquals("x", inner.config().leftFields().get(0).name());
        assertEquals("sub1", as(inner.right(), UnresolvedRelation.class).indexPattern().indexPattern());
        assertEquals("main", as(inner.left(), UnresolvedRelation.class).indexPattern().indexPattern());
        Attribute markM1 = inner.markAttribute();
        Or orExpr = as(isNotNull.field(), Or.class);
        assertEquals(markM1.id(), as(orExpr.left(), Attribute.class).id());
        assertEquals(markM2.id(), as(orExpr.right(), Attribute.class).id());
        filter.forEachExpression(InSubquery.class, inSub -> fail("InSubquery survived: " + inSub));
    }

    /**
     * {@code WHERE (x IN (FROM sub1) AND y IN (FROM sub2)) IS NULL}: IS NULL wraps a conjunction
     * of two IN subqueries. Both are rewritten into MarkJoins:
     * <pre>
     * Filter[IsNull($$m1 AND $$m2)]
     *   MarkJoin[y → $$m2, left=innerJoin, right=sub2]
     *     MarkJoin[x → $$m1, left=main, right=sub1]
     *       UnresolvedRelation[main]
     * </pre>
     */
    public void testIsNullWithConjunctiveInSubqueries() {
        LogicalPlan plan = resolve("FROM main | WHERE (x IN (FROM sub1) AND y IN (FROM sub2)) IS NULL");
        Filter filter = as(plan, Filter.class);
        IsNull isNull = as(filter.condition(), IsNull.class);
        MarkJoin outer = as(filter.child(), MarkJoin.class);
        assertEquals("y", outer.config().leftFields().get(0).name());
        assertEquals("sub2", as(outer.right(), UnresolvedRelation.class).indexPattern().indexPattern());
        Attribute markM2 = outer.markAttribute();
        MarkJoin inner = as(outer.left(), MarkJoin.class);
        assertEquals("x", inner.config().leftFields().get(0).name());
        assertEquals("sub1", as(inner.right(), UnresolvedRelation.class).indexPattern().indexPattern());
        assertEquals("main", as(inner.left(), UnresolvedRelation.class).indexPattern().indexPattern());
        Attribute markM1 = inner.markAttribute();
        And andExpr = as(isNull.field(), And.class);
        assertEquals(markM1.id(), as(andExpr.left(), Attribute.class).id());
        assertEquals(markM2.id(), as(andExpr.right(), Attribute.class).id());
        filter.forEachExpression(InSubquery.class, inSub -> fail("InSubquery survived: " + inSub));
    }

    // ---- positive: IN subquery inside COALESCE ----

    /**
     * {@code WHERE COALESCE(x IN (FROM sub), false)}: the IN subquery is a COALESCE argument.
     * <pre>
     * Filter[COALESCE($$mark, false)]
     *   MarkJoin[x → $$mark, left=main, right=sub]
     *     UnresolvedRelation[main]
     * </pre>
     */
    public void testCoalesceInSubquery() {
        LogicalPlan plan = resolve("FROM main | WHERE COALESCE(x IN (FROM sub), false)");
        Filter filter = as(plan, Filter.class);
        UnresolvedFunction coalesceExpr = as(filter.condition(), UnresolvedFunction.class);
        assertEquals("COALESCE", coalesceExpr.name());
        Attribute mark = as(coalesceExpr.children().get(0), Attribute.class);
        MarkJoin mj = as(filter.child(), MarkJoin.class);
        assertEquals(JoinTypes.MARK, mj.config().type());
        assertEquals("x", mj.config().leftFields().get(0).name());
        assertEquals(mark.id(), mj.markAttribute().id());
        assertEquals("sub", as(mj.right(), UnresolvedRelation.class).indexPattern().indexPattern());
        assertEquals("main", as(mj.left(), UnresolvedRelation.class).indexPattern().indexPattern());
    }

    /**
     * {@code WHERE COALESCE(x NOT IN (FROM sub), false)}: NOT IN inside COALESCE.
     */
    public void testCoalesceNotInSubquery() {
        LogicalPlan plan = resolve("FROM main | WHERE COALESCE(x NOT IN (FROM sub), false)");
        Filter filter = as(plan, Filter.class);
        UnresolvedFunction coalesceExpr = as(filter.condition(), UnresolvedFunction.class);
        Not notMark = as(coalesceExpr.children().get(0), Not.class);
        Attribute mark = as(notMark.field(), Attribute.class);
        MarkJoin mj = as(filter.child(), MarkJoin.class);
        assertEquals("x", mj.config().leftFields().get(0).name());
        assertEquals(mark.id(), mj.markAttribute().id());
    }

    /**
     * {@code WHERE COALESCE(null, x IN (FROM sub))}: the IN subquery is the second (non-first)
     * COALESCE argument. The resolver recurses into all COALESCE children regardless of position:
     * <pre>
     * Filter[COALESCE(null, $$mark)]
     *   MarkJoin[x → $$mark, left=main, right=sub]
     *     UnresolvedRelation[main]
     * </pre>
     */
    public void testCoalesceInSubqueryNotFirstArg() {
        LogicalPlan plan = resolve("FROM main | WHERE COALESCE(null, x IN (FROM sub))");
        Filter filter = as(plan, Filter.class);
        UnresolvedFunction coalesceExpr = as(filter.condition(), UnresolvedFunction.class);
        assertEquals("COALESCE", coalesceExpr.name());
        Attribute mark = as(coalesceExpr.children().get(1), Attribute.class);
        MarkJoin mj = as(filter.child(), MarkJoin.class);
        assertEquals(JoinTypes.MARK, mj.config().type());
        assertEquals("x", mj.config().leftFields().get(0).name());
        assertEquals(mark.id(), mj.markAttribute().id());
        assertEquals("sub", as(mj.right(), UnresolvedRelation.class).indexPattern().indexPattern());
        assertEquals("main", as(mj.left(), UnresolvedRelation.class).indexPattern().indexPattern());
    }

    /**
     * {@code WHERE COALESCE(null, false, x IN (FROM sub))}: the IN subquery is the last of three
     * COALESCE arguments:
     * <pre>
     * Filter[COALESCE(null, false, $$mark)]
     *   MarkJoin[x → $$mark, left=main, right=sub]
     *     UnresolvedRelation[main]
     * </pre>
     */
    public void testCoalesceInSubqueryLastArg() {
        LogicalPlan plan = resolve("FROM main | WHERE COALESCE(null, false, x IN (FROM sub))");
        Filter filter = as(plan, Filter.class);
        UnresolvedFunction coalesceExpr = as(filter.condition(), UnresolvedFunction.class);
        assertEquals("COALESCE", coalesceExpr.name());
        Attribute mark = as(coalesceExpr.children().get(2), Attribute.class);
        MarkJoin mj = as(filter.child(), MarkJoin.class);
        assertEquals(JoinTypes.MARK, mj.config().type());
        assertEquals("x", mj.config().leftFields().get(0).name());
        assertEquals(mark.id(), mj.markAttribute().id());
        assertEquals("sub", as(mj.right(), UnresolvedRelation.class).indexPattern().indexPattern());
        assertEquals("main", as(mj.left(), UnresolvedRelation.class).indexPattern().indexPattern());
    }

    /**
     * {@code WHERE COALESCE(x IN (FROM sub1) OR y IN (FROM sub2), false)}: COALESCE whose first
     * argument is a disjunction of two IN subqueries. Both are rewritten into MarkJoins:
     * <pre>
     * Filter[COALESCE($$m1 OR $$m2, false)]
     *   MarkJoin[y → $$m2, left=innerJoin, right=sub2]
     *     MarkJoin[x → $$m1, left=main, right=sub1]
     *       UnresolvedRelation[main]
     * </pre>
     */
    public void testCoalesceWithDisjunctiveInSubqueries() {
        LogicalPlan plan = resolve("FROM main | WHERE COALESCE(x IN (FROM sub1) OR y IN (FROM sub2), false)");
        Filter filter = as(plan, Filter.class);
        UnresolvedFunction coalesceExpr = as(filter.condition(), UnresolvedFunction.class);
        assertEquals("COALESCE", coalesceExpr.name());
        MarkJoin outer = as(filter.child(), MarkJoin.class);
        assertEquals("y", outer.config().leftFields().get(0).name());
        assertEquals("sub2", as(outer.right(), UnresolvedRelation.class).indexPattern().indexPattern());
        Attribute markM2 = outer.markAttribute();
        MarkJoin inner = as(outer.left(), MarkJoin.class);
        assertEquals("x", inner.config().leftFields().get(0).name());
        assertEquals("sub1", as(inner.right(), UnresolvedRelation.class).indexPattern().indexPattern());
        assertEquals("main", as(inner.left(), UnresolvedRelation.class).indexPattern().indexPattern());
        Attribute markM1 = inner.markAttribute();
        Or orExpr = as(coalesceExpr.children().get(0), Or.class);
        assertEquals(markM1.id(), as(orExpr.left(), Attribute.class).id());
        assertEquals(markM2.id(), as(orExpr.right(), Attribute.class).id());
        filter.forEachExpression(InSubquery.class, inSub -> fail("InSubquery survived: " + inSub));
    }

    /**
     * {@code WHERE COALESCE(x IN (FROM sub1), y IN (FROM sub2), false)}: COALESCE with two
     * separate IN subquery arguments. Both are rewritten into MarkJoins:
     * <pre>
     * Filter[COALESCE($$m1, $$m2, false)]
     *   MarkJoin[y → $$m2, left=innerJoin, right=sub2]
     *     MarkJoin[x → $$m1, left=main, right=sub1]
     *       UnresolvedRelation[main]
     * </pre>
     */
    public void testCoalesceWithConjunctiveInSubqueries() {
        LogicalPlan plan = resolve("FROM main | WHERE COALESCE(x IN (FROM sub1), y IN (FROM sub2), false)");
        Filter filter = as(plan, Filter.class);
        UnresolvedFunction coalesceExpr = as(filter.condition(), UnresolvedFunction.class);
        assertEquals("COALESCE", coalesceExpr.name());
        MarkJoin outer = as(filter.child(), MarkJoin.class);
        assertEquals("y", outer.config().leftFields().get(0).name());
        assertEquals("sub2", as(outer.right(), UnresolvedRelation.class).indexPattern().indexPattern());
        Attribute markM2 = outer.markAttribute();
        MarkJoin inner = as(outer.left(), MarkJoin.class);
        assertEquals("x", inner.config().leftFields().get(0).name());
        assertEquals("sub1", as(inner.right(), UnresolvedRelation.class).indexPattern().indexPattern());
        assertEquals("main", as(inner.left(), UnresolvedRelation.class).indexPattern().indexPattern());
        Attribute markM1 = inner.markAttribute();
        assertEquals(markM1.id(), as(coalesceExpr.children().get(0), Attribute.class).id());
        assertEquals(markM2.id(), as(coalesceExpr.children().get(1), Attribute.class).id());
        filter.forEachExpression(InSubquery.class, inSub -> fail("InSubquery survived: " + inSub));
    }

    // ---- positive: complex combinations of CASE, COALESCE, IS [NOT] NULL with multiple IN subqueries ----

    /**
     * {@code WHERE CASE(COALESCE(x IN (FROM sub1), false) AND (y IN (FROM sub2)) IS NOT NULL, true, false)}:
     * CASE WHEN condition mixes COALESCE and IS NOT NULL, each wrapping an IN subquery. Both are
     * rewritten into MarkJoins:
     * <pre>
     * Filter[CASE(COALESCE($$m1, false) AND IsNotNull($$m2), true, false)]
     *   MarkJoin[y → $$m2, left=innerJoin, right=sub2]
     *     MarkJoin[x → $$m1, left=main, right=sub1]
     *       UnresolvedRelation[main]
     * </pre>
     */
    public void testCaseMixingCoalesceAndIsNotNull() {
        LogicalPlan plan = resolve(
            "FROM main | WHERE CASE(COALESCE(x IN (FROM sub1), false) AND (y IN (FROM sub2)) IS NOT NULL, true, false)"
        );
        Filter filter = as(plan, Filter.class);
        UnresolvedFunction caseExpr = as(filter.condition(), UnresolvedFunction.class);
        MarkJoin outer = as(filter.child(), MarkJoin.class);
        assertEquals("y", outer.config().leftFields().get(0).name());
        assertEquals("sub2", as(outer.right(), UnresolvedRelation.class).indexPattern().indexPattern());
        Attribute markM2 = outer.markAttribute();
        MarkJoin inner = as(outer.left(), MarkJoin.class);
        assertEquals("x", inner.config().leftFields().get(0).name());
        assertEquals("sub1", as(inner.right(), UnresolvedRelation.class).indexPattern().indexPattern());
        assertEquals("main", as(inner.left(), UnresolvedRelation.class).indexPattern().indexPattern());
        Attribute markM1 = inner.markAttribute();
        And whenAnd = as(caseExpr.children().get(0), And.class);
        assertEquals(markM1.id(), as(as(whenAnd.left(), UnresolvedFunction.class).children().get(0), Attribute.class).id());
        assertEquals(markM2.id(), as(as(whenAnd.right(), IsNotNull.class).field(), Attribute.class).id());
        filter.forEachExpression(InSubquery.class, inSub -> fail("InSubquery survived: " + inSub));
    }

    /**
     * {@code WHERE (x IN (FROM sub1) AND y IN (FROM sub2)) IS NOT NULL OR COALESCE(CASE(z IN (FROM sub3), true, false), false)}:
     * top-level OR whose left branch wraps a conjunction in IS NOT NULL and whose right branch
     * nests a CASE inside COALESCE. All three IN subqueries are rewritten into MarkJoins:
     * <pre>
     * Filter[IsNotNull($$m1 AND $$m2) OR COALESCE(CASE($$m3, true, false), false)]
     *   MarkJoin[z → $$m3, left=innerJoin, right=sub3]
     *     MarkJoin[y → $$m2, left=innerJoin, right=sub2]
     *       MarkJoin[x → $$m1, left=main, right=sub1]
     *         UnresolvedRelation[main]
     * </pre>
     */
    public void testDisjunctiveIsNotNullAndCoalesceCase() {
        LogicalPlan plan = resolve(
            "FROM main | WHERE (x IN (FROM sub1) AND y IN (FROM sub2)) IS NOT NULL"
                + " OR COALESCE(CASE(z IN (FROM sub3), true, false), false)"
        );
        Filter filter = as(plan, Filter.class);
        // Three stacked MarkJoins; outermost is z/sub3 (last processed)
        MarkJoin mj3 = as(filter.child(), MarkJoin.class);
        assertEquals("z", mj3.config().leftFields().get(0).name());
        assertEquals("sub3", as(mj3.right(), UnresolvedRelation.class).indexPattern().indexPattern());
        Attribute markM3 = mj3.markAttribute();
        MarkJoin mj2 = as(mj3.left(), MarkJoin.class);
        assertEquals("y", mj2.config().leftFields().get(0).name());
        assertEquals("sub2", as(mj2.right(), UnresolvedRelation.class).indexPattern().indexPattern());
        Attribute markM2 = mj2.markAttribute();
        MarkJoin mj1 = as(mj2.left(), MarkJoin.class);
        assertEquals("x", mj1.config().leftFields().get(0).name());
        assertEquals("sub1", as(mj1.right(), UnresolvedRelation.class).indexPattern().indexPattern());
        assertEquals("main", as(mj1.left(), UnresolvedRelation.class).indexPattern().indexPattern());
        Attribute markM1 = mj1.markAttribute();
        // Verify mark attributes are wired into the correct positions in the rewritten condition
        Or orExpr = as(filter.condition(), Or.class);
        And andInsideIsNotNull = as(as(orExpr.left(), IsNotNull.class).field(), And.class);
        assertEquals(markM1.id(), as(andInsideIsNotNull.left(), Attribute.class).id());
        assertEquals(markM2.id(), as(andInsideIsNotNull.right(), Attribute.class).id());
        UnresolvedFunction coalesce = as(orExpr.right(), UnresolvedFunction.class);
        UnresolvedFunction caseExpr = as(coalesce.children().get(0), UnresolvedFunction.class);
        assertEquals(markM3.id(), as(caseExpr.children().get(0), Attribute.class).id());
        filter.forEachExpression(InSubquery.class, inSub -> fail("InSubquery survived: " + inSub));
    }

    /**
     * {@code WHERE salary > 50000 AND COALESCE(x IN (FROM sub1) OR y IN (FROM sub2), false) AND (z IN (FROM sub3)) IS NULL}:
     * top-level AND chain mixing a plain predicate, a COALESCE wrapping a disjunction of two IN
     * subqueries, and an IS NULL wrapping a third IN subquery. All three IN subqueries become
     * MarkJoins while the plain predicate is left untouched:
     * <pre>
     * Filter[salary &gt; 50000 AND COALESCE($$m1 OR $$m2, false) AND IsNull($$m3)]
     *   MarkJoin[z → $$m3, left=innerJoin, right=sub3]
     *     MarkJoin[y → $$m2, left=innerJoin, right=sub2]
     *       MarkJoin[x → $$m1, left=main, right=sub1]
     *         UnresolvedRelation[main]
     * </pre>
     */
    public void testConjunctiveWithCoalesceDisjunctionAndIsNull() {
        LogicalPlan plan = resolve(
            "FROM main | WHERE salary > 50000"
                + " AND COALESCE(x IN (FROM sub1) OR y IN (FROM sub2), false)"
                + " AND (z IN (FROM sub3)) IS NULL"
        );
        Filter filter = as(plan, Filter.class);
        // Three stacked MarkJoins; outermost is z/sub3 (last processed)
        MarkJoin mj3 = as(filter.child(), MarkJoin.class);
        assertEquals("z", mj3.config().leftFields().get(0).name());
        assertEquals("sub3", as(mj3.right(), UnresolvedRelation.class).indexPattern().indexPattern());
        Attribute markM3 = mj3.markAttribute();
        MarkJoin mj2 = as(mj3.left(), MarkJoin.class);
        assertEquals("y", mj2.config().leftFields().get(0).name());
        assertEquals("sub2", as(mj2.right(), UnresolvedRelation.class).indexPattern().indexPattern());
        Attribute markM2 = mj2.markAttribute();
        MarkJoin mj1 = as(mj2.left(), MarkJoin.class);
        assertEquals("x", mj1.config().leftFields().get(0).name());
        assertEquals("sub1", as(mj1.right(), UnresolvedRelation.class).indexPattern().indexPattern());
        assertEquals("main", as(mj1.left(), UnresolvedRelation.class).indexPattern().indexPattern());
        Attribute markM1 = mj1.markAttribute();
        // All three marks must appear in the rewritten filter condition; no InSubquery should survive
        Set<NameId> expectedMarks = Set.of(markM1.id(), markM2.id(), markM3.id());
        HashSet<NameId> foundMarks = new HashSet<>();
        filter.condition().forEachDown(Attribute.class, a -> {
            if (expectedMarks.contains(a.id())) {
                foundMarks.add(a.id());
            }
        });
        assertEquals(expectedMarks, foundMarks);
        filter.forEachExpression(InSubquery.class, inSub -> fail("InSubquery survived: " + inSub));
    }

    // ---- positive: CASE with IN subquery mixed with bare IN subquery and regular predicates ----

    /**
     * {@code WHERE CASE(x IN (FROM sub1), true, false) AND salary == 50000 AND y IN (FROM sub2)}:
     * top-level AND chain mixing a CASE-wrapped IN subquery, a plain equality predicate, and a bare
     * IN subquery. The bare conjunct {@code y IN (sub2)} becomes a {@link SemiJoin} (the efficient
     * filtering shape); the wrapped {@code x IN (sub1)} inside CASE becomes a {@link MarkJoin} so
     * its mark can flow into the CASE expression:
     * <pre>
     * SemiJoin[y → sub2]
     *   Filter[CASE($$m1, true, false) AND salary == 50000]
     *     MarkJoin[x → $$m1, left=main, right=sub1]
     *       UnresolvedRelation[main]
     * </pre>
     */
    public void testCaseInSubqueryAndBareInSubqueryWithAndPredicate() {
        LogicalPlan plan = resolve("FROM main | WHERE CASE(x IN (FROM sub1), true, false) AND salary == 50000 AND y IN (FROM sub2)");
        // Bare IN conjunct → SemiJoin at the top
        SemiJoin sj = as(plan, SemiJoin.class);
        assertEquals("y", sj.config().leftFields().get(0).name());
        assertEquals("sub2", as(sj.right(), UnresolvedRelation.class).indexPattern().indexPattern());
        // Remaining conjuncts (CASE + salary) kept in a Filter
        Filter filter = as(sj.left(), Filter.class);
        // CASE's IN subquery → MarkJoin below the Filter
        MarkJoin mj = as(filter.child(), MarkJoin.class);
        assertEquals("x", mj.config().leftFields().get(0).name());
        assertEquals("sub1", as(mj.right(), UnresolvedRelation.class).indexPattern().indexPattern());
        assertEquals("main", as(mj.left(), UnresolvedRelation.class).indexPattern().indexPattern());
        Attribute markM1 = mj.markAttribute();
        // $$m1 must appear in the CASE inside the filter condition
        And andCond = as(filter.condition(), And.class);
        UnresolvedFunction caseExpr = as(andCond.left(), UnresolvedFunction.class);
        assertEquals(markM1.id(), as(caseExpr.children().get(0), Attribute.class).id());
        filter.forEachExpression(InSubquery.class, inSub -> fail("InSubquery survived: " + inSub));
    }

    /**
     * {@code WHERE CASE(x IN (FROM sub1), true, false) OR salary == 50000 OR y IN (FROM sub2)}:
     * top-level OR chain mixing a CASE-wrapped IN subquery, a plain equality predicate, and a bare
     * IN subquery. All occurrences are reachable through OR, so both IN subqueries become
     * {@link MarkJoin}s:
     * <pre>
     * Filter[(CASE($$m1, true, false) OR salary == 50000) OR $$m2]
     *   MarkJoin[y → $$m2, left=innerJoin, right=sub2]
     *     MarkJoin[x → $$m1, left=main, right=sub1]
     *       UnresolvedRelation[main]
     * </pre>
     */
    public void testCaseInSubqueryAndBareInSubqueryWithOrPredicate() {
        LogicalPlan plan = resolve("FROM main | WHERE CASE(x IN (FROM sub1), true, false) OR salary == 50000 OR y IN (FROM sub2)");
        Filter filter = as(plan, Filter.class);
        // Outer MarkJoin for y/sub2 (bare IN subquery, processed last)
        MarkJoin outerMj = as(filter.child(), MarkJoin.class);
        assertEquals("y", outerMj.config().leftFields().get(0).name());
        assertEquals("sub2", as(outerMj.right(), UnresolvedRelation.class).indexPattern().indexPattern());
        Attribute markM2 = outerMj.markAttribute();
        // Inner MarkJoin for x/sub1 (CASE-wrapped IN subquery, processed first)
        MarkJoin innerMj = as(outerMj.left(), MarkJoin.class);
        assertEquals("x", innerMj.config().leftFields().get(0).name());
        assertEquals("sub1", as(innerMj.right(), UnresolvedRelation.class).indexPattern().indexPattern());
        assertEquals("main", as(innerMj.left(), UnresolvedRelation.class).indexPattern().indexPattern());
        Attribute markM1 = innerMj.markAttribute();
        // Both marks must appear in the filter condition; no InSubquery should survive
        Set<NameId> expectedMarks = Set.of(markM1.id(), markM2.id());
        HashSet<NameId> foundMarks = new HashSet<>();
        filter.condition().forEachDown(Attribute.class, a -> {
            if (expectedMarks.contains(a.id())) {
                foundMarks.add(a.id());
            }
        });
        assertEquals(expectedMarks, foundMarks);
        filter.forEachExpression(InSubquery.class, inSub -> fail("InSubquery survived: " + inSub));
    }

    /**
     * {@code WHERE CASE(x IN (FROM sub1), true, false) AND (salary == 50000 OR y IN (FROM sub2))}:
     * top-level AND whose left conjunct is a CASE-wrapped IN subquery and whose right conjunct is
     * an OR mixing a regular predicate with a bare IN subquery. Neither conjunct is a direct
     * {@code InSubquery}, so both IN subqueries become {@link MarkJoin}s — the bare {@code y IN
     * (sub2)} is nested inside an OR and therefore cannot be promoted to a {@link SemiJoin}:
     * <pre>
     * Filter[CASE($$m1, true, false) AND (salary == 50000 OR $$m2)]
     *   MarkJoin[y → $$m2, left=innerJoin, right=sub2]
     *     MarkJoin[x → $$m1, left=main, right=sub1]
     *       UnresolvedRelation[main]
     * </pre>
     */
    public void testCaseInSubqueryAndOrWithBareInSubqueryAndPredicate() {
        LogicalPlan plan = resolve("FROM main | WHERE CASE(x IN (FROM sub1), true, false) AND (salary == 50000 OR y IN (FROM sub2))");
        Filter filter = as(plan, Filter.class);
        // Outer MarkJoin for y/sub2 (inside the OR, processed after x/sub1)
        MarkJoin outerMj = as(filter.child(), MarkJoin.class);
        assertEquals("y", outerMj.config().leftFields().get(0).name());
        assertEquals("sub2", as(outerMj.right(), UnresolvedRelation.class).indexPattern().indexPattern());
        Attribute markM2 = outerMj.markAttribute();
        // Inner MarkJoin for x/sub1 (inside CASE, processed first)
        MarkJoin innerMj = as(outerMj.left(), MarkJoin.class);
        assertEquals("x", innerMj.config().leftFields().get(0).name());
        assertEquals("sub1", as(innerMj.right(), UnresolvedRelation.class).indexPattern().indexPattern());
        assertEquals("main", as(innerMj.left(), UnresolvedRelation.class).indexPattern().indexPattern());
        Attribute markM1 = innerMj.markAttribute();
        // Verify mark wiring: $$m1 inside CASE (left of AND), $$m2 inside OR (right of AND)
        And andCond = as(filter.condition(), And.class);
        UnresolvedFunction caseExpr = as(andCond.left(), UnresolvedFunction.class);
        assertEquals(markM1.id(), as(caseExpr.children().get(0), Attribute.class).id());
        Or orExpr = as(andCond.right(), Or.class);
        assertEquals(markM2.id(), as(orExpr.right(), Attribute.class).id());
        filter.forEachExpression(InSubquery.class, inSub -> fail("InSubquery survived: " + inSub));
    }

    // ---- positive: eligible wrappers nested inside ordinary expressions ----

    public void testInSubqueryNestedInCaseAndEquals() {
        Filter filter = as(resolve("FROM main | WHERE CASE(x IN (FROM sub), true, false) == true"), Filter.class);
        Equals equals = as(filter.condition(), Equals.class);
        UnresolvedFunction caseExpr = as(equals.left(), UnresolvedFunction.class);
        assertEquals("CASE", caseExpr.name());
        assertMarkJoinReplacedInSubquery(filter, caseExpr.children().get(0), "x", "sub");
    }

    public void testInSubqueryNestedInCoalesceAndNotEquals() {
        Filter filter = as(resolve("FROM main | WHERE COALESCE(x IN (FROM sub), false) != false"), Filter.class);
        MarkJoin markJoin = assertSingleMarkJoin(filter, "x", "sub");
        assertTrue(filter.condition().anyMatch(e -> e instanceof UnresolvedFunction uf && uf.name().equals("COALESCE")));
        assertTrue(filter.condition().anyMatch(e -> e instanceof Attribute a && a.id().equals(markJoin.markAttribute().id())));
    }

    public void testInSubqueryNestedInIsNullAndEquals() {
        Filter filter = as(resolve("FROM main | WHERE ((x IN (FROM sub)) IS NULL) == true"), Filter.class);
        Equals equals = as(filter.condition(), Equals.class);
        IsNull isNull = as(equals.left(), IsNull.class);
        assertMarkJoinReplacedInSubquery(filter, isNull.field(), "x", "sub");
    }

    public void testInSubqueryNestedInIsNotNullAndNotEquals() {
        Filter filter = as(resolve("FROM main | WHERE ((x IN (FROM sub)) IS NOT NULL) != false"), Filter.class);
        MarkJoin markJoin = assertSingleMarkJoin(filter, "x", "sub");
        assertTrue(filter.condition().anyMatch(e -> e instanceof IsNotNull));
        assertTrue(filter.condition().anyMatch(e -> e instanceof Attribute a && a.id().equals(markJoin.markAttribute().id())));
    }

    public void testInSubqueryNestedInsideToStringAndEquals() {
        Filter filter = as(resolve("FROM main | WHERE TO_STRING(CASE(x IN (FROM sub), true, false)) == \"true\""), Filter.class);
        MarkJoin markJoin = assertSingleMarkJoin(filter, "x", "sub");
        assertTrue(filter.condition().anyMatch(e -> e instanceof UnresolvedFunction uf && uf.name().equals("TO_STRING")));
        assertTrue(filter.condition().anyMatch(e -> e instanceof UnresolvedFunction uf && uf.name().equals("CASE")));
        assertTrue(filter.condition().anyMatch(e -> e instanceof Attribute a && a.id().equals(markJoin.markAttribute().id())));
    }

    // ---- negative: complex LHS inside transparent wrappers ----

    /**
     * {@code WHERE CASE(abs(x) IN (FROM sub), true, false)}: the LHS of the IN subquery
     * is a non-attribute, non-foldable expression. The resolver cannot create a MarkJoin for it
     * and reports the "Complicated IN subquery" error pointing at the whole WHERE clause.
     */
    public void testRejectsComplexLHSInCase() {
        var e = expectThrows(VerificationException.class, () -> resolve("FROM main | WHERE CASE(abs(x) IN (FROM sub), true, false)"));
        assertThat(
            e.getMessage(),
            containsString("Complicated IN subquery is not yet supported in Filter [WHERE CASE(abs(x) IN (FROM sub), true, false)]")
        );
    }

    /**
     * {@code WHERE (abs(x) IN (FROM sub)) IS NULL}: complex LHS inside IS NULL.
     */
    public void testRejectsComplexLHSInIsNull() {
        var e = expectThrows(VerificationException.class, () -> resolve("FROM main | WHERE (abs(x) IN (FROM sub)) IS NULL"));
        assertThat(
            e.getMessage(),
            containsString("Complicated IN subquery is not yet supported in Filter [WHERE (abs(x) IN (FROM sub)) IS NULL]")
        );
    }

    public void testRejectsInSubqueryWithExpressionOnLHS() {
        assertResolveError(
            "FROM main | WHERE a + b IN (FROM sub)",
            "line 1:19: Complicated IN subquery is not yet supported in Filter [WHERE a + b IN (FROM sub)]"
        );

        assertResolveError(
            "FROM main | WHERE abs(a) IN (FROM sub)",
            "line 1:19: Complicated IN subquery is not yet supported in Filter [WHERE abs(a) IN (FROM sub)]"
        );
    }

    public void testRejectsInSubqueryDirectlyNestedInEquals() {
        var e = expectThrows(VerificationException.class, () -> resolve("FROM main | WHERE (x IN (FROM sub)) == true"));
        assertThat(e.getMessage(), containsString("IN subquery is not supported within expression [(x IN (FROM sub)) == true]"));
    }

    public void testRejectsComplexLHSInCaseNestedInEquals() {
        var e = expectThrows(
            VerificationException.class,
            () -> resolve("FROM main | WHERE CASE(abs(x) IN (FROM sub), true, false) == true")
        );
        assertThat(
            e.getMessage(),
            containsString(
                "Complicated IN subquery is not yet supported in Filter " + "[WHERE CASE(abs(x) IN (FROM sub), true, false) == true]"
            )
        );
    }

    public void testRejectsInSubqueryNestedInsideLambda() {
        requireLambda();
        assertResolveError(
            "FROM main | WHERE filter(a, x -> x IN (FROM sub))",
            "line 1:34: IN subquery is not supported within expression [filter(a, x -> x IN (FROM sub))]"
        );
    }

    public void testRejectsInSubqueryNestedInsideCoalesceAndLambda() {
        requireLambda();
        assertResolveError(
            "FROM main | WHERE filter(a, x -> COALESCE(x IN (FROM sub), false))",
            "line 1:43: IN subquery is not supported within expression [filter(a, x -> COALESCE(x IN (FROM sub), false))]"
        );
    }

    // ---- positive: multi-column IN subquery → SemiJoin with 2 left fields ----

    public void testMultiColumnInSubquerySemiJoin() {
        checkMultiColumnInSubquery();
        LogicalPlan plan = resolve("FROM main | WHERE (f1, f2) IN (FROM sub | KEEP f1, f2)");
        SemiJoin semiJoin = as(plan, SemiJoin.class);
        assertEquals(JoinTypes.SEMI, semiJoin.config().type());
        assertEquals(2, semiJoin.config().leftFields().size());
        assertEquals("f1", semiJoin.config().leftFields().get(0).name());
        assertEquals("f2", semiJoin.config().leftFields().get(1).name());
        assertTrue(semiJoin.config().rightFields().isEmpty());
        UnresolvedRelation relation = as(semiJoin.left(), UnresolvedRelation.class);
        assertEquals("main", relation.indexPattern().indexPattern());
        Keep keep = as(semiJoin.right(), Keep.class);
        relation = as(keep.child(), UnresolvedRelation.class);
        assertEquals("sub", relation.indexPattern().indexPattern());
    }

    // ---- positive: multi-column NOT IN subquery → AntiJoin with 2 left fields ----

    public void testMultiColumnNotInSubqueryAntiJoin() {
        checkMultiColumnInSubquery();
        LogicalPlan plan = resolve("FROM main | WHERE (f1, f2) NOT IN (FROM sub | KEEP f1, f2)");
        AntiJoin antiJoin = as(plan, AntiJoin.class);
        assertEquals(JoinTypes.ANTI, antiJoin.config().type());
        assertEquals(2, antiJoin.config().leftFields().size());
        assertEquals("f1", antiJoin.config().leftFields().get(0).name());
        assertEquals("f2", antiJoin.config().leftFields().get(1).name());
        assertTrue(antiJoin.config().rightFields().isEmpty());
        UnresolvedRelation relation = as(antiJoin.left(), UnresolvedRelation.class);
        assertEquals("main", relation.indexPattern().indexPattern());
        Keep keep = as(antiJoin.right(), Keep.class);
        relation = as(keep.child(), UnresolvedRelation.class);
        assertEquals("sub", relation.indexPattern().indexPattern());
    }

    // ---- positive: multi-column IN subquery inside OR → MarkJoin with 2 left fields ----

    public void testMultiColumnInSubqueryMarkJoin() {
        checkMultiColumnInSubquery();
        LogicalPlan plan = resolve("FROM main | WHERE (f1, f2) IN (FROM sub | KEEP f1, f2) OR f1 > 0");
        Filter filter = as(plan, Filter.class);
        Or or = as(filter.condition(), Or.class);
        Attribute mark = as(or.left(), Attribute.class);
        as(or.right(), GreaterThan.class);
        MarkJoin markJoin = as(filter.child(), MarkJoin.class);
        assertEquals(JoinTypes.MARK, markJoin.config().type());
        assertEquals(2, markJoin.config().leftFields().size());
        assertEquals("f1", markJoin.config().leftFields().get(0).name());
        assertEquals("f2", markJoin.config().leftFields().get(1).name());
        assertTrue(markJoin.config().rightFields().isEmpty());
        assertEquals(mark.id(), markJoin.markAttribute().id());
        UnresolvedRelation relation = as(markJoin.left(), UnresolvedRelation.class);
        assertEquals("main", relation.indexPattern().indexPattern());
        Keep keep = as(markJoin.right(), Keep.class);
        relation = as(keep.child(), UnresolvedRelation.class);
        assertEquals("sub", relation.indexPattern().indexPattern());
    }

    // ---- positive: mixed single-column and multi-column IN subqueries ----

    public void testMixedSingleAndMultiColumnInSubqueryConjunctive() {
        checkMultiColumnInSubquery();
        LogicalPlan plan = resolve("FROM main | WHERE x IN (FROM sub1) AND (f1, f2) IN (FROM sub2 | KEEP f1, f2)");
        SemiJoin outer = as(plan, SemiJoin.class);
        assertEquals(JoinTypes.SEMI, outer.config().type());
        assertEquals(2, outer.config().leftFields().size());
        assertEquals("f1", outer.config().leftFields().get(0).name());
        assertEquals("f2", outer.config().leftFields().get(1).name());
        Keep outerRight = as(outer.right(), Keep.class);
        assertEquals("sub2", as(outerRight.child(), UnresolvedRelation.class).indexPattern().indexPattern());
        SemiJoin inner = as(outer.left(), SemiJoin.class);
        assertEquals(JoinTypes.SEMI, inner.config().type());
        assertEquals(1, inner.config().leftFields().size());
        assertEquals("x", inner.config().leftFields().get(0).name());
        assertEquals("sub1", as(inner.right(), UnresolvedRelation.class).indexPattern().indexPattern());
        assertEquals("main", as(inner.left(), UnresolvedRelation.class).indexPattern().indexPattern());
    }

    public void testMixedMultiAndSingleColumnNotInSubqueryConjunctive() {
        checkMultiColumnInSubquery();
        LogicalPlan plan = resolve("FROM main | WHERE (f1, f2) NOT IN (FROM sub1 | KEEP f1, f2) AND x NOT IN (FROM sub2)");
        AntiJoin outer = as(plan, AntiJoin.class);
        assertEquals(JoinTypes.ANTI, outer.config().type());
        assertEquals(1, outer.config().leftFields().size());
        assertEquals("x", outer.config().leftFields().get(0).name());
        assertEquals("sub2", as(outer.right(), UnresolvedRelation.class).indexPattern().indexPattern());
        AntiJoin inner = as(outer.left(), AntiJoin.class);
        assertEquals(JoinTypes.ANTI, inner.config().type());
        assertEquals(2, inner.config().leftFields().size());
        assertEquals("f1", inner.config().leftFields().get(0).name());
        assertEquals("f2", inner.config().leftFields().get(1).name());
        Keep innerRight = as(inner.right(), Keep.class);
        assertEquals("sub1", as(innerRight.child(), UnresolvedRelation.class).indexPattern().indexPattern());
        assertEquals("main", as(inner.left(), UnresolvedRelation.class).indexPattern().indexPattern());
    }

    public void testMixedSingleAndMultiColumnInSubqueryDisjunctive() {
        checkMultiColumnInSubquery();
        LogicalPlan plan = resolve("FROM main | WHERE x IN (FROM sub1) OR (f1, f2) IN (FROM sub2 | KEEP f1, f2)");
        Filter filter = as(plan, Filter.class);
        Or or = as(filter.condition(), Or.class);
        Attribute leftMark = as(or.left(), Attribute.class);
        Attribute rightMark = as(or.right(), Attribute.class);
        MarkJoin outer = as(filter.child(), MarkJoin.class);
        assertEquals(JoinTypes.MARK, outer.config().type());
        assertEquals(2, outer.config().leftFields().size());
        assertEquals("f1", outer.config().leftFields().get(0).name());
        assertEquals("f2", outer.config().leftFields().get(1).name());
        assertEquals(rightMark.id(), outer.markAttribute().id());
        Keep outerRight = as(outer.right(), Keep.class);
        assertEquals("sub2", as(outerRight.child(), UnresolvedRelation.class).indexPattern().indexPattern());
        MarkJoin inner = as(outer.left(), MarkJoin.class);
        assertEquals(JoinTypes.MARK, inner.config().type());
        assertEquals(1, inner.config().leftFields().size());
        assertEquals("x", inner.config().leftFields().get(0).name());
        assertEquals(leftMark.id(), inner.markAttribute().id());
        assertEquals("sub1", as(inner.right(), UnresolvedRelation.class).indexPattern().indexPattern());
        assertEquals("main", as(inner.left(), UnresolvedRelation.class).indexPattern().indexPattern());
    }

    public void testMixedSemiJoinAndMarkJoinWithMultiColumn() {
        checkMultiColumnInSubquery();
        LogicalPlan plan = resolve("FROM main | WHERE x IN (FROM sub1) AND ((f1, f2) IN (FROM sub2 | KEEP f1, f2) OR a > 0)");
        SemiJoin xJoin = as(plan, SemiJoin.class);
        assertEquals(JoinTypes.SEMI, xJoin.config().type());
        assertEquals(1, xJoin.config().leftFields().size());
        assertEquals("x", xJoin.config().leftFields().get(0).name());
        assertEquals("sub1", as(xJoin.right(), UnresolvedRelation.class).indexPattern().indexPattern());

        Filter filter = as(xJoin.left(), Filter.class);
        Or or = as(filter.condition(), Or.class);
        Attribute fMark = as(or.left(), Attribute.class);
        as(or.right(), GreaterThan.class);

        MarkJoin fJoin = as(filter.child(), MarkJoin.class);
        assertEquals(JoinTypes.MARK, fJoin.config().type());
        assertEquals(2, fJoin.config().leftFields().size());
        assertEquals("f1", fJoin.config().leftFields().get(0).name());
        assertEquals("f2", fJoin.config().leftFields().get(1).name());
        assertEquals(fMark.id(), fJoin.markAttribute().id());
        Keep fRight = as(fJoin.right(), Keep.class);
        assertEquals("sub2", as(fRight.child(), UnresolvedRelation.class).indexPattern().indexPattern());
        assertEquals("main", as(fJoin.left(), UnresolvedRelation.class).indexPattern().indexPattern());
    }

    public void testThreeMixedSubqueriesConjunctive() {
        checkMultiColumnInSubquery();
        LogicalPlan plan = resolve("""
            FROM main | WHERE x IN (FROM sub1)
              AND (f1, f2) IN (FROM sub2 | KEEP f1, f2)
              AND y NOT IN (FROM sub3)
            """);
        AntiJoin antiJoin = as(plan, AntiJoin.class);
        assertEquals(JoinTypes.ANTI, antiJoin.config().type());
        assertEquals(1, antiJoin.config().leftFields().size());
        assertEquals("y", antiJoin.config().leftFields().get(0).name());
        assertEquals("sub3", as(antiJoin.right(), UnresolvedRelation.class).indexPattern().indexPattern());

        SemiJoin multiJoin = as(antiJoin.left(), SemiJoin.class);
        assertEquals(JoinTypes.SEMI, multiJoin.config().type());
        assertEquals(2, multiJoin.config().leftFields().size());
        assertEquals("f1", multiJoin.config().leftFields().get(0).name());
        assertEquals("f2", multiJoin.config().leftFields().get(1).name());
        assertEquals("sub2", as(as(multiJoin.right(), Keep.class).child(), UnresolvedRelation.class).indexPattern().indexPattern());

        SemiJoin xJoin = as(multiJoin.left(), SemiJoin.class);
        assertEquals(JoinTypes.SEMI, xJoin.config().type());
        assertEquals(1, xJoin.config().leftFields().size());
        assertEquals("x", xJoin.config().leftFields().get(0).name());
        assertEquals("sub1", as(xJoin.right(), UnresolvedRelation.class).indexPattern().indexPattern());
        assertEquals("main", as(xJoin.left(), UnresolvedRelation.class).indexPattern().indexPattern());
    }

    public void testMixedMultiAndSingleColumnNotInSubqueryDisjunctive() {
        checkMultiColumnInSubquery();
        LogicalPlan plan = resolve("FROM main | WHERE (f1, f2) NOT IN (FROM sub1 | KEEP f1, f2) OR x NOT IN (FROM sub2)");
        Filter filter = as(plan, Filter.class);
        Or or = as(filter.condition(), Or.class);
        Attribute leftMark = as(as(or.left(), Not.class).field(), Attribute.class);
        Attribute rightMark = as(as(or.right(), Not.class).field(), Attribute.class);

        MarkJoin outer = as(filter.child(), MarkJoin.class);
        assertEquals(JoinTypes.MARK, outer.config().type());
        assertEquals(1, outer.config().leftFields().size());
        assertEquals("x", outer.config().leftFields().get(0).name());
        assertEquals(rightMark.id(), outer.markAttribute().id());
        assertEquals("sub2", as(outer.right(), UnresolvedRelation.class).indexPattern().indexPattern());

        MarkJoin inner = as(outer.left(), MarkJoin.class);
        assertEquals(JoinTypes.MARK, inner.config().type());
        assertEquals(2, inner.config().leftFields().size());
        assertEquals("f1", inner.config().leftFields().get(0).name());
        assertEquals("f2", inner.config().leftFields().get(1).name());
        assertEquals(leftMark.id(), inner.markAttribute().id());
        assertEquals("sub1", as(as(inner.right(), Keep.class).child(), UnresolvedRelation.class).indexPattern().indexPattern());
        assertEquals("main", as(inner.left(), UnresolvedRelation.class).indexPattern().indexPattern());
    }

    // ---- positive: nested multi-column IN subqueries ----

    public void testNestedMultiColumnInSubqueryInsideMultiColumnInSubquery() {
        checkMultiColumnInSubquery();
        LogicalPlan plan = resolve("""
            FROM main | WHERE (f1, f2) IN (FROM sub1 | WHERE (g1, g2) IN (FROM sub2 | KEEP g1, g2) | KEEP f1, f2)
            """);
        SemiJoin outer = as(plan, SemiJoin.class);
        assertEquals(JoinTypes.SEMI, outer.config().type());
        assertEquals(2, outer.config().leftFields().size());
        assertEquals("f1", outer.config().leftFields().get(0).name());
        assertEquals("f2", outer.config().leftFields().get(1).name());
        assertEquals("main", as(outer.left(), UnresolvedRelation.class).indexPattern().indexPattern());

        Keep outerKeep = as(outer.right(), Keep.class);
        SemiJoin inner = as(outerKeep.child(), SemiJoin.class);
        assertEquals(JoinTypes.SEMI, inner.config().type());
        assertEquals(2, inner.config().leftFields().size());
        assertEquals("g1", inner.config().leftFields().get(0).name());
        assertEquals("g2", inner.config().leftFields().get(1).name());
        assertEquals("sub1", as(inner.left(), UnresolvedRelation.class).indexPattern().indexPattern());
        Keep innerKeep = as(inner.right(), Keep.class);
        assertEquals("sub2", as(innerKeep.child(), UnresolvedRelation.class).indexPattern().indexPattern());
    }

    public void testNestedMultiColumnNotInSubqueryInsideMultiColumnNotInSubquery() {
        checkMultiColumnInSubquery();
        LogicalPlan plan = resolve("""
            FROM main | WHERE (f1, f2) NOT IN (FROM sub1 | WHERE (g1, g2) NOT IN (FROM sub2 | KEEP g1, g2) | KEEP f1, f2)
            """);
        AntiJoin outer = as(plan, AntiJoin.class);
        assertEquals(JoinTypes.ANTI, outer.config().type());
        assertEquals(2, outer.config().leftFields().size());
        assertEquals("f1", outer.config().leftFields().get(0).name());
        assertEquals("f2", outer.config().leftFields().get(1).name());
        assertEquals("main", as(outer.left(), UnresolvedRelation.class).indexPattern().indexPattern());

        Keep outerKeep = as(outer.right(), Keep.class);
        AntiJoin inner = as(outerKeep.child(), AntiJoin.class);
        assertEquals(JoinTypes.ANTI, inner.config().type());
        assertEquals(2, inner.config().leftFields().size());
        assertEquals("g1", inner.config().leftFields().get(0).name());
        assertEquals("g2", inner.config().leftFields().get(1).name());
        assertEquals("sub1", as(inner.left(), UnresolvedRelation.class).indexPattern().indexPattern());
        Keep innerKeep = as(inner.right(), Keep.class);
        assertEquals("sub2", as(innerKeep.child(), UnresolvedRelation.class).indexPattern().indexPattern());
    }

    public void testNestedSingleColumnInSubqueryInsideMultiColumnInSubquery() {
        checkMultiColumnInSubquery();
        LogicalPlan plan = resolve("""
            FROM main | WHERE (f1, f2) IN (FROM sub1 | WHERE x IN (FROM sub2 | KEEP b) | KEEP f1, f2)
            """);
        SemiJoin outer = as(plan, SemiJoin.class);
        assertEquals(JoinTypes.SEMI, outer.config().type());
        assertEquals(2, outer.config().leftFields().size());
        assertEquals("f1", outer.config().leftFields().get(0).name());
        assertEquals("f2", outer.config().leftFields().get(1).name());
        assertEquals("main", as(outer.left(), UnresolvedRelation.class).indexPattern().indexPattern());

        Keep outerKeep = as(outer.right(), Keep.class);
        SemiJoin inner = as(outerKeep.child(), SemiJoin.class);
        assertEquals(JoinTypes.SEMI, inner.config().type());
        assertEquals(1, inner.config().leftFields().size());
        assertEquals("x", inner.config().leftFields().get(0).name());
        assertEquals("sub1", as(inner.left(), UnresolvedRelation.class).indexPattern().indexPattern());
        Keep innerKeep = as(inner.right(), Keep.class);
        assertEquals("sub2", as(innerKeep.child(), UnresolvedRelation.class).indexPattern().indexPattern());
    }

    public void testNestedMultiColumnNotInSubqueryInsideSingleColumnInSubquery() {
        checkMultiColumnInSubquery();
        LogicalPlan plan = resolve("""
            FROM main | WHERE x IN (FROM sub1 | WHERE (g1, g2) NOT IN (FROM sub2 | KEEP g1, g2) | KEEP a)
            """);
        SemiJoin outer = as(plan, SemiJoin.class);
        assertEquals(JoinTypes.SEMI, outer.config().type());
        assertEquals(1, outer.config().leftFields().size());
        assertEquals("x", outer.config().leftFields().get(0).name());
        assertEquals("main", as(outer.left(), UnresolvedRelation.class).indexPattern().indexPattern());

        Keep outerKeep = as(outer.right(), Keep.class);
        AntiJoin inner = as(outerKeep.child(), AntiJoin.class);
        assertEquals(JoinTypes.ANTI, inner.config().type());
        assertEquals(2, inner.config().leftFields().size());
        assertEquals("g1", inner.config().leftFields().get(0).name());
        assertEquals("g2", inner.config().leftFields().get(1).name());
        assertEquals("sub1", as(inner.left(), UnresolvedRelation.class).indexPattern().indexPattern());
        Keep innerKeep = as(inner.right(), Keep.class);
        assertEquals("sub2", as(innerKeep.child(), UnresolvedRelation.class).indexPattern().indexPattern());
    }

    // ---- multi-column IN subquery inside CASE, COALESCE, IS [NOT] NULL ----

    public void testMultiColumnCaseWhenConditionInSubquery() {
        checkMultiColumnInSubquery();
        LogicalPlan plan = resolve("FROM main | WHERE CASE((f1, f2) IN (FROM sub | KEEP f1, f2), true, false)");
        Filter filter = as(plan, Filter.class);
        UnresolvedFunction caseExpr = as(filter.condition(), UnresolvedFunction.class);
        Attribute mark = as(caseExpr.children().get(0), Attribute.class);
        MarkJoin mj = as(filter.child(), MarkJoin.class);
        assertEquals(JoinTypes.MARK, mj.config().type());
        assertEquals(2, mj.config().leftFields().size());
        assertEquals("f1", mj.config().leftFields().get(0).name());
        assertEquals("f2", mj.config().leftFields().get(1).name());
        assertEquals(mark.id(), mj.markAttribute().id());
    }

    public void testMultiColumnCoalesceInSubquery() {
        checkMultiColumnInSubquery();
        LogicalPlan plan = resolve("FROM main | WHERE COALESCE((f1, f2) IN (FROM sub | KEEP f1, f2), false)");
        Filter filter = as(plan, Filter.class);
        UnresolvedFunction coalesceExpr = as(filter.condition(), UnresolvedFunction.class);
        Attribute mark = as(coalesceExpr.children().get(0), Attribute.class);
        MarkJoin mj = as(filter.child(), MarkJoin.class);
        assertEquals(2, mj.config().leftFields().size());
        assertEquals(mark.id(), mj.markAttribute().id());
    }

    public void testMultiColumnIsNullInSubquery() {
        checkMultiColumnInSubquery();
        LogicalPlan plan = resolve("FROM main | WHERE ((f1, f2) IN (FROM sub | KEEP f1, f2)) IS NULL");
        Filter filter = as(plan, Filter.class);
        IsNull isNull = as(filter.condition(), IsNull.class);
        Attribute mark = as(isNull.field(), Attribute.class);
        MarkJoin mj = as(filter.child(), MarkJoin.class);
        assertEquals(2, mj.config().leftFields().size());
        assertEquals(mark.id(), mj.markAttribute().id());
    }

    public void testMultiColumnIsNotNullInSubquery() {
        checkMultiColumnInSubquery();
        LogicalPlan plan = resolve("FROM main | WHERE ((f1, f2) IN (FROM sub | KEEP f1, f2)) IS NOT NULL");
        Filter filter = as(plan, Filter.class);
        IsNotNull isNotNull = as(filter.condition(), IsNotNull.class);
        Attribute mark = as(isNotNull.field(), Attribute.class);
        MarkJoin mj = as(filter.child(), MarkJoin.class);
        assertEquals(2, mj.config().leftFields().size());
        assertEquals(mark.id(), mj.markAttribute().id());
    }

    public void testMultiColumnInSubqueryNestedInCaseAndEquals() {
        checkMultiColumnInSubquery();
        Filter filter = as(resolve("FROM main | WHERE CASE((f1, f2) IN (FROM sub | KEEP f1, f2), 1, 0) == 1"), Filter.class);
        Equals equals = as(filter.condition(), Equals.class);
        UnresolvedFunction caseExpr = as(equals.left(), UnresolvedFunction.class);
        Attribute mark = as(caseExpr.children().get(0), Attribute.class);
        MarkJoin mj = as(filter.child(), MarkJoin.class);
        assertEquals(2, mj.config().leftFields().size());
        assertEquals(mark.id(), mj.markAttribute().id());
    }

    public void testMultiColumnInSubqueryNestedInCoalesceAndNotEquals() {
        checkMultiColumnInSubquery();
        Filter filter = as(resolve("FROM main | WHERE COALESCE((f1, f2) IN (FROM sub | KEEP f1, f2), false) != false"), Filter.class);
        Not not = as(filter.condition(), Not.class);
        Equals equals = as(not.field(), Equals.class);
        UnresolvedFunction coalesceExpr = as(equals.left(), UnresolvedFunction.class);
        Attribute mark = as(coalesceExpr.children().get(0), Attribute.class);
        MarkJoin mj = as(filter.child(), MarkJoin.class);
        assertEquals(2, mj.config().leftFields().size());
        assertEquals(mark.id(), mj.markAttribute().id());
    }

    public void testMultiColumnInSubqueryWithCaseAndGreaterThan() {
        checkMultiColumnInSubquery();
        LogicalPlan plan = resolve("FROM main | WHERE CASE((f1, f2) IN (FROM sub | KEEP f1, f2), true, false) AND f1 > 0");
        Filter filter = as(plan, Filter.class);
        And and = as(filter.condition(), And.class);
        UnresolvedFunction caseExpr = as(and.left(), UnresolvedFunction.class);
        Attribute mark = as(caseExpr.children().get(0), Attribute.class);
        MarkJoin mj = as(filter.child(), MarkJoin.class);
        assertEquals(2, mj.config().leftFields().size());
        assertEquals(mark.id(), mj.markAttribute().id());
    }

    public void testMultiColumnInSubqueryWithCoalesceOrLessThan() {
        checkMultiColumnInSubquery();
        LogicalPlan plan = resolve("FROM main | WHERE COALESCE((f1, f2) IN (FROM sub | KEEP f1, f2), false) OR f1 < 0");
        Filter filter = as(plan, Filter.class);
        Or or = as(filter.condition(), Or.class);
        UnresolvedFunction coalesceExpr = as(or.left(), UnresolvedFunction.class);
        Attribute mark = as(coalesceExpr.children().get(0), Attribute.class);
        MarkJoin mj = as(filter.child(), MarkJoin.class);
        assertEquals(2, mj.config().leftFields().size());
        assertEquals(mark.id(), mj.markAttribute().id());
    }

    public void testMultiColumnInSubqueryWithComplexBooleanExpressions() {
        checkMultiColumnInSubquery();
        String query = """
            FROM main
            | WHERE (f1 > 0
                     AND (((f1, f2) IN (FROM sub1 | KEEP f1, f2)) IS NULL)
                    )
              OR (((f1, f2) IN (FROM sub2 | KEEP f1, f2)) IS NOT NULL
                  AND f2 < 0
                 )
            """;
        LogicalPlan plan = resolve(query);
        Filter filter = as(plan, Filter.class);
        Or topOr = as(filter.condition(), Or.class);

        // MarkJoins are stacked. Outermost is sub2 (right side of OR).
        MarkJoin mj2 = as(filter.child(), MarkJoin.class);
        assertEquals("sub2", as(as(mj2.right(), Keep.class).child(), UnresolvedRelation.class).indexPattern().indexPattern());
        Attribute mark2 = mj2.markAttribute();

        MarkJoin mj1 = as(mj2.left(), MarkJoin.class);
        assertEquals("sub1", as(as(mj1.right(), Keep.class).child(), UnresolvedRelation.class).indexPattern().indexPattern());
        Attribute mark1 = mj1.markAttribute();

        // Verify boolean condition structure
        And leftAnd = as(topOr.left(), And.class);
        assertEquals(mark1.id(), as(as(leftAnd.right(), IsNull.class).field(), Attribute.class).id());

        And rightAnd = as(topOr.right(), And.class);
        assertEquals(mark2.id(), as(as(rightAnd.left(), IsNotNull.class).field(), Attribute.class).id());
    }

    // negative: multi-column IN subquery inside lambda, equals, etc. is not supported

    public void testRejectsMultiColumnInSubqueryNestedInsideLambda() {
        requireLambda();
        checkMultiColumnInSubquery();
        assertResolveError(
            "FROM main | WHERE filter(a, x -> (x, f2) IN (FROM sub | KEEP x, f2))",
            "line 1:34: IN subquery is not supported within expression [filter(a, x -> (x, f2) IN (FROM sub | KEEP x, f2))]"
        );
    }

    public void testRejectsMultiColumnInSubqueryNestedInsideCoalesceAndLambda() {
        requireLambda();
        checkMultiColumnInSubquery();
        assertResolveError(
            "FROM main | WHERE filter(a, x -> COALESCE((x, f2) IN (FROM sub | KEEP x, f2), false))",
            "line 1:43: IN subquery is not supported within expression "
                + "[filter(a, x -> COALESCE((x, f2) IN (FROM sub | KEEP x, f2), false))]"
        );
    }

    public void testRejectsMultiColumnInSubqueryDirectlyNestedInEquals() {
        checkMultiColumnInSubquery();
        var e = expectThrows(
            VerificationException.class,
            () -> resolve("FROM main | WHERE ((f1, f2) IN (FROM sub | KEEP f1, f2)) == true")
        );
        assertThat(
            e.getMessage(),
            containsString("IN subquery is not supported within expression [((f1, f2) IN (FROM sub | KEEP f1, f2)) == true]")
        );
    }

    // ---- positive: synthetic constant aliases stay unique within one WHERE rewrite ----

    /**
     * Repeated equal constants in a multi-column tuple hash identically, so without a per-alias ordinal in the synthetic name both
     * Eval fields would share one name — and the Eval's output merging drops earlier same-named fields, orphaning the join key that
     * references the dropped alias.
     */
    public void testRepeatedConstantsInMultiColumnInSubqueryGetDistinctNames() {
        checkMultiColumnInSubquery();
        LogicalPlan plan = resolve("FROM main | WHERE (1, 1) IN (FROM sub | KEEP a, b)");
        SemiJoin semiJoin = as(plan, SemiJoin.class);
        var leftFields = semiJoin.config().leftFields();
        assertEquals(2, leftFields.size());
        assertThat(leftFields.get(0).name(), containsString("$$in_subquery_const$"));
        assertThat(leftFields.get(1).name(), containsString("$$in_subquery_const$"));
        assertNotEquals(leftFields.get(0).name(), leftFields.get(1).name());
        Eval eval = as(semiJoin.left(), Eval.class);
        assertEquals(2, eval.fields().size());
        assertEquals(leftFields.get(0).id(), eval.fields().get(0).id());
        assertEquals(leftFields.get(1).id(), eval.fields().get(1).id());
    }

    /**
     * The same constant IN predicate repeated across conjuncts materializes two synthetic aliases in the same Eval; their names must
     * differ for the same reason as in {@link #testRepeatedConstantsInMultiColumnInSubqueryGetDistinctNames}.
     */
    public void testRepeatedConstantInSubqueriesGetDistinctNames() {
        LogicalPlan plan = resolve("FROM main | WHERE 42 IN (FROM sub | KEEP y) AND 42 IN (FROM sub | KEEP y)");
        // The two SemiJoins stack in conjunct order, so the outer join belongs to the second conjunct.
        SemiJoin outer = as(plan, SemiJoin.class);
        SemiJoin inner = as(outer.left(), SemiJoin.class);
        Eval eval = as(inner.left(), Eval.class);
        assertEquals(2, eval.fields().size());
        assertNotEquals(eval.fields().get(0).name(), eval.fields().get(1).name());
        assertEquals(inner.config().leftFields().get(0).id(), eval.fields().get(0).id());
        assertEquals(outer.config().leftFields().get(0).id(), eval.fields().get(1).id());
    }

    /**
     * {@code WHERE x IN (FROM sub) OR x IN (FROM sub)}: the same {@link InSubquery} predicate appearing twice in one OR
     * produces two distinct mark attribute names. Without an ordinal in {@code syntheticMarkName} both marks would share
     * a name (differing only by {@link NameId}), making EXPLAIN output misleading.
     */
    public void testDuplicateInSubqueryInOrGetsDistinctMarkNames() {
        LogicalPlan plan = resolve("FROM main | WHERE x IN (FROM sub) OR x IN (FROM sub)");
        Filter filter = as(plan, Filter.class);
        Or or = as(filter.condition(), Or.class);
        Attribute leftMark = as(or.left(), Attribute.class);
        Attribute rightMark = as(or.right(), Attribute.class);
        assertNotEquals(leftMark.name(), rightMark.name());
        MarkJoin outer = as(filter.child(), MarkJoin.class);
        assertEquals(rightMark.id(), outer.markAttribute().id());
        MarkJoin inner = as(outer.left(), MarkJoin.class);
        assertEquals(leftMark.id(), inner.markAttribute().id());
        assertEquals("main", as(inner.left(), UnresolvedRelation.class).indexPattern().indexPattern());
    }

    // ---- positive: EVAL IN subquery → MarkJoin ----

    /**
     * {@code FROM main | EVAL z = x IN (FROM sub)}:
     * the resolver produces {@code Eval[z=$mark] → MarkJoin[left=x] → UnresolvedRelation[main]}.
     */
    public void testInSubqueryInEval() {
        LogicalPlan plan = resolve("FROM main | EVAL z = x IN (FROM sub)");
        Eval eval = as(plan, Eval.class);
        assertEquals(1, eval.fields().size());
        assertEquals("z", eval.fields().get(0).name());
        Attribute mark = as(eval.fields().get(0).child(), Attribute.class);
        MarkJoin join = as(eval.child(), MarkJoin.class);
        assertEquals(JoinTypes.MARK, join.config().type());
        assertEquals("x", join.config().leftFields().get(0).name());
        assertEquals(mark.id(), join.markAttribute().id());
        UnresolvedRelation main = as(join.left(), UnresolvedRelation.class);
        assertEquals("main", main.indexPattern().indexPattern());
        UnresolvedRelation sub = as(join.right(), UnresolvedRelation.class);
        assertEquals("sub", sub.indexPattern().indexPattern());
    }

    /**
     * {@code FROM main | EVAL z = x NOT IN (FROM sub)}:
     * the resolver produces {@code Eval[z=NOT($mark)] → MarkJoin[left=x] → UnresolvedRelation[main]}.
     */
    public void testNotInSubqueryInEval() {
        LogicalPlan plan = resolve("FROM main | EVAL z = x NOT IN (FROM sub)");
        Eval eval = as(plan, Eval.class);
        assertEquals(1, eval.fields().size());
        assertEquals("z", eval.fields().get(0).name());
        Not not = as(eval.fields().get(0).child(), Not.class);
        Attribute mark = as(not.field(), Attribute.class);
        MarkJoin join = as(eval.child(), MarkJoin.class);
        assertEquals("x", join.config().leftFields().get(0).name());
        assertEquals(mark.id(), join.markAttribute().id());
        UnresolvedRelation main = as(join.left(), UnresolvedRelation.class);
        assertEquals("main", main.indexPattern().indexPattern());
        UnresolvedRelation sub = as(join.right(), UnresolvedRelation.class);
        assertEquals("sub", sub.indexPattern().indexPattern());
    }

    /**
     * {@code FROM main | EVAL a = 1, z = x IN (FROM sub), b = 2}: multi-field EVAL with no
     * intra-EVAL LHS dependency — single MarkJoin below the whole Eval, no split.
     */
    public void testInSubqueryInEvalWithMultipleFields() {
        LogicalPlan plan = resolve("FROM main | EVAL a = 1, z = x IN (FROM sub), b = 2");
        Eval eval = as(plan, Eval.class);
        assertEquals(3, eval.fields().size());
        assertEquals("a", eval.fields().get(0).name());
        assertEquals("z", eval.fields().get(1).name());
        assertEquals("b", eval.fields().get(2).name());
        Attribute mark = as(eval.fields().get(1).child(), Attribute.class);
        // Single MarkJoin below the one Eval — no split
        MarkJoin join = as(eval.child(), MarkJoin.class);
        assertEquals("x", join.config().leftFields().get(0).name());
        assertEquals(mark.id(), join.markAttribute().id());
        UnresolvedRelation main = as(join.left(), UnresolvedRelation.class);
        assertEquals("main", main.indexPattern().indexPattern());
        UnresolvedRelation sub = as(join.right(), UnresolvedRelation.class);
        assertEquals("sub", sub.indexPattern().indexPattern());
    }

    /**
     * {@code FROM main | EVAL a = x + 1, z = a IN (FROM sub)}: intra-EVAL LHS dependency —
     * the resolver must split: {@code Eval[z=$mark] → MarkJoin[left=a] → Eval[a=x+1] → Relation}.
     */
    public void testInSubqueryInEvalWithLHSDerivedFromAnotherField() {
        LogicalPlan plan = resolve("FROM main | EVAL a = x + 1, z = a IN (FROM sub)");

        // Upper Eval: just z
        Eval upperEval = as(plan, Eval.class);
        assertEquals(1, upperEval.fields().size());
        assertEquals("z", upperEval.fields().get(0).name());
        Attribute mark = as(upperEval.fields().get(0).child(), Attribute.class);

        // MarkJoin with left field "a"
        MarkJoin join = as(upperEval.child(), MarkJoin.class);
        assertEquals("a", join.config().leftFields().get(0).name());
        assertEquals(mark.id(), join.markAttribute().id());

        // Lower Eval: a = x + 1
        Eval lowerEval = as(join.left(), Eval.class);
        assertEquals(1, lowerEval.fields().size());
        assertEquals("a", lowerEval.fields().get(0).name());

        UnresolvedRelation main = as(lowerEval.child(), UnresolvedRelation.class);
        assertEquals("main", main.indexPattern().indexPattern());
        UnresolvedRelation sub = as(join.right(), UnresolvedRelation.class);
        assertEquals("sub", sub.indexPattern().indexPattern());
    }

    // ---- positive: EVAL IN subquery — name-collision flush ----

    /**
     * {@code FROM main | EVAL a = 1, z = a + 1, a = y IN (FROM sub)}: the third field reuses name {@code a},
     * which is already in {@code pending}. Without the name-collision flush the first {@code Alias(a,1,NameId1)}
     * would be silently overwritten and {@code z}'s reference to {@code NameId1} would become dangling.
     * Expected plan: {@code Eval[a=$$mark] → MarkJoin[y→$$mark] → Eval[a=1, z=a+1] → Relation[main]}.
     */
    public void testInSubqueryInEvalNameCollisionFlushesInterveningAlias() {
        LogicalPlan plan = resolve("FROM main | EVAL a = 1, z = a + 1, a = y IN (FROM sub)");

        // Upper Eval: just the redefined `a`
        Eval upperEval = as(plan, Eval.class);
        assertEquals(1, upperEval.fields().size());
        assertEquals("a", upperEval.fields().get(0).name());
        Attribute mark = as(upperEval.fields().get(0).child(), Attribute.class);

        // MarkJoin with left field "y"
        MarkJoin join = as(upperEval.child(), MarkJoin.class);
        assertEquals("y", join.config().leftFields().get(0).name());
        assertEquals(mark.id(), join.markAttribute().id());

        // Lower Eval: a=1 and z=a+1 were flushed together
        Eval lowerEval = as(join.left(), Eval.class);
        assertEquals(2, lowerEval.fields().size());
        assertEquals("a", lowerEval.fields().get(0).name());
        assertEquals("z", lowerEval.fields().get(1).name());

        assertEquals("main", as(lowerEval.child(), UnresolvedRelation.class).indexPattern().indexPattern());
        assertEquals("sub", as(join.right(), UnresolvedRelation.class).indexPattern().indexPattern());
    }

    /**
     * {@code FROM main | EVAL a = 1, a = y IN (FROM sub)}: simpler name collision with no
     * dependent field between the two definitions.
     * Expected plan: {@code Eval[a=$$mark] → MarkJoin[y→$$mark] → Eval[a=1] → Relation[main]}.
     */
    public void testInSubqueryInEvalNameCollisionNoInterveningField() {
        LogicalPlan plan = resolve("FROM main | EVAL a = 1, a = y IN (FROM sub)");

        Eval upperEval = as(plan, Eval.class);
        assertEquals(1, upperEval.fields().size());
        assertEquals("a", upperEval.fields().get(0).name());
        Attribute mark = as(upperEval.fields().get(0).child(), Attribute.class);

        MarkJoin join = as(upperEval.child(), MarkJoin.class);
        assertEquals("y", join.config().leftFields().get(0).name());
        assertEquals(mark.id(), join.markAttribute().id());

        Eval lowerEval = as(join.left(), Eval.class);
        assertEquals(1, lowerEval.fields().size());
        assertEquals("a", lowerEval.fields().get(0).name());

        assertEquals("main", as(lowerEval.child(), UnresolvedRelation.class).indexPattern().indexPattern());
        assertEquals("sub", as(join.right(), UnresolvedRelation.class).indexPattern().indexPattern());
    }

    /**
     * {@code FROM main | EVAL a = x IN (FROM sub1), a = y IN (FROM sub2)}: both definitions use
     * InSubquery; the second must flush the first's pending entry before stacking its own MarkJoin.
     * Expected plan:
     * {@code Eval[a=$$m2] → MarkJoin[y→$$m2,sub2] → Eval[a=$$m1] → MarkJoin[x→$$m1,sub1] → Relation[main]}.
     */
    public void testInSubqueryInEvalNameCollisionBothInSubquery() {
        LogicalPlan plan = resolve("FROM main | EVAL a = x IN (FROM sub1), a = y IN (FROM sub2)");

        // Upper Eval: a = $$m2
        Eval upperEval = as(plan, Eval.class);
        assertEquals(1, upperEval.fields().size());
        assertEquals("a", upperEval.fields().get(0).name());
        Attribute mark2 = as(upperEval.fields().get(0).child(), Attribute.class);

        // Outer MarkJoin: y → $$m2, right=sub2
        MarkJoin outerJoin = as(upperEval.child(), MarkJoin.class);
        assertEquals("y", outerJoin.config().leftFields().get(0).name());
        assertEquals(mark2.id(), outerJoin.markAttribute().id());
        assertEquals("sub2", as(outerJoin.right(), UnresolvedRelation.class).indexPattern().indexPattern());

        // Middle Eval: a = $$m1 (flushed when the second `a` was encountered)
        Eval middleEval = as(outerJoin.left(), Eval.class);
        assertEquals(1, middleEval.fields().size());
        assertEquals("a", middleEval.fields().get(0).name());
        Attribute mark1 = as(middleEval.fields().get(0).child(), Attribute.class);

        // Inner MarkJoin: x → $$m1, right=sub1
        MarkJoin innerJoin = as(middleEval.child(), MarkJoin.class);
        assertEquals("x", innerJoin.config().leftFields().get(0).name());
        assertEquals(mark1.id(), innerJoin.markAttribute().id());
        assertEquals("sub1", as(innerJoin.right(), UnresolvedRelation.class).indexPattern().indexPattern());
        assertEquals("main", as(innerJoin.left(), UnresolvedRelation.class).indexPattern().indexPattern());
    }

    /**
     * {@code FROM main | EVAL z = x IN (FROM sub) | WHERE z}: mark feeds a later WHERE —
     * the Filter references the boolean column produced by the EVAL.
     */
    public void testInSubqueryInEvalReferencedInWhere() {
        LogicalPlan plan = resolve("FROM main | EVAL z = x IN (FROM sub) | WHERE z");
        Filter filter = as(plan, Filter.class);
        Eval eval = as(filter.child(), Eval.class);
        assertEquals("z", eval.fields().get(0).name());
        MarkJoin join = as(eval.child(), MarkJoin.class);
        UnresolvedRelation main = as(join.left(), UnresolvedRelation.class);
        assertEquals("main", main.indexPattern().indexPattern());
        UnresolvedRelation sub = as(join.right(), UnresolvedRelation.class);
        assertEquals("sub", sub.indexPattern().indexPattern());
    }

    // ---- positive: IN subquery inside CASE, COALESCE, IS [NOT] NULL in EVAL ----

    /**
     * {@code FROM main | EVAL z = CASE(x IN (FROM sub), "yes", "no")}: CASE wrapping an IN subquery
     * in an EVAL field — the resolver produces a MarkJoin below the Eval and replaces the InSubquery
     * with the mark attribute inside the CASE expression:
     * <pre>
     * Eval[z=CASE($$mark, "yes", "no")]
     *   MarkJoin[x → $$mark, left=main, right=sub]
     *     UnresolvedRelation[main]
     * </pre>
     */
    public void testCaseInSubqueryInEval() {
        LogicalPlan plan = resolve("FROM main | EVAL z = CASE(x IN (FROM sub), \"yes\", \"no\")");
        Eval eval = as(plan, Eval.class);
        assertEquals(1, eval.fields().size());
        assertEquals("z", eval.fields().get(0).name());
        UnresolvedFunction caseExpr = as(eval.fields().get(0).child(), UnresolvedFunction.class);
        assertEquals("CASE", caseExpr.name());
        Attribute mark = as(caseExpr.children().get(0), Attribute.class);
        MarkJoin join = as(eval.child(), MarkJoin.class);
        assertEquals(JoinTypes.MARK, join.config().type());
        assertEquals("x", join.config().leftFields().get(0).name());
        assertEquals(mark.id(), join.markAttribute().id());
        assertEquals("sub", as(join.right(), UnresolvedRelation.class).indexPattern().indexPattern());
        assertEquals("main", as(join.left(), UnresolvedRelation.class).indexPattern().indexPattern());
    }

    /**
     * {@code FROM main | EVAL z = COALESCE(x IN (FROM sub), false)}: COALESCE wrapping an IN subquery
     * in an EVAL field:
     * <pre>
     * Eval[z=COALESCE($$mark, false)]
     *   MarkJoin[x → $$mark, left=main, right=sub]
     *     UnresolvedRelation[main]
     * </pre>
     */
    public void testCoalesceInSubqueryInEval() {
        LogicalPlan plan = resolve("FROM main | EVAL z = COALESCE(x IN (FROM sub), false)");
        Eval eval = as(plan, Eval.class);
        assertEquals(1, eval.fields().size());
        assertEquals("z", eval.fields().get(0).name());
        UnresolvedFunction coalesceExpr = as(eval.fields().get(0).child(), UnresolvedFunction.class);
        assertEquals("COALESCE", coalesceExpr.name());
        Attribute mark = as(coalesceExpr.children().get(0), Attribute.class);
        MarkJoin join = as(eval.child(), MarkJoin.class);
        assertEquals(JoinTypes.MARK, join.config().type());
        assertEquals("x", join.config().leftFields().get(0).name());
        assertEquals(mark.id(), join.markAttribute().id());
        assertEquals("sub", as(join.right(), UnresolvedRelation.class).indexPattern().indexPattern());
        assertEquals("main", as(join.left(), UnresolvedRelation.class).indexPattern().indexPattern());
    }

    /**
     * {@code FROM main | EVAL z = (x IN (FROM sub)) IS NULL}: IS NULL applied to an IN subquery mark
     * in an EVAL field:
     * <pre>
     * Eval[z=IsNull($$mark)]
     *   MarkJoin[x → $$mark, left=main, right=sub]
     *     UnresolvedRelation[main]
     * </pre>
     */
    public void testIsNullInSubqueryInEval() {
        LogicalPlan plan = resolve("FROM main | EVAL z = (x IN (FROM sub)) IS NULL");
        Eval eval = as(plan, Eval.class);
        assertEquals(1, eval.fields().size());
        assertEquals("z", eval.fields().get(0).name());
        IsNull isNull = as(eval.fields().get(0).child(), IsNull.class);
        Attribute mark = as(isNull.field(), Attribute.class);
        MarkJoin join = as(eval.child(), MarkJoin.class);
        assertEquals(JoinTypes.MARK, join.config().type());
        assertEquals("x", join.config().leftFields().get(0).name());
        assertEquals(mark.id(), join.markAttribute().id());
        assertEquals("sub", as(join.right(), UnresolvedRelation.class).indexPattern().indexPattern());
        assertEquals("main", as(join.left(), UnresolvedRelation.class).indexPattern().indexPattern());
    }

    /**
     * {@code FROM main | EVAL z = (x IN (FROM sub)) IS NOT NULL}: IS NOT NULL applied to an IN
     * subquery mark in an EVAL field:
     * <pre>
     * Eval[z=IsNotNull($$mark)]
     *   MarkJoin[x → $$mark, left=main, right=sub]
     *     UnresolvedRelation[main]
     * </pre>
     */
    public void testIsNotNullInSubqueryInEval() {
        LogicalPlan plan = resolve("FROM main | EVAL z = (x IN (FROM sub)) IS NOT NULL");
        Eval eval = as(plan, Eval.class);
        assertEquals(1, eval.fields().size());
        assertEquals("z", eval.fields().get(0).name());
        IsNotNull isNotNull = as(eval.fields().get(0).child(), IsNotNull.class);
        Attribute mark = as(isNotNull.field(), Attribute.class);
        MarkJoin join = as(eval.child(), MarkJoin.class);
        assertEquals(JoinTypes.MARK, join.config().type());
        assertEquals("x", join.config().leftFields().get(0).name());
        assertEquals(mark.id(), join.markAttribute().id());
        assertEquals("sub", as(join.right(), UnresolvedRelation.class).indexPattern().indexPattern());
        assertEquals("main", as(join.left(), UnresolvedRelation.class).indexPattern().indexPattern());
    }

    /**
     * {@code FROM main | EVAL z = x IN (FROM sub1) AND y IN (FROM sub2)}: two IN subqueries connected by AND
     * produce two MarkJoins whose marks replace the original predicates.
     */
    public void testConjunctiveInSubqueriesInEval() {
        LogicalPlan plan = resolve("FROM main | EVAL z = x IN (FROM sub1) AND y IN (FROM sub2)");
        Eval eval = as(plan, Eval.class);
        And and = as(eval.fields().get(0).child(), And.class);

        MarkJoin outerJoin = as(eval.child(), MarkJoin.class);
        Attribute yMark = assertMarkJoin(outerJoin, "y", "sub2");
        MarkJoin innerJoin = as(outerJoin.left(), MarkJoin.class);
        Attribute xMark = assertMarkJoin(innerJoin, "x", "sub1");

        assertEquals(xMark.id(), as(and.left(), Attribute.class).id());
        assertEquals(yMark.id(), as(and.right(), Attribute.class).id());
        assertEquals("main", as(innerJoin.left(), UnresolvedRelation.class).indexPattern().indexPattern());
    }

    /**
     * {@code FROM main | EVAL z = x IN (FROM sub1) OR y IN (FROM sub2)}: two IN subqueries connected by OR
     * produce two MarkJoins whose marks replace the original predicates.
     */
    public void testDisjunctiveInSubqueriesInEval() {
        LogicalPlan plan = resolve("FROM main | EVAL z = x IN (FROM sub1) OR y IN (FROM sub2)");
        Eval eval = as(plan, Eval.class);
        Or or = as(eval.fields().get(0).child(), Or.class);

        MarkJoin outerJoin = as(eval.child(), MarkJoin.class);
        Attribute yMark = assertMarkJoin(outerJoin, "y", "sub2");
        MarkJoin innerJoin = as(outerJoin.left(), MarkJoin.class);
        Attribute xMark = assertMarkJoin(innerJoin, "x", "sub1");

        assertEquals(xMark.id(), as(or.left(), Attribute.class).id());
        assertEquals(yMark.id(), as(or.right(), Attribute.class).id());
        assertEquals("main", as(innerJoin.left(), UnresolvedRelation.class).indexPattern().indexPattern());
    }

    /**
     * {@code FROM main | EVAL z = x IN (FROM sub) OR x IN (FROM sub)}: the same {@link InSubquery} predicate appearing twice in one
     * EVAL field produces two MarkJoins with distinct mark attribute names. Mirrors
     * {@link #testDuplicateInSubqueryInOrGetsDistinctMarkNames} for the EVAL context.
     */
    public void testDuplicateInSubqueryInEvalGetsDistinctMarkNames() {
        LogicalPlan plan = resolve("FROM main | EVAL z = x IN (FROM sub) OR x IN (FROM sub)");
        Eval eval = as(plan, Eval.class);
        Or or = as(eval.fields().get(0).child(), Or.class);
        Attribute leftMark = as(or.left(), Attribute.class);
        Attribute rightMark = as(or.right(), Attribute.class);
        assertNotEquals(leftMark.name(), rightMark.name());
        MarkJoin outerJoin = as(eval.child(), MarkJoin.class);
        assertEquals(rightMark.id(), outerJoin.markAttribute().id());
        MarkJoin innerJoin = as(outerJoin.left(), MarkJoin.class);
        assertEquals(leftMark.id(), innerJoin.markAttribute().id());
        assertEquals("main", as(innerJoin.left(), UnresolvedRelation.class).indexPattern().indexPattern());
    }

    /**
     * {@code FROM main | EVAL a = x IN (FROM sub), b = x IN (FROM sub)}: two fields with identical IN subquery expressions must receive
     * distinct mark attribute names and NameIds — otherwise downstream name-based resolution would silently return the wrong boolean for
     * one of the fields.
     * This could be a potential optimization in the future, but for now we require distinct names to avoid confusion.
     */
    public void testTwoEvalFieldsWithIdenticalInSubqueryGetDistinctMarkNames() {
        LogicalPlan plan = resolve("FROM main | EVAL a = x IN (FROM sub), b = x IN (FROM sub)");

        Eval eval = as(plan, Eval.class);
        assertEquals(2, eval.fields().size());
        Attribute markA = as(eval.fields().get(0).child(), Attribute.class);
        Attribute markB = as(eval.fields().get(1).child(), Attribute.class);

        assertNotEquals("mark names must be distinct", markA.name(), markB.name());
        assertNotEquals("mark NameIds must be distinct", markA.id(), markB.id());

        // Two stacked MarkJoins: b's join is outermost (stacked last)
        MarkJoin joinB = as(eval.child(), MarkJoin.class);
        assertEquals(markB.id(), joinB.markAttribute().id());
        MarkJoin joinA = as(joinB.left(), MarkJoin.class);
        assertEquals(markA.id(), joinA.markAttribute().id());
    }

    /**
     * {@code FROM main | EVAL a = 42 IN (FROM sub), b = 42 IN (FROM sub)}: two fields with identical foldable LHS constants and identical
     * subqueries must receive distinct synthetic const alias names, so the join key for each MarkJoin resolves to its own materialized
     * constant column.
     */
    public void testTwoEvalFieldsWithIdenticalConstInSubqueryGetDistinctConstNames() {
        LogicalPlan plan = resolve("FROM main | EVAL a = 42 IN (FROM sub), b = 42 IN (FROM sub)");

        Eval eval = as(plan, Eval.class);
        assertEquals(2, eval.fields().size());
        Attribute markA = as(eval.fields().get(0).child(), Attribute.class);
        Attribute markB = as(eval.fields().get(1).child(), Attribute.class);

        assertNotEquals("mark names must be distinct", markA.name(), markB.name());
        assertNotEquals("mark NameIds must be distinct", markA.id(), markB.id());

        // Each MarkJoin has its own const Eval below it — the join key names must differ
        MarkJoin joinB = as(eval.child(), MarkJoin.class);
        assertEquals(markB.id(), joinB.markAttribute().id());
        Eval constEvalB = as(joinB.left(), Eval.class);

        MarkJoin joinA = as(constEvalB.child(), MarkJoin.class);
        assertEquals(markA.id(), joinA.markAttribute().id());
        Eval constEvalA = as(joinA.left(), Eval.class);

        String constNameA = constEvalA.fields().get(0).name();
        String constNameB = constEvalB.fields().get(0).name();
        assertNotEquals("const alias names must be distinct", constNameA, constNameB);
        assertNotNull(joinA.left()); // sanity: main relation is below
    }

    /**
     * {@code FROM main | EVAL a = x IN (FROM sub), b = 42 IN (FROM sub)}: two fields in one EVAL
     * with different LHS kinds — attribute and foldable constant. Field {@code a} uses an attribute LHS
     * (no synthetic const alias), field {@code b} uses a foldable constant (materialised as a synthetic
     * const alias). Both must receive distinct mark attribute names and NameIds:
     * <pre>
     * Eval[a=$$markA, b=$$markB]
     *   MarkJoin_b[left=$$const, right=sub]
     *     Eval[$$const=42]
     *       MarkJoin_a[left=x, right=sub]
     *         FROM main
     * </pre>
     */
    public void testEvalWithMixedLhsInSubqueries() {
        LogicalPlan plan = resolve("FROM main | EVAL a = x IN (FROM sub), b = 42 IN (FROM sub)");

        Eval eval = as(plan, Eval.class);
        assertEquals(2, eval.fields().size());
        assertEquals("a", eval.fields().get(0).name());
        assertEquals("b", eval.fields().get(1).name());
        Attribute markA = as(eval.fields().get(0).child(), Attribute.class);
        Attribute markB = as(eval.fields().get(1).child(), Attribute.class);
        assertNotEquals("mark names must be distinct", markA.name(), markB.name());
        assertNotEquals("mark NameIds must be distinct", markA.id(), markB.id());

        // b's MarkJoin is outermost (stacked after a's)
        MarkJoin joinB = as(eval.child(), MarkJoin.class);
        assertEquals(markB.id(), joinB.markAttribute().id());

        // const Eval materialises 42 below b's MarkJoin
        Eval constEval = as(joinB.left(), Eval.class);
        assertEquals(1, constEval.fields().size());
        assertTrue(constEval.fields().get(0).name().startsWith("$$in_subquery_const"));
        assertEquals("sub", as(joinB.right(), UnresolvedRelation.class).indexPattern().indexPattern());

        // a's MarkJoin is below the const Eval
        MarkJoin joinA = as(constEval.child(), MarkJoin.class);
        assertEquals(markA.id(), joinA.markAttribute().id());
        assertEquals("x", joinA.config().leftFields().get(0).name());
        as(joinA.left(), UnresolvedRelation.class); // FROM main
        assertEquals("sub", as(joinA.right(), UnresolvedRelation.class).indexPattern().indexPattern());
    }

    /**
     * two chained EVAL commands each containing multiple IN subqueries. The two {@link Eval} nodes are
     * processed independently by {@code transformUp}, so each gets its own fresh accumulator. Within each
     * EVAL the shared accumulator guarantees globally-unique ordinals. All five mark attributes must have
     * distinct names and NameIds; the two const aliases inside the second EVAL must also be distinct:
     * <pre>
     * Eval[c=$$markC, d=$$markD, e=$$markE]
     *   MarkJoin_e[left=x, right=sub]
     *     Eval[$$constD=42]
     *       MarkJoin_d[left=$$constD, right=sub]
     *         Eval[$$constC=42]
     *           MarkJoin_c[left=$$constC, right=sub]
     *             Eval[a=$$markA, b=$$markB]
     *               MarkJoin_b[left=x, right=sub]
     *                 MarkJoin_a[left=x, right=sub]
     *                   FROM main
     * </pre>
     */
    public void testChainedEvalsWithMultipleInSubqueriesGetDistinctMarkNames() {
        LogicalPlan plan = resolve("""
            FROM main
            | EVAL a = x IN (FROM sub), b = x IN (FROM sub)
            | EVAL c = 42 IN (FROM sub), d = 42 IN (FROM sub), e = x IN (FROM sub)
            """);

        // Outer EVAL for c, d, e
        Eval outerEval = as(plan, Eval.class);
        assertEquals(3, outerEval.fields().size());
        assertEquals("c", outerEval.fields().get(0).name());
        assertEquals("d", outerEval.fields().get(1).name());
        assertEquals("e", outerEval.fields().get(2).name());
        Attribute markC = as(outerEval.fields().get(0).child(), Attribute.class);
        Attribute markD = as(outerEval.fields().get(1).child(), Attribute.class);
        Attribute markE = as(outerEval.fields().get(2).child(), Attribute.class);

        // e's MarkJoin is outermost — no const Eval needed because e's LHS is an attribute
        MarkJoin joinE = as(outerEval.child(), MarkJoin.class);
        assertEquals(markE.id(), joinE.markAttribute().id());
        assertEquals("x", joinE.config().leftFields().get(0).name());

        // d's MarkJoin is directly below e's (const Eval is below d's MarkJoin, not above)
        MarkJoin joinD = as(joinE.left(), MarkJoin.class);
        assertEquals(markD.id(), joinD.markAttribute().id());

        // const Eval for d is below d's MarkJoin
        Eval constEvalD = as(joinD.left(), Eval.class);
        assertEquals(1, constEvalD.fields().size());
        String constNameD = constEvalD.fields().get(0).name();

        // c's MarkJoin is below d's const Eval
        MarkJoin joinC = as(constEvalD.child(), MarkJoin.class);
        assertEquals(markC.id(), joinC.markAttribute().id());

        // const Eval for c is below c's MarkJoin
        Eval constEvalC = as(joinC.left(), Eval.class);
        assertEquals(1, constEvalC.fields().size());
        String constNameC = constEvalC.fields().get(0).name();
        assertNotEquals("const names for c and d must be distinct", constNameC, constNameD);

        // Inner EVAL for a and b
        Eval innerEval = as(constEvalC.child(), Eval.class);
        assertEquals(2, innerEval.fields().size());
        assertEquals("a", innerEval.fields().get(0).name());
        assertEquals("b", innerEval.fields().get(1).name());
        Attribute markA = as(innerEval.fields().get(0).child(), Attribute.class);
        Attribute markB = as(innerEval.fields().get(1).child(), Attribute.class);

        MarkJoin joinB = as(innerEval.child(), MarkJoin.class);
        assertEquals(markB.id(), joinB.markAttribute().id());
        MarkJoin joinA = as(joinB.left(), MarkJoin.class);
        assertEquals(markA.id(), joinA.markAttribute().id());
        as(joinA.left(), UnresolvedRelation.class); // FROM main

        // All five marks have distinct names and NameIds
        Set<String> markNames = new HashSet<>();
        Set<NameId> markIds = new HashSet<>();
        for (Attribute mark : new Attribute[] { markA, markB, markC, markD, markE }) {
            assertTrue("mark name must be unique: " + mark.name(), markNames.add(mark.name()));
            assertTrue("mark NameId must be unique: " + mark.id(), markIds.add(mark.id()));
        }
    }

    /**
     * {@code FROM main | EVAL z = x IN (FROM sub) AND y > 0}: an IN subquery connected to a regular predicate by AND
     * produces one MarkJoin and preserves the regular predicate.
     */
    public void testInSubqueryAndGreaterThanInEval() {
        LogicalPlan plan = resolve("FROM main | EVAL z = x IN (FROM sub) AND y > 0");
        Eval eval = as(plan, Eval.class);
        And and = as(eval.fields().get(0).child(), And.class);

        MarkJoin join = as(eval.child(), MarkJoin.class);
        Attribute mark = assertMarkJoin(join, "x", "sub");

        assertEquals(mark.id(), as(and.left(), Attribute.class).id());
        as(and.right(), GreaterThan.class);
        assertEquals("main", as(join.left(), UnresolvedRelation.class).indexPattern().indexPattern());
    }

    /**
     * {@code FROM main | EVAL z = x IN (FROM sub) OR y > 0}: an IN subquery connected to a regular predicate by OR
     * produces one MarkJoin and preserves the regular predicate.
     */
    public void testInSubqueryOrGreaterThanInEval() {
        LogicalPlan plan = resolve("FROM main | EVAL z = x IN (FROM sub) OR y > 0");
        Eval eval = as(plan, Eval.class);
        Or or = as(eval.fields().get(0).child(), Or.class);

        MarkJoin join = as(eval.child(), MarkJoin.class);
        Attribute mark = assertMarkJoin(join, "x", "sub");

        assertEquals(mark.id(), as(or.left(), Attribute.class).id());
        as(or.right(), GreaterThan.class);
        assertEquals("main", as(join.left(), UnresolvedRelation.class).indexPattern().indexPattern());
    }

    /**
     * {@code FROM main | EVAL z = CASE(x IN (FROM sub1) AND y IN (FROM sub2), true, false)}:
     * CASE can wrap a compound expression containing two IN subqueries.
     */
    public void testConjunctiveInSubqueriesInsideCaseInEval() {
        LogicalPlan plan = resolve("FROM main | EVAL z = CASE(x IN (FROM sub1) AND y IN (FROM sub2), true, false)");
        Eval eval = as(plan, Eval.class);
        UnresolvedFunction caseExpression = as(eval.fields().get(0).child(), UnresolvedFunction.class);
        assertEquals("CASE", caseExpression.name());
        And and = as(caseExpression.children().get(0), And.class);

        MarkJoin outerJoin = as(eval.child(), MarkJoin.class);
        Attribute yMark = assertMarkJoin(outerJoin, "y", "sub2");
        MarkJoin innerJoin = as(outerJoin.left(), MarkJoin.class);
        Attribute xMark = assertMarkJoin(innerJoin, "x", "sub1");

        assertEquals(xMark.id(), as(and.left(), Attribute.class).id());
        assertEquals(yMark.id(), as(and.right(), Attribute.class).id());
        assertEquals("main", as(innerJoin.left(), UnresolvedRelation.class).indexPattern().indexPattern());
    }

    /**
     * {@code FROM main | EVAL z = COALESCE(x IN (FROM sub1) OR y IN (FROM sub2), false)}:
     * COALESCE can wrap a compound expression containing two IN subqueries.
     */
    public void testDisjunctiveInSubqueriesInsideCoalesceInEval() {
        LogicalPlan plan = resolve("FROM main | EVAL z = COALESCE(x IN (FROM sub1) OR y IN (FROM sub2), false)");
        Eval eval = as(plan, Eval.class);
        UnresolvedFunction coalesceExpression = as(eval.fields().get(0).child(), UnresolvedFunction.class);
        assertEquals("COALESCE", coalesceExpression.name());
        Or or = as(coalesceExpression.children().get(0), Or.class);

        MarkJoin outerJoin = as(eval.child(), MarkJoin.class);
        Attribute yMark = assertMarkJoin(outerJoin, "y", "sub2");
        MarkJoin innerJoin = as(outerJoin.left(), MarkJoin.class);
        Attribute xMark = assertMarkJoin(innerJoin, "x", "sub1");

        assertEquals(xMark.id(), as(or.left(), Attribute.class).id());
        assertEquals(yMark.id(), as(or.right(), Attribute.class).id());
        assertEquals("main", as(innerJoin.left(), UnresolvedRelation.class).indexPattern().indexPattern());
    }

    /**
     * {@code FROM main | EVAL z = (x IN (FROM sub) AND y > 0) IS NULL}:
     * IS NULL can wrap a compound expression containing an IN subquery and a regular predicate.
     */
    public void testCompoundInSubqueryInsideIsNullInEval() {
        LogicalPlan plan = resolve("FROM main | EVAL z = (x IN (FROM sub) AND y > 0) IS NULL");
        Eval eval = as(plan, Eval.class);
        IsNull isNull = as(eval.fields().get(0).child(), IsNull.class);
        And and = as(isNull.field(), And.class);

        MarkJoin join = as(eval.child(), MarkJoin.class);
        Attribute mark = assertMarkJoin(join, "x", "sub");

        assertEquals(mark.id(), as(and.left(), Attribute.class).id());
        as(and.right(), GreaterThan.class);
        assertEquals("main", as(join.left(), UnresolvedRelation.class).indexPattern().indexPattern());
    }

    /**
     * {@code FROM main | EVAL z = (x IN (FROM sub) OR y > 0) IS NOT NULL}:
     * IS NOT NULL can wrap a compound expression containing an IN subquery and a regular predicate.
     */
    public void testCompoundInSubqueryInsideIsNotNullInEval() {
        LogicalPlan plan = resolve("FROM main | EVAL z = (x IN (FROM sub) OR y > 0) IS NOT NULL");
        Eval eval = as(plan, Eval.class);
        IsNotNull isNotNull = as(eval.fields().get(0).child(), IsNotNull.class);
        Or or = as(isNotNull.field(), Or.class);

        MarkJoin join = as(eval.child(), MarkJoin.class);
        Attribute mark = assertMarkJoin(join, "x", "sub");

        assertEquals(mark.id(), as(or.left(), Attribute.class).id());
        as(or.right(), GreaterThan.class);
        assertEquals("main", as(join.left(), UnresolvedRelation.class).indexPattern().indexPattern());
    }

    // ---- positive: IN subquery with TS, ROW source in EVAL ----

    /**
     * {@code FROM main | EVAL z = x IN (TS sub)}
     */
    public void testInTsSubqueryInEval() {
        LogicalPlan plan = resolve("FROM main | EVAL z = x IN (TS sub)");
        Eval eval = as(plan, Eval.class);
        Attribute mark = as(eval.fields().get(0).child(), Attribute.class);
        MarkJoin join = as(eval.child(), MarkJoin.class);
        assertEquals("x", join.config().leftFields().get(0).name());
        assertEquals(mark.id(), join.markAttribute().id());
        UnresolvedRelation sub = as(join.right(), UnresolvedRelation.class);
        assertEquals("sub", sub.indexPattern().indexPattern());
    }

    /**
     * {@code FROM main | EVAL z = (f1, f2) IN (TS sub | KEEP f1, f2)}
     */
    public void testMultiColumnInTsSubqueryInEval() {
        checkMultiColumnInSubquery();
        LogicalPlan plan = resolve("FROM main | EVAL z = (f1, f2) IN (TS sub | KEEP f1, f2)");
        Eval eval = as(plan, Eval.class);
        Attribute mark = as(eval.fields().get(0).child(), Attribute.class);
        MarkJoin join = as(eval.child(), MarkJoin.class);
        assertEquals(2, join.config().leftFields().size());
        assertEquals("f1", join.config().leftFields().get(0).name());
        assertEquals("f2", join.config().leftFields().get(1).name());
        assertEquals(mark.id(), join.markAttribute().id());
        Keep keep = as(join.right(), Keep.class);
        UnresolvedRelation sub = as(keep.child(), UnresolvedRelation.class);
        assertEquals("sub", sub.indexPattern().indexPattern());
    }

    /**
     * {@code FROM main | EVAL z = x IN (ROW a = 1)}
     */
    public void testInRowSubqueryInEval() {
        LogicalPlan plan = resolve("FROM main | EVAL z = x IN (ROW a = 1)");
        Eval eval = as(plan, Eval.class);
        Attribute mark = as(eval.fields().get(0).child(), Attribute.class);
        MarkJoin join = as(eval.child(), MarkJoin.class);
        assertEquals("x", join.config().leftFields().get(0).name());
        assertEquals(mark.id(), join.markAttribute().id());
        as(join.right(), Row.class);
    }

    /**
     * {@code FROM main | EVAL z = (f1, f2) IN (ROW f1 = 1, f2 = 2)}
     */
    public void testMultiColumnInRowSubqueryInEval() {
        checkMultiColumnInSubquery();
        LogicalPlan plan = resolve("FROM main | EVAL z = (f1, f2) IN (ROW f1 = 1, f2 = 2)");
        Eval eval = as(plan, Eval.class);
        Attribute mark = as(eval.fields().get(0).child(), Attribute.class);
        MarkJoin join = as(eval.child(), MarkJoin.class);
        assertEquals(2, join.config().leftFields().size());
        assertEquals("f1", join.config().leftFields().get(0).name());
        assertEquals("f2", join.config().leftFields().get(1).name());
        assertEquals(mark.id(), join.markAttribute().id());
        as(join.right(), Row.class);
    }

    /**
     * {@code FROM main | EVAL z = CASE((f1, f2) IN (FROM sub | KEEP f1, f2), true, false)}
     */
    public void testMultiColumnInSubqueryNestedInCaseInEval() {
        checkMultiColumnInSubquery();
        LogicalPlan plan = resolve("FROM main | EVAL z = CASE((f1, f2) IN (FROM sub | KEEP f1, f2), true, false)");
        Eval eval = as(plan, Eval.class);
        UnresolvedFunction caseExpr = as(eval.fields().get(0).child(), UnresolvedFunction.class);
        assertEquals("CASE", caseExpr.name());
        Attribute mark = as(caseExpr.children().get(0), Attribute.class);
        MarkJoin join = as(eval.child(), MarkJoin.class);
        assertEquals(2, join.config().leftFields().size());
        assertEquals(mark.id(), join.markAttribute().id());
        assertEquals("sub", as(as(join.right(), Keep.class).child(), UnresolvedRelation.class).indexPattern().indexPattern());
    }

    /**
     * {@code FROM main | EVAL z = COALESCE((f1, f2) IN (FROM sub | KEEP f1, f2), false)}
     */
    public void testMultiColumnInSubqueryNestedInCoalesceInEval() {
        checkMultiColumnInSubquery();
        LogicalPlan plan = resolve("FROM main | EVAL z = COALESCE((f1, f2) IN (FROM sub | KEEP f1, f2), false)");
        Eval eval = as(plan, Eval.class);
        UnresolvedFunction coalesceExpr = as(eval.fields().get(0).child(), UnresolvedFunction.class);
        assertEquals("COALESCE", coalesceExpr.name());
        Attribute mark = as(coalesceExpr.children().get(0), Attribute.class);
        MarkJoin join = as(eval.child(), MarkJoin.class);
        assertEquals(2, join.config().leftFields().size());
        assertEquals(mark.id(), join.markAttribute().id());
        assertEquals("sub", as(as(join.right(), Keep.class).child(), UnresolvedRelation.class).indexPattern().indexPattern());
    }

    /**
     * {@code FROM main | EVAL z = ((f1, f2) IN (FROM sub | KEEP f1, f2)) IS NULL}
     */
    public void testMultiColumnInSubqueryNestedInIsNullInEval() {
        checkMultiColumnInSubquery();
        LogicalPlan plan = resolve("FROM main | EVAL z = ((f1, f2) IN (FROM sub | KEEP f1, f2)) IS NULL");
        Eval eval = as(plan, Eval.class);
        IsNull isNull = as(eval.fields().get(0).child(), IsNull.class);
        Attribute mark = as(isNull.field(), Attribute.class);
        MarkJoin join = as(eval.child(), MarkJoin.class);
        assertEquals(2, join.config().leftFields().size());
        assertEquals(mark.id(), join.markAttribute().id());
        assertEquals("sub", as(as(join.right(), Keep.class).child(), UnresolvedRelation.class).indexPattern().indexPattern());
    }

    /**
     * {@code FROM main | EVAL z = ((f1, f2) IN (FROM sub | KEEP f1, f2)) IS NOT NULL}
     */
    public void testMultiColumnInSubqueryNestedInIsNotNullInEval() {
        checkMultiColumnInSubquery();
        LogicalPlan plan = resolve("FROM main | EVAL z = ((f1, f2) IN (FROM sub | KEEP f1, f2)) IS NOT NULL");
        Eval eval = as(plan, Eval.class);
        IsNotNull isNotNull = as(eval.fields().get(0).child(), IsNotNull.class);
        Attribute mark = as(isNotNull.field(), Attribute.class);
        MarkJoin join = as(eval.child(), MarkJoin.class);
        assertEquals(2, join.config().leftFields().size());
        assertEquals(mark.id(), join.markAttribute().id());
        assertEquals("sub", as(as(join.right(), Keep.class).child(), UnresolvedRelation.class).indexPattern().indexPattern());
    }

    // ---- negative: IN subquery in EVAL (rejected shapes) ----

    /**
     * {@code EVAL z = abs(x) IN (FROM sub)}: complex LHS — "Complicated IN subquery" error.
     */
    public void testRejectsComplexLHSInSubqueryInEval() {
        var e = expectThrows(VerificationException.class, () -> resolve("FROM main | EVAL z = abs(x) IN (FROM sub)"));
        assertThat(e.getMessage(), containsString("Complicated IN subquery is not yet supported in Eval [EVAL z = abs(x) IN (FROM sub)]"));
    }

    /**
     * {@code EVAL z = TO_STRING(x IN (FROM sub))}: non-allowlisted wrapper — "not supported within expression" error.
     */
    public void testRejectsInSubqueryInsideNonAllowlistedFunctionInEval() {
        var e = expectThrows(VerificationException.class, () -> resolve("FROM main | EVAL z = TO_STRING(x IN (FROM sub))"));
        assertThat(e.getMessage(), containsString("IN subquery is not supported within expression [TO_STRING(x IN (FROM sub))]"));
    }

    // ---- positive: IN subquery in INLINE STATS WHERE filter ----
    /**
     * <pre>
     * InlineStats
     *   Aggregate[c = COUNT(*) WHERE $$mark]
     *     MarkJoin[x → $$mark, left=main, right=sub]
     *       UnresolvedRelation[main]
     * </pre>
     */
    public void testInSubqueryInInlineStatsWhere() {
        LogicalPlan plan = resolve("FROM main | INLINE STATS c = COUNT(*) WHERE x IN (FROM sub)");
        InlineStats inlineStats = as(plan, InlineStats.class);
        Aggregate agg = as(inlineStats.child(), Aggregate.class);
        FilteredExpression filtered = as(as(agg.aggregates().get(0), Alias.class).child(), FilteredExpression.class);
        Attribute mark = as(filtered.filter(), Attribute.class);
        MarkJoin mj = as(agg.child(), MarkJoin.class);
        assertEquals(JoinTypes.MARK, mj.config().type());
        assertEquals("x", mj.config().leftFields().get(0).name());
        assertEquals(mark.id(), mj.markAttribute().id());
        assertEquals("main", as(mj.left(), UnresolvedRelation.class).indexPattern().indexPattern());
        assertEquals("sub", as(mj.right(), UnresolvedRelation.class).indexPattern().indexPattern());
    }

    /**
     * {@code INLINE STATS c = COUNT(*) WHERE x NOT IN (FROM sub)}: negation stays as {@code NOT($$mark)}.
     */
    public void testNotInSubqueryInInlineStatsWhere() {
        LogicalPlan plan = resolve("FROM main | INLINE STATS c = COUNT(*) WHERE x NOT IN (FROM sub)");
        InlineStats inlineStats = as(plan, InlineStats.class);
        Aggregate agg = as(inlineStats.child(), Aggregate.class);
        FilteredExpression filtered = as(as(agg.aggregates().get(0), Alias.class).child(), FilteredExpression.class);
        Not not = as(filtered.filter(), Not.class);
        Attribute mark = as(not.field(), Attribute.class);
        MarkJoin mj = as(agg.child(), MarkJoin.class);
        assertEquals(mark.id(), mj.markAttribute().id());
    }

    /**
     * {@code INLINE STATS c = COUNT(*) WHERE x IN (FROM sub) BY g}: groupings untouched.
     */
    public void testInSubqueryInInlineStatsWhereWithGrouping() {
        LogicalPlan plan = resolve("FROM main | INLINE STATS c = COUNT(*) WHERE x IN (FROM sub) BY g");
        InlineStats inlineStats = as(plan, InlineStats.class);
        Aggregate agg = as(inlineStats.child(), Aggregate.class);
        assertEquals(1, agg.groupings().size());
        FilteredExpression filtered = as(as(agg.aggregates().get(0), Alias.class).child(), FilteredExpression.class);
        Attribute mark = as(filtered.filter(), Attribute.class);
        MarkJoin mj = as(agg.child(), MarkJoin.class);
        assertEquals(mark.id(), mj.markAttribute().id());
    }

    /**
     * {@code INLINE STATS c = COUNT(*) WHERE CASE(x IN (FROM sub), true, false)}: CASE wrapper in
     * the INLINE STATS aggregate filter.
     */
    public void testInSubqueryInCaseInInlineStatsWhere() {
        LogicalPlan plan = resolve("FROM main | INLINE STATS c = COUNT(*) WHERE CASE(x IN (FROM sub), true, false)");
        InlineStats inlineStats = as(plan, InlineStats.class);
        Aggregate agg = as(inlineStats.child(), Aggregate.class);
        FilteredExpression filtered = as(as(agg.aggregates().get(0), Alias.class).child(), FilteredExpression.class);
        UnresolvedFunction caseExpr = as(filtered.filter(), UnresolvedFunction.class);
        Attribute mark = as(caseExpr.children().get(0), Attribute.class);
        MarkJoin mj = as(agg.child(), MarkJoin.class);
        assertEquals(mark.id(), mj.markAttribute().id());
    }

    /**
     * {@code WHERE x IN (FROM sub) | INLINE STATS c = COUNT(*) WHERE z IN (FROM sub2)}: both the
     * WHERE-command subquery (SemiJoin) and the INLINE STATS aggregate-filter subquery (MarkJoin)
     * are rewritten in the same pass.
     */
    public void testWhereInSubqueryBeforeInSubqueryInInlineStatsWhere() {
        LogicalPlan plan = resolve("FROM main | WHERE x IN (FROM sub) | INLINE STATS c = COUNT(*) WHERE z IN (FROM sub2)");
        InlineStats inlineStats = as(plan, InlineStats.class);
        Aggregate agg = as(inlineStats.child(), Aggregate.class);
        FilteredExpression filtered = as(as(agg.aggregates().get(0), Alias.class).child(), FilteredExpression.class);
        Attribute mark = as(filtered.filter(), Attribute.class);
        MarkJoin mj = as(agg.child(), MarkJoin.class);
        assertEquals("z", mj.config().leftFields().get(0).name());
        assertEquals(mark.id(), mj.markAttribute().id());
        assertEquals("sub2", as(mj.right(), UnresolvedRelation.class).indexPattern().indexPattern());
        SemiJoin semiJoin = as(mj.left(), SemiJoin.class);
        assertEquals("x", semiJoin.config().leftFields().get(0).name());
        assertEquals("main", as(semiJoin.left(), UnresolvedRelation.class).indexPattern().indexPattern());
        assertEquals("sub", as(semiJoin.right(), UnresolvedRelation.class).indexPattern().indexPattern());
    }

    /**
     * {@code INLINE STATS c1 = COUNT(*) WHERE x IN (FROM sub1), c2 = SUM(y) WHERE z IN (FROM sub2)}: each filtered aggregate gets its
     * own MarkJoin; they stack below the InlineStats-owned Aggregate in reverse encounter order.
     */
    public void testInSubqueryInInlineStatsWhereWithMultipleAggs() {
        LogicalPlan plan = resolve("FROM main | INLINE STATS c1 = COUNT(*) WHERE x IN (FROM sub1), c2 = SUM(y) WHERE z IN (FROM sub2)");
        InlineStats inlineStats = as(plan, InlineStats.class);
        Aggregate agg = as(inlineStats.child(), Aggregate.class);
        Attribute mark1 = as(as(as(agg.aggregates().get(0), Alias.class).child(), FilteredExpression.class).filter(), Attribute.class);
        Attribute mark2 = as(as(as(agg.aggregates().get(1), Alias.class).child(), FilteredExpression.class).filter(), Attribute.class);
        MarkJoin outer = as(agg.child(), MarkJoin.class);
        assertEquals("z", outer.config().leftFields().get(0).name());
        assertEquals("sub2", as(outer.right(), UnresolvedRelation.class).indexPattern().indexPattern());
        assertEquals(mark2.id(), outer.markAttribute().id());
        MarkJoin inner = as(outer.left(), MarkJoin.class);
        assertEquals("x", inner.config().leftFields().get(0).name());
        assertEquals("sub1", as(inner.right(), UnresolvedRelation.class).indexPattern().indexPattern());
        assertEquals(mark1.id(), inner.markAttribute().id());
        assertEquals("main", as(inner.left(), UnresolvedRelation.class).indexPattern().indexPattern());
    }

    /**
     * Two INLINE STATS aggregates filtered by the textually identical IN subquery: each still gets
     * its own MarkJoin and mark attribute with distinct {@link NameId}s.
     */
    public void testInSubqueryInInlineStatsWhereWithTwoAggsSameSubquery() {
        LogicalPlan plan = resolve("FROM main | INLINE STATS c1 = COUNT(*) WHERE x IN (FROM sub), c2 = SUM(y) WHERE x IN (FROM sub)");
        InlineStats inlineStats = as(plan, InlineStats.class);
        Aggregate agg = as(inlineStats.child(), Aggregate.class);
        Attribute mark1 = as(as(as(agg.aggregates().get(0), Alias.class).child(), FilteredExpression.class).filter(), Attribute.class);
        Attribute mark2 = as(as(as(agg.aggregates().get(1), Alias.class).child(), FilteredExpression.class).filter(), Attribute.class);
        assertNotEquals(mark1.id(), mark2.id());
        MarkJoin outer = as(agg.child(), MarkJoin.class);
        MarkJoin inner = as(outer.left(), MarkJoin.class);
        assertEquals(mark2.id(), outer.markAttribute().id());
        assertEquals(mark1.id(), inner.markAttribute().id());
    }

    /**
     * {@code INLINE STATS c = COUNT(*) WHERE 42 IN (FROM sub)}: a foldable LHS is materialized as
     * a synthetic Eval column below the MarkJoin.
     */
    public void testInSubqueryWithFoldableLHSInInlineStatsWhere() {
        LogicalPlan plan = resolve("FROM main | INLINE STATS c = COUNT(*) WHERE 42 IN (FROM sub)");
        InlineStats inlineStats = as(plan, InlineStats.class);
        Aggregate agg = as(inlineStats.child(), Aggregate.class);
        MarkJoin mj = as(agg.child(), MarkJoin.class);
        assertTrue(mj.config().leftFields().get(0).name().startsWith("$$in_subquery_const$"));
        Eval eval = as(mj.left(), Eval.class);
        assertEquals(1, eval.fields().size());
        Literal constant = as(eval.fields().get(0).child(), Literal.class);
        assertEquals(42, constant.value());
        as(eval.child(), UnresolvedRelation.class);
    }

    /**
     * {@code INLINE STATS c = COUNT(*) WHERE x IN (FROM sub) AND y > 1}: the IN subquery is one
     * conjunct of the aggregate filter; the rest of the condition is preserved around the mark.
     */
    public void testInSubqueryAndGreaterThanInInlineStatsWhere() {
        LogicalPlan plan = resolve("FROM main | INLINE STATS c = COUNT(*) WHERE x IN (FROM sub) AND y > 1");
        InlineStats inlineStats = as(plan, InlineStats.class);
        Aggregate agg = as(inlineStats.child(), Aggregate.class);
        FilteredExpression filtered = as(as(agg.aggregates().get(0), Alias.class).child(), FilteredExpression.class);
        And and = as(filtered.filter(), And.class);
        Attribute mark = as(and.left(), Attribute.class);
        as(and.right(), GreaterThan.class);
        MarkJoin mj = as(agg.child(), MarkJoin.class);
        assertEquals(mark.id(), mj.markAttribute().id());
    }

    /**
     * {@code INLINE STATS c = COUNT(*) WHERE x IN (FROM sub1) OR y IN (FROM sub2)}: two disjunctive
     * IN subqueries in the aggregate filter, each rewritten to its own MarkJoin.
     */
    public void testDisjunctiveInSubqueryInInlineStatsWhere() {
        LogicalPlan plan = resolve("FROM main | INLINE STATS c = COUNT(*) WHERE x IN (FROM sub1) OR y IN (FROM sub2)");
        InlineStats inlineStats = as(plan, InlineStats.class);
        Aggregate agg = as(inlineStats.child(), Aggregate.class);
        FilteredExpression filtered = as(as(agg.aggregates().get(0), Alias.class).child(), FilteredExpression.class);
        Or or = as(filtered.filter(), Or.class);
        Attribute mark1 = as(or.left(), Attribute.class);
        Attribute mark2 = as(or.right(), Attribute.class);
        MarkJoin outer = as(agg.child(), MarkJoin.class);
        assertEquals("y", outer.config().leftFields().get(0).name());
        assertEquals(mark2.id(), outer.markAttribute().id());
        MarkJoin inner = as(outer.left(), MarkJoin.class);
        assertEquals("x", inner.config().leftFields().get(0).name());
        assertEquals(mark1.id(), inner.markAttribute().id());
    }

    /**
     * {@code INLINE STATS c = COUNT(*) WHERE COALESCE(x IN (FROM sub), false)}: IN subquery inside
     * a COALESCE argument of the INLINE STATS aggregate filter.
     */
    public void testInSubqueryInCoalesceInInlineStatsWhere() {
        LogicalPlan plan = resolve("FROM main | INLINE STATS c = COUNT(*) WHERE COALESCE(x IN (FROM sub), false)");
        InlineStats inlineStats = as(plan, InlineStats.class);
        Aggregate agg = as(inlineStats.child(), Aggregate.class);
        FilteredExpression filtered = as(as(agg.aggregates().get(0), Alias.class).child(), FilteredExpression.class);
        UnresolvedFunction coalesce = as(filtered.filter(), UnresolvedFunction.class);
        Attribute mark = as(coalesce.children().get(0), Attribute.class);
        MarkJoin mj = as(agg.child(), MarkJoin.class);
        assertEquals(mark.id(), mj.markAttribute().id());
    }

    /**
     * {@code INLINE STATS c = COUNT(*) WHERE (x IN (FROM sub)) IS NULL}: IN subquery under IS NULL
     * in the INLINE STATS aggregate filter.
     */
    public void testInSubqueryInIsNullInInlineStatsWhere() {
        LogicalPlan plan = resolve("FROM main | INLINE STATS c = COUNT(*) WHERE (x IN (FROM sub)) IS NULL");
        InlineStats inlineStats = as(plan, InlineStats.class);
        Aggregate agg = as(inlineStats.child(), Aggregate.class);
        FilteredExpression filtered = as(as(agg.aggregates().get(0), Alias.class).child(), FilteredExpression.class);
        IsNull isNull = as(filtered.filter(), IsNull.class);
        Attribute mark = as(isNull.field(), Attribute.class);
        MarkJoin mj = as(agg.child(), MarkJoin.class);
        assertEquals(mark.id(), mj.markAttribute().id());
    }

    /**
     * {@code INLINE STATS c = COUNT(*) WHERE (x IN (FROM sub)) IS NOT NULL}: IN subquery under
     * IS NOT NULL in the INLINE STATS aggregate filter.
     */
    public void testInSubqueryInIsNotNullInInlineStatsWhere() {
        LogicalPlan plan = resolve("FROM main | INLINE STATS c = COUNT(*) WHERE (x IN (FROM sub)) IS NOT NULL");
        InlineStats inlineStats = as(plan, InlineStats.class);
        Aggregate agg = as(inlineStats.child(), Aggregate.class);
        FilteredExpression filtered = as(as(agg.aggregates().get(0), Alias.class).child(), FilteredExpression.class);
        IsNotNull isNotNull = as(filtered.filter(), IsNotNull.class);
        Attribute mark = as(isNotNull.field(), Attribute.class);
        MarkJoin mj = as(agg.child(), MarkJoin.class);
        assertEquals(mark.id(), mj.markAttribute().id());
    }

    // ---- positive: IN subquery in INLINE STATS WHERE filter with ROW and TS sources ----

    /**
     * {@code INLINE STATS c = COUNT(*) WHERE x IN (ROW a = 1 | KEEP a)}
     */
    public void testInSubqueryInInlineStatsWhereWithRow() {
        LogicalPlan plan = resolve("FROM main | INLINE STATS c = COUNT(*) WHERE x IN (ROW a = 1 | KEEP a)");
        InlineStats inlineStats = as(plan, InlineStats.class);
        Aggregate agg = as(inlineStats.child(), Aggregate.class);
        FilteredExpression filtered = as(as(agg.aggregates().get(0), Alias.class).child(), FilteredExpression.class);
        Attribute mark = as(filtered.filter(), Attribute.class);
        MarkJoin mj = as(agg.child(), MarkJoin.class);
        assertEquals("x", mj.config().leftFields().get(0).name());
        assertEquals(mark.id(), mj.markAttribute().id());
        Keep keep = as(mj.right(), Keep.class);
        as(keep.child(), Row.class);
    }

    /**
     * {@code INLINE STATS c = COUNT(*) WHERE (x, y) IN (TS sub | STATS r = rate(foo) BY bar | KEEP r, bar)}
     */
    public void testMultiColumnInSubqueryInInlineStatsWhereWithTs() {
        checkMultiColumnInSubquery();
        LogicalPlan plan = resolve("""
            FROM main
            | INLINE STATS c = COUNT(*) WHERE (x, y) IN (TS sub | STATS r = rate(foo) BY bar | KEEP r, bar)""");
        InlineStats inlineStats = as(plan, InlineStats.class);
        Aggregate agg = as(inlineStats.child(), Aggregate.class);
        FilteredExpression filtered = as(as(agg.aggregates().get(0), Alias.class).child(), FilteredExpression.class);
        Attribute mark = as(filtered.filter(), Attribute.class);
        MarkJoin mj = as(agg.child(), MarkJoin.class);
        assertEquals(2, mj.config().leftFields().size());
        assertEquals("x", mj.config().leftFields().get(0).name());
        assertEquals("y", mj.config().leftFields().get(1).name());
        assertEquals(mark.id(), mj.markAttribute().id());

        Keep keep = as(mj.right(), Keep.class);
        Aggregate subAgg = as(keep.child(), Aggregate.class);
        as(subAgg.child(), UnresolvedRelation.class);
    }

    /**
     * {@code INLINE STATS c = COUNT(*) WHERE CASE(x IN (ROW a = 1 | KEEP a), true, false)}
     */
    public void testInSubqueryInCaseInInlineStatsWhereWithRow() {
        LogicalPlan plan = resolve("FROM main | INLINE STATS c = COUNT(*) WHERE CASE(x IN (ROW a = 1 | KEEP a), true, false)");
        InlineStats inlineStats = as(plan, InlineStats.class);
        Aggregate agg = as(inlineStats.child(), Aggregate.class);
        FilteredExpression filtered = as(as(agg.aggregates().get(0), Alias.class).child(), FilteredExpression.class);
        UnresolvedFunction caseExpr = as(filtered.filter(), UnresolvedFunction.class);
        assertEquals("CASE", caseExpr.name());
        Attribute mark = as(caseExpr.children().get(0), Attribute.class);
        MarkJoin mj = as(agg.child(), MarkJoin.class);
        assertEquals(mark.id(), mj.markAttribute().id());
        as(as(mj.right(), Keep.class).child(), Row.class);
    }

    /**
     * {@code INLINE STATS c = COUNT(*) WHERE COALESCE(x IN (TS sub | STATS r = avg_over_time(foo) BY bar | KEEP r), false)}
     */
    public void testInSubqueryInCoalesceInInlineStatsWhereWithTs() {
        LogicalPlan plan = resolve("""
            FROM main
            | INLINE STATS c = COUNT(*) WHERE COALESCE(x IN (TS sub | STATS r = avg_over_time(foo) BY bar | KEEP r), false)""");
        InlineStats inlineStats = as(plan, InlineStats.class);
        Aggregate agg = as(inlineStats.child(), Aggregate.class);
        FilteredExpression filtered = as(as(agg.aggregates().get(0), Alias.class).child(), FilteredExpression.class);
        UnresolvedFunction coalesce = as(filtered.filter(), UnresolvedFunction.class);
        assertEquals("COALESCE", coalesce.name());
        Attribute mark = as(coalesce.children().get(0), Attribute.class);
        MarkJoin mj = as(agg.child(), MarkJoin.class);
        assertEquals(mark.id(), mj.markAttribute().id());
        Aggregate subAgg = as(as(mj.right(), Keep.class).child(), Aggregate.class);
        as(subAgg.child(), UnresolvedRelation.class);
    }

    /**
     * {@code INLINE STATS c = COUNT(*) WHERE (x IN (ROW a = 1 | KEEP a)) IS NULL}
     */
    public void testInSubqueryInIsNullInInlineStatsWhereWithRow() {
        LogicalPlan plan = resolve("FROM main | INLINE STATS c = COUNT(*) WHERE (x IN (ROW a = 1 | KEEP a)) IS NULL");
        InlineStats inlineStats = as(plan, InlineStats.class);
        Aggregate agg = as(inlineStats.child(), Aggregate.class);
        FilteredExpression filtered = as(as(agg.aggregates().get(0), Alias.class).child(), FilteredExpression.class);
        IsNull isNull = as(filtered.filter(), IsNull.class);
        Attribute mark = as(isNull.field(), Attribute.class);
        MarkJoin mj = as(agg.child(), MarkJoin.class);
        assertEquals(mark.id(), mj.markAttribute().id());
    }

    /**
     * {@code INLINE STATS c = COUNT(*) WHERE (x IN (TS sub | STATS r = rate(foo) BY bar | KEEP r)) IS NOT NULL}
     */
    public void testInSubqueryInIsNotNullInInlineStatsWhereWithTs() {
        LogicalPlan plan = resolve(
            "FROM main | INLINE STATS c = COUNT(*) WHERE (x IN (TS sub | STATS r = rate(foo) BY bar | KEEP r)) IS NOT NULL"
        );
        InlineStats inlineStats = as(plan, InlineStats.class);
        Aggregate agg = as(inlineStats.child(), Aggregate.class);
        FilteredExpression filtered = as(as(agg.aggregates().get(0), Alias.class).child(), FilteredExpression.class);
        IsNotNull isNotNull = as(filtered.filter(), IsNotNull.class);
        Attribute mark = as(isNotNull.field(), Attribute.class);
        MarkJoin mj = as(agg.child(), MarkJoin.class);
        assertEquals(mark.id(), mj.markAttribute().id());
    }

    // negative inline stats aggregate filter tests

    /**
     * A non-attribute, non-foldable LHS in an INLINE STATS aggregate filter is not supported.
     */
    public void testRejectsComplexLHSInSubqueryInInlineStatsWhere() {
        assertResolveError(
            "FROM main | INLINE STATS c = COUNT(*) WHERE abs(x) IN (FROM sub)",
            "line 1:45: Complicated IN subquery is not yet supported in Aggregate "
                + "[INLINE STATS c = COUNT(*) WHERE abs(x) IN (FROM sub)]"
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

    private static Attribute assertMarkJoin(MarkJoin join, String leftField, String rightIndex) {
        assertEquals(JoinTypes.MARK, join.config().type());
        assertEquals(1, join.config().leftFields().size());
        assertEquals(leftField, join.config().leftFields().get(0).name());
        assertTrue(join.config().rightFields().isEmpty());
        assertEquals(rightIndex, as(join.right(), UnresolvedRelation.class).indexPattern().indexPattern());
        return join.markAttribute();
    }

    private static void assertMarkJoinReplacedInSubquery(Filter filter, Expression replacement, String leftField, String subqueryIndex) {
        MarkJoin markJoin = assertSingleMarkJoin(filter, leftField, subqueryIndex);
        assertEquals(markJoin.markAttribute().id(), as(replacement, Attribute.class).id());
    }

    private static MarkJoin assertSingleMarkJoin(Filter filter, String leftField, String subqueryIndex) {
        MarkJoin markJoin = as(filter.child(), MarkJoin.class);
        assertEquals(leftField, markJoin.config().leftFields().get(0).name());
        assertEquals(subqueryIndex, as(markJoin.right(), UnresolvedRelation.class).indexPattern().indexPattern());
        assertEquals("main", as(markJoin.left(), UnresolvedRelation.class).indexPattern().indexPattern());
        assertFalse(filter.condition().anyMatch(e -> e instanceof InSubquery));
        return markJoin;
    }
}
