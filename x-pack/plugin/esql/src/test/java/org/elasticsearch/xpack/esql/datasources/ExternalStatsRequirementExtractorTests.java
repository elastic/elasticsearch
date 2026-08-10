/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.UnresolvedAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThan;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.Fork;
import org.elasticsearch.xpack.esql.plan.logical.InlineStats;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedExternalRelation;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Unit tests for {@link ExternalStatsRequirementExtractor}. The detector marks a path only when an
 * <b>ungrouped</b> aggregate is an ancestor of its {@link UnresolvedExternalRelation}; every other
 * shape (grouped {@code STATS ... BY}, {@code INLINESTATS}, {@code LIMIT}, {@code SELECT *},
 * {@code WHERE}-only) leaves the path absent so the resolver defers its per-file footer reads.
 */
public class ExternalStatsRequirementExtractorTests extends ESTestCase {

    private static final Source SRC = Source.EMPTY;
    private static final String PATH = "s3://bucket/data/*.parquet";

    public void testUngroupedStatsRequiresEagerStats() {
        // ungrouped STATS COUNT(*) -> path present
        LogicalPlan plan = ungroupedAggregate(externalRelation(PATH));

        Set<String> paths = ExternalStatsRequirementExtractor.pathsRequiringEagerStats(plan);
        assertEquals(Set.of(PATH), paths);
    }

    public void testUngroupedStatsAboveIntermediateNodesStillRequiresEagerStats() {
        // ... | WHERE x > 5 | LIMIT 10 | STATS COUNT(*) — the ancestor-anywhere safety bias keeps it eager
        LogicalPlan relation = externalRelation(PATH);
        LogicalPlan filter = new Filter(SRC, relation, new GreaterThan(SRC, unresolved("x"), intLiteral(5)));
        LogicalPlan limit = new Limit(SRC, intLiteral(10), filter);
        LogicalPlan plan = ungroupedAggregate(limit);

        assertEquals(Set.of(PATH), ExternalStatsRequirementExtractor.pathsRequiringEagerStats(plan));
    }

    public void testUngroupedStatsBelowOtherNodesStillRequiresEagerStats() {
        // STATS COUNT(*) | LIMIT 5 — the ungrouped aggregate is not the plan root. The
        // ancestor-anywhere walk still propagates the flag down to the relation. This mirrors what
        // optimization can produce: nodes above the aggregate are irrelevant to detection, only the
        // aggregate-to-relation ancestry matters, and that ancestry is preserved across the
        // prune/pushdown rules that reshape the plan between parsing and the physical fast path.
        LogicalPlan agg = ungroupedAggregate(externalRelation(PATH));
        LogicalPlan plan = new Limit(SRC, intLiteral(5), agg);

        assertEquals(Set.of(PATH), ExternalStatsRequirementExtractor.pathsRequiringEagerStats(plan));
    }

    public void testGroupedStatsDoesNotRequireEagerStats() {
        // STATS COUNT(*) BY g -> grouped aggregate, path absent
        LogicalPlan plan = new Aggregate(SRC, externalRelation(PATH), List.of(unresolved("g")), List.<NamedExpression>of());

        assertTrue(ExternalStatsRequirementExtractor.pathsRequiringEagerStats(plan).isEmpty());
    }

    public void testInlineStatsDoesNotRequireEagerStats() {
        // INLINESTATS embeds an (ungrouped) aggregate as its child, but produces an InlineJoin that
        // never reaches the ungrouped-aggregate metadata fast path, so the path must stay deferred.
        Aggregate embedded = ungroupedAggregate(externalRelation(PATH));
        LogicalPlan plan = new InlineStats(SRC, embedded);

        assertTrue(ExternalStatsRequirementExtractor.pathsRequiringEagerStats(plan).isEmpty());
    }

    public void testLimitOnlyDoesNotRequireEagerStats() {
        LogicalPlan plan = new Limit(SRC, intLiteral(10), externalRelation(PATH));

        assertTrue(ExternalStatsRequirementExtractor.pathsRequiringEagerStats(plan).isEmpty());
    }

    public void testSelectStarDoesNotRequireEagerStats() {
        // A bare relation (SELECT *) has no aggregate ancestor.
        LogicalPlan plan = externalRelation(PATH);

        assertTrue(ExternalStatsRequirementExtractor.pathsRequiringEagerStats(plan).isEmpty());
    }

    public void testWhereOnlyDoesNotRequireEagerStats() {
        LogicalPlan plan = new Filter(SRC, externalRelation(PATH), new GreaterThan(SRC, unresolved("salary"), intLiteral(100)));

        assertTrue(ExternalStatsRequirementExtractor.pathsRequiringEagerStats(plan).isEmpty());
    }

    public void testMixedBranchesUnionMarksPathEager() {
        // Same path under an ungrouped aggregate in one FORK arm and under LIMIT in another: the
        // union marks it eager because the single resolution feeds both branches.
        LogicalPlan aggBranch = ungroupedAggregate(externalRelation(PATH));
        LogicalPlan limitBranch = new Limit(SRC, intLiteral(10), externalRelation(PATH));
        LogicalPlan plan = new Fork(SRC, List.of(aggBranch, limitBranch), List.of());

        assertEquals(Set.of(PATH), ExternalStatsRequirementExtractor.pathsRequiringEagerStats(plan));
    }

    public void testDistinctPathsTrackedIndependently() {
        String aggPath = "s3://bucket/agg/*.parquet";
        String limitPath = "s3://bucket/limit/*.parquet";
        LogicalPlan aggBranch = ungroupedAggregate(externalRelation(aggPath));
        LogicalPlan limitBranch = new Limit(SRC, intLiteral(10), externalRelation(limitPath));
        LogicalPlan plan = new Fork(SRC, List.of(aggBranch, limitBranch), List.of());

        // Only the path under the ungrouped aggregate is marked eager.
        assertEquals(Set.of(aggPath), ExternalStatsRequirementExtractor.pathsRequiringEagerStats(plan));
    }

    public void testNonLiteralTablePathIsSkipped() {
        // Detection never throws on a non-literal tablePath; the path is simply not keyed (resolver
        // stays eager by legacy default for paths it cannot match).
        UnresolvedExternalRelation relation = new UnresolvedExternalRelation(SRC, unresolved("?param"), Map.of());
        LogicalPlan plan = ungroupedAggregate(relation);

        assertTrue(ExternalStatsRequirementExtractor.pathsRequiringEagerStats(plan).isEmpty());
    }

    private static Aggregate ungroupedAggregate(LogicalPlan child) {
        return new Aggregate(SRC, child, List.of(), List.<NamedExpression>of());
    }

    private static UnresolvedExternalRelation externalRelation(String path) {
        return new UnresolvedExternalRelation(SRC, Literal.keyword(SRC, path), Map.of());
    }

    private static UnresolvedAttribute unresolved(String name) {
        return new UnresolvedAttribute(SRC, name);
    }

    private static Expression intLiteral(int value) {
        return new Literal(SRC, value, DataType.INTEGER);
    }
}
