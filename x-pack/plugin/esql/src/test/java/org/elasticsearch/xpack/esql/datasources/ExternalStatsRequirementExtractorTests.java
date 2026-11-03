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
import org.elasticsearch.xpack.esql.plan.logical.Keep;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Sample;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedExternalRelation;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Unit tests for {@link ExternalStatsRequirementExtractor}. The detector marks a path only when an
 * <b>ungrouped</b> aggregate is an ancestor of its {@link UnresolvedExternalRelation} and no
 * {@link Filter}, {@link Limit}, or {@link Sample} sits on that path. {@code KEEP} (and other
 * projection-like nodes) stay non-blocking. Every other shape (grouped {@code STATS ... BY},
 * {@code INLINESTATS}, {@code LIMIT}-only, {@code SELECT *}, {@code WHERE}-only, filtered COUNT)
 * leaves the path absent so the resolver defers its per-file footer reads.
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

    public void testFilterBetweenStatsAndRelationDoesNotRequireEagerStats() {
        // ... | WHERE x > 5 | STATS COUNT(*) — skip-discovery cannot fire; do not harvest every footer
        LogicalPlan filter = new Filter(SRC, externalRelation(PATH), new GreaterThan(SRC, unresolved("x"), intLiteral(5)));
        LogicalPlan plan = ungroupedAggregate(filter);

        assertTrue(ExternalStatsRequirementExtractor.pathsRequiringEagerStats(plan).isEmpty());
    }

    public void testLimitBetweenStatsAndRelationDoesNotRequireEagerStats() {
        // ... | LIMIT 10 | STATS COUNT(*)
        LogicalPlan limit = new Limit(SRC, intLiteral(10), externalRelation(PATH));
        LogicalPlan plan = ungroupedAggregate(limit);

        assertTrue(ExternalStatsRequirementExtractor.pathsRequiringEagerStats(plan).isEmpty());
    }

    public void testSampleBetweenStatsAndRelationDoesNotRequireEagerStats() {
        // ... | SAMPLE 0.1 | STATS COUNT(*)
        LogicalPlan sample = new Sample(SRC, new Literal(SRC, 0.1d, DataType.DOUBLE), externalRelation(PATH));
        LogicalPlan plan = ungroupedAggregate(sample);

        assertTrue(ExternalStatsRequirementExtractor.pathsRequiringEagerStats(plan).isEmpty());
    }

    public void testFilterAndLimitBetweenStatsAndRelationDoesNotRequireEagerStats() {
        // ... | WHERE x > 5 | LIMIT 10 | STATS COUNT(*) — the VPC hive-WHERE waste case
        LogicalPlan relation = externalRelation(PATH);
        LogicalPlan filter = new Filter(SRC, relation, new GreaterThan(SRC, unresolved("x"), intLiteral(5)));
        LogicalPlan limit = new Limit(SRC, intLiteral(10), filter);
        LogicalPlan plan = ungroupedAggregate(limit);

        assertTrue(ExternalStatsRequirementExtractor.pathsRequiringEagerStats(plan).isEmpty());
    }

    public void testKeepBetweenStatsAndRelationStillRequiresEagerStats() {
        // ... | KEEP x | STATS COUNT(*) — KEEP is non-blocking (PruneColumns strips unused KEEP)
        LogicalPlan keep = new Keep(SRC, externalRelation(PATH), List.of(unresolved("x")));
        LogicalPlan plan = ungroupedAggregate(keep);

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

    public void testForkBareStatsArmKeepsPathEagerDespiteFilteredArm() {
        // Bare ungrouped STATS in one arm, filtered COUNT in the other: union stays eager.
        LogicalPlan bare = ungroupedAggregate(externalRelation(PATH));
        LogicalPlan filtered = ungroupedAggregate(
            new Filter(SRC, externalRelation(PATH), new GreaterThan(SRC, unresolved("x"), intLiteral(5)))
        );
        LogicalPlan plan = new Fork(SRC, List.of(bare, filtered), List.of());

        assertEquals(Set.of(PATH), ExternalStatsRequirementExtractor.pathsRequiringEagerStats(plan));
    }

    public void testForkAllFilteredArmsStayDeferred() {
        LogicalPlan filtered = ungroupedAggregate(
            new Filter(SRC, externalRelation(PATH), new GreaterThan(SRC, unresolved("x"), intLiteral(5)))
        );
        LogicalPlan limited = ungroupedAggregate(new Limit(SRC, intLiteral(10), externalRelation(PATH)));
        LogicalPlan plan = new Fork(SRC, List.of(filtered, limited), List.of());

        assertTrue(ExternalStatsRequirementExtractor.pathsRequiringEagerStats(plan).isEmpty());
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
