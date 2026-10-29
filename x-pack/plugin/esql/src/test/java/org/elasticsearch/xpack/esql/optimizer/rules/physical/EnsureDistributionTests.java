/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.physical;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.compute.aggregation.AggregatorMode;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.analysis.Analyzer;
import org.elasticsearch.xpack.esql.analysis.AnalyzerTestUtils;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.optimizer.PhysicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.PlanShape;
import org.elasticsearch.xpack.esql.optimizer.TestPlannerOptimizer;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.physical.AggregateExec;
import org.elasticsearch.xpack.esql.plan.physical.DistributedHashJoinExec;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeDistribution;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeExec;
import org.elasticsearch.xpack.esql.plan.physical.FragmentExec;
import org.elasticsearch.xpack.esql.plan.physical.LimitExec;
import org.elasticsearch.xpack.esql.plan.physical.LookupJoinExec;
import org.elasticsearch.xpack.esql.plan.physical.MergeExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.ShuffleExchangeExec;
import org.elasticsearch.xpack.esql.plan.physical.TopNExec;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;

import java.util.List;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.analyzer;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.configuration;

public class EnsureDistributionTests extends ESTestCase {
    private static final String AGGREGATE_QUERY = "FROM test | STATS c = COUNT(*) BY first_name | SORT c DESC | LIMIT 10";

    private static final Settings SHUFFLE_AGG_SETTINGS = Settings.builder()
        .put(QueryPragmas.SHUFFLE_PARTITIONS_PER_NODE.getKey(), 2)
        .put(QueryPragmas.SHUFFLE_AGG.getKey(), true)
        .build();
    private static final Settings SHUFFLE_JOIN_AGG_SETTINGS = Settings.builder()
        .put(QueryPragmas.SHUFFLE_PARTITIONS_PER_NODE.getKey(), 2)
        .put(QueryPragmas.SHUFFLE_AGG.getKey(), true)
        .put(QueryPragmas.SHUFFLE_JOIN.getKey(), true)
        .build();
    private static final String LOOKUP_JOIN_QUERY = """
        FROM test
        | RENAME emp_no AS language_code
        | LOOKUP JOIN languages_lookup ON language_code
        | STATS c = COUNT(*) BY language_name
        | SORT c DESC
        | LIMIT 10
        """;
    private static final String SUBQUERY_JOIN_QUERY = """
        FROM test
        | LEFT OUTER JOIN (FROM test | STATS total = SUM(salary) BY emp_no) ON emp_no
        | STATS s = SUM(total) BY first_name
        | SORT first_name
        | LIMIT 1000
        """;

    public void testFinalAggregateUsesHashExchangeAndCopiesTopN() {
        PhysicalPlan plan = distributedPlan(AGGREGATE_QUERY, SHUFFLE_AGG_SETTINGS);

        assertEquals("""
            TOPN 10
              GATHER
                TOPN 10
                  AGG FINAL [first_name]
                    HASH* [first_name]
                      FRAGMENT test
            """, PlanShape.of(plan));

        TopNExec coordinatorTopN = as(plan, TopNExec.class);
        ShuffleExchangeExec gather = as(coordinatorTopN.child(), ShuffleExchangeExec.class);
        assertEquals(ExchangeDistribution.GATHER, gather.distribution());
        assertFalse(gather.inBetweenAggs());

        TopNExec workerTopN = as(gather.child(), TopNExec.class);
        assertEquals(coordinatorTopN.limit(), workerTopN.limit());
        assertEquals(coordinatorTopN.order(), workerTopN.order());

        AggregateExec finalAggregate = as(workerTopN.child(), AggregateExec.class);
        assertEquals(AggregatorMode.FINAL, finalAggregate.getMode());

        ShuffleExchangeExec hash = as(finalAggregate.child(), ShuffleExchangeExec.class);
        assertEquals(ExchangeDistribution.HASH, hash.distribution());
        assertTrue(hash.inBetweenAggs());
        assertEquals(finalAggregate.groupings().size(), hash.keys().size());
        assertEquals(hash.output().subList(0, hash.keys().size()), hash.keys());
        assertTrue(hash.child() instanceof FragmentExec);
        assertEquals(2, plan.collect(ShuffleExchangeExec.class).size());
    }

    public void testSingleAggregateIsSplitAcrossGather() {
        PhysicalPlan plan = distributedPlan("FROM test | STATS BY first_name | STATS n = COUNT(*) | LIMIT 1000", SHUFFLE_AGG_SETTINGS);

        AggregateExec finalAggregate = as(as(plan, LimitExec.class).child(), AggregateExec.class);
        assertEquals(AggregatorMode.FINAL, finalAggregate.getMode());

        ShuffleExchangeExec intermediateGather = as(finalAggregate.child(), ShuffleExchangeExec.class);
        assertEquals(ExchangeDistribution.GATHER, intermediateGather.distribution());
        assertTrue(intermediateGather.inBetweenAggs());

        AggregateExec initialAggregate = as(intermediateGather.child(), AggregateExec.class);
        assertEquals(AggregatorMode.INITIAL, initialAggregate.getMode());

        AggregateExec priorFinalAggregate = as(initialAggregate.child(), AggregateExec.class);
        assertEquals(AggregatorMode.FINAL, priorFinalAggregate.getMode());

        ShuffleExchangeExec hash = as(priorFinalAggregate.child(), ShuffleExchangeExec.class);
        assertEquals(ExchangeDistribution.HASH, hash.distribution());
        assertTrue(hash.child() instanceof FragmentExec);
    }

    public void testNoChangeWithoutPragmas() {
        PhysicalPlan plan = distributedPlan(AGGREGATE_QUERY, Settings.EMPTY);

        assertFalse(plan.anyMatch(ShuffleExchangeExec.class::isInstance));
        assertTrue(plan.anyMatch(ExchangeExec.class::isInstance));
    }

    public void testNoChangeForForkPlans() {
        PhysicalPlan plan = distributedPlan("FROM test | FORK (WHERE true) (WHERE true) | LIMIT 1000", SHUFFLE_AGG_SETTINGS);

        assertTrue(plan.anyMatch(MergeExec.class::isInstance));
        assertFalse(plan.anyMatch(ShuffleExchangeExec.class::isInstance));
    }

    public void testUnionOnEitherJoinSideGetsHashExchanges() {
        FragmentExec fragment = distributedPlan("FROM test | LIMIT 1000", Settings.EMPTY).collect(FragmentExec.class).getFirst();
        Attribute joinKey = fragment.output().stream().filter(attribute -> attribute.name().equals("emp_no")).findFirst().orElseThrow();
        List<Attribute> joinKeys = List.of(joinKey);

        for (boolean unionOnLeft : List.of(true, false)) {
            MergeExec union = new MergeExec(
                fragment.source(),
                List.of(copyFragment(fragment), copyFragment(fragment)),
                fragment.output(),
                MergeExec.Kind.UNION
            );
            DistributedHashJoinExec join = unionOnLeft
                ? new DistributedHashJoinExec(fragment.source(), union, copyFragment(fragment), joinKeys, joinKeys, List.of())
                : new DistributedHashJoinExec(fragment.source(), copyFragment(fragment), union, joinKeys, joinKeys, List.of());
            PhysicalPlan plan = new EnsureDistribution().apply(
                join,
                new PhysicalOptimizerContext(
                    configuration(new QueryPragmas(SHUFFLE_JOIN_AGG_SETTINGS), "FROM test | LIMIT 1000"),
                    TransportVersion.current()
                )
            );
            DistributedHashJoinExec distributedJoin = plan.collect(DistributedHashJoinExec.class).getFirst();

            assertHashPartitionedUnion(unionOnLeft ? distributedJoin.left() : distributedJoin.right(), 2);
        }
    }

    public void testBroadcastJoinKeepsScanProbeAndBroadcastsBuild() {
        FragmentExec fragment = distributedPlan("FROM test | LIMIT 1000", Settings.EMPTY).collect(FragmentExec.class).getFirst();
        Attribute joinKey = fragment.output().stream().filter(attribute -> attribute.name().equals("emp_no")).findFirst().orElseThrow();
        DistributedHashJoinExec join = broadcastJoin(copyFragment(fragment), copyFragment(fragment), joinKey);

        PhysicalPlan plan = ensureDistribution(join);

        assertEquals("""
            GATHER
              DHJ [emp_no] = [emp_no] BROADCAST
                FRAGMENT test
                BROADCAST
                  FRAGMENT test
            """, PlanShape.of(plan));
        DistributedHashJoinExec plannedJoin = plan.collect(DistributedHashJoinExec.class).getFirst();
        assertTrue(plannedJoin.left() instanceof FragmentExec);
        assertEquals(ExchangeDistribution.BROADCAST, as(plannedJoin.right(), ShuffleExchangeExec.class).distribution());
    }

    public void testBroadcastJoinPreservesProbeHashPartitioning() {
        FragmentExec fragment = distributedPlan("FROM test | LIMIT 1000", Settings.EMPTY).collect(FragmentExec.class).getFirst();
        Attribute joinKey = fragment.output().stream().filter(attribute -> attribute.name().equals("emp_no")).findFirst().orElseThrow();
        DistributedHashJoinExec partitionedJoin = new DistributedHashJoinExec(
            fragment.source(),
            copyFragment(fragment),
            copyFragment(fragment),
            List.of(joinKey),
            List.of(joinKey),
            List.of()
        );
        DistributedHashJoinExec broadcastJoin = broadcastJoin(partitionedJoin, copyFragment(fragment), joinKey);

        PhysicalPlan plan = ensureDistribution(broadcastJoin);

        assertEquals("""
            GATHER
              DHJ [emp_no] = [emp_no] BROADCAST
                DHJ [emp_no] = [emp_no]
                  HASH [emp_no]
                    FRAGMENT test
                  HASH [emp_no]
                    FRAGMENT test
                BROADCAST
                  FRAGMENT test
            """, PlanShape.of(plan));
    }

    public void testBroadcastJoinCanAppearUnderUnion() {
        FragmentExec fragment = distributedPlan("FROM test | LIMIT 1000", Settings.EMPTY).collect(FragmentExec.class).getFirst();
        Attribute joinKey = fragment.output().stream().filter(attribute -> attribute.name().equals("emp_no")).findFirst().orElseThrow();
        DistributedHashJoinExec first = broadcastJoin(copyFragment(fragment), copyFragment(fragment), joinKey);
        DistributedHashJoinExec second = broadcastJoin(copyFragment(fragment), copyFragment(fragment), joinKey);
        MergeExec union = new MergeExec(first.source(), List.of(first, second), first.output(), MergeExec.Kind.UNION);

        PhysicalPlan plan = ensureDistribution(union);

        assertEquals("""
            UNION
              GATHER
                DHJ [emp_no] = [emp_no] BROADCAST
                  FRAGMENT test
                  BROADCAST
                    FRAGMENT test
              GATHER
                DHJ [emp_no] = [emp_no] BROADCAST
                  FRAGMENT test
                  BROADCAST
                    FRAGMENT test
            """, PlanShape.of(plan));
    }

    public void testStatsOverLeafUnionGetsHashExchanges() {
        assumeTrue("subquery in FROM required", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        PhysicalPlan plan = distributedPlan("FROM test, (FROM test) | STATS c = COUNT(*) BY emp_no | LIMIT 1000", SHUFFLE_AGG_SETTINGS);

        assertHashPartitionedUnion(plan, 2);
    }

    public void testStatsOverSubqueryUnionGetsHashExchanges() {
        assumeTrue("subquery in FROM required", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        PhysicalPlan plan = distributedPlan(
            "FROM test, (FROM test | WHERE languages > 2) | STATS c = COUNT(*) BY emp_no | LIMIT 1000",
            SHUFFLE_AGG_SETTINGS
        );

        assertHashPartitionedUnion(plan, 2);
    }

    public void testNestedUnionIsFlattenedUnderHashRequirement() {
        FragmentExec fragment = distributedPlan("FROM test | LIMIT 1000", Settings.EMPTY).collect(FragmentExec.class).getFirst();
        Attribute joinKey = fragment.output().stream().filter(attribute -> attribute.name().equals("emp_no")).findFirst().orElseThrow();
        MergeExec nestedUnion = new MergeExec(
            fragment.source(),
            List.of(copyFragment(fragment), copyFragment(fragment)),
            fragment.output(),
            MergeExec.Kind.UNION
        );
        MergeExec outerUnion = new MergeExec(
            fragment.source(),
            List.of(copyFragment(fragment), nestedUnion),
            fragment.output(),
            MergeExec.Kind.UNION
        );
        DistributedHashJoinExec join = new DistributedHashJoinExec(
            fragment.source(),
            copyFragment(fragment),
            outerUnion,
            List.of(joinKey),
            List.of(joinKey),
            List.of()
        );
        PhysicalPlan plan = new EnsureDistribution().apply(
            join,
            new PhysicalOptimizerContext(
                configuration(new QueryPragmas(SHUFFLE_JOIN_AGG_SETTINGS), "FROM test | LIMIT 1000"),
                TransportVersion.current()
            )
        );
        DistributedHashJoinExec distributedJoin = plan.collect(DistributedHashJoinExec.class).getFirst();
        List<MergeExec> unions = distributedJoin.right()
            .collect(MergeExec.class)
            .stream()
            .filter(merge -> merge.kind() == MergeExec.Kind.UNION)
            .toList();

        assertEquals(PlanShape.of(distributedJoin.right()), 1, unions.size());
        assertHashPartitionedUnion(distributedJoin.right(), 3);
    }

    public void testRootOnlyUnionStaysOnLegacyPath() {
        assumeTrue("subquery in FROM required", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        PhysicalPlan plan = distributedPlan(
            "FROM test, (FROM test | WHERE languages > 2) | SORT first_name | LIMIT 10",
            SHUFFLE_AGG_SETTINGS
        );

        assertTrue(plan.anyMatch(MergeExec.class::isInstance));
        assertFalse(plan.anyMatch(ShuffleExchangeExec.class::isInstance));
    }

    public void testLookupJoinFragmentsArePrunedAfterDistributedAggregation() {
        PhysicalPlan plan = distributedPlan(LOOKUP_JOIN_QUERY, SHUFFLE_JOIN_AGG_SETTINGS, true);

        assertEquals("""
            TOPN 10
              GATHER
                TOPN 10
                  AGG FINAL [language_name]
                    HASH* [language_name]
                      AGG INITIAL [language_name]
                        DHJ [emp_no] = [language_code]
                          HASH [emp_no]
                            FRAGMENT test
                          HASH [language_code]
                            FRAGMENT languages_lookup
            """, PlanShape.of(plan));

        TopNExec topN = as(plan, TopNExec.class);
        ShuffleExchangeExec topGather = as(topN.child(), ShuffleExchangeExec.class);
        TopNExec workerTopN = as(topGather.child(), TopNExec.class);
        AggregateExec finalAggregate = as(workerTopN.child(), AggregateExec.class);
        assertEquals(AggregatorMode.FINAL, finalAggregate.getMode());
        ShuffleExchangeExec hash = as(finalAggregate.child(), ShuffleExchangeExec.class);
        assertEquals(ExchangeDistribution.HASH, hash.distribution());
        assertTrue(hash.inBetweenAggs());

        AggregateExec initialAggregate = as(hash.child(), AggregateExec.class);
        assertEquals(AggregatorMode.INITIAL, initialAggregate.getMode());
        DistributedHashJoinExec join = as(initialAggregate.child(), DistributedHashJoinExec.class);

        ShuffleExchangeExec leftHash = as(join.left(), ShuffleExchangeExec.class);
        ShuffleExchangeExec rightHash = as(join.right(), ShuffleExchangeExec.class);
        FragmentExec leftFragment = as(leftHash.child(), FragmentExec.class);
        FragmentExec rightFragment = as(rightHash.child(), FragmentExec.class);
        Project leftProject = as(leftFragment.fragment(), Project.class);
        Project rightProject = as(rightFragment.fragment(), Project.class);

        assertEquals(join.leftFields(), leftProject.output());
        assertEquals(join.rightFields(), rightProject.output().subList(0, join.rightFields().size()));
        assertEquals(join.addedFields(), rightProject.output().subList(join.rightFields().size(), rightProject.output().size()));
    }

    public void testSubqueryJoinReusesRightSideHashPartitioning() {
        assumeTrue("JOIN on a subquery required", EsqlCapabilities.Cap.JOIN_ON_SUBQUERY.isEnabled());
        PhysicalPlan plan = distributedPlan(SUBQUERY_JOIN_QUERY, SHUFFLE_JOIN_AGG_SETTINGS);

        DistributedHashJoinExec join = plan.collect(DistributedHashJoinExec.class).getFirst();
        AggregateExec rightAggregate = as(join.right(), AggregateExec.class);
        ShuffleExchangeExec rightHash = as(rightAggregate.child(), ShuffleExchangeExec.class);
        assertEquals(ExchangeDistribution.HASH, rightHash.distribution());
        assertTrue(rightHash.inBetweenAggs());
        assertFalse(
            join.right()
                .anyMatch(node -> node instanceof ShuffleExchangeExec exchange && exchange.distribution() == ExchangeDistribution.GATHER)
        );
    }

    public void testJoinChainOnSameKeyReusesPartitioning() {
        PhysicalPlan plan = distributedPlan("""
            FROM test
            | LEFT OUTER JOIN (FROM test | KEEP emp_no, salary | RENAME salary AS s1) ON emp_no
            | LEFT OUTER JOIN (FROM test | KEEP emp_no, languages | RENAME languages AS l2) ON emp_no
            | STATS c = COUNT(*) BY gender
            | LIMIT 1000
            """, SHUFFLE_JOIN_AGG_SETTINGS);

        assertEquals("""
            LIMIT 1000
              GATHER
                LIMIT 1000
                  AGG FINAL [gender]
                    HASH* [gender]
                      AGG INITIAL [gender]
                        DHJ [emp_no] = [emp_no]
                          DHJ [emp_no] = [emp_no]
                            HASH [emp_no]
                              FRAGMENT test
                            HASH [emp_no]
                              FRAGMENT test
                          HASH [emp_no]
                            FRAGMENT test
            """, PlanShape.of(plan));
    }

    public void testJoinChainOnDifferentKeysAddsExchangeForOuterJoin() {
        PhysicalPlan plan = distributedPlan("""
            FROM test
            | LEFT OUTER JOIN (FROM test | KEEP emp_no, salary | RENAME salary AS s1) ON emp_no
            | LEFT OUTER JOIN (FROM test | KEEP languages, gender | RENAME gender AS g2) ON languages
            | STATS c = COUNT(*) BY first_name
            | LIMIT 1000
            """, SHUFFLE_JOIN_AGG_SETTINGS);

        assertEquals("""
            LIMIT 1000
              GATHER
                LIMIT 1000
                  AGG FINAL [first_name]
                    HASH* [first_name]
                      AGG INITIAL [first_name]
                        DHJ [languages] = [languages]
                          HASH [languages]
                            DHJ [emp_no] = [emp_no]
                              HASH [emp_no]
                                FRAGMENT test
                              HASH [emp_no]
                                FRAGMENT test
                          HASH [languages]
                            FRAGMENT test
            """, PlanShape.of(plan));
    }

    public void testBushySubqueryJoin() {
        assumeTrue("JOIN on a subquery required", EsqlCapabilities.Cap.JOIN_ON_SUBQUERY.isEnabled());
        PhysicalPlan plan = distributedPlan("""
            FROM test
            | LEFT OUTER JOIN (
                FROM test
                | KEEP emp_no, languages
                | LEFT OUTER JOIN (FROM test | KEEP emp_no, gender) ON emp_no
                | KEEP emp_no, languages, gender
            ) ON emp_no
            | STATS c = COUNT(*) BY gender
            | SORT gender
            | LIMIT 1000
            """, SHUFFLE_JOIN_AGG_SETTINGS);

        assertEquals("""
            TOPN 1000
              GATHER
                TOPN 1000
                  AGG FINAL [gender]
                    HASH* [gender]
                      AGG INITIAL [gender]
                        DHJ [emp_no] = [emp_no]
                          HASH [emp_no]
                            FRAGMENT test
                          DHJ [emp_no] = [emp_no]
                            HASH [emp_no]
                              FRAGMENT test
                            HASH [emp_no]
                              FRAGMENT test
            """, PlanShape.of(plan));
    }

    public void testAggregationAfterJoinOnJoinKeyAddsItsOwnHashExchange() {
        PhysicalPlan plan = distributedPlan("""
            FROM test
            | LEFT OUTER JOIN (FROM test | STATS total = SUM(salary) BY emp_no) ON emp_no
            | STATS c = COUNT(*), s = SUM(total) BY emp_no
            | SORT emp_no
            | LIMIT 200
            """, SHUFFLE_JOIN_AGG_SETTINGS);

        assertEquals("""
            TOPN 200
              GATHER
                TOPN 200
                  AGG FINAL [emp_no]
                    HASH* [emp_no]
                      AGG INITIAL [emp_no]
                        DHJ [emp_no] = [emp_no]
                          HASH [emp_no]
                            FRAGMENT test
                          AGG FINAL [emp_no]
                            HASH* [emp_no]
                              FRAGMENT test
            """, PlanShape.of(plan));
    }

    public void testJoinOnAggregatedInputsReusesExactPartitioning() {
        PhysicalPlan plan = distributedPlan("""
            FROM test
            | STATS b = SUM(salary) BY emp_no
            | LEFT OUTER JOIN (FROM test | STATS total = SUM(salary) BY emp_no) ON emp_no
            | SORT emp_no
            | LIMIT 200
            """, SHUFFLE_JOIN_AGG_SETTINGS);

        assertEquals("""
            TOPN 200
              GATHER
                TOPN 200
                  DHJ [emp_no] = [emp_no]
                    AGG FINAL [emp_no]
                      HASH* [emp_no]
                        FRAGMENT test
                    AGG FINAL [emp_no]
                      HASH* [emp_no]
                        FRAGMENT test
            """, PlanShape.of(plan));
    }

    public void testLookupJoinAfterDistributedJoin() {
        PhysicalPlan plan = distributedPlan("""
            FROM test
            | LEFT OUTER JOIN (FROM test | KEEP emp_no, salary) ON emp_no
            | RENAME languages AS language_code
            | LOOKUP JOIN languages_lookup ON language_code
            | STATS c = COUNT(*) BY language_name
            | SORT language_name
            | LIMIT 1000
            """, SHUFFLE_JOIN_AGG_SETTINGS, true);
        assertEquals(1, plan.collect(DistributedHashJoinExec.class).size());
        assertEquals(1, plan.collect(LookupJoinExec.class).size());
        LookupJoinExec lookupJoin = plan.collect(LookupJoinExec.class).getFirst();
        assertTrue("Lookup join input:\n" + PlanShape.of(lookupJoin.left()), PlanShape.of(lookupJoin.left()).startsWith("DHJ"));
    }

    private static PhysicalPlan distributedPlan(String query, Settings settings) {
        return distributedPlan(query, settings, false);
    }

    private static PhysicalPlan distributedPlan(String query, Settings settings, boolean withLookup) {
        var configuration = configuration(new QueryPragmas(settings), query);
        var analyzerBuilder = analyzer().minimumTransportVersion(TransportVersion.current())
            .configuration(configuration)
            .addIndex("test", "mapping-basic.json")
            .addNoFieldsIndex();
        if (withLookup) {
            analyzerBuilder.addLookupIndex(AnalyzerTestUtils.defaultLookupResolution().get("languages_lookup"));
        }
        Analyzer analyzer = analyzerBuilder.buildAnalyzer();
        return new TestPlannerOptimizer(configuration, analyzer).distributedPlan(query);
    }

    private static void assertHashPartitionedUnion(PhysicalPlan plan, int expectedBranches) {
        MergeExec union = plan.collect(MergeExec.class)
            .stream()
            .filter(merge -> merge.kind() == MergeExec.Kind.UNION)
            .findFirst()
            .orElseThrow();
        assertEquals(expectedBranches, union.children().size());
        for (PhysicalPlan branch : union.children()) {
            ShuffleExchangeExec exchange = as(branch, ShuffleExchangeExec.class);
            assertEquals(PlanShape.of(plan), ExchangeDistribution.HASH, exchange.distribution());
            assertEquals(1, exchange.keys().size());
            assertEquals("emp_no", exchange.keys().getFirst().name());
        }
    }

    private static FragmentExec copyFragment(FragmentExec fragment) {
        return new FragmentExec(
            fragment.source(),
            fragment.fragment(),
            fragment.esFilter(),
            fragment.estimatedRowSize(),
            fragment.isFromViewBranch()
        );
    }

    private static DistributedHashJoinExec broadcastJoin(PhysicalPlan left, PhysicalPlan right, Attribute joinKey) {
        return new DistributedHashJoinExec(
            left.source(),
            left,
            right,
            List.of(joinKey),
            List.of(joinKey),
            List.of(),
            DistributedHashJoinExec.Mode.BROADCAST
        );
    }

    private static PhysicalPlan ensureDistribution(PhysicalPlan plan) {
        return new EnsureDistribution().apply(
            plan,
            new PhysicalOptimizerContext(
                configuration(new QueryPragmas(SHUFFLE_JOIN_AGG_SETTINGS), "FROM test | LIMIT 1000"),
                TransportVersion.current()
            )
        );
    }
}
