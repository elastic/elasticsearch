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
import org.elasticsearch.xpack.esql.optimizer.TestPlannerOptimizer;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.physical.AggregateExec;
import org.elasticsearch.xpack.esql.plan.physical.DistributedHashJoinExec;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeDistribution;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeExec;
import org.elasticsearch.xpack.esql.plan.physical.FragmentExec;
import org.elasticsearch.xpack.esql.plan.physical.LimitExec;
import org.elasticsearch.xpack.esql.plan.physical.MergeExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.ShuffleExchangeExec;
import org.elasticsearch.xpack.esql.plan.physical.TopNExec;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.analyzer;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.configuration;

public class ShuffleRulesTests extends ESTestCase {
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

    public void testShuffleRulesDoNotRunWithoutPragmas() {
        PhysicalPlan plan = distributedPlan(AGGREGATE_QUERY, Settings.EMPTY);

        assertFalse(plan.anyMatch(ShuffleExchangeExec.class::isInstance));
        assertTrue(plan.anyMatch(ExchangeExec.class::isInstance));
    }

    public void testShuffleRulesDoNotRunForForkPlans() {
        PhysicalPlan plan = distributedPlan("FROM test | FORK (WHERE true) (WHERE true) | LIMIT 1000", SHUFFLE_AGG_SETTINGS);

        assertTrue(plan.anyMatch(MergeExec.class::isInstance));
        assertFalse(plan.anyMatch(ShuffleExchangeExec.class::isInstance));
    }

    public void testLookupJoinFragmentsArePrunedAfterDistributedAggregation() {
        PhysicalPlan plan = distributedPlan(LOOKUP_JOIN_QUERY, SHUFFLE_JOIN_AGG_SETTINGS, true);

        TopNExec topN = as(plan, TopNExec.class);
        AggregateExec finalAggregate = as(topN.child(), AggregateExec.class);
        assertEquals(AggregatorMode.FINAL, finalAggregate.getMode());
        ShuffleExchangeExec gather = as(finalAggregate.child(), ShuffleExchangeExec.class);
        assertEquals(ExchangeDistribution.GATHER, gather.distribution());
        assertTrue(gather.inBetweenAggs());

        AggregateExec initialAggregate = as(gather.child(), AggregateExec.class);
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

    public void testStackedGatherIsCollapsedForDistributedSubqueryJoin() {
        assumeTrue("JOIN on a subquery required", EsqlCapabilities.Cap.JOIN_ON_SUBQUERY.isEnabled());
        PhysicalPlan plan = distributedPlan(SUBQUERY_JOIN_QUERY, SHUFFLE_JOIN_AGG_SETTINGS);

        DistributedHashJoinExec join = plan.collect(DistributedHashJoinExec.class).getFirst();
        ShuffleExchangeExec rightHash = as(join.right(), ShuffleExchangeExec.class);
        assertEquals(ExchangeDistribution.HASH, rightHash.distribution());
        assertTrue(rightHash.child() instanceof AggregateExec);
        assertFalse(
            rightHash.child()
                .anyMatch(node -> node instanceof ShuffleExchangeExec exchange && exchange.distribution() == ExchangeDistribution.GATHER)
        );
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
}
