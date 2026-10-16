/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.compute.aggregation.AggregatorMode;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.analysis.Analyzer;
import org.elasticsearch.xpack.esql.analysis.AnalyzerTestUtils;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.spi.FileList;
import org.elasticsearch.xpack.esql.datasources.spi.SimpleSourceMetadata;
import org.elasticsearch.xpack.esql.optimizer.TestPlannerOptimizer;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.ExternalRelation;
import org.elasticsearch.xpack.esql.plan.physical.AggregateExec;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeDistribution;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeSinkExec;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.FragmentExec;
import org.elasticsearch.xpack.esql.plan.physical.LookupJoinExec;
import org.elasticsearch.xpack.esql.plan.physical.MergeExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.ShuffleExchangeExec;
import org.elasticsearch.xpack.esql.plan.physical.StageSinkExec;
import org.elasticsearch.xpack.esql.plan.physical.StageSourceExec;
import org.elasticsearch.xpack.esql.plugin.StagedPlanner.Edge;
import org.elasticsearch.xpack.esql.plugin.StagedPlanner.Stage;
import org.elasticsearch.xpack.esql.plugin.StagedPlanner.StagePlacement;
import org.elasticsearch.xpack.esql.plugin.StagedPlanner.StagedPlan;
import org.elasticsearch.xpack.esql.session.Configuration;

import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.analyzer;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.configuration;
import static org.hamcrest.Matchers.containsString;

public class StagedPlannerTests extends ESTestCase {
    private static final Settings SHUFFLE_AGG_SETTINGS = Settings.builder()
        .put(QueryPragmas.SHUFFLE_PARTITIONS_PER_NODE.getKey(), 2)
        .put(QueryPragmas.SHUFFLE_AGG.getKey(), true)
        .build();
    private static final Settings SHUFFLE_JOIN_SETTINGS = Settings.builder()
        .put(QueryPragmas.SHUFFLE_PARTITIONS_PER_NODE.getKey(), 2)
        .put(QueryPragmas.SHUFFLE_JOIN.getKey(), true)
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

    public void testSplitAggregateStagesAndShardDriverPlan() {
        PhysicalPlan plan = distributedPlan("FROM test | STATS c = COUNT(*) BY first_name | SORT c DESC | LIMIT 10");
        assertTrue(StagedPlanner.hasShuffle(plan));

        StagedPlan stagedPlan = StagedPlanner.split(plan);
        assertEquals(3, stagedPlan.stages().size());
        assertEquals(2, stagedPlan.edges().size());

        Stage root = stagedPlan.root();
        Stage workers = stagedPlan.stages().get(1);
        Stage shards = stagedPlan.stages().get(2);
        assertEquals(StagePlacement.COORDINATOR, root.placement());
        assertEquals(StagePlacement.WORKERS, workers.placement());
        assertEquals(StagePlacement.SHARDS, shards.placement());
        assertEquals(List.of("e0"), root.inputEdges());
        assertEquals(List.of("e1"), workers.inputEdges());
        assertEquals(List.of(), shards.inputEdges());
        assertNull(root.outputEdge());
        assertEquals("e0", workers.outputEdge());
        assertEquals("e1", shards.outputEdge());
        assertEquals(new Edge("e0", 1, 0, ExchangeDistribution.GATHER), stagedPlan.edge("e0"));
        assertEquals(new Edge("e1", 2, 1, ExchangeDistribution.HASH), stagedPlan.edge("e1"));
        assertEquals("e0", root.plan().collect(StageSourceExec.class).getFirst().edgeKey());
        assertEquals("e1", workers.plan().collect(StageSourceExec.class).getFirst().edgeKey());

        assertFalse(root.plan().anyMatch(ShuffleExchangeExec.class::isInstance));
        assertFalse(workers.plan().anyMatch(ShuffleExchangeExec.class::isInstance));
        assertFalse(shards.plan().anyMatch(ShuffleExchangeExec.class::isInstance));

        StageSinkExec shardsPlan = as(shards.plan(), StageSinkExec.class);
        FragmentExec fragment = as(shardsPlan.child(), FragmentExec.class);
        var split = StagedPlanner.splitShardsStage(shardsPlan);
        ExchangeSinkExec dataPlan = split.v1();
        StageSinkExec nodePlan = as(split.v2(), StageSinkExec.class);
        ExchangeSourceExec nodeInput = as(nodePlan.child(), ExchangeSourceExec.class);
        assertSame(fragment, dataPlan.child());
        assertEquals(shardsPlan.output(), dataPlan.output());
        assertTrue(dataPlan.isIntermediateAgg());
        assertEquals(shardsPlan.output(), nodeInput.output());
        assertTrue(nodeInput.isIntermediateAgg());
    }

    public void testExternalSourceStageIsRoutedBySplits() {
        Attribute key = new ReferenceAttribute(Source.EMPTY, "key", DataType.KEYWORD);
        String path = "s3://bucket/events/*.parquet";
        ExternalRelation relation = new ExternalRelation(
            Source.EMPTY,
            path,
            new SimpleSourceMetadata(List.of(key), "parquet", path),
            List.of(key),
            FileList.UNRESOLVED,
            Map.of()
        );

        StagedPlan stagedPlan = StagedPlanner.split(
            exchange(ExchangeDistribution.GATHER, exchange(ExchangeDistribution.HASH, new FragmentExec(relation)))
        );
        Stage externalStage = stagedPlan.stages().get(2);

        assertEquals(StagePlacement.SHARDS, externalStage.placement());
        assertTrue(StagedPlanner.isExternalSourceStage(externalStage.plan()));
        assertEquals(path, StagedPlanner.externalSourcePath(externalStage.plan()));
        assertFalse(StagedPlanner.isExternalSourceStage(stagedPlan.root().plan()));
    }

    public void testSplitSingleAggregationPlacesInitialAggregationOnWorkers() {
        PhysicalPlan plan = distributedPlan("FROM test | STATS BY first_name | STATS n = COUNT(*) | LIMIT 1000");
        StagedPlan stagedPlan = StagedPlanner.split(plan);

        assertEquals(3, stagedPlan.stages().size());
        assertEquals(StagePlacement.COORDINATOR, stagedPlan.root().placement());
        Stage workers = stagedPlan.stages().get(1);
        assertEquals(StagePlacement.WORKERS, workers.placement());
        assertTrue(
            workers.plan().collect(AggregateExec.class).stream().anyMatch(aggregate -> aggregate.getMode() == AggregatorMode.INITIAL)
        );
        assertEquals(ExchangeDistribution.GATHER, stagedPlan.edge("e0").distribution());
        assertEquals(ExchangeDistribution.HASH, stagedPlan.edge("e1").distribution());
    }

    public void testLookupJoinStagesAndLookupFilterStripping() {
        PhysicalPlan plan = distributedPlan(LOOKUP_JOIN_QUERY, SHUFFLE_JOIN_AGG_SETTINGS, true);
        MatchAllQueryBuilder requestFilter = new MatchAllQueryBuilder();
        PhysicalPlan filteredPlan = plan.transformDown(FragmentExec.class, fragment -> fragment.withFilter(requestFilter));

        StagedPlan stagedPlan = StagedPlanner.split(filteredPlan);

        assertEquals(
            List.of(StagePlacement.COORDINATOR, StagePlacement.WORKERS, StagePlacement.SHARDS, StagePlacement.SHARDS),
            stagedPlan.stages().stream().map(Stage::placement).toList()
        );
        List<FragmentExec> fragments = stagedPlan.stages()
            .stream()
            .flatMap(stage -> stage.plan().collect(FragmentExec.class).stream())
            .toList();
        assertEquals(2, fragments.size());
        for (FragmentExec fragment : fragments) {
            EsRelation relation = fragment.fragment().collect(EsRelation.class).getFirst();
            boolean lookupIndex = relation.indexProperties()
                .values()
                .stream()
                .allMatch(properties -> properties.indexMode() == IndexMode.LOOKUP);
            if (lookupIndex) {
                assertNull(fragment.esFilter());
            } else {
                assertSame(requestFilter, fragment.esFilter());
            }
        }
    }

    public void testSubqueryJoinStagesIncludeSingleSubplanStage() {
        assumeTrue("JOIN on a subquery required", EsqlCapabilities.Cap.JOIN_ON_SUBQUERY.isEnabled());
        PhysicalPlan plan = distributedPlan(SUBQUERY_JOIN_QUERY, SHUFFLE_JOIN_SETTINGS, false);

        StagedPlan stagedPlan = StagedPlanner.split(plan);

        assertEquals(1, stagedPlan.stages().stream().filter(stage -> stage.placement() == StagePlacement.COORDINATOR).count());
        assertEquals(1, stagedPlan.stages().stream().filter(stage -> stage.placement() == StagePlacement.WORKERS).count());
        assertEquals(2, stagedPlan.stages().stream().filter(stage -> stage.placement() == StagePlacement.SHARDS).count());
        assertEquals(1, stagedPlan.stages().stream().filter(stage -> stage.placement() == StagePlacement.SINGLE).count());
    }

    public void testLookupFragmentDoesNotMakeRootStageShardsPlaced() {
        PhysicalPlan plan = distributedPlan(
            "FROM test | RENAME languages AS language_code | STATS c = COUNT(*) BY language_code "
                + "| LOOKUP JOIN languages_lookup ON language_code | LIMIT 1000",
            true
        );
        LookupJoinExec lookupJoin = plan.collect(LookupJoinExec.class).getFirst();
        assertTrue(lookupJoin.right() instanceof FragmentExec);

        Stage root = StagedPlanner.split(plan).root();
        assertEquals(StagePlacement.COORDINATOR, root.placement());
        LookupJoinExec stagedLookupJoin = root.plan().collect(LookupJoinExec.class).getFirst();
        assertTrue(stagedLookupJoin.right() instanceof FragmentExec);
    }

    public void testSplitShardsStageWithFragmentBelowNodeLevelPlan() {
        PhysicalPlan plan = distributedPlan("FROM test | STATS c = COUNT(*) BY first_name | LIMIT 1000");
        AggregateExec aggregate = plan.collect(AggregateExec.class).getFirst();
        FragmentExec fragment = plan.collect(FragmentExec.class).getFirst();
        PhysicalPlan nodeLevelPlan = aggregate.replaceChild(fragment);
        StageSinkExec stagePlan = new StageSinkExec(
            aggregate.source(),
            aggregate.output(),
            false,
            "e0",
            ExchangeDistribution.GATHER,
            List.of(),
            nodeLevelPlan
        );

        var split = StagedPlanner.splitShardsStage(stagePlan);
        assertSame(fragment, split.v1().child());
        assertEquals(fragment.output(), split.v1().output());
        assertFalse(split.v1().isIntermediateAgg());
        AggregateExec rewrittenAggregate = as(as(split.v2(), StageSinkExec.class).child(), AggregateExec.class);
        assertTrue(rewrittenAggregate.child() instanceof ExchangeSourceExec);
        assertEquals(fragment.output(), rewrittenAggregate.child().output());
        assertFalse(as(rewrittenAggregate.child(), ExchangeSourceExec.class).isIntermediateAgg());
    }

    public void testHasShuffleReturnsFalseForLegacyExchanges() {
        PhysicalPlan plan = distributedPlan("FROM test | LIMIT 1000", Settings.EMPTY, false);

        assertFalse(StagedPlanner.hasShuffle(plan));
    }

    public void testNestedGatherCreatesSingleStage() {
        PhysicalPlan plan = exchange(ExchangeDistribution.GATHER, exchange(ExchangeDistribution.GATHER, baseFragment()));

        StagedPlan stagedPlan = StagedPlanner.split(plan);

        assertEquals(StagePlacement.COORDINATOR, stagedPlan.root().placement());
        assertEquals(StagePlacement.SINGLE, stagedPlan.stages().get(1).placement());
        assertEquals(StagePlacement.SHARDS, stagedPlan.stages().get(2).placement());
    }

    public void testRejectsMultipleShardFragments() {
        FragmentExec fragment = baseFragment();
        PhysicalPlan plan = new MergeExec(
            fragment.source(),
            List.of(fragment, copyFragment(fragment)),
            fragment.output(),
            MergeExec.Kind.UNION
        );

        IllegalStateException e = expectThrows(IllegalStateException.class, () -> StagedPlanner.split(plan));
        assertThat(e.getMessage(), containsString("multiple non-ignored fragments"));
    }

    public void testRejectsBinaryOperatorInShardsStage() {
        FragmentExec left = baseFragment();
        FragmentExec right = copyFragment(left);
        LookupJoinExec plan = new LookupJoinExec(
            left.source(),
            left,
            right,
            List.of(left.output().getFirst()),
            List.of(right.output().getFirst()),
            List.of(),
            null
        );

        IllegalStateException e = expectThrows(IllegalStateException.class, () -> StagedPlanner.split(plan));
        assertThat(e.getMessage(), containsString("contains a binary operator"));
    }

    public void testRejectsShardsStageWithInputEdges() {
        FragmentExec fragment = baseFragment();
        PhysicalPlan plan = new MergeExec(
            fragment.source(),
            List.of(fragment, exchange(ExchangeDistribution.GATHER, copyFragment(fragment))),
            fragment.output(),
            MergeExec.Kind.UNION
        );

        IllegalStateException e = expectThrows(IllegalStateException.class, () -> StagedPlanner.split(plan));
        assertThat(e.getMessage(), containsString("SHARDS stage"));
    }

    public void testRejectsMixedInputDistributions() {
        FragmentExec fragment = baseFragment();
        PhysicalPlan plan = new MergeExec(
            fragment.source(),
            List.of(exchange(ExchangeDistribution.GATHER, fragment), exchange(ExchangeDistribution.HASH, copyFragment(fragment))),
            fragment.output(),
            MergeExec.Kind.UNION
        );

        IllegalStateException e = expectThrows(IllegalStateException.class, () -> StagedPlanner.split(plan));
        assertThat(e.getMessage(), containsString("mixed input exchange distributions"));
    }

    public void testRejectsHashEdgeIntoCoordinatorStage() {
        PhysicalPlan plan = exchange(ExchangeDistribution.HASH, baseFragment());

        IllegalStateException e = expectThrows(IllegalStateException.class, () -> StagedPlanner.split(plan));
        assertThat(e.getMessage(), containsString("HASH edge"));
    }

    private static PhysicalPlan distributedPlan(String query) {
        return distributedPlan(query, SHUFFLE_AGG_SETTINGS, false);
    }

    private static PhysicalPlan distributedPlan(String query, boolean withLookup) {
        return distributedPlan(query, SHUFFLE_AGG_SETTINGS, withLookup);
    }

    private static PhysicalPlan distributedPlan(String query, Settings settings, boolean withLookup) {
        Configuration configuration = configuration(new QueryPragmas(settings), query);
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

    private static FragmentExec baseFragment() {
        PhysicalPlan plan = distributedPlan("FROM test | LIMIT 1000", Settings.EMPTY, false);
        return plan.collect(FragmentExec.class).getFirst();
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

    private static ShuffleExchangeExec exchange(ExchangeDistribution distribution, PhysicalPlan child) {
        List<Attribute> keys = distribution == ExchangeDistribution.HASH ? List.of(child.output().getFirst()) : List.of();
        return new ShuffleExchangeExec(Source.EMPTY, List.of(), false, distribution, keys, child);
    }

    private static <T> T as(Object value, Class<T> type) {
        assertTrue("expected [" + type.getSimpleName() + "] but got [" + value.getClass().getSimpleName() + "]", type.isInstance(value));
        return type.cast(value);
    }
}
