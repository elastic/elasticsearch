/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.planner.mapper;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.analysis.Analyzer;
import org.elasticsearch.xpack.esql.analysis.AnalyzerTestUtils;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNotNull;
import org.elasticsearch.xpack.esql.optimizer.TestPlannerOptimizer;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.join.Join;
import org.elasticsearch.xpack.esql.plan.logical.join.JoinConfig;
import org.elasticsearch.xpack.esql.plan.physical.AggregateExec;
import org.elasticsearch.xpack.esql.plan.physical.DistributedHashJoinExec;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeDistribution;
import org.elasticsearch.xpack.esql.plan.physical.FilterExec;
import org.elasticsearch.xpack.esql.plan.physical.FragmentExec;
import org.elasticsearch.xpack.esql.plan.physical.LookupJoinExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.ProjectExec;
import org.elasticsearch.xpack.esql.plan.physical.ShuffleExchangeExec;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.elasticsearch.xpack.esql.session.Versioned;

import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_PARSER;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.analyzer;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.configuration;

public class DistributedJoinMapperTests extends ESTestCase {
    private static final Settings SHUFFLE_JOIN_SETTINGS = Settings.builder()
        .put(QueryPragmas.SHUFFLE_PARTITIONS_PER_NODE.getKey(), 2)
        .put(QueryPragmas.SHUFFLE_JOIN.getKey(), true)
        .build();
    private static final Settings BROADCAST_SHUFFLE_JOIN_SETTINGS = Settings.builder()
        .put(QueryPragmas.SHUFFLE_PARTITIONS_PER_NODE.getKey(), 2)
        .put(QueryPragmas.SHUFFLE_JOIN.getKey(), true)
        .put(QueryPragmas.SHUFFLE_JOIN_STRATEGY.getKey(), "broadcast")
        .build();

    private static final String DISTRIBUTED_QUERY = """
        FROM test
        | RENAME emp_no AS language_code
        | LOOKUP JOIN languages_lookup ON language_code
        | STATS c = COUNT(*) BY language_name
        | LIMIT 1000
        """;

    public void testDistributesLookupJoinToLookupIndex() {
        PhysicalPlan plan = distributedPlan(DISTRIBUTED_QUERY, BROADCAST_SHUFFLE_JOIN_SETTINGS);

        DistributedHashJoinExec join = plan.collect(DistributedHashJoinExec.class).getFirst();
        assertEquals(DistributedHashJoinExec.Mode.PARTITIONED, join.mode());
        ShuffleExchangeExec leftHash = as(join.left(), ShuffleExchangeExec.class);
        ShuffleExchangeExec rightHash = as(join.right(), ShuffleExchangeExec.class);
        assertEquals(ExchangeDistribution.HASH, leftHash.distribution());
        assertEquals(ExchangeDistribution.HASH, rightHash.distribution());
        assertEquals(join.leftFields(), leftHash.keys());
        assertEquals(join.rightFields(), rightHash.keys());
        assertTrue(leftHash.child() instanceof FragmentExec);

        FragmentExec lookupFragment = as(rightHash.child(), FragmentExec.class);
        EsRelation lookupRelation = lookupFragment.fragment().collect(EsRelation.class).getFirst();
        assertEquals("languages_lookup", lookupRelation.indexPattern());
        assertEquals(IndexMode.STANDARD, lookupRelation.indexMode());
        assertEquals(Map.of("", List.of("languages_lookup")), lookupRelation.originalIndices());
    }

    public void testBroadcastsEligibleSubqueryJoinBuildSide() {
        assumeTrue("JOIN on a subquery required", EsqlCapabilities.Cap.JOIN_ON_SUBQUERY.isEnabled());
        PhysicalPlan plan = distributedPlan("""
            FROM test
            | RENAME emp_no AS language_code
            | LEFT OUTER JOIN (FROM languages_lookup) ON language_code
            | STATS c = COUNT(*) BY language_name
            | LIMIT 1000
            """, BROADCAST_SHUFFLE_JOIN_SETTINGS);

        DistributedHashJoinExec join = plan.collect(DistributedHashJoinExec.class).getFirst();
        assertEquals(DistributedHashJoinExec.Mode.BROADCAST, join.mode());
        assertTrue(join.left() instanceof FragmentExec);
        assertEquals(ExchangeDistribution.BROADCAST, as(join.right(), ShuffleExchangeExec.class).distribution());
    }

    public void testDistributesLeftOuterSubqueryJoin() {
        assumeTrue("JOIN on a subquery required", EsqlCapabilities.Cap.JOIN_ON_SUBQUERY.isEnabled());
        PhysicalPlan plan = distributedPlan("""
            FROM test
            | RENAME emp_no AS language_code
            | LEFT OUTER JOIN (FROM languages_lookup) ON language_code
            | STATS c = COUNT(*) BY language_name
            | LIMIT 1000
            """, SHUFFLE_JOIN_SETTINGS);

        DistributedHashJoinExec join = distributedJoinUnderAggregate(plan);
        assertFalse(join.addedFields().stream().anyMatch(Mapper::isJoinMarker));
        assertTrue(join.addedFields().stream().anyMatch(attribute -> attribute.name().equals("language_name")));
    }

    public void testDistributesSubqueryJoinAfterStats() {
        assumeTrue("JOIN on a subquery required", EsqlCapabilities.Cap.JOIN_ON_SUBQUERY.isEnabled());
        PhysicalPlan plan = distributedPlan("""
            FROM test
            | STATS total = SUM(salary) BY emp_no
            | LEFT OUTER JOIN (FROM test | STATS total2 = SUM(salary) BY emp_no) ON emp_no
            | LIMIT 1000
            """, SHUFFLE_JOIN_SETTINGS);

        DistributedHashJoinExec join = plan.collect(DistributedHashJoinExec.class).getFirst();
        assertTrue(join.left() instanceof ShuffleExchangeExec);
        assertTrue(join.right() instanceof ShuffleExchangeExec);
        assertTrue(join.left().anyMatch(AggregateExec.class::isInstance));
        assertTrue(join.right().anyMatch(AggregateExec.class::isInstance));
        assertEquals(List.of("total", "total2", "emp_no"), join.output().stream().map(attribute -> attribute.name()).toList());
    }

    public void testLookupJoinAfterStatsRemainsLookupJoin() {
        PhysicalPlan plan = distributedPlan("""
            FROM test
            | STATS total = SUM(salary) BY emp_no
            | RENAME emp_no AS language_code
            | LOOKUP JOIN languages_lookup ON language_code
            | LIMIT 1000
            """, SHUFFLE_JOIN_SETTINGS);

        assertTrue(plan.anyMatch(LookupJoinExec.class::isInstance));
        assertFalse(plan.anyMatch(DistributedHashJoinExec.class::isInstance));
    }

    public void testDistributesInnerSubqueryJoinWithMarkerFilter() {
        assumeTrue("JOIN on a subquery required", EsqlCapabilities.Cap.JOIN_ON_SUBQUERY.isEnabled());
        PhysicalPlan plan = distributedPlan("""
            FROM test
            | RENAME emp_no AS language_code
            | INNER JOIN (FROM languages_lookup) ON language_code
            | STATS c = COUNT(*) BY language_name
            | LIMIT 1000
            """, SHUFFLE_JOIN_SETTINGS);

        DistributedHashJoinExec join = plan.collect(DistributedHashJoinExec.class).getFirst();
        FilterExec filter = plan.collect(FilterExec.class)
            .stream()
            .filter(candidate -> candidate.condition() instanceof IsNotNull)
            .findFirst()
            .orElseThrow();
        assertTrue(filter.condition() instanceof IsNotNull);
        assertSame(join, filter.child());
        ProjectExec project = plan.collect(ProjectExec.class)
            .stream()
            .filter(candidate -> candidate.child() == filter)
            .findFirst()
            .orElseThrow();
        assertNotNull(project);
        assertTrue(join.addedFields().stream().anyMatch(Mapper::isJoinMarker));
    }

    public void testExpressionLookupJoinFallsBack() {
        PhysicalPlan plan = distributedPlan("""
            FROM test
            | LOOKUP JOIN languages_lookup ON emp_no == language_code AND language_name == "English"
            | STATS c = COUNT(*) BY language_name
            | LIMIT 1000
            """, SHUFFLE_JOIN_SETTINGS);

        assertFragmentJoinFallback(plan);
    }

    public void testMismatchedLookupKeyTypesFallBack() {
        String query = """
            FROM test
            | RENAME emp_no AS language_code
            | LOOKUP JOIN languages_lookup ON language_code
            | LIMIT 1000
            """;
        var configuration = configuration(new QueryPragmas(SHUFFLE_JOIN_SETTINGS), query);
        Analyzer analyzer = analyzer().minimumTransportVersion(TransportVersion.current())
            .configuration(configuration)
            .addIndex("test", "mapping-basic.json")
            .addIndex("languages_lookup", "mapping-languages.json")
            .addLookupIndex(AnalyzerTestUtils.defaultLookupResolution().get("languages_lookup"))
            .addNoFieldsIndex()
            .buildAnalyzer();
        Join join = (Join) analyzer.analyze(TEST_PARSER.parseQuery(query)).collect(Join.class::isInstance).getFirst();
        var rightKey = join.right()
            .output()
            .stream()
            .filter(attribute -> attribute.name().equals("language_name"))
            .findFirst()
            .orElseThrow();
        Join mismatched = join.withConfig(
            new JoinConfig(join.config().type(), join.config().leftFields(), List.of(rightKey), join.config().joinOnConditions())
        );
        PhysicalPlan plan = new Mapper(true).map(new Versioned<>(mismatched, analyzer.context().minimumVersion()));
        assertFragmentJoinFallback(plan);
    }

    public void testShuffleJoinDisabledKeepsJoinInDataFragment() {
        PhysicalPlan disabled = distributedPlan(
            DISTRIBUTED_QUERY,
            Settings.builder().put(QueryPragmas.SHUFFLE_PARTITIONS_PER_NODE.getKey(), 2).build()
        );
        assertFragmentJoinFallback(disabled);
    }

    private static void assertFragmentJoinFallback(PhysicalPlan plan) {
        assertFalse(plan.anyMatch(DistributedHashJoinExec.class::isInstance));
        assertTrue(plan.collect(FragmentExec.class).stream().anyMatch(fragment -> fragment.fragment().anyMatch(Join.class::isInstance)));
    }

    private static DistributedHashJoinExec distributedJoinUnderAggregate(PhysicalPlan plan) {
        return plan.collect(DistributedHashJoinExec.class).getFirst();
    }

    private static PhysicalPlan distributedPlan(String query, Settings settings) {
        var configuration = configuration(new QueryPragmas(settings), query);
        Analyzer analyzer = analyzer().minimumTransportVersion(TransportVersion.current())
            .configuration(configuration)
            .addIndex("test", "mapping-basic.json")
            .addIndex("languages_lookup", "mapping-languages.json")
            .addLookupIndex(AnalyzerTestUtils.defaultLookupResolution().get("languages_lookup"))
            .addNoFieldsIndex()
            .buildAnalyzer();
        return new TestPlannerOptimizer(configuration, analyzer).distributedPlan(query);
    }
}
