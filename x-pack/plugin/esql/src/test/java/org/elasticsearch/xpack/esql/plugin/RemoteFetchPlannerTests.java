/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.CsvTestsDataLoader;
import org.elasticsearch.xpack.esql.TestAnalyzer;
import org.elasticsearch.xpack.esql.analysis.Analyzer;
import org.elasticsearch.xpack.esql.analysis.PreAnalyzer;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.optimizer.GoldenTestCase;
import org.elasticsearch.xpack.esql.optimizer.LocalPhysicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.LogicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.LogicalPlanOptimizer;
import org.elasticsearch.xpack.esql.optimizer.PhysicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.PhysicalPlanOptimizer;
import org.elasticsearch.xpack.esql.plan.EsqlStatement;
import org.elasticsearch.xpack.esql.plan.IndexPattern;
import org.elasticsearch.xpack.esql.plan.QuerySettings;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.EmitRemoteFetchHandleExec;
import org.elasticsearch.xpack.esql.plan.physical.EsQueryExec;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeSinkExec;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.FilterExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.ProjectExec;
import org.elasticsearch.xpack.esql.plan.physical.RemoteFetchExec;
import org.elasticsearch.xpack.esql.plan.physical.RemoteFetchSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.UnaryExec;
import org.elasticsearch.xpack.esql.planner.PlannerSettings;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;
import org.elasticsearch.xpack.esql.planner.mapper.Mapper;
import org.elasticsearch.xpack.esql.session.Configuration;
import org.elasticsearch.xpack.esql.session.Versioned;
import org.elasticsearch.xpack.esql.stats.SearchStats;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static org.elasticsearch.xpack.esql.CsvTestsDataLoader.CSV_DATASET;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_PARSER;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.analyzer;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.instanceOf;

public class RemoteFetchPlannerTests extends ESTestCase {
    public void testCoordinatorPlanIsRewrittenToRemoteFetch() {
        PlannedQuery planned = planQuery("""
            FROM employees
            | KEEP hire_date, salary
            | SORT hire_date
            | LIMIT 5
            """);
        Tuple<PhysicalPlan, PhysicalPlan> split = PlannerUtils.breakPlanBetweenCoordinatorAndDataNode(
            planned.physicalPlan(),
            planned.configuration()
        );

        PhysicalPlan coordinatorPlan = RemoteFetchPlanner.planCoordinatorTopN(
            contextFactory(planned.configuration()),
            split.v1(),
            (ExchangeSinkExec) split.v2()
        ).orElseThrow();

        ProjectExec projectExec = (ProjectExec) coordinatorPlan;
        assertThat(projectExec.child(), instanceOf(RemoteFetchExec.class));

        RemoteFetchExec remoteFetchExec = (RemoteFetchExec) projectExec.child();
        assertThat(remoteFetchExec.attributesToFetch().stream().map(Attribute::name).toList(), contains("salary"));

        ExchangeSourceExec exchangeSource = singleValue(remoteFetchExec.child().collect(ExchangeSourceExec.class));
        assertTrue(
            exchangeSource.output().stream().anyMatch(attribute -> RemoteFetchPlanner.REMOTE_FETCH_HANDLE_NAME.equals(attribute.name()))
        );
        assertFalse(exchangeSource.output().stream().anyMatch(EsQueryExec::isDocAttribute));
    }

    public void testReductionPlanEmitsRemoteFetchHandles() {
        PlannedQuery planned = planQuery("""
            FROM employees
            | KEEP hire_date, salary
            | SORT hire_date
            | LIMIT 5
            """);
        Tuple<PhysicalPlan, PhysicalPlan> split = PlannerUtils.breakPlanBetweenCoordinatorAndDataNode(
            planned.physicalPlan(),
            planned.configuration()
        );

        ReductionPlan reductionPlan = RemoteFetchPlanner.planReduceDriverTopN(
            contextFactory(planned.configuration()),
            (ExchangeSinkExec) split.v2()
        ).orElseThrow();

        assertThat(reductionPlan.nodeReducePlan().child(), instanceOf(EmitRemoteFetchHandleExec.class));
        EmitRemoteFetchHandleExec emitRemoteFetchHandleExec = (EmitRemoteFetchHandleExec) reductionPlan.nodeReducePlan().child();
        assertTrue(EsQueryExec.isDocAttribute(emitRemoteFetchHandleExec.sourceAttribute()));
        assertEquals(RemoteFetchPlanner.REMOTE_FETCH_HANDLE_NAME, emitRemoteFetchHandleExec.handleAttribute().name());
        assertTrue(reductionPlan.dataNodePlan().output().stream().anyMatch(EsQueryExec::isDocAttribute));
        assertFalse(reductionPlan.nodeReducePlan().output().stream().anyMatch(EsQueryExec::isDocAttribute));
    }

    public void testChainedPushdownsReuseRemoteFetchPositionIdentity() {
        ReferenceAttribute handleAttribute = new ReferenceAttribute(
            Source.EMPTY,
            null,
            RemoteFetchPlanner.REMOTE_FETCH_HANDLE_NAME,
            DataType.KEYWORD
        );
        ReferenceAttribute fetchedAttribute = new ReferenceAttribute(Source.EMPTY, null, "salary", DataType.LONG);
        ReferenceAttribute existingPositionAttribute = new ReferenceAttribute(
            Source.EMPTY,
            null,
            RemoteFetchPlanner.REMOTE_FETCH_POSITION_NAME,
            DataType.INTEGER
        );
        PhysicalPlan existingPushdown = new ProjectExec(
            Source.EMPTY,
            new RemoteFetchSourceExec(Source.EMPTY, List.of(fetchedAttribute, existingPositionAttribute)),
            List.of(fetchedAttribute, existingPositionAttribute)
        );
        RemoteFetchExec remoteFetchExec = new RemoteFetchExec(
            Source.EMPTY,
            new ExchangeSourceExec(Source.EMPTY, List.of(handleAttribute), false),
            handleAttribute,
            List.of(fetchedAttribute),
            List.of(fetchedAttribute),
            existingPushdown
        );
        ProjectExec nextPushdown = new ProjectExec(Source.EMPTY, remoteFetchExec.child(), List.of(fetchedAttribute));
        RemoteFetchExec appendedPushdownRemoteFetch = invokeAppendPushdown(remoteFetchExec, nextPushdown);
        assertNotNull(appendedPushdownRemoteFetch.pushdownPlan());

        Set<NameId> positionAttributeIds = new HashSet<>();
        collectPositionAttributeIds(appendedPushdownRemoteFetch.pushdownPlan(), positionAttributeIds);
        assertEquals(1, positionAttributeIds.size());
    }

    public void testPushdownPositionStrippingKeepsUserNamedField() {
        ReferenceAttribute handleAttribute = new ReferenceAttribute(
            Source.EMPTY,
            null,
            RemoteFetchPlanner.REMOTE_FETCH_HANDLE_NAME,
            DataType.KEYWORD
        );
        ReferenceAttribute fetchedAttribute = new ReferenceAttribute(Source.EMPTY, null, "salary", DataType.LONG);
        ReferenceAttribute userNamedPositionAttribute = new ReferenceAttribute(
            Source.EMPTY,
            null,
            RemoteFetchPlanner.REMOTE_FETCH_POSITION_NAME,
            DataType.LONG
        );
        RemoteFetchExec remoteFetchExec = new RemoteFetchExec(
            Source.EMPTY,
            new ExchangeSourceExec(Source.EMPTY, List.of(handleAttribute), false),
            handleAttribute,
            List.of(fetchedAttribute, userNamedPositionAttribute),
            List.of(fetchedAttribute, userNamedPositionAttribute),
            null
        );
        ProjectExec pushdown = new ProjectExec(
            Source.EMPTY,
            remoteFetchExec.child(),
            List.of(fetchedAttribute, userNamedPositionAttribute)
        );
        RemoteFetchExec appendedPushdownRemoteFetch = invokeAppendPushdown(remoteFetchExec, pushdown);

        assertTrue(
            appendedPushdownRemoteFetch.fetchedOutputAttributes().stream().anyMatch(a -> a.id().equals(userNamedPositionAttribute.id()))
        );
    }

    public void testLimitBoundaryStillPushesDownFilter() {
        PlannedQuery planned = planQuery("""
            FROM employees
            | SORT hire_date DESC
            | LIMIT 5
            | WHERE salary > 1000
            | KEEP hire_date, salary
            """);
        Tuple<PhysicalPlan, PhysicalPlan> split = PlannerUtils.breakPlanBetweenCoordinatorAndDataNode(
            planned.physicalPlan(),
            planned.configuration()
        );

        PhysicalPlan coordinatorPlan = RemoteFetchPlanner.planCoordinatorTopN(
            contextFactory(planned.configuration()),
            split.v1(),
            (ExchangeSinkExec) split.v2()
        ).orElseThrow();

        RemoteFetchExec remoteFetchExec = singleValue(coordinatorPlan.collect(RemoteFetchExec.class));
        assertNotNull(remoteFetchExec.pushdownPlan());
        assertFalse(remoteFetchExec.pushdownPlan().collect(FilterExec.class).isEmpty());
        assertTrue(remoteFetchExec.child().collect(FilterExec.class).isEmpty());
    }

    private PlannedQuery planQuery(String query) {
        TransportVersion transportVersion = TransportVersion.current();
        EsqlStatement statement = TEST_PARSER.createStatement(query);
        LogicalPlan parsedPlan = statement.plan();
        TestAnalyzer testAnalyzer = analyzer().addLanguagesLookup()
            .addAnalysisTestsEnrichResolution()
            .addAnalysisTestsInferenceResolution()
            .minimumTransportVersion(transportVersion)
            .unmappedResolution(statement.setting(QuerySettings.UNMAPPED_FIELDS));
        GoldenTestCase.loadIndexResolution(testDatasets(parsedPlan))
            .forEach((pattern, resolution) -> testAnalyzer.addIndex(pattern.indexPattern(), resolution));
        Analyzer analyzer = testAnalyzer.buildAnalyzer();
        Configuration configuration = analyzer.context().configuration();
        LogicalPlan logicalPlan = new LogicalPlanOptimizer(
            new LogicalOptimizerContext(configuration, configuration.newFoldContext(), transportVersion)
        ).optimize(analyzer.analyze(parsedPlan));
        PhysicalPlan physicalPlan = new PhysicalPlanOptimizer(new PhysicalOptimizerContext(configuration, transportVersion)).optimize(
            new Mapper().map(new Versioned<>(logicalPlan, transportVersion))
        );
        return new PlannedQuery(configuration, physicalPlan);
    }

    private static Function<SearchStats, LocalPhysicalOptimizerContext> contextFactory(Configuration configuration) {
        return stats -> new LocalPhysicalOptimizerContext(
            PlannerSettings.DEFAULTS,
            new EsqlFlags(false),
            configuration,
            configuration.newFoldContext(),
            stats
        );
    }

    private static Map<IndexPattern, CsvTestsDataLoader.MultiIndexTestDataset> testDatasets(LogicalPlan parsed) {
        var preAnalysis = new PreAnalyzer().preAnalyze(parsed);
        if (preAnalysis.indexes().isEmpty()) {
            return Map.of(
                new IndexPattern(Source.EMPTY, "employees"),
                CsvTestsDataLoader.MultiIndexTestDataset.of(CSV_DATASET.get("employees"))
            );
        }

        List<String> missing = new ArrayList<>();
        Map<IndexPattern, CsvTestsDataLoader.MultiIndexTestDataset> all = new HashMap<>();
        for (IndexPattern indexPattern : preAnalysis.indexes().keySet()) {
            List<CsvTestsDataLoader.TestDataset> datasets = new ArrayList<>();
            String indexName = indexPattern.indexPattern();
            if (indexName.endsWith("*")) {
                String indexPrefix = indexName.substring(0, indexName.length() - 1);
                for (var entry : CSV_DATASET.entrySet()) {
                    if (entry.getKey().startsWith(indexPrefix)) {
                        datasets.add(entry.getValue());
                    }
                }
            } else {
                for (String index : indexName.split(",")) {
                    var dataset = CSV_DATASET.get(index);
                    if (dataset == null) {
                        throw new IllegalArgumentException("unknown CSV dataset for table [" + index + "]");
                    }
                    datasets.add(dataset);
                }
            }
            if (datasets.isEmpty() == false) {
                all.put(indexPattern, new CsvTestsDataLoader.MultiIndexTestDataset(indexName, datasets));
            } else {
                missing.add(indexName);
            }
        }
        if (all.isEmpty()) {
            throw new IllegalArgumentException("Found no CSV datasets for table [" + preAnalysis.indexes() + "]");
        }
        if (missing.isEmpty() == false) {
            throw new IllegalArgumentException("Did not find datasets for tables: " + missing);
        }
        return all;
    }

    private static <T> T singleValue(List<T> values) {
        assertEquals(1, values.size());
        return values.get(0);
    }

    private static void collectPositionAttributeIds(PhysicalPlan plan, Set<NameId> ids) {
        for (Attribute attribute : plan.output()) {
            if (RemoteFetchPlanner.REMOTE_FETCH_POSITION_NAME.equals(attribute.name())) {
                ids.add(attribute.id());
            }
        }
        for (PhysicalPlan child : plan.children()) {
            collectPositionAttributeIds(child, ids);
        }
    }

    private static RemoteFetchExec invokeAppendPushdown(RemoteFetchExec remoteFetchExec, UnaryExec pushdownNode) {
        return RemoteFetchPlanner.appendPushdown(remoteFetchExec, pushdownNode);
    }

    private record PlannedQuery(Configuration configuration, PhysicalPlan physicalPlan) {}
}
