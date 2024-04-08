/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.analysis.Analyzer;
import org.elasticsearch.xpack.esql.analysis.AnalyzerContext;
import org.elasticsearch.xpack.esql.expression.function.EsqlFunctionRegistry;
import org.elasticsearch.xpack.esql.optimizer.LogicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.LogicalPlanOptimizer;
import org.elasticsearch.xpack.esql.optimizer.PhysicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.PhysicalPlanOptimizer;
import org.elasticsearch.xpack.esql.parser.EsqlParser;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.planner.Mapper;
import org.elasticsearch.xpack.esql.session.EsqlConfigurationSerializationTests;
import org.elasticsearch.xpack.ql.expression.function.FunctionRegistry;
import org.elasticsearch.xpack.ql.index.EsIndex;
import org.elasticsearch.xpack.ql.index.IndexResolution;
import org.elasticsearch.xpack.ql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.ql.type.EsField;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_CFG;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_VERIFIER;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.emptyPolicyResolution;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.loadMapping;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.withDefaultLimitWarning;

public class DataNodeRequestTests extends AbstractWireSerializingTestCase<DataNodeRequest> {

    @Override
    protected Writeable.Reader<DataNodeRequest> instanceReader() {
        return DataNodeRequest::new;
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        SearchModule searchModule = new SearchModule(Settings.EMPTY, List.of());
        return new NamedWriteableRegistry(searchModule.getNamedWriteables());
    }

    @Override
    protected DataNodeRequest createTestInstance() {
        var sessionId = randomAlphaOfLength(10);
        String query = randomFrom("""
            from test
            | where round(emp_no) > 10
            | eval c = salary
            | stats x = avg(c)
            """, """
            from test
            | sort last_name
            | limit 10
            | where round(emp_no) > 10
            | eval c = first_name
            | stats x = avg(salary)
            """);
        List<ShardId> shardIds = randomList(1, 10, () -> new ShardId("index-" + between(1, 10), "n/a", between(1, 10)));
        PhysicalPlan physicalPlan = mapAndMaybeOptimize(parse(query));
        Map<Index, AliasFilter> aliasFilters = Map.of(
            new Index("concrete-index", "n/a"),
            AliasFilter.of(new TermQueryBuilder("id", "1"), "alias-1")
        );
        DataNodeRequest request = new DataNodeRequest(
            sessionId,
            EsqlConfigurationSerializationTests.randomConfiguration(query),
            randomAlphaOfLength(10),
            shardIds,
            aliasFilters,
            physicalPlan
        );
        request.setParentTask(randomAlphaOfLength(10), randomNonNegativeLong());
        return request;
    }

    @Override
    protected DataNodeRequest mutateInstance(DataNodeRequest in) throws IOException {
        return switch (between(0, 6)) {
            case 0 -> {
                var request = new DataNodeRequest(
                    randomAlphaOfLength(20),
                    in.configuration(),
                    in.clusterAlias(),
                    in.shardIds(),
                    in.aliasFilters(),
                    in.plan()
                );
                request.setParentTask(in.getParentTask());
                yield request;
            }
            case 1 -> {
                var request = new DataNodeRequest(
                    in.sessionId(),
                    EsqlConfigurationSerializationTests.randomConfiguration(),
                    in.clusterAlias(),
                    in.shardIds(),
                    in.aliasFilters(),
                    in.plan()
                );
                request.setParentTask(in.getParentTask());
                yield request;
            }
            case 2 -> {
                List<ShardId> shardIds = randomList(1, 10, () -> new ShardId("new-index-" + between(1, 10), "n/a", between(1, 10)));
                var request = new DataNodeRequest(
                    in.sessionId(),
                    in.configuration(),
                    in.clusterAlias(),
                    shardIds,
                    in.aliasFilters(),
                    in.plan()
                );
                request.setParentTask(in.getParentTask());
                yield request;
            }
            case 3 -> {
                String newQuery = randomFrom("""
                    from test
                    | where round(emp_no) > 100
                    | eval c = salary
                    | stats x = avg(c)
                    """, """
                    from test
                    | sort last_name
                    | limit 10
                    | where round(emp_no) > 100
                    | eval c = first_name
                    | stats x = avg(salary)
                    """);
                var request = new DataNodeRequest(
                    in.sessionId(),
                    in.configuration(),
                    in.clusterAlias(),
                    in.shardIds(),
                    in.aliasFilters(),
                    mapAndMaybeOptimize(parse(newQuery))
                );
                request.setParentTask(in.getParentTask());
                yield request;
            }
            case 4 -> {
                final Map<Index, AliasFilter> aliasFilters;
                if (randomBoolean()) {
                    aliasFilters = Map.of();
                } else {
                    aliasFilters = Map.of(new Index("concrete-index", "n/a"), AliasFilter.of(new TermQueryBuilder("id", "2"), "alias-2"));
                }
                var request = new DataNodeRequest(
                    in.sessionId(),
                    in.configuration(),
                    in.clusterAlias(),
                    in.shardIds(),
                    aliasFilters,
                    in.plan()
                );
                request.setParentTask(request.getParentTask());
                yield request;
            }
            case 5 -> {
                var request = new DataNodeRequest(
                    in.sessionId(),
                    in.configuration(),
                    in.clusterAlias(),
                    in.shardIds(),
                    in.aliasFilters(),
                    in.plan()
                );
                request.setParentTask(
                    randomValueOtherThan(request.getParentTask().getNodeId(), () -> randomAlphaOfLength(10)),
                    randomNonNegativeLong()
                );
                yield request;
            }
            case 6 -> {
                var clusterAlias = randomValueOtherThan(in.clusterAlias(), () -> randomAlphaOfLength(10));
                var request = new DataNodeRequest(
                    in.sessionId(),
                    in.configuration(),
                    clusterAlias,
                    in.shardIds(),
                    in.aliasFilters(),
                    in.plan()
                );
                request.setParentTask(request.getParentTask());
                yield request;
            }
            default -> throw new AssertionError("invalid value");
        };
    }

    static LogicalPlan parse(String query) {
        Map<String, EsField> mapping = loadMapping("mapping-basic.json");
        EsIndex test = new EsIndex("test", mapping, Set.of("test"));
        IndexResolution getIndexResult = IndexResolution.valid(test);
        var logicalOptimizer = new LogicalPlanOptimizer(new LogicalOptimizerContext(TEST_CFG));
        var analyzer = new Analyzer(
            new AnalyzerContext(EsqlTestUtils.TEST_CFG, new EsqlFunctionRegistry(), getIndexResult, emptyPolicyResolution()),
            TEST_VERIFIER
        );
        return logicalOptimizer.optimize(analyzer.analyze(new EsqlParser().createStatement(query)));
    }

    static PhysicalPlan mapAndMaybeOptimize(LogicalPlan logicalPlan) {
        var physicalPlanOptimizer = new PhysicalPlanOptimizer(new PhysicalOptimizerContext(TEST_CFG));
        FunctionRegistry functionRegistry = new EsqlFunctionRegistry();
        var mapper = new Mapper(functionRegistry);
        var physical = mapper.map(logicalPlan);
        if (randomBoolean()) {
            physical = physicalPlanOptimizer.optimize(physical);
        }
        return physical;
    }

    @Override
    protected List<String> filteredWarnings() {
        return withDefaultLimitWarning(super.filteredWarnings());
    }
}
