/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.routing.SplitShardCountSummary;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.esql.SerializationTestUtils;
import org.elasticsearch.xpack.esql.analysis.Analyzer;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.expression.function.EsqlFunctionRegistry;
import org.elasticsearch.xpack.esql.index.EsIndex;
import org.elasticsearch.xpack.esql.index.EsIndexGenerator;
import org.elasticsearch.xpack.esql.optimizer.LogicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.LogicalPlanOptimizer;
import org.elasticsearch.xpack.esql.optimizer.PhysicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.PhysicalPlanOptimizer;
import org.elasticsearch.xpack.esql.parser.EsqlParser;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.planner.mapper.Mapper;
import org.elasticsearch.xpack.esql.session.Versioned;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.ConfigurationTestUtils.randomConfiguration;
import static org.elasticsearch.xpack.esql.ConfigurationTestUtils.randomTables;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_CFG;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_VERIFIER;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.emptyInferenceResolution;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.emptyPolicyResolution;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.loadMapping;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.testAnalyzerContext;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.withDefaultLimitWarning;
import static org.elasticsearch.xpack.esql.analysis.AnalyzerTestUtils.indexResolutions;

public class DataNodeRequestSerializationTests extends AbstractWireSerializingTestCase<DataNodeRequest> {
    @Override
    protected Writeable.Reader<DataNodeRequest> instanceReader() {
        return in -> new DataNodeRequest(in, new SerializationTestUtils.TestNameIdMapper());
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        List<NamedWriteableRegistry.Entry> writeables = new ArrayList<>();
        writeables.addAll(new SearchModule(Settings.EMPTY, List.of()).getNamedWriteables());
        writeables.addAll(new EsqlPlugin().getNamedWriteables());
        return new NamedWriteableRegistry(writeables);
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
        List<DataNodeRequest.Shard> shards = randomList(
            1,
            10,
            () -> new DataNodeRequest.Shard(
                new ShardId("index-" + between(1, 10), "n/a", between(1, 10)),
                SplitShardCountSummary.fromInt(randomIntBetween(0, 1024))
            )
        );
        PhysicalPlan physicalPlan = mapAndMaybeOptimize(parse(query));
        Map<Index, AliasFilter> aliasFilters = Map.of(
            new Index("concrete-index", "n/a"),
            AliasFilter.of(new TermQueryBuilder("id", "1"), "alias-1")
        );
        DataNodeRequest request = new DataNodeRequest(
            sessionId,
            randomConfiguration(query, randomTables()),
            randomAlphaOfLength(10),
            shards,
            aliasFilters,
            physicalPlan,
            generateRandomStringArray(10, 10, false, false),
            IndicesOptions.fromOptions(randomBoolean(), randomBoolean(), randomBoolean(), randomBoolean()),
            randomBoolean(),
            randomBoolean()
        );
        request.setParentTask(randomAlphaOfLength(10), randomNonNegativeLong());
        return request;
    }

    @Override
    protected DataNodeRequest mutateInstance(DataNodeRequest in) throws IOException {
        return switch (between(0, 9)) {
            case 0 -> {
                var request = new DataNodeRequest(
                    randomAlphaOfLength(20),
                    in.configuration(),
                    in.clusterAlias(),
                    in.shards(),
                    in.aliasFilters(),
                    in.plan(),
                    in.indices(),
                    in.indicesOptions(),
                    in.runNodeLevelReduction(),
                    in.reductionLateMaterialization()
                );
                request.setParentTask(in.getParentTask());
                yield request;
            }
            case 1 -> {
                var request = new DataNodeRequest(
                    in.sessionId(),
                    randomConfiguration(),
                    in.clusterAlias(),
                    in.shards(),
                    in.aliasFilters(),
                    in.plan(),
                    in.indices(),
                    in.indicesOptions(),
                    in.runNodeLevelReduction(),
                    in.reductionLateMaterialization()
                );
                request.setParentTask(in.getParentTask());
                yield request;
            }
            case 2 -> {
                List<DataNodeRequest.Shard> shards = randomList(
                    1,
                    10,
                    () -> new DataNodeRequest.Shard(
                        new ShardId("new-index-" + between(1, 10), "n/a", between(1, 10)),
                        SplitShardCountSummary.fromInt(randomIntBetween(0, 1024))
                    )
                );
                var request = new DataNodeRequest(
                    in.sessionId(),
                    in.configuration(),
                    in.clusterAlias(),
                    shards,
                    in.aliasFilters(),
                    in.plan(),
                    in.indices(),
                    in.indicesOptions(),
                    in.runNodeLevelReduction(),
                    in.reductionLateMaterialization()
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
                    in.shards(),
                    in.aliasFilters(),
                    mapAndMaybeOptimize(parse(newQuery)),
                    in.indices(),
                    in.indicesOptions(),
                    in.runNodeLevelReduction(),
                    in.reductionLateMaterialization()
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
                    in.shards(),
                    aliasFilters,
                    in.plan(),
                    in.indices(),
                    in.indicesOptions(),
                    in.runNodeLevelReduction(),
                    in.reductionLateMaterialization()
                );
                request.setParentTask(request.getParentTask());
                yield request;
            }
            case 5 -> {
                var request = new DataNodeRequest(
                    in.sessionId(),
                    in.configuration(),
                    in.clusterAlias(),
                    in.shards(),
                    in.aliasFilters(),
                    in.plan(),
                    in.indices(),
                    in.indicesOptions(),
                    in.runNodeLevelReduction(),
                    in.reductionLateMaterialization()
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
                    in.shards(),
                    in.aliasFilters(),
                    in.plan(),
                    in.indices(),
                    in.indicesOptions(),
                    in.runNodeLevelReduction(),
                    in.reductionLateMaterialization()
                );
                request.setParentTask(request.getParentTask());
                yield request;
            }
            case 7 -> {
                var indices = randomArrayOtherThan(in.indices(), () -> generateRandomStringArray(10, 10, false, false));
                var request = new DataNodeRequest(
                    in.sessionId(),
                    in.configuration(),
                    in.clusterAlias(),
                    in.shards(),
                    in.aliasFilters(),
                    in.plan(),
                    indices,
                    in.indicesOptions(),
                    in.runNodeLevelReduction(),
                    in.reductionLateMaterialization()
                );
                request.setParentTask(request.getParentTask());
                yield request;
            }
            case 8 -> {
                var indicesOptions = randomValueOtherThan(
                    in.indicesOptions(),
                    () -> IndicesOptions.fromOptions(randomBoolean(), randomBoolean(), randomBoolean(), randomBoolean())
                );
                var request = new DataNodeRequest(
                    in.sessionId(),
                    in.configuration(),
                    in.clusterAlias(),
                    in.shards(),
                    in.aliasFilters(),
                    in.plan(),
                    in.indices(),
                    indicesOptions,
                    in.runNodeLevelReduction(),
                    in.reductionLateMaterialization()
                );
                request.setParentTask(request.getParentTask());
                yield request;
            }
            case 9 -> {
                var request = new DataNodeRequest(
                    in.sessionId(),
                    in.configuration(),
                    in.clusterAlias(),
                    in.shards(),
                    in.aliasFilters(),
                    in.plan(),
                    in.indices(),
                    in.indicesOptions(),
                    in.runNodeLevelReduction() == false,
                    in.reductionLateMaterialization() == false
                );
                request.setParentTask(request.getParentTask());
                yield request;
            }
            default -> throw new AssertionError("invalid value");
        };
    }

    static Versioned<LogicalPlan> parse(String query) {
        Map<String, EsField> mapping = loadMapping("mapping-basic.json");
        EsIndex test = EsIndexGenerator.esIndex("test", mapping, Map.of("test", IndexMode.STANDARD));
        var analyzer = new Analyzer(
            testAnalyzerContext(
                TEST_CFG,
                new EsqlFunctionRegistry(),
                indexResolutions(test),
                emptyPolicyResolution(),
                emptyInferenceResolution()
            ),
            TEST_VERIFIER
        );
        TransportVersion minimumVersion = analyzer.context().minimumVersion();
        var logicalOptimizer = new LogicalPlanOptimizer(new LogicalOptimizerContext(TEST_CFG, FoldContext.small(), minimumVersion));
        return new Versioned<>(logicalOptimizer.optimize(analyzer.analyze(new EsqlParser().createStatement(query))), minimumVersion);
    }

    static PhysicalPlan mapAndMaybeOptimize(Versioned<LogicalPlan> logicalPlan) {
        var physicalPlanOptimizer = new PhysicalPlanOptimizer(new PhysicalOptimizerContext(TEST_CFG, logicalPlan.minimumVersion()));
        var mapper = new Mapper();
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
