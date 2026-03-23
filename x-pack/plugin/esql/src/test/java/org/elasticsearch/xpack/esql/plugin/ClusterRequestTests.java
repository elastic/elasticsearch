/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.esql.ConfigurationTestUtils;
import org.elasticsearch.xpack.esql.SerializationTestUtils;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.optimizer.LogicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.LogicalPlanOptimizer;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.session.Versioned;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.xpack.esql.ConfigurationTestUtils.randomConfiguration;
import static org.elasticsearch.xpack.esql.ConfigurationTestUtils.randomTables;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_CFG;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_PARSER;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.analyzer;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.withDefaultLimitWarning;

public class ClusterRequestTests extends AbstractWireSerializingTestCase<ClusterComputeRequest> {

    @Override
    protected Writeable.Reader<ClusterComputeRequest> instanceReader() {
        return (in) -> new ClusterComputeRequest(in, new SerializationTestUtils.TestNameIdMapper());
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        List<NamedWriteableRegistry.Entry> writeables = new ArrayList<>();
        writeables.addAll(new SearchModule(Settings.EMPTY, List.of()).getNamedWriteables());
        writeables.addAll(new EsqlPlugin().getNamedWriteables());
        return new NamedWriteableRegistry(writeables);
    }

    @Override
    protected ClusterComputeRequest createTestInstance() {
        var sessionId = randomAlphaOfLength(10);
        String query = randomQuery();
        PhysicalPlan physicalPlan = DataNodeRequestSerializationTests.mapAndMaybeOptimize(parse(query));
        OriginalIndices originalIndices = new OriginalIndices(
            generateRandomStringArray(10, 10, false, false),
            IndicesOptions.fromOptions(randomBoolean(), randomBoolean(), randomBoolean(), randomBoolean())
        );
        String[] targetIndices = generateRandomStringArray(10, 10, false, false);
        ClusterComputeRequest request = new ClusterComputeRequest(
            randomAlphaOfLength(10),
            sessionId,
            randomConfiguration(query, randomTables()),
            new RemoteClusterPlan(physicalPlan, targetIndices, originalIndices)
        );
        request.setParentTask(randomAlphaOfLength(10), randomNonNegativeLong());
        return request;
    }

    @Override
    protected ClusterComputeRequest mutateInstance(ClusterComputeRequest in) throws IOException {
        return switch (between(0, 4)) {
            case 0 -> {
                var request = new ClusterComputeRequest(
                    randomValueOtherThan(in.clusterAlias(), () -> randomAlphaOfLength(10)),
                    in.sessionId(),
                    in.configuration(),
                    in.remoteClusterPlan()
                );
                request.setParentTask(in.getParentTask());
                yield request;
            }
            case 1 -> {
                var request = new ClusterComputeRequest(
                    in.clusterAlias(),
                    randomValueOtherThan(in.sessionId(), () -> randomAlphaOfLength(10)),
                    in.configuration(),
                    in.remoteClusterPlan()
                );
                request.setParentTask(in.getParentTask());
                yield request;
            }
            case 2 -> {
                var request = new ClusterComputeRequest(
                    in.clusterAlias(),
                    in.sessionId(),
                    randomValueOtherThan(in.configuration(), ConfigurationTestUtils::randomConfiguration),
                    in.remoteClusterPlan()
                );
                request.setParentTask(in.getParentTask());
                yield request;
            }
            case 3 -> {
                RemoteClusterPlan plan = in.remoteClusterPlan();
                var request = new ClusterComputeRequest(
                    in.clusterAlias(),
                    in.sessionId(),
                    in.configuration(),
                    new RemoteClusterPlan(
                        plan.plan(),
                        randomArrayOtherThan(plan.targetIndices(), () -> generateRandomStringArray(10, 10, false, false)),
                        plan.originalIndices()
                    )
                );
                request.setParentTask(in.getParentTask());
                yield request;
            }
            case 4 -> {
                RemoteClusterPlan plan = in.remoteClusterPlan();
                var request = new ClusterComputeRequest(
                    in.clusterAlias(),
                    in.sessionId(),
                    in.configuration(),
                    new RemoteClusterPlan(
                        plan.plan(),
                        plan.targetIndices(),
                        new OriginalIndices(
                            plan.originalIndices().indices(),
                            randomValueOtherThan(
                                plan.originalIndices().indicesOptions(),
                                () -> IndicesOptions.fromOptions(randomBoolean(), randomBoolean(), randomBoolean(), randomBoolean())
                            )
                        )
                    )
                );
                request.setParentTask(in.getParentTask());
                yield request;
            }
            default -> throw new AssertionError("invalid value");
        };
    }

    private static String randomQuery() {
        return randomFrom("""
            from test
            | where round(emp_no) > 10
            | limit 10
            """, """
            from test
            | sort last_name
            | limit 10
            | where round(emp_no) > 10
            | eval c = first_name
            """);
    }

    static Versioned<LogicalPlan> parse(String query) {
        var analyzer = analyzer().addEmployees("test").buildAnalyzer();
        TransportVersion minimumVersion = analyzer.context().minimumVersion();
        var logicalOptimizer = new LogicalPlanOptimizer(new LogicalOptimizerContext(TEST_CFG, FoldContext.small(), minimumVersion));
        return new Versioned<>(logicalOptimizer.optimize(analyzer.analyze(TEST_PARSER.parseQuery(query))), minimumVersion);
    }

    @Override
    protected List<String> filteredWarnings() {
        return withDefaultLimitWarning(super.filteredWarnings());
    }
}
