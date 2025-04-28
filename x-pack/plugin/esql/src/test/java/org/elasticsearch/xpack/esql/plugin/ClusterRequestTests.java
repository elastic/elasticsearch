/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.xpack.esql.ConfigurationTestUtils;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.analysis.Analyzer;
import org.elasticsearch.xpack.esql.analysis.AnalyzerContext;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.expression.function.EsqlFunctionRegistry;
import org.elasticsearch.xpack.esql.index.EsIndex;
import org.elasticsearch.xpack.esql.index.IndexResolution;
import org.elasticsearch.xpack.esql.optimizer.LogicalPlanOptimizer;
import org.elasticsearch.xpack.esql.parser.EsqlParser;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.ConfigurationTestUtils.randomConfiguration;
import static org.elasticsearch.xpack.esql.ConfigurationTestUtils.randomTables;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_VERIFIER;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.emptyInferenceResolution;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.emptyPolicyResolution;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.loadMapping;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.unboundLogicalOptimizerContext;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.withDefaultLimitWarning;
import static org.hamcrest.Matchers.equalTo;

public class ClusterRequestTests extends AbstractWireSerializingTestCase<ClusterComputeRequest> {

    @Override
    protected Writeable.Reader<ClusterComputeRequest> instanceReader() {
        return ClusterComputeRequest::new;
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
                        randomValueOtherThan(plan.targetIndices(), () -> generateRandomStringArray(10, 10, false, false)),
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

    public void testFallbackIndicesOptions() throws Exception {
        ClusterComputeRequest request = createTestInstance();
        var oldVersion = TransportVersionUtils.randomVersionBetween(
            random(),
            TransportVersions.V_8_14_0,
            TransportVersionUtils.getPreviousVersion(TransportVersions.V_8_16_0)
        );
        ClusterComputeRequest cloned = copyInstance(request, oldVersion);
        assertThat(cloned.clusterAlias(), equalTo(request.clusterAlias()));
        assertThat(cloned.sessionId(), equalTo(request.sessionId()));
        RemoteClusterPlan plan = cloned.remoteClusterPlan();
        assertThat(plan.plan(), equalTo(request.remoteClusterPlan().plan()));
        assertThat(plan.targetIndices(), equalTo(request.remoteClusterPlan().targetIndices()));
        OriginalIndices originalIndices = plan.originalIndices();
        assertThat(originalIndices.indices(), equalTo(request.remoteClusterPlan().originalIndices().indices()));
        assertThat(originalIndices.indicesOptions(), equalTo(SearchRequest.DEFAULT_INDICES_OPTIONS));
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

    static LogicalPlan parse(String query) {
        Map<String, EsField> mapping = loadMapping("mapping-basic.json");
        EsIndex test = new EsIndex("test", mapping, Map.of("test", IndexMode.STANDARD));
        IndexResolution getIndexResult = IndexResolution.valid(test);
        var logicalOptimizer = new LogicalPlanOptimizer(unboundLogicalOptimizerContext());
        var analyzer = new Analyzer(
            new AnalyzerContext(
                EsqlTestUtils.TEST_CFG,
                new EsqlFunctionRegistry(),
                getIndexResult,
                emptyPolicyResolution(),
                emptyInferenceResolution()
            ),
            TEST_VERIFIER
        );
        return logicalOptimizer.optimize(analyzer.analyze(new EsqlParser().createStatement(query)));
    }

    @Override
    protected List<String> filteredWarnings() {
        return withDefaultLimitWarning(super.filteredWarnings());
    }
}
