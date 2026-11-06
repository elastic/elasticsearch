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
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.xpack.esql.ConfigurationTestUtils;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.SerializationTestUtils;
import org.elasticsearch.xpack.esql.analysis.Analyzer;
import org.elasticsearch.xpack.esql.analysis.AnalyzerContext;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.expression.promql.function.PromqlFunctionRegistry;
import org.elasticsearch.xpack.esql.index.EsIndex;
import org.elasticsearch.xpack.esql.index.EsIndexGenerator;
import org.elasticsearch.xpack.esql.optimizer.LogicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.LogicalPlanOptimizer;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.session.Versioned;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.ConfigurationTestUtils.randomConfiguration;
import static org.elasticsearch.xpack.esql.ConfigurationTestUtils.randomTables;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_FUNCTION_REGISTRY;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_PARSER;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_VERIFIER;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.emptyInferenceResolution;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.emptyPolicyResolution;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.loadMapping;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.unboundLogicalOptimizerContext;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.withDefaultLimitWarning;
import static org.elasticsearch.xpack.esql.analysis.AnalyzerTestUtils.indexResolutions;
import static org.elasticsearch.xpack.esql.plan.QuerySettings.UNMAPPED_FIELDS;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.nullValue;

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
            new RemoteClusterPlan(physicalPlan, targetIndices, originalIndices),
            randomFetchContextsKeepAlive()
        );
        request.setParentTask(randomAlphaOfLength(10), randomNonNegativeLong());
        return request;
    }

    @Override
    protected ClusterComputeRequest mutateInstance(ClusterComputeRequest in) throws IOException {
        return switch (between(0, 5)) {
            case 0 -> {
                var request = new ClusterComputeRequest(
                    randomValueOtherThan(in.clusterAlias(), () -> randomAlphaOfLength(10)),
                    in.sessionId(),
                    in.configuration(),
                    in.remoteClusterPlan(),
                    in.fetchContextsKeepAlive()
                );
                request.setParentTask(in.getParentTask());
                yield request;
            }
            case 1 -> {
                var request = new ClusterComputeRequest(
                    in.clusterAlias(),
                    randomValueOtherThan(in.sessionId(), () -> randomAlphaOfLength(10)),
                    in.configuration(),
                    in.remoteClusterPlan(),
                    in.fetchContextsKeepAlive()
                );
                request.setParentTask(in.getParentTask());
                yield request;
            }
            case 2 -> {
                var request = new ClusterComputeRequest(
                    in.clusterAlias(),
                    in.sessionId(),
                    randomValueOtherThan(in.configuration(), ConfigurationTestUtils::randomConfiguration),
                    in.remoteClusterPlan(),
                    in.fetchContextsKeepAlive()
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
                    ),
                    in.fetchContextsKeepAlive()
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
                    ),
                    in.fetchContextsKeepAlive()
                );
                request.setParentTask(in.getParentTask());
                yield request;
            }
            case 5 -> {
                var request = new ClusterComputeRequest(
                    in.clusterAlias(),
                    in.sessionId(),
                    in.configuration(),
                    in.remoteClusterPlan(),
                    randomValueOtherThan(in.fetchContextsKeepAlive(), ClusterRequestTests::randomFetchContextsKeepAlive)
                );
                request.setParentTask(in.getParentTask());
                yield request;
            }
            default -> throw new AssertionError("invalid value");
        };
    }

    @Nullable
    private static TimeValue randomFetchContextsKeepAlive() {
        return randomBoolean() ? null : TimeValue.timeValueSeconds(between(1, 600));
    }

    /**
     * A cluster without the fetch phase can't keep fetch contexts, so the request refuses to ask one for them, rather than
     * leaving out what its plan depends on.
     */
    public void testAClusterWithoutTheFetchPhaseIsNotAskedForFetchContexts() {
        ClusterComputeRequest request = createTestInstance();
        ClusterComputeRequest withContexts = new ClusterComputeRequest(
            request.clusterAlias(),
            request.sessionId(),
            request.configuration(),
            request.remoteClusterPlan(),
            TimeValue.timeValueMinutes(5)
        );
        TransportVersion older = TransportVersionUtils.getPreviousVersion(DataNodeRequest.ESQL_FETCH_CONTEXTS);

        IllegalStateException e = expectThrows(IllegalStateException.class, () -> copyInstance(withContexts, older));

        assertThat(e.getMessage(), containsString("for fetch contexts"));
    }

    /**
     * A request that keeps no fetch contexts still reaches a cluster without the fetch phase.
     */
    public void testARequestWithoutFetchContextsReachesAnOlderCluster() throws IOException {
        ClusterComputeRequest request = createTestInstance();
        ClusterComputeRequest withoutContexts = new ClusterComputeRequest(
            request.clusterAlias(),
            request.sessionId(),
            request.configuration(),
            request.remoteClusterPlan(),
            null
        );
        withoutContexts.setParentTask(request.getParentTask());
        TransportVersion older = TransportVersionUtils.getPreviousVersion(DataNodeRequest.ESQL_FETCH_CONTEXTS);

        ClusterComputeRequest copy = copyInstance(withoutContexts, older);

        assertThat(copy.fetchContextsKeepAlive(), nullValue());
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
        Map<String, EsField> mapping = loadMapping("mapping-basic.json");
        EsIndex test = EsIndexGenerator.esIndex("test", mapping, Map.of("test", IndexMode.STANDARD));
        LogicalOptimizerContext context = unboundLogicalOptimizerContext();
        TransportVersion minimumVersion = context.minimumVersion();
        var logicalOptimizer = new LogicalPlanOptimizer(context);
        var analyzer = new Analyzer(
            new AnalyzerContext(
                EsqlTestUtils.TEST_CFG,
                TEST_FUNCTION_REGISTRY,
                PromqlFunctionRegistry.INSTANCE,
                EsqlTestUtils.TEST_ANALYSIS_REGISTRY,
                indexResolutions(test),
                Map.of(),
                emptyPolicyResolution(),
                emptyInferenceResolution(),
                minimumVersion,
                UNMAPPED_FIELDS.defaultValue()
            ),
            TEST_VERIFIER
        );
        return new Versioned<>(logicalOptimizer.optimize(analyzer.analyze(TEST_PARSER.parseQuery(query))), minimumVersion);
    }

    @Override
    protected List<String> filteredWarnings() {
        return withDefaultLimitWarning(super.filteredWarnings());
    }
}
