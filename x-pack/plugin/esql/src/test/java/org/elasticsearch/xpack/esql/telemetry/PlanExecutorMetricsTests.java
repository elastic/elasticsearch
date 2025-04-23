/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.telemetry;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.fieldcaps.FieldCapabilities;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesBuilder;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesIndexResponse;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesResponse;
import org.elasticsearch.action.fieldcaps.IndexFieldCapabilitiesBuilder;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.SlowLogFieldProvider;
import org.elasticsearch.index.SlowLogFields;
import org.elasticsearch.indices.IndicesExpressionGrouper;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.action.EsqlExecutionInfo;
import org.elasticsearch.xpack.esql.action.EsqlQueryRequest;
import org.elasticsearch.xpack.esql.action.EsqlResolveFieldsAction;
import org.elasticsearch.xpack.esql.analysis.EnrichResolution;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.enrich.EnrichPolicyResolver;
import org.elasticsearch.xpack.esql.execution.PlanExecutor;
import org.elasticsearch.xpack.esql.plugin.EsqlPlugin;
import org.elasticsearch.xpack.esql.querylog.EsqlQueryLog;
import org.elasticsearch.xpack.esql.session.EsqlSession;
import org.elasticsearch.xpack.esql.session.IndexResolver;
import org.elasticsearch.xpack.esql.session.Result;
import org.junit.After;
import org.junit.Before;
import org.mockito.stubbing.Answer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.withDefaultLimitWarning;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PlanExecutorMetricsTests extends ESTestCase {

    private ThreadPool threadPool;

    @Before
    public void setUpThreadPool() throws Exception {
        threadPool = new TestThreadPool(PlanExecutorMetricsTests.class.getSimpleName());
    }

    @After
    public void shutdownThreadPool() throws Exception {
        terminate(threadPool);
    }

    @SuppressWarnings("unchecked")
    EnrichPolicyResolver mockEnrichResolver() {
        EnrichPolicyResolver enrichResolver = mock(EnrichPolicyResolver.class);
        doAnswer(invocation -> {
            Object[] arguments = invocation.getArguments();
            ActionListener<EnrichResolution> listener = (ActionListener<EnrichResolution>) arguments[arguments.length - 1];
            listener.onResponse(new EnrichResolution());
            return null;
        }).when(enrichResolver).resolvePolicies(any(), any(), any());
        return enrichResolver;
    }

    EsqlQueryLog mockQueryLog() {
        ClusterSettings clusterSettings = new ClusterSettings(
            Settings.EMPTY,
            new HashSet<>(
                Arrays.asList(
                    EsqlPlugin.ESQL_QUERYLOG_THRESHOLD_WARN_SETTING,
                    EsqlPlugin.ESQL_QUERYLOG_THRESHOLD_INFO_SETTING,
                    EsqlPlugin.ESQL_QUERYLOG_THRESHOLD_DEBUG_SETTING,
                    EsqlPlugin.ESQL_QUERYLOG_THRESHOLD_TRACE_SETTING,
                    EsqlPlugin.ESQL_QUERYLOG_INCLUDE_USER_SETTING
                )
            )
        );
        return new EsqlQueryLog(clusterSettings, new SlowLogFieldProvider() {
            @Override
            public SlowLogFields create(IndexSettings indexSettings) {
                return create();
            }

            @Override
            public SlowLogFields create() {
                return new SlowLogFields() {
                    @Override
                    public Map<String, String> indexFields() {
                        return Map.of();
                    }

                    @Override
                    public Map<String, String> searchFields() {
                        return Map.of();
                    }
                };
            }
        });
    }

    public void testFailedMetric() {
        String[] indices = new String[] { "test" };

        Client qlClient = mock(Client.class);
        IndexResolver idxResolver = new IndexResolver(qlClient);
        // simulate a valid field_caps response so we can parse and correctly analyze de query
        FieldCapabilitiesResponse fieldCapabilitiesResponse = mock(FieldCapabilitiesResponse.class);
        when(fieldCapabilitiesResponse.getIndices()).thenReturn(indices);
        when(fieldCapabilitiesResponse.get()).thenReturn(fields(indices));
        doAnswer((Answer<Void>) invocation -> {
            @SuppressWarnings("unchecked")
            ActionListener<FieldCapabilitiesResponse> listener = (ActionListener<FieldCapabilitiesResponse>) invocation.getArguments()[2];
            // simulate a valid field_caps response so we can parse and correctly analyze de query
            listener.onResponse(fieldCapabilitiesResponse);
            return null;
        }).when(qlClient).execute(eq(EsqlResolveFieldsAction.TYPE), any(), any());

        Client esqlClient = mock(Client.class);
        IndexResolver indexResolver = new IndexResolver(esqlClient);
        doAnswer((Answer<Void>) invocation -> {
            @SuppressWarnings("unchecked")
            ActionListener<FieldCapabilitiesResponse> listener = (ActionListener<FieldCapabilitiesResponse>) invocation.getArguments()[2];
            // simulate a valid field_caps response so we can parse and correctly analyze de query
            listener.onResponse(new FieldCapabilitiesResponse(indexFieldCapabilities(indices), List.of()));
            return null;
        }).when(esqlClient).execute(eq(EsqlResolveFieldsAction.TYPE), any(), any());

        var planExecutor = new PlanExecutor(indexResolver, MeterRegistry.NOOP, new XPackLicenseState(() -> 0L), mockQueryLog());
        var enrichResolver = mockEnrichResolver();

        var request = new EsqlQueryRequest();
        // test a failed query: xyz field doesn't exist
        request.query("from test | stats m = max(xyz)");
        EsqlSession.PlanRunner runPhase = (p, r) -> fail("this shouldn't happen");
        IndicesExpressionGrouper groupIndicesByCluster = (remoteClusterNames, indicesOptions, indexExpressions) -> Map.of(
            "",
            new OriginalIndices(new String[] { "test" }, IndicesOptions.DEFAULT)
        );

        planExecutor.esql(
            request,
            randomAlphaOfLength(10),
            EsqlTestUtils.TEST_CFG,
            FoldContext.small(),
            enrichResolver,
            new EsqlExecutionInfo(randomBoolean()),
            groupIndicesByCluster,
            runPhase,
            EsqlTestUtils.MOCK_TRANSPORT_ACTION_SERVICES,
            new ActionListener<>() {
                @Override
                public void onResponse(Result result) {
                    fail("this shouldn't happen");
                }

                @Override
                public void onFailure(Exception e) {
                    assertThat(e, instanceOf(VerificationException.class));
                }
            }
        );

        // check we recorded the failure and that the query actually came
        assertEquals(1, planExecutor.metrics().stats().get("queries._all.failed"));
        assertEquals(1, planExecutor.metrics().stats().get("queries._all.total"));
        assertEquals(0, planExecutor.metrics().stats().get("features.stats"));

        // fix the failing query: foo field does exist
        request.query("from test | stats m = max(foo)");
        runPhase = (p, r) -> r.onResponse(null);
        planExecutor.esql(
            request,
            randomAlphaOfLength(10),
            EsqlTestUtils.TEST_CFG,
            FoldContext.small(),
            enrichResolver,
            new EsqlExecutionInfo(randomBoolean()),
            groupIndicesByCluster,
            runPhase,
            EsqlTestUtils.MOCK_TRANSPORT_ACTION_SERVICES,
            new ActionListener<>() {
                @Override
                public void onResponse(Result result) {}

                @Override
                public void onFailure(Exception e) {
                    fail("this shouldn't happen");
                }
            }
        );

        // check the new metrics
        assertEquals(1, planExecutor.metrics().stats().get("queries._all.failed"));
        assertEquals(2, planExecutor.metrics().stats().get("queries._all.total"));
        assertEquals(1, planExecutor.metrics().stats().get("features.stats"));
    }

    private List<FieldCapabilitiesIndexResponse> indexFieldCapabilities(String[] indices) {
        List<FieldCapabilitiesIndexResponse> responses = new ArrayList<>();
        for (String idx : indices) {
            responses.add(
                new FieldCapabilitiesIndexResponse(
                    idx,
                    idx,
                    Map.ofEntries(
                        Map.entry("foo", new IndexFieldCapabilitiesBuilder("foo", "integer").build()),
                        Map.entry("bar", new IndexFieldCapabilitiesBuilder("bar", "long").build())
                    ),
                    true,
                    IndexMode.STANDARD
                )
            );
        }
        return responses;
    }

    private Map<String, Map<String, FieldCapabilities>> fields(String[] indices) {
        FieldCapabilities fooField = new FieldCapabilitiesBuilder("foo", "integer").indices(indices).build();
        FieldCapabilities barField = new FieldCapabilitiesBuilder("bar", "long").indices(indices).build();
        Map<String, Map<String, FieldCapabilities>> fields = new HashMap<>();
        fields.put(fooField.getName(), Map.of(fooField.getName(), fooField));
        fields.put(barField.getName(), Map.of(barField.getName(), barField));
        return fields;
    }

    @Override
    protected List<String> filteredWarnings() {
        return withDefaultLimitWarning(super.filteredWarnings());
    }
}
