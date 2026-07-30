/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.view;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.metadata.View;
import org.elasticsearch.cluster.metadata.ViewMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.search.crossproject.CrossProjectModeDecider;
import org.elasticsearch.transport.RemoteClusterAware;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.action.EsqlFetchRemoteViewsAction;
import org.elasticsearch.xpack.esql.action.EsqlResolveViewAction;
import org.elasticsearch.xpack.esql.inference.InferenceSettings;
import org.elasticsearch.xpack.esql.parser.AbstractStatementParserTests;
import org.elasticsearch.xpack.esql.parser.QueryParams;
import org.elasticsearch.xpack.esql.plan.SettingsValidationContext;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedRelation;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_PARSER;
import static org.hamcrest.Matchers.is;

public class ViewResolverRemoteViewsTests extends AbstractStatementParserTests {

    private static final InferenceSettings EMPTY_INFERENCE_SETTINGS = new InferenceSettings(Settings.EMPTY);

    static InMemoryViewService viewService;

    @BeforeClass
    public static void setup() {
        viewService = InMemoryViewService.makeViewService();
    }

    @AfterClass
    public static void afterTearDown() {
        viewService.close();
    }

    @Before
    public void setupTest() {
        viewService.clearAllViewsAndIndices();
    }

    // ---------------------------------------------------------------------------------
    // qualifyWithCluster static method tests
    // ---------------------------------------------------------------------------------

    public void testQualifyWithClusterSimple() {
        LogicalPlan plan = query("FROM logs-2");
        LogicalPlan qualified = ViewResolver.qualifyWithCluster(plan, "cluster-a");
        UnresolvedRelation ur = singleUnresolvedRelation(qualified);
        assertThat(ur.indexPattern().indexPattern(), is("cluster-a:logs-2"));
    }

    public void testQualifyWithClusterAlreadyQualified() {
        LogicalPlan plan = query("FROM cluster-b:logs-2");
        LogicalPlan qualified = ViewResolver.qualifyWithCluster(plan, "cluster-a");
        UnresolvedRelation ur = singleUnresolvedRelation(qualified);
        // Already has cluster-b, must stay as cluster-b (not double-qualified)
        assertThat(ur.indexPattern().indexPattern(), is("cluster-b:logs-2"));
    }

    public void testQualifyWithClusterMultiIndex() {
        LogicalPlan plan = query("FROM logs-1,logs-2");
        LogicalPlan qualified = ViewResolver.qualifyWithCluster(plan, "cluster-a");
        UnresolvedRelation ur = singleUnresolvedRelation(qualified);
        assertThat(ur.indexPattern().indexPattern(), is("cluster-a:logs-1,cluster-a:logs-2"));
    }

    // ---------------------------------------------------------------------------------
    // Remote view expansion end-to-end tests
    // ---------------------------------------------------------------------------------

    public void testRemoteViewExpansion() {
        assumeTrue("Requires REMOTE_VIEW_RESOLUTION capability", EsqlCapabilities.Cap.REMOTE_VIEW_RESOLUTION.isEnabled());

        FakeRemoteViewResolver resolver = new FakeRemoteViewResolver();
        resolver.addRemoteViews("cluster-a", new View("remote-view", "FROM logs-2 | LIMIT 10"));

        LogicalPlan plan = query("FROM cluster-a:remote-view");
        LogicalPlan result = replaceViews(plan, resolver);

        // The expanded plan should contain an UnresolvedRelation for cluster-a:logs-2 (the view body, qualified)
        List<UnresolvedRelation> relations = result.collect(UnresolvedRelation.class);
        assertFalse("Expected at least one UnresolvedRelation in expanded plan", relations.isEmpty());
        assertTrue(
            "Expected cluster-a:logs-2 in expanded plan, got: " + relations,
            relations.stream().anyMatch(ur -> ur.indexPattern().indexPattern().contains("cluster-a:logs-2"))
        );
    }

    public void testRemoteViewWildcardExpansion() {
        assumeTrue("Requires REMOTE_VIEW_RESOLUTION capability", EsqlCapabilities.Cap.REMOTE_VIEW_RESOLUTION.isEnabled());

        FakeRemoteViewResolver resolver = new FakeRemoteViewResolver();
        resolver.addRemoteViews("cluster-a", new View("logs-nginx", "FROM raw-logs | WHERE type == \"nginx\""));

        LogicalPlan plan = query("FROM cluster-a:logs-*");
        LogicalPlan result = replaceViews(plan, resolver);

        List<UnresolvedRelation> relations = result.collect(UnresolvedRelation.class);
        assertFalse("Expected at least one UnresolvedRelation in expanded plan", relations.isEmpty());
        assertTrue(
            "Expected cluster-a:raw-logs in expanded plan, got: " + relations,
            relations.stream().anyMatch(ur -> ur.indexPattern().indexPattern().contains("cluster-a:raw-logs"))
        );
    }

    // ---------------------------------------------------------------------------------
    // Remote cluster unavailability handling tests
    // ---------------------------------------------------------------------------------

    /**
     * When a remote cluster is unavailable during view fetch, the failure is absorbed and the
     * pattern passes through to field-caps, which enforces skip_unavailable semantics there.
     */
    public void testRemoteViewFetchFailureIsAbsorbedAndPatternPassesThrough() {
        assumeTrue("Requires REMOTE_VIEW_RESOLUTION capability", EsqlCapabilities.Cap.REMOTE_VIEW_RESOLUTION.isEnabled());

        FakeRemoteViewResolver resolver = new FakeRemoteViewResolver();
        resolver.addRemoteFailure("cluster-a", new RuntimeException("cluster-a is unavailable"));

        // Query should succeed — the cluster failure is absorbed and the pattern passes through
        LogicalPlan plan = query("FROM cluster-a:remote-view");
        LogicalPlan result = replaceViews(plan, resolver);

        // No view expansion occurred; the original UnresolvedRelation passes through unchanged
        List<UnresolvedRelation> relations = result.collect(UnresolvedRelation.class);
        assertFalse("Expected at least one UnresolvedRelation", relations.isEmpty());
        assertTrue(
            "Expected cluster-a:remote-view to pass through unchanged, got: " + relations,
            relations.stream().anyMatch(ur -> ur.indexPattern().indexPattern().contains("cluster-a:remote-view"))
        );
    }

    // ---------------------------------------------------------------------------------
    // Helpers
    // ---------------------------------------------------------------------------------

    private LogicalPlan replaceViews(LogicalPlan plan, ViewResolver resolver) {
        PlainActionFuture<ViewResolver.ViewResolutionResult> future = new PlainActionFuture<>();
        resolver.replaceViews(plan, null, this::parse, future);
        return future.actionGet().plan();
    }

    private LogicalPlan parse(String q, String viewName) {
        return TEST_PARSER.parseView(q, new QueryParams(), new SettingsValidationContext(false, false), EMPTY_INFERENCE_SETTINGS, viewName)
            .plan();
    }

    private static UnresolvedRelation singleUnresolvedRelation(LogicalPlan plan) {
        List<UnresolvedRelation> urs = plan.collect(UnresolvedRelation.class);
        assertThat("Expected exactly one UnresolvedRelation", urs.size(), is(1));
        return urs.get(0);
    }

    // ---------------------------------------------------------------------------------
    // FakeRemoteViewResolver — simulates remote view fetching without network I/O
    // ---------------------------------------------------------------------------------

    /**
     * ViewResolver subclass that intercepts the remote view fetch and returns canned results,
     * allowing end-to-end tests of the remote view expansion path without real transport connections.
     * Simulates the behaviour of {@link ViewResolver#doEsqlResolveViewsRequest}: per-cluster
     * failures are absorbed (the caller is responsible for enforcing skip_unavailable at field-caps).
     */
    private class FakeRemoteViewResolver extends InMemoryViewResolver {
        private final Map<String, View[]> remoteViewsByCluster = new LinkedHashMap<>();
        private final Map<String, Exception> remoteFailuresByCluster = new LinkedHashMap<>();

        FakeRemoteViewResolver() {
            super(viewService.clusterService, () -> ViewMetadata.EMPTY, CrossProjectModeDecider.NOOP);
        }

        void addRemoteViews(String clusterAlias, View... views) {
            remoteViewsByCluster.put(clusterAlias, views);
        }

        void addRemoteFailure(String clusterAlias, Exception ex) {
            remoteFailuresByCluster.put(clusterAlias, ex);
        }

        @Override
        protected void doEsqlResolveViewsRequest(
            EsqlResolveViewAction.Request request,
            ActionListener<EsqlResolveViewAction.Response> listener
        ) {
            if (EsqlCapabilities.Cap.REMOTE_VIEW_RESOLUTION.isEnabled() == false) {
                super.doEsqlResolveViewsRequest(request, listener);
                return;
            }

            Map<String, List<String>> remotePatternsByCluster = new LinkedHashMap<>();
            for (String pattern : request.indices()) {
                var split = RemoteClusterAware.splitIndexName(pattern);
                if (split.clusterAlias() != null) {
                    remotePatternsByCluster.computeIfAbsent(split.clusterAlias(), k -> new ArrayList<>()).add(split.indexExpression());
                }
            }

            if (remotePatternsByCluster.isEmpty()) {
                super.doEsqlResolveViewsRequest(request, listener);
                return;
            }

            super.doEsqlResolveViewsRequest(request, listener.delegateFailureAndWrap((l, localResponse) -> {
                Map<String, EsqlFetchRemoteViewsAction.Response> remoteResponses = new LinkedHashMap<>();
                for (String clusterAlias : remotePatternsByCluster.keySet()) {
                    if (remoteFailuresByCluster.containsKey(clusterAlias)) {
                        // Absorb failure — mirrors production behaviour: field-caps enforces skip_unavailable
                        continue;
                    }
                    View[] views = remoteViewsByCluster.getOrDefault(clusterAlias, new View[0]);
                    remoteResponses.put(clusterAlias, new EsqlFetchRemoteViewsAction.Response(views));
                }
                l.onResponse(ViewResolver.mergeViewResponses(localResponse, remoteResponses, remotePatternsByCluster));
            }));
        }
    }
}
