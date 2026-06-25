/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.view;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.View;
import org.elasticsearch.cluster.project.DefaultProjectResolver;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.indices.EmptySystemIndices;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.action.EsqlResolveViewAction;
import org.elasticsearch.xpack.esql.inference.InferenceSettings;
import org.elasticsearch.xpack.esql.parser.AbstractStatementParserTests;
import org.elasticsearch.xpack.esql.parser.QueryParams;
import org.elasticsearch.xpack.esql.plan.SettingsValidationContext;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.junit.After;
import org.junit.Before;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.rest.RestUtils.REST_MASTER_TIMEOUT_DEFAULT;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_PARSER;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.startsWith;
import static org.mockito.Mockito.mock;

/**
 * Standalone behaviour tests for {@link ViewGraph} — the static view-reference graph that polices circular references
 * and max view depth, independently of {@code ViewResolver}'s eager plan-shaped resolution.
 * <p>
 * Each scenario is a direct reconstruction of an {@code InMemoryViewServiceTests} case at the graph-unit level, asserting
 * the verbatim error chain (for the rejecting cases) or pass-without-throw (for the false-circular cases). The wildcard
 * expander is driven through the real {@link EsqlResolveViewAction} — the same expansion the resolver uses — so the
 * graph's edge semantics (matched names, exclusions, ordering) are identical to production, which is what makes the
 * false-circular cases resolve correctly here.
 */
public class ViewGraphTests extends AbstractStatementParserTests {

    private static final InferenceSettings EMPTY_INFERENCE_SETTINGS = new InferenceSettings(Settings.EMPTY);

    private final QueryParams queryParams = new QueryParams();
    private final ProjectId projectId = ProjectId.DEFAULT;

    private InMemoryViewService viewService;
    private ExpandingViewResolver expander;

    @Before
    public void setupTest() {
        viewService = InMemoryViewService.makeViewService();
        expander = new ExpandingViewResolver(viewService);
        for (String idx : List.of("emp", "emp1", "emp2", "emp3", "logs")) {
            viewService.addIndex(projectId, idx);
        }
    }

    @After
    public void tearDownTest() {
        viewService.close();
    }

    // ---------------------------------------------------------------------------------------------
    // Circular reference — exact chain string.
    // ---------------------------------------------------------------------------------------------

    public void testCircularViewSelfReference() {
        addView("view_a", "FROM view_a");
        VerificationException e = expectThrows(VerificationException.class, () -> check("FROM view_a"));
        assertThat(e.getMessage(), equalTo("circular view reference 'view_a': view_a"));
    }

    public void testCircularViewMutualReference() {
        addView("view_a", "FROM view_b");
        addView("view_b", "FROM view_a");
        VerificationException e = expectThrows(VerificationException.class, () -> check("FROM view_a"));
        assertThat(e.getMessage(), equalTo("circular view reference 'view_a': view_a -> view_b"));
    }

    public void testCircularViewChain() {
        addView("chain_a", "FROM chain_b");
        addView("chain_b", "FROM chain_c");
        addView("chain_c", "FROM chain_a");
        VerificationException e = expectThrows(VerificationException.class, () -> check("FROM chain_a"));
        assertThat(e.getMessage(), equalTo("circular view reference 'chain_a': chain_a -> chain_b -> chain_c"));
    }

    public void testCircularViewWithPipes() {
        addView("view_a", "FROM view_b | WHERE emp.age > 30");
        addView("view_b", "FROM view_a | WHERE emp.salary > 50000");
        VerificationException e = expectThrows(VerificationException.class, () -> check("FROM view_a"));
        assertThat(e.getMessage(), equalTo("circular view reference 'view_a': view_a -> view_b"));
    }

    public void testCircularViewViaWildcard() {
        addView("v_1", "FROM v_*");
        VerificationException e = expectThrows(VerificationException.class, () -> check("FROM v_*"));
        assertThat(e.getMessage(), equalTo("circular view reference 'v_1': v_1"));
    }

    public void testCircularViewViaWildcardWithIndex() {
        viewService.addIndex(projectId, "v_idx");
        addView("v_1", "FROM v_*");
        VerificationException e = expectThrows(VerificationException.class, () -> check("FROM v_*"));
        assertThat(e.getMessage(), equalTo("circular view reference 'v_1': v_1"));
    }

    /**
     * A view whose name matches its own wildcard pattern IS a genuine self-reference when it does not self-exclude.
     */
    public void testGenuineSelfReferenceViaWildcardInComposedView() {
        viewService.addIndex(projectId, "svc-auth-logs");
        addView("svc-auth-failures", "FROM svc-auth-* | STATS attempts = COUNT(*) BY source_ip");
        VerificationException e = expectThrows(VerificationException.class, () -> check("FROM svc-auth-failures"));
        assertThat(e.getMessage(), containsString("circular view reference 'svc-auth-failures'"));
    }

    // ---------------------------------------------------------------------------------------------
    // Circular reference with exclusions — exclusion ordering changes the outcome.
    // ---------------------------------------------------------------------------------------------

    /** {@code -v_1} removes the would-be-circular view from the entry expansion, so no cycle remains. */
    public void testCircularViewExcludedByWildcard() {
        addView("v_1", "FROM v_*");
        check("FROM v_*,-v_1"); // must not throw
    }

    /** The exclusion {@code -view_b} does not remove view_b from view_a's body, so the a-b cycle still fires. */
    public void testCircularViewExcludedByConcreteExclusion() {
        addView("view_a", "FROM view_b");
        addView("view_b", "FROM view_a");
        VerificationException e = expectThrows(VerificationException.class, () -> check("FROM view_a,-view_b"));
        assertThat(e.getMessage(), equalTo("circular view reference 'view_a': view_a -> view_b"));
    }

    /** Self-exclusion in the body ({@code -v_1}) removes v_1 from its own wildcard expansion, so no self-cycle. */
    public void testCircularViewBodyWithSelfExclusion() {
        addView("v_1", "FROM v_*,-v_1");
        check("FROM v_1"); // must not throw
    }

    // ---------------------------------------------------------------------------------------------
    // False-circular suppression — the whole point. None of these may throw.
    // ---------------------------------------------------------------------------------------------

    /**
     * FROM *,-employees* against the csv-spec view population. Several non-employee views share underlying wildcard
     * patterns; the employee views are excluded by the outer pattern. The eager DFS produced a false circular reference
     * here (#146208) via re-entrant wildcard expansion; the graph does not.
     */
    public void testFromStarExcludingEmployeesWithCsvSpecViews_Issue146208() {
        viewService.addIndex(projectId, "addresses");
        viewService.addIndex(projectId, "airports");
        viewService.addIndex(projectId, "airports_mp");
        viewService.addIndex(projectId, "languages_lookup_non_unique_key");
        viewService.addIndex(projectId, "employees");

        addView("country_addresses", "FROM addresses | STATS count=COUNT() BY country");
        addView("country_languages", "FROM languages_lookup_non_unique_key | STATS count=COUNT() BY country");
        addView("airports_mp_filtered", "FROM airports | LOOKUP JOIN airports_mp ON abbrev == abbrev");
        addView("country_airports", "FROM airports | STATS count=COUNT() BY country");

        addView("employees_all", "FROM employees");
        addView("employees_extra", "FROM employees, employees_all");
        addView("employees_rehired", "FROM employees | WHERE is_rehired == true");
        addView("employees_not_rehired", "FROM employees | WHERE is_rehired == false");

        check("FROM *,-employees*"); // must not throw a false circular reference
    }

    /**
     * Two sibling views share a wildcard pattern ({@code svc-auth-*}) inside subqueries, composed in a parent view.
     * Neither references the other (#146097 / shared-wildcard family). No cycle.
     */
    public void testFalseCircularReferenceWithSharedWildcardInSubqueries() {
        viewService.addIndex(projectId, "svc-gateway-logs");
        viewService.addIndex(projectId, "svc-payments-logs");
        viewService.addIndex(projectId, "svc-auth-logs");

        addView(
            "error_triage",
            "FROM (FROM svc-gateway-* | WHERE http_status >= 500 | KEEP @timestamp, http_status),"
                + "(FROM svc-payments-* | WHERE http_status >= 500 | KEEP @timestamp, http_status),"
                + "(FROM svc-auth-* | WHERE http_status >= 500 | KEEP @timestamp, http_status)"
        );
        addView("suspicious_ips", "FROM svc-auth-* | STATS attempts = COUNT(*) BY source_ip");
        addView("incident_dashboard", "FROM error_triage, suspicious_ips");

        check("FROM incident_dashboard"); // must not throw
    }

    /**
     * A wildcard in one view's subquery matches a sibling view's name, but that sibling self-excludes. The eager DFS
     * polluted seenViews with sibling names from the outer scope and produced a false cycle; the graph does not.
     */
    public void testFalseCircularReferenceWhenWildcardMatchesSiblingViewName() {
        viewService.addIndex(projectId, "svc-gateway-logs");
        viewService.addIndex(projectId, "svc-auth-logs");

        addView(
            "error_view",
            "FROM (FROM svc-gateway-* | WHERE http_status >= 500 | KEEP @timestamp, http_status),"
                + "(FROM svc-auth-* | WHERE http_status >= 500 | KEEP @timestamp, http_status)"
        );
        addView("svc-auth-failures", "FROM svc-auth-*,-svc-auth-failures | STATS attempts = COUNT(*) BY source_ip");
        addView("dashboard", "FROM error_view, svc-auth-failures");

        check("FROM dashboard"); // must not throw
    }

    /**
     * Subquery-based view composed with a simple view sharing a wildcard pattern (#146097). Neither references the
     * other; composing them must not produce a circular reference.
     */
    public void testFalseCircularReferenceFromSharedWildcardPattern_Issue146097() {
        viewService.addIndex(projectId, "app-events-001");
        viewService.addIndex(projectId, "auth-events-001");

        addView("view_x", "FROM (FROM app-events-* | KEEP msg, level), (FROM auth-events-* | KEEP msg, level)");
        addView("view_y", "FROM auth-events-* | KEEP msg, level");
        addView("view_xy", "FROM view_x, view_y");

        check("FROM view_xy"); // must not throw
    }

    /**
     * view_b -&gt; view_c -&gt; view_a -&gt; emp is a valid chain; view_a appearing both as a direct entry and as a
     * transitive dependency of view_b is fine (sibling branches are independent).
     */
    public void testNonCircularViewInMultiSource() {
        addView("view_a", "FROM emp");
        addView("view_b", "FROM view_c");
        addView("view_c", "FROM view_a");
        check("FROM view_a, view_b"); // must not throw
    }

    // ---------------------------------------------------------------------------------------------
    // Max depth — exact prefix + the view11-fails / view10-ok boundary, and the modified-setting case.
    // ---------------------------------------------------------------------------------------------

    public void testViewDepthExceeded() {
        addView("view1", "FROM emp");
        for (int i = 2; i <= 11; i++) {
            addView("view" + i, "FROM view" + (i - 1));
        }

        VerificationException e = expectThrows(VerificationException.class, () -> check("FROM view11"));
        assertThat(e.getMessage(), startsWith("The maximum allowed view depth of 10 has been exceeded"));
        // The chain is the full depth-11 path that overflowed.
        assertThat(
            e.getMessage(),
            equalTo(
                "The maximum allowed view depth of 10 has been exceeded: "
                    + "view11 -> view10 -> view9 -> view8 -> view7 -> view6 -> view5 -> view4 -> view3 -> view2 -> view1"
            )
        );

        check("FROM view10"); // depth 10 is allowed — must not throw
    }

    public void testModifiedViewDepth() {
        addView("view1", "FROM emp");
        addView("view2", "FROM view1");
        addView("view3", "FROM view2");

        // maxViewDepth = 1: FROM view2 (depth 2) fails, FROM view1 (depth 1) passes.
        ViewGraph graphDepth1 = graph(1);
        VerificationException e = expectThrows(VerificationException.class, () -> graphDepth1.check(expander.expand(List.of("view2"))));
        assertThat(e.getMessage(), startsWith("The maximum allowed view depth of 1 has been exceeded"));
        assertThat(e.getMessage(), equalTo("The maximum allowed view depth of 1 has been exceeded: view2 -> view1"));

        graph(1).check(expander.expand(List.of("view1"))); // must not throw
    }

    // ---------------------------------------------------------------------------------------------
    // Harness.
    // ---------------------------------------------------------------------------------------------

    /** Build a {@link ViewGraph} over the current view set with the default max depth (10). */
    private ViewGraph graph() {
        return graph(ViewResolver.MAX_VIEW_DEPTH_SETTING.getDefault(Settings.EMPTY));
    }

    private ViewGraph graph(int maxViewDepth) {
        Map<String, View> views = viewService.getMetadata(projectId).views();
        Map<String, String> viewQueries = new HashMap<>();
        views.forEach((name, view) -> viewQueries.put(name, view.query()));
        return new ViewGraph(viewQueries, this::parse, expander::expand, maxViewDepth);
    }

    /** Run the depth/cycle check for a query, expanding its top-level FROM patterns to entry views. */
    private void check(String fromQuery) {
        graph().checkPatterns(fromPatterns(fromQuery));
    }

    /** Extract the comma-split patterns of a single top-level {@code FROM <patterns>} query. */
    private static List<String> fromPatterns(String fromQuery) {
        String trimmed = fromQuery.trim();
        assertThat("test harness only supports a single FROM clause", trimmed, startsWith("FROM "));
        String afterFrom = trimmed.substring("FROM ".length());
        int pipe = afterFrom.indexOf('|');
        if (pipe >= 0) {
            afterFrom = afterFrom.substring(0, pipe);
        }
        return Arrays.stream(afterFrom.split(",")).map(String::trim).filter(s -> s.isEmpty() == false).toList();
    }

    private LogicalPlan parse(String query, String viewName) {
        return TEST_PARSER.parseView(query, queryParams, new SettingsValidationContext(false, false), EMPTY_INFERENCE_SETTINGS, viewName)
            .plan();
    }

    private void addView(String name, String query) {
        PutViewAction.Request request = new PutViewAction.Request(TimeValue.ONE_MINUTE, TimeValue.ONE_MINUTE, new View(name, query));
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Exception> err = new AtomicReference<>(null);
        viewService.putView(projectId, request, ActionListener.wrap(r -> latch.countDown(), e -> {
            err.set(e);
            latch.countDown();
        }));
        try {
            assert latch.await(1, TimeUnit.MILLISECONDS) : "should never timeout";
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        if (err.get() != null) {
            throw new RuntimeException(err.get());
        }
    }

    /**
     * Drives the real {@link EsqlResolveViewAction} to expand a list of index patterns into the matched <b>view
     * names</b> in resolution order, with exclusions applied — identical to the expansion {@code ViewResolver} performs
     * per {@link org.elasticsearch.xpack.esql.plan.logical.UnresolvedRelation}. Wiring mirrors {@link InMemoryViewResolver}.
     */
    private static final class ExpandingViewResolver {
        private final ClusterService clusterService;
        private final ProjectResolver projectResolver = DefaultProjectResolver.INSTANCE;
        private final IndexNameExpressionResolver indexNameExpressionResolver;

        ExpandingViewResolver(InMemoryViewService viewService) {
            this.clusterService = viewService.clusterService;
            this.indexNameExpressionResolver = new IndexNameExpressionResolver(
                new ThreadContext(Settings.EMPTY),
                EmptySystemIndices.INSTANCE,
                projectResolver
            );
        }

        List<String> expand(List<String> patterns) {
            if (patterns.isEmpty()) {
                return List.of();
            }
            var action = new EsqlResolveViewAction(
                mock(TransportService.class),
                new ActionFilters(Set.of()),
                indexNameExpressionResolver,
                clusterService,
                projectResolver
            );
            var req = new EsqlResolveViewAction.Request(REST_MASTER_TIMEOUT_DEFAULT, false);
            req.indices(patterns.toArray(new String[0]));
            PlainActionFuture<EsqlResolveViewAction.Response> future = new PlainActionFuture<>();
            action.execute(mock(Task.class), req, future);
            return Arrays.stream(future.actionGet().views()).map(View::name).toList();
        }
    }
}
