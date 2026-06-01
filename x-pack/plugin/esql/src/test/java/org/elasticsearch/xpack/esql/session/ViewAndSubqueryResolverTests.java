/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.session;

import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.View;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.analysis.InSubqueryResolver;
import org.elasticsearch.xpack.esql.inference.InferenceSettings;
import org.elasticsearch.xpack.esql.parser.AbstractStatementParserTests;
import org.elasticsearch.xpack.esql.parser.QueryParams;
import org.elasticsearch.xpack.esql.plan.SettingsValidationContext;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedRelation;
import org.elasticsearch.xpack.esql.plan.logical.join.AntiJoin;
import org.elasticsearch.xpack.esql.plan.logical.join.SemiJoin;
import org.elasticsearch.xpack.esql.telemetry.FeatureMetric;
import org.elasticsearch.xpack.esql.telemetry.Metrics;
import org.elasticsearch.xpack.esql.view.InMemoryViewResolver;
import org.elasticsearch.xpack.esql.view.InMemoryViewService;
import org.elasticsearch.xpack.esql.view.PutViewAction;
import org.elasticsearch.xpack.esql.view.ViewResolver;
import org.junit.After;
import org.junit.Before;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_FUNCTION_REGISTRY;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_PARSER;
import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.not;

/**
 * Tests for {@link ViewAndSubqueryResolver}, which iterates {@link ViewResolver} and {@link InSubqueryResolver} to a fixed point so that
 * views referenced from inside IN subqueries (and IN subqueries nested in view bodies) are fully expanded. Backed by the in-memory view
 * fixture {@link InMemoryViewService}; only view/IN-subquery resolution is exercised here — index and field resolution (the analyzer) is
 * intentionally out of scope, so relations stay as {@link UnresolvedRelation}s in the resolved plan.
 */
public class ViewAndSubqueryResolverTests extends AbstractStatementParserTests {

    private static final InferenceSettings EMPTY_INFERENCE_SETTINGS = new InferenceSettings(Settings.EMPTY);

    private final QueryParams queryParams = new QueryParams();
    private final ProjectId projectId = ProjectId.DEFAULT;
    private InMemoryViewService viewService;
    private InMemoryViewResolver viewResolver;

    @Before
    public void setup() {
        assumeTrue("requires WHERE IN subquery capability", EsqlCapabilities.Cap.WHERE_IN_SUBQUERY_WITH_VIEW.isEnabled());
        viewService = InMemoryViewService.makeViewService();
        viewResolver = viewService.getViewResolver();
        for (String index : List.of("employees", "departments")) {
            viewService.addIndex(projectId, index);
        }
    }

    @After
    public void teardown() {
        if (viewService != null) {
            viewService.close();
        }
    }

    // ---- resolution results ----

    public void testPlanWithoutViewsOrSubqueriesIsUnchanged() {
        ViewResolver.ViewResolutionResult result = resolve("FROM employees | WHERE emp_no > 1");
        assertThat(countOf(result.plan(), SemiJoin.class), equalTo(0));
        assertThat(countOf(result.plan(), AntiJoin.class), equalTo(0));
        assertThat(relationNames(result.plan()), contains("employees"));
        assertThat(result.viewQueries(), anEmptyMap());
    }

    public void testViewReferencedInsideInSubqueryIsExpanded() {
        addView("dept_view", "FROM departments | KEEP dept_id");
        ViewResolver.ViewResolutionResult result = resolve("FROM employees | WHERE dept_id IN (FROM dept_view | KEEP dept_id)");

        // The IN subquery becomes a SemiJoin, and the view that was hidden inside it is expanded to its underlying index.
        assertThat(countOf(result.plan(), SemiJoin.class), equalTo(1));
        assertThat(relationNames(result.plan()), containsInAnyOrder("employees", "departments"));
        assertThat(relationNames(result.plan()), not(hasItem("dept_view")));
        // The expanded view's query text is accumulated for the configuration.
        assertThat(result.viewQueries(), hasEntry("dept_view", "FROM departments | KEEP dept_id"));
    }

    public void testNotInSubqueryReferencingViewProducesAntiJoin() {
        addView("dept_view", "FROM departments | KEEP dept_id");
        ViewResolver.ViewResolutionResult result = resolve("FROM employees | WHERE dept_id NOT IN (FROM dept_view | KEEP dept_id)");

        assertThat(countOf(result.plan(), AntiJoin.class), equalTo(1));
        assertThat(countOf(result.plan(), SemiJoin.class), equalTo(0));
        assertThat(relationNames(result.plan()), containsInAnyOrder("employees", "departments"));
        assertThat(relationNames(result.plan()), not(hasItem("dept_view")));
    }

    public void testInSubqueryNestedInViewBodyIsResolved() {
        addView("v_with_in", "FROM employees | WHERE emp_no IN (FROM departments | KEEP emp_no)");
        ViewResolver.ViewResolutionResult result = resolve("FROM v_with_in");

        // The view expands and the IN subquery in its body is rewritten into a SemiJoin.
        assertThat(countOf(result.plan(), SemiJoin.class), equalTo(1));
        assertThat(relationNames(result.plan()), containsInAnyOrder("employees", "departments"));
        assertThat(relationNames(result.plan()), not(hasItem("v_with_in")));
    }

    public void testChainedViewsResolveToFixedPoint() {
        addView("in_layer_1", "FROM employees | WHERE emp_no IN (FROM employees | KEEP emp_no) | KEEP emp_no");
        addView("in_layer_2", "FROM employees | WHERE emp_no IN (FROM in_layer_1 | KEEP emp_no) | KEEP emp_no");
        addView("in_layer_3", "FROM employees | WHERE emp_no IN (FROM in_layer_2 | KEEP emp_no) | KEEP emp_no");
        ViewResolver.ViewResolutionResult result = resolve("FROM employees | WHERE emp_no IN (FROM in_layer_3 | KEEP emp_no)");

        // Every layered view is peeled away; only the base index remains, and each IN becomes a SemiJoin (outer + three layers).
        for (String name : relationNames(result.plan())) {
            assertThat(name, equalTo("employees"));
        }
        assertThat(countOf(result.plan(), SemiJoin.class), greaterThanOrEqualTo(4));
        assertThat(result.viewQueries().keySet(), containsInAnyOrder("in_layer_1", "in_layer_2", "in_layer_3"));
    }

    // ---- IN_SUBQUERY telemetry ----

    /**
     * IN_SUBQUERY telemetry must be counted exactly once per query even when the originating IN subquery is only revealed across several
     * resolution iterations. Here {@code in_layer_3} -> {@code in_layer_2} -> {@code in_layer_1} each wrap an
     * {@code IN (FROM <previous_view>)}, so an InSubquery surfaces on more than one view-resolution pass. This exercises the same
     * collection {@code EsqlSession.gatherInSubqueryMetrics} performs: the resolver reports every pass, while an {@link AtomicBoolean}
     * keeps the counter at one.
     */
    public void testInSubqueryMetricCountedOncePerQueryAcrossIterations() {
        addView("in_layer_1", "FROM employees | WHERE emp_no IN (FROM employees | KEEP emp_no) | KEEP emp_no");
        addView("in_layer_2", "FROM employees | WHERE emp_no IN (FROM in_layer_1 | KEEP emp_no) | KEEP emp_no");
        addView("in_layer_3", "FROM employees | WHERE emp_no IN (FROM in_layer_2 | KEEP emp_no) | KEEP emp_no");

        AtomicInteger passesWithInSubquery = new AtomicInteger();
        long inSubqueryCount = inSubqueryMetric("FROM employees | WHERE emp_no IN (FROM in_layer_3 | KEEP emp_no)", passesWithInSubquery);

        assertThat(
            "the IN subquery should be visible on more than one view-resolution pass",
            passesWithInSubquery.get(),
            greaterThanOrEqualTo(2)
        );
        assertEquals("IN_SUBQUERY must be counted once per query", 1L, inSubqueryCount);
    }

    /**
     * Control for {@link #testInSubqueryMetricCountedOncePerQueryAcrossIterations}: a single top-level IN subquery with no views is
     * visible on exactly one pass and is still counted once.
     */
    public void testInSubqueryMetricCountedOnceForSingleSubquery() {
        AtomicInteger passesWithInSubquery = new AtomicInteger();
        long inSubqueryCount = inSubqueryMetric("FROM employees | WHERE emp_no IN (FROM employees | KEEP emp_no)", passesWithInSubquery);

        assertThat(passesWithInSubquery.get(), equalTo(1));
        assertEquals("IN_SUBQUERY must be counted once per query", 1L, inSubqueryCount);
    }

    // ---- helpers ----

    private ViewResolver.ViewResolutionResult resolve(String query) {
        return resolve(query, plan -> {});
    }

    private ViewResolver.ViewResolutionResult resolve(String query, Consumer<LogicalPlan> viewResolvedListener) {
        ViewAndSubqueryResolver resolver = new ViewAndSubqueryResolver(viewResolver, viewService.getClusterService());
        PlainActionFuture<ViewResolver.ViewResolutionResult> future = new PlainActionFuture<>();
        resolver.resolve(query(query), null, this::parse, viewResolvedListener, future);
        return future.actionGet();
    }

    /**
     * Collects the {@code IN_SUBQUERY} feature metric exactly as {@code EsqlSession.execute} does: the resolver surfaces every
     * view-resolution pass and the collector de-duplicates with an {@link AtomicBoolean}. {@code passesWithInSubquery} records how many
     * passes still contained an InSubquery in a {@code WHERE} filter, so callers can assert the expression was visible across iterations.
     * Returns the resulting {@code IN_SUBQUERY} counter value.
     */
    private long inSubqueryMetric(String query, AtomicInteger passesWithInSubquery) {
        Metrics metrics = new Metrics(TEST_FUNCTION_REGISTRY, true, true);
        AtomicBoolean alreadyCounted = new AtomicBoolean();
        resolve(query, afterViews -> {
            if (InSubqueryResolver.hasInSubqueryInFilter(afterViews)) {
                passesWithInSubquery.incrementAndGet();
                if (alreadyCounted.compareAndSet(false, true)) {
                    metrics.inc(FeatureMetric.IN_SUBQUERY);
                }
            }
        });
        return metrics.stats().get("features." + FeatureMetric.IN_SUBQUERY);
    }

    private LogicalPlan parse(String query, String viewName) {
        return TEST_PARSER.parseView(query, queryParams, new SettingsValidationContext(false, false), EMPTY_INFERENCE_SETTINGS, viewName)
            .plan();
    }

    private void addView(String name, String query) {
        PutViewAction.Request request = new PutViewAction.Request(TimeValue.ONE_MINUTE, TimeValue.ONE_MINUTE, new View(name, query));
        PlainActionFuture<AcknowledgedResponse> future = new PlainActionFuture<>();
        viewService.putView(projectId, request, future);
        future.actionGet();
    }

    private static List<String> relationNames(LogicalPlan plan) {
        List<String> names = new ArrayList<>();
        plan.forEachDown(UnresolvedRelation.class, relation -> names.add(relation.indexPattern().indexPattern()));
        return names;
    }

    /**
     * Counts nodes of exactly {@code type}. Matching the exact class (rather than {@code instanceof}) matters because
     * {@link AntiJoin} extends {@link SemiJoin}, so a {@code NOT IN} rewrite would otherwise also be counted as a SemiJoin.
     */
    private static int countOf(LogicalPlan plan, Class<? extends LogicalPlan> type) {
        int[] count = { 0 };
        plan.forEachDown(type, node -> {
            if (node.getClass() == type) {
                count[0]++;
            }
        });
        return count[0];
    }
}
