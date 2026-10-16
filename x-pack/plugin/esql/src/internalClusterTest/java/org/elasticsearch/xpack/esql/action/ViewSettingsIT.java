/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.metadata.View;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.view.PutViewAction;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.not;

/**
 * The caller's query settings stop at view boundaries: a view must produce the same rows
 * whatever the caller SET — a materialized view would have baked its contents in. Covers {@code unmapped_fields}
 * (not reaching inside view bodies, null-filling at the view output instead) and {@code wildcards_match_views}
 * (a wildcard inside a view body never matches views). See also {@code SETTINGS_SCOPED_AT_VIEW_BOUNDARIES}.
 */
public class ViewSettingsIT extends AbstractEsqlIntegTestCase {

    /**
     * Two indices with dynamic mapping off, so {@code extra} is present in {@code _source} but unmapped:
     * {@code view_source} (ids 1-2) backs the views, {@code caller_idx} (id 3) is the caller's own pattern.
     */
    private void setupUnmappedIndices() {
        String mapping = """
            {"dynamic": false, "properties": {"id": {"type": "integer"}}}
            """;
        assertAcked(indicesAdmin().prepareCreate("view_source").setMapping(mapping));
        assertAcked(indicesAdmin().prepareCreate("caller_idx").setMapping(mapping));
        prepareIndex("view_source").setSource("id", 1, "extra", "e1").setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();
        prepareIndex("view_source").setSource("id", 2, "extra", "e2").setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();
        prepareIndex("caller_idx").setSource("id", 3, "extra", "e3").setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();
    }

    /**
     * {@code n} is mapped as an integer in {@code punk_a} (id 1) and present in {@code _source} but unmapped in
     * {@code punk_b} (id 2): a partially unmapped non-keyword field (PUNK) over {@code FROM punk_a, punk_b}.
     */
    private void setupPunkIndices() {
        assertAcked(indicesAdmin().prepareCreate("punk_a").setMapping("""
            {"dynamic": false, "properties": {"id": {"type": "integer"}, "n": {"type": "integer"}}}
            """));
        assertAcked(indicesAdmin().prepareCreate("punk_b").setMapping("""
            {"dynamic": false, "properties": {"id": {"type": "integer"}}}
            """));
        prepareIndex("punk_a").setSource("id", 1, "n", 10).setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();
        prepareIndex("punk_b").setSource("id", 2, "n", 20).setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();
    }

    public void testLoadStillLoadsPartiallyUnmappedFieldOutsideViews() {
        setupPunkIndices();
        try (var resp = run("""
            SET unmapped_fields="load";
            FROM punk_a, punk_b | KEEP id, n | SORT id
            """)) {
            assertThat(getValuesList(resp), equalTo(List.of(List.of(1, 10), List.of(2, 20))));
        }
    }

    public void testLoadDoesNotLoadPartiallyUnmappedFieldInsideViewBody() {
        setupPunkIndices();
        assertAcked(createView("punk_view", "FROM punk_a, punk_b"));
        try (var resp = run("""
            SET unmapped_fields="load";
            FROM punk_view | KEEP id, n | SORT id
            """)) {
            assertThat(getValuesList(resp), equalTo(List.of(Arrays.asList(1, 10), Arrays.asList(2, null))));
        }
    }

    public void testLoadDoesNotLoadPartiallyUnmappedFieldInsideViewBodyBesideCallerPattern() {
        setupPunkIndices();
        assertAcked(createView("punk_view", "FROM punk_a, punk_b"));
        try (var resp = run("""
            SET unmapped_fields="load";
            FROM punk_a, punk_view | KEEP id, n | SORT id
            """)) {
            assertThat(getValuesList(resp), equalTo(List.of(Arrays.asList(1, 10), Arrays.asList(1, 10), Arrays.asList(2, null))));
        }
    }

    /**
     * The caller and the view body name the very same pattern, so they would share one index resolution if it were keyed by the
     * pattern alone: the caller's {@code n} is loaded from {@code _source} where it is unmapped, the view's is not.
     */
    public void testLoadDoesNotLoadPartiallyUnmappedFieldInViewBodySharingCallerPattern() {
        setupPunkIndices();
        assertAcked(createView("punk_view", "FROM punk_a, punk_b"));
        try (var resp = run("""
            SET unmapped_fields="load";
            FROM punk_a, punk_b, punk_view | KEEP id, n | SORT id, n
            """)) {
            assertThat(
                getValuesList(resp),
                equalTo(List.of(Arrays.asList(1, 10), Arrays.asList(1, 10), Arrays.asList(2, 20), Arrays.asList(2, null)))
            );
        }
    }

    public void testLoadDoesNotLoadPartiallyUnmappedKeywordFieldInsideViewBody() {
        assertAcked(indicesAdmin().prepareCreate("kpunk_a").setMapping("""
            {"dynamic": false, "properties": {"id": {"type": "integer"}, "k": {"type": "keyword"}}}
            """));
        assertAcked(indicesAdmin().prepareCreate("kpunk_b").setMapping("""
            {"dynamic": false, "properties": {"id": {"type": "integer"}}}
            """));
        prepareIndex("kpunk_a").setSource("id", 1, "k", "a1").setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();
        prepareIndex("kpunk_b").setSource("id", 2, "k", "b1").setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();
        assertAcked(createView("kpunk_view", "FROM kpunk_a, kpunk_b"));
        try (var resp = run("""
            SET unmapped_fields="load";
            FROM kpunk_view | KEEP id, k | SORT id
            """)) {
            assertThat(getValuesList(resp), equalTo(List.of(Arrays.asList(1, "a1"), Arrays.asList(2, null))));
        }
    }

    public void testLoadAppliesToCallerOwnPattern() {
        setupUnmappedIndices();
        try (var resp = run("""
            SET unmapped_fields="load";
            FROM view_source | KEEP id, extra | SORT id
            """)) {
            assertThat(getValuesList(resp), equalTo(List.of(List.of(1, "e1"), List.of(2, "e2"))));
        }
    }

    public void testLoadDoesNotReachInsideViewBody() {
        setupUnmappedIndices();
        assertAcked(createView("filtered_view", "FROM view_source | WHERE id > 0"));
        try (var resp = run("""
            SET unmapped_fields="load";
            FROM filtered_view | KEEP id, extra | SORT id
            """)) {
            assertThat(getValuesList(resp), equalTo(List.of(Arrays.asList(1, null), Arrays.asList(2, null))));
        }
    }

    public void testLoadDoesNotReachInsidePassThroughViewBody() {
        setupUnmappedIndices();
        assertAcked(createView("pass_through_view", "FROM view_source"));
        try (var resp = run("""
            SET unmapped_fields="load";
            FROM pass_through_view | KEEP id, extra | SORT id
            """)) {
            assertThat(getValuesList(resp), equalTo(List.of(Arrays.asList(1, null), Arrays.asList(2, null))));
        }
    }

    public void testLoadAppliesToCallerPatternButNotSiblingView() {
        setupUnmappedIndices();
        assertAcked(createView("filtered_view", "FROM view_source | WHERE id > 0"));
        try (var resp = run("""
            SET unmapped_fields="load";
            FROM caller_idx, filtered_view | KEEP id, extra | SORT id
            """)) {
            assertThat(getValuesList(resp), equalTo(List.of(Arrays.asList(1, null), Arrays.asList(2, null), Arrays.asList(3, "e3"))));
        }
    }

    /**
     * The view output boundary here sits below the branch root (the user's own subquery processing is above it),
     * and the outer union's alignment Projects predate the fill — the regression shape for the patch re-run in
     * {@code ResolveUnmapped}.
     */
    public void testLoadOuterReferenceOverSubqueryWrappedViews() {
        setupUnmappedIndices();
        assertAcked(createView("filtered_view", "FROM view_source | WHERE id > 0"));
        try (var resp = run("""
            SET unmapped_fields="load";
            FROM (FROM filtered_view | WHERE id == 1), (FROM filtered_view | WHERE id == 2)
            | KEEP id, extra | SORT id
            """)) {
            assertThat(getValuesList(resp), equalTo(List.of(Arrays.asList(1, null), Arrays.asList(2, null))));
        }
    }

    public void testLoadOuterReferenceOverSingleSubqueryWrappedView() {
        setupUnmappedIndices();
        assertAcked(createView("filtered_view", "FROM view_source | WHERE id > 0"));
        try (var resp = run("""
            SET unmapped_fields="load";
            FROM (FROM filtered_view | WHERE id == 1)
            | KEEP id, extra | SORT id
            """)) {
            assertThat(getValuesList(resp), equalTo(List.of(Arrays.asList(1, null))));
        }
    }

    public void testLoadOuterReferenceOverAllViewUnion() {
        setupUnmappedIndices();
        assertAcked(createView("filtered_view", "FROM view_source | WHERE id > 0"));
        assertAcked(createView("other_view", "FROM caller_idx | WHERE id > 0"));
        try (var resp = run("""
            SET unmapped_fields="load";
            FROM filtered_view, other_view
            | KEEP id, extra | SORT id
            """)) {
            assertThat(getValuesList(resp), equalTo(List.of(Arrays.asList(1, null), Arrays.asList(2, null), Arrays.asList(3, null))));
        }
    }

    /**
     * A grouping reference resolves through a different path than a projection; it regresses separately when the
     * marked-unresolvable refresh in {@code ResolveUnmapped}'s patch path is missing.
     */
    public void testLoadOuterStatsByOverSubqueryWrappedViews() {
        setupUnmappedIndices();
        assertAcked(createView("filtered_view", "FROM view_source | WHERE id > 0"));
        try (var resp = run("""
            SET unmapped_fields="load";
            FROM (FROM filtered_view | WHERE id == 1), (FROM filtered_view | WHERE id == 2)
            | STATS c = COUNT(*) BY extra
            """)) {
            assertThat(getValuesList(resp), equalTo(List.of(Arrays.asList(2L, null))));
        }
    }

    public void testLoadReferenceInsideViewBodyFailsResolution() {
        setupUnmappedIndices();
        assertAcked(createView("bad_view", "FROM view_source | WHERE extra == \"e1\""));
        // Without the setting the body reference fails, and the caller's LOAD must not change that.
        expectThrows(VerificationException.class, containsString("Unknown column [extra]"), () -> run("FROM bad_view").close());
        expectThrows(VerificationException.class, containsString("Unknown column [extra]"), () -> run("""
            SET unmapped_fields="load";
            FROM bad_view
            """).close());
    }

    public void testNullifyReferenceInsideViewBodyFailsResolution() {
        setupUnmappedIndices();
        assertAcked(createView("bad_view", "FROM view_source | WHERE extra == \"e1\""));
        expectThrows(VerificationException.class, containsString("Unknown column [extra]"), () -> run("""
            SET unmapped_fields="nullify";
            FROM bad_view
            """).close());
    }

    public void testWildcardInsideViewBodyDoesNotMatchViews() {
        String mapping = """
            {"dynamic": false, "properties": {"id": {"type": "integer"}}}
            """;
        assertAcked(indicesAdmin().prepareCreate("wvt_data").setMapping(mapping));
        prepareIndex("wvt_data").setSource("id", 1).setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();
        prepareIndex("wvt_data").setSource("id", 2).setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();
        // Matched by the wvt_* wildcard; the marker column reveals when it leaks into a result.
        assertAcked(createView("wvt_leak", "FROM wvt_data | EVAL marker = 1"));
        assertAcked(createView("wrapper_view", "FROM wvt_*"));

        // Sanity: at the caller's own level the setting does make the wildcard match the view.
        try (var resp = run("""
            SET wildcards_match_views=true;
            FROM wvt_* | KEEP id | SORT id
            """)) {
            List<String> columns = resp.columns().stream().map(c -> c.name()).toList();
            assertThat(columns, equalTo(List.of("id")));
            assertThat(getValuesList(resp).size(), equalTo(4)); // 2 docs from the index + 2 through the matched view
        }

        // Inside the view body the same wildcard matches only the index, whatever the caller SET.
        try (var resp = run("""
            SET wildcards_match_views=true;
            FROM wrapper_view | SORT id
            """)) {
            List<String> columns = resp.columns().stream().map(c -> c.name()).toList();
            assertThat(columns, not(hasItem("marker")));
            assertThat(getValuesList(resp).size(), equalTo(2));
        }
    }

    /**
     * {@code wcv_data} (ids 1-2), and the view {@code wcv_leak} that the wildcard {@code wcv_*} matches when it is allowed to match
     * views. {@code wcv_leak} yields ids 101-102 and a {@code marker} column, so a leak shows up both in the rows and the columns.
     * The views the tests query are named without the {@code wcv_} prefix, so the wildcard never matches them.
     */
    private void setupWildcardViewsData() {
        String mapping = """
            {"dynamic": false, "properties": {"id": {"type": "integer"}}}
            """;
        assertAcked(indicesAdmin().prepareCreate("wcv_data").setMapping(mapping));
        prepareIndex("wcv_data").setSource("id", 1).setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();
        prepareIndex("wcv_data").setSource("id", 2).setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();
        assertAcked(createView("wcv_leak", "FROM wcv_data | EVAL id = id + 100, marker = 1"));
    }

    private static List<String> columnNames(org.elasticsearch.xpack.esql.action.EsqlQueryResponse resp) {
        return resp.columns().stream().map(c -> c.name()).toList();
    }

    public void testWildcardInNonPassThroughViewBodyDoesNotMatchViews() {
        setupWildcardViewsData();
        assertAcked(createView("body_view", "FROM wcv_* | WHERE id > 0"));
        try (var resp = run("""
            SET wildcards_match_views=true;
            FROM body_view | SORT id
            """)) {
            assertThat(columnNames(resp), not(hasItem("marker")));
            assertThat(getValuesList(resp), equalTo(List.of(List.of(1), List.of(2))));
        }
    }

    public void testWildcardInNestedViewBodyDoesNotMatchViews() {
        setupWildcardViewsData();
        assertAcked(createView("inner_view", "FROM wcv_* | WHERE id > 0"));
        assertAcked(createView("outer_view", "FROM inner_view"));
        try (var resp = run("""
            SET wildcards_match_views=true;
            FROM outer_view | SORT id
            """)) {
            assertThat(columnNames(resp), not(hasItem("marker")));
            assertThat(getValuesList(resp), equalTo(List.of(List.of(1), List.of(2))));
        }
    }

    public void testWildcardInSubqueryInsideViewBodyDoesNotMatchViews() {
        setupWildcardViewsData();
        assertAcked(createView("sub_view", "FROM (FROM wcv_*), (FROM wcv_data)"));
        try (var resp = run("""
            SET wildcards_match_views=true;
            FROM sub_view | SORT id
            """)) {
            assertThat(columnNames(resp), not(hasItem("marker")));
            assertThat(getValuesList(resp), equalTo(List.of(List.of(1), List.of(1), List.of(2), List.of(2))));
        }
    }

    public void testWildcardInViewBodyWithForkDoesNotMatchViews() {
        assumeTrue("Requires FORK", EsqlCapabilities.Cap.FORK_V9.isEnabled());
        setupWildcardViewsData();
        assertAcked(createView("fork_view", "FROM wcv_* | FORK (WHERE id == 1) (WHERE id == 2)"));
        try (var resp = run("""
            SET wildcards_match_views=true;
            FROM fork_view | SORT _fork, id
            """)) {
            assertThat(columnNames(resp), not(hasItem("marker")));
            assertThat(getValuesList(resp), equalTo(List.of(List.of(1, "fork1"), List.of(2, "fork2"))));
        }
    }

    /**
     * The view is a pass-through to the very pattern the caller's wildcard would match views with, and it sits next to a caller
     * index: the view must contribute its own rows (the index's, but not the matched view's), instead of being merged away.
     */
    public void testWildcardInPassThroughViewBodyBesideCallerIndexDoesNotMatchViews() {
        setupWildcardViewsData();
        assertAcked(createView("pass_view", "FROM wcv_*"));
        try (var resp = run("""
            SET wildcards_match_views=true;
            FROM wcv_data, pass_view | KEEP id | SORT id
            """)) {
            assertThat(getValuesList(resp), equalTo(List.of(List.of(1), List.of(1), List.of(2), List.of(2))));
        }
    }

    public void testWildcardInViewBodyUsedAsInSubqueryDoesNotMatchViews() {
        setupWildcardViewsData();
        String mapping = """
            {"dynamic": false, "properties": {"id": {"type": "integer"}}}
            """;
        assertAcked(indicesAdmin().prepareCreate("other_target").setMapping(mapping));
        for (int id : new int[] { 1, 2, 101, 102 }) {
            prepareIndex("other_target").setSource("id", id).setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();
        }
        assertAcked(createView("pass_view", "FROM wcv_*"));
        // Were the body's wildcard allowed to match wcv_leak, the subquery would also produce 101 and 102.
        try (var resp = run("""
            SET wildcards_match_views=true;
            FROM other_target | WHERE id IN (FROM pass_view | KEEP id) | KEEP id | SORT id
            """)) {
            assertThat(getValuesList(resp), equalTo(List.of(List.of(1), List.of(2))));
        }
    }

    /**
     * A view body is not subject to the cluster default either: inside it a wildcard never matches views, so the view returns the
     * same rows whatever the cluster is configured with.
     */
    public void testClusterDefaultOfWildcardsMatchViewsDoesNotReachViewBody() {
        setupWildcardViewsData();
        assertAcked(createView("body_view", "FROM wcv_* | WHERE id > 0"));
        String key = "esql.query.settings.wildcards_match_views";
        updateClusterSettings(org.elasticsearch.common.settings.Settings.builder().put(key, true));
        try {
            // The cluster default does apply to the caller's own wildcard: the index and the matched view.
            try (var resp = run("FROM wcv_* | KEEP id | SORT id")) {
                assertThat(getValuesList(resp), equalTo(List.of(List.of(1), List.of(2), List.of(101), List.of(102))));
            }
            try (var resp = run("FROM body_view | SORT id")) {
                assertThat(columnNames(resp), not(hasItem("marker")));
                assertThat(getValuesList(resp), equalTo(List.of(List.of(1), List.of(2))));
            }
        } finally {
            updateClusterSettings(org.elasticsearch.common.settings.Settings.builder().putNull(key));
        }
    }

    private AcknowledgedResponse createView(String viewName, String query) {
        return client().execute(
            PutViewAction.INSTANCE,
            new PutViewAction.Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, new View(viewName, query, null, false))
        ).actionGet(30, TimeUnit.SECONDS);
    }
}
