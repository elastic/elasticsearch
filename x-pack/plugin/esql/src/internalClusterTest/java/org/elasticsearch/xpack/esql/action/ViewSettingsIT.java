/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.LatchedActionListener;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.View;
import org.elasticsearch.common.logging.HeaderWarning;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.view.DeleteViewAction;
import org.elasticsearch.xpack.esql.view.PutViewAction;
import org.hamcrest.Matcher;
import org.junit.After;

import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.not;

/**
 * The caller's query settings stop at view boundaries: a view must produce the same rows
 * whatever the caller SET — a materialized view would have baked its contents in. Covers {@code unmapped_fields}
 * (not reaching inside view bodies, null-filling at the view output instead) and {@code wildcards_match_views}
 * (a wildcard inside a view body never matches views). A view definition may {@code SET} those same settings for
 * its own body; that {@code SET} does not leak into a nested view. Dataset wildcards are covered in
 * {@link FromDatasetViewIT}. See {@code SETTINGS_SCOPED_AT_VIEW_BOUNDARIES} and {@code SET_IN_VIEW_DEFINITIONS}.
 */
public class ViewSettingsIT extends AbstractEsqlIntegTestCase {

    private final Set<String> createdViews = new LinkedHashSet<>();

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
     * The caller and the view body name the very same pattern, and each gets an index resolution of its own (resolutions
     * are keyed by the pattern plus its view scope): the caller's {@code n} is loaded from {@code _source} where it is
     * unmapped, the view's is not.
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

    /**
     * {@code LOAD_ALL} may be used with views, which it does not reach into either: the view yields no unmapped fields, and a null
     * {@code _unmapped_fields}. The query is the shape of a conversion that resolves on a later analyzer pass, which has looped
     * in the analyzer before: the reference to an unmapped field resolves one pass after the identical conversion of the WHERE.
     */
    public void testLoadAllSameConversionResolvedOnLaterPassOverViewAndIndex() {
        assumeTrue("Requires OPTIONAL_FIELDS_LOAD_ALL_VIEWS", EsqlCapabilities.Cap.OPTIONAL_FIELDS_LOAD_ALL_VIEWS.isEnabled());
        setupUnmappedIndices();
        assertAcked(createView("filtered_view", "FROM view_source | WHERE id > 0"));
        try (var resp = run("""
            SET unmapped_fields="LOAD_ALL";
            FROM caller_idx, filtered_view
            | WHERE TO_STRING(id) == "1" OR does_not_exist IS NOT NULL
            | EVAL u = TO_STRING(id)
            | STATS total = COUNT(*) BY u
            """)) {
            assertThat(getValuesList(resp), equalTo(List.of(List.of(1L, "1"))));
        }
    }

    public void testLoadAllViewOutputsNullUnmappedFieldsAndCallerPatternLoadsThem() {
        assumeTrue("Requires OPTIONAL_FIELDS_LOAD_ALL_VIEWS", EsqlCapabilities.Cap.OPTIONAL_FIELDS_LOAD_ALL_VIEWS.isEnabled());
        setupUnmappedIndices();
        assertAcked(createView("filtered_view", "FROM view_source | WHERE id > 0"));
        try (var resp = run("""
            SET unmapped_fields="LOAD_ALL";
            FROM caller_idx, filtered_view
            | SORT id
            """)) {
            assertThat(columnNames(resp), equalTo(List.of("id", "_unmapped_fields")));
            // The caller's own index returns its unmapped field, the view's rows get none.
            List<List<Object>> rows = getValuesList(resp);
            assertThat(rows.stream().map(row -> row.get(1) == null).toList(), equalTo(List.of(true, true, false)));
            assertThat(rows.get(2).get(1), equalTo(Map.of("extra", "e3")));
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
     * Unlike the caller's {@code SET}, the cluster default reaches a view body whose definition does not {@code SET} the flag:
     * it is the operator's default for every query, inside views too. The view's own {@code SET} still overrides it.
     */
    public void testClusterDefaultOfWildcardsMatchViewsReachesViewBody() throws Exception {
        setupWildcardViewsData();
        assertAcked(createView("body_view", "FROM wcv_* | WHERE id > 0 | KEEP id"));
        assertAcked(createView("opted_out_view", """
            SET wildcards_match_views=false;
            FROM wcv_* | WHERE id > 0 | KEEP id
            """));
        String key = "esql.query.settings.wildcards_match_views";
        updateClusterSettings(Settings.builder().put(key, true));
        try {
            // The cluster default applies to the caller's own wildcard: the index and the matched view.
            try (var resp = run("FROM wcv_* | KEEP id | SORT id")) {
                assertThat(getValuesList(resp), equalTo(List.of(List.of(1), List.of(2), List.of(101), List.of(102))));
            }
            // And to the view body's: the same rows as the body run on its own.
            try (var resp = run("FROM body_view | SORT id")) {
                assertThat(getValuesList(resp), equalTo(List.of(List.of(1), List.of(2), List.of(101), List.of(102))));
            }
            // The caller's SET does not reach inside, so turning it off at the caller does not change the view.
            try (var resp = run("SET wildcards_match_views=false; FROM body_view | SORT id")) {
                assertThat(getValuesList(resp), equalTo(List.of(List.of(1), List.of(2), List.of(101), List.of(102))));
            }
            // The view's own SET beats the cluster default.
            try (var resp = run("FROM opted_out_view | SORT id")) {
                assertThat(getValuesList(resp), equalTo(List.of(List.of(1), List.of(2))));
            }
            // The caller and the view body agree, so no boundary stopped anything and nothing is reported.
            assertThat(warningHeaders("FROM body_view"), not(hasItem(containsString("does not affect the contents of view"))));
        } finally {
            updateClusterSettings(Settings.builder().putNull(key));
        }
    }

    /**
     * A cluster default of {@code wildcards_match_views=true} lets a view body's wildcard match the view itself, which is a
     * circular reference, as it is when the definition {@code SET}s the flag. A definition that excludes itself from its
     * wildcard, or {@code SET}s the flag off, is unaffected.
     */
    public void testClusterDefaultOfWildcardsMatchViewsMakesSelfMatchingViewCircular() throws Exception {
        setupWildcardViewsData();
        assertAcked(createView("wcv_self", "FROM wcv_* | WHERE id > 0 | KEEP id"));
        assertAcked(createView("wcv_self_set", """
            SET wildcards_match_views=true;
            FROM wcv_data, wcv_self_s* | WHERE id > 0 | KEEP id
            """));
        assertAcked(createView("wcv_excluding", "FROM wcv_data, wcv_l*, wcv_e*, -wcv_excluding | WHERE id > 0 | KEEP id"));
        assertAcked(createView("wcv_opted_out", """
            SET wildcards_match_views=false;
            FROM wcv_d*, wcv_o* | WHERE id > 0 | KEEP id
            """));
        // Without the cluster default, only the view that SETs the flag matches itself.
        try (var resp = run("FROM wcv_self | SORT id")) {
            assertThat(getValuesList(resp), equalTo(List.of(List.of(1), List.of(2))));
        }
        expectThrows(VerificationException.class, containsString("circular view reference 'wcv_self_set'"), () -> {
            run("FROM wcv_self_set").close();
        });
        String key = "esql.query.settings.wildcards_match_views";
        updateClusterSettings(Settings.builder().put(key, true));
        try {
            // The body also matches wcv_self_set, itself circular, so either view may be the one reported.
            expectThrows(
                VerificationException.class,
                containsString("circular view reference 'wcv_self"),
                () -> { run("FROM wcv_self").close(); }
            );
            try (var resp = run("FROM wcv_excluding | SORT id")) {
                assertThat(getValuesList(resp), equalTo(List.of(List.of(1), List.of(2), List.of(101), List.of(102))));
            }
            try (var resp = run("FROM wcv_opted_out | SORT id")) {
                assertThat(getValuesList(resp), equalTo(List.of(List.of(1), List.of(2))));
            }
        } finally {
            updateClusterSettings(Settings.builder().putNull(key));
        }
    }

    /**
     * A caller who knows a view's contents may expect their {@code SET wildcards_match_views=true} to reach the view's wildcards.
     * When a view's wildcard would then have matched more views, the response says so, naming the view and what was missed.
     */
    public void testWildcardsMatchViewsWarnsWhenViewBoundaryStopsIt() throws Exception {
        setupWildcardViewsData();
        assertAcked(createView("wcv_other", "FROM wcv_data"));
        assertAcked(createView("body_view", "FROM wcv_* | WHERE id > 0"));
        assertAcked(createView("exact_view", "FROM wcv_data, wcv_leak"));
        assertAcked(createView("index_only_view", "FROM wcv_d*"));
        assertAcked(createView("opted_in_view", """
            SET wildcards_match_views=true;
            FROM wcv_* | KEEP id
            """));
        assertAcked(createView("inner_plain_view", "FROM wcv_* | KEEP id"));
        assertAcked(createView("outer_opted_in_view", """
            SET wildcards_match_views=true;
            FROM inner_plain_view
            """));
        assertAcked(createView(IndexNameExpressionResolver.resolveDateMathExpression("<wcvdm-{now/d}>"), "FROM wcv_data"));
        assertAcked(createView("date_math_view", "FROM wcvdm-*, \"<wcvdm-{now/d}>\" | WHERE id > 0"));

        assertThat(
            warningHeaders("SET wildcards_match_views=true; FROM body_view"),
            hasItem(wildcardsStoppedWarning("views", "body_view", "views [wcv_leak], [wcv_other]"))
        );
        // The enclosing view's SET stops at the nested view in the same way, and the warning names the view that SETs it.
        assertThat(
            warningHeaders("FROM outer_opted_in_view"),
            hasItem(wildcardsStoppedWarning("views", "outer_opted_in_view", "inner_plain_view", "views [wcv_leak], [wcv_other]", ""))
        );

        for (String query : List.of(
            // The caller did not SET it, so nothing would have matched.
            "FROM body_view",
            // The view SETs it itself.
            "SET wildcards_match_views=true; FROM opted_in_view",
            // The view names the views it reads exactly.
            "SET wildcards_match_views=true; FROM exact_view",
            // The view names the view its wildcard matches exactly, through date math.
            "SET wildcards_match_views=true; FROM date_math_view",
            // The view's wildcard matches no view.
            "SET wildcards_match_views=true; FROM index_only_view"
        )) {
            assertThat(query, warningHeaders(query), not(hasItem(containsString("does not affect the contents of view"))));
        }

        // Created last, since the other views' wildcards would match it too. Following the plain advice would make the view's
        // wildcard match the view itself, which fails every query on it as a circular reference.
        assertAcked(createView("wcv_self", "FROM wcv_* | WHERE id > 0"));
        assertThat(
            warningHeaders("SET wildcards_match_views=true; FROM wcv_self"),
            hasItem(
                wildcardsStoppedWarning(
                    "views",
                    null,
                    "wcv_self",
                    "views [wcv_leak], [wcv_other]",
                    ", excluding view [wcv_self] from its wildcards since matching it would be a circular reference"
                )
            )
        );
    }

    private static Matcher<String> wildcardsStoppedWarning(String kind, String viewName, String matched) {
        return wildcardsStoppedWarning(kind, null, viewName, matched, "");
    }

    private static Matcher<String> wildcardsStoppedWarning(
        String kind,
        @Nullable String settingView,
        String viewName,
        String matched,
        String adviceSuffix
    ) {
        return equalTo(
            "SET wildcards_match_"
                + kind
                + "=true"
                + (settingView == null ? "" : " in view [" + settingView + "]")
                + " does not affect the contents of view ["
                + viewName
                + "], so its wildcards did not match "
                + matched
                + "; use that setting inside the view definition if you wish to change the view behaviour"
                + adviceSuffix
        );
    }

    /**
     * A cluster default of {@code unmapped_fields} applies to a query run on its own, and the same text stored as a view
     * picks it up too: a definition that does not {@code SET} the mode takes the cluster default, like a query that does not.
     * The caller's own {@code SET} still stops at the view boundary.
     */
    public void testClusterDefaultOfUnmappedFieldsReachesViewBody() throws Exception {
        setupUnmappedIndices();
        assertAcked(createView("plain_view", "FROM view_source | KEEP id, extra"));
        String key = "esql.query.settings.unmapped_fields";
        updateClusterSettings(Settings.builder().put(key, "nullify"));
        try {
            try (var resp = run("FROM view_source | KEEP id, extra | SORT id")) {
                assertThat(getValuesList(resp), equalTo(List.of(Arrays.asList(1, null), Arrays.asList(2, null))));
            }
            try (var resp = run("FROM plain_view | SORT id")) {
                assertThat(getValuesList(resp), equalTo(List.of(Arrays.asList(1, null), Arrays.asList(2, null))));
            }
            // The caller's SET does not reach inside: the view keeps the cluster default.
            try (var resp = run("SET unmapped_fields=\"default\"; FROM plain_view | SORT id")) {
                assertThat(getValuesList(resp), equalTo(List.of(Arrays.asList(1, null), Arrays.asList(2, null))));
            }
            try (var resp = run("SET unmapped_fields=\"load\"; FROM plain_view | SORT id")) {
                assertThat(getValuesList(resp), equalTo(List.of(Arrays.asList(1, null), Arrays.asList(2, null))));
            }

            updateClusterSettings(Settings.builder().put(key, "load"));
            try (var resp = run("FROM view_source | KEEP id, extra | SORT id")) {
                assertThat(getValuesList(resp), equalTo(List.of(List.of(1, "e1"), List.of(2, "e2"))));
            }
            try (var resp = run("FROM plain_view | SORT id")) {
                assertThat(getValuesList(resp), equalTo(List.of(List.of(1, "e1"), List.of(2, "e2"))));
            }
            // The caller and the view body agree, so no boundary stopped anything and nothing is reported.
            assertThat(warningHeaders("FROM plain_view"), not(hasItem(containsString("does not affect the contents of view"))));
            // As with a view's own SET unmapped_fields="load", the body loads only the fields it references itself.
            assertAcked(createView("pass_view", "FROM view_source"));
            try (var resp = run("FROM pass_view | KEEP id, extra | SORT id")) {
                assertThat(getValuesList(resp), equalTo(List.of(Arrays.asList(1, null), Arrays.asList(2, null))));
            }
        } finally {
            updateClusterSettings(Settings.builder().putNull(key));
        }
    }

    /**
     * {@code time_zone} is not scoped at view boundaries: the caller's {@code SET} applies to the whole query, view bodies
     * included. Its cluster default must behave the same way, so a view's date logic follows the cluster's time zone.
     * Truncating to the day is time-zone sensitive, and the instant is just past midnight in UTC, still the previous day at -05:00.
     */
    public void testClusterDefaultOfTimeZoneReachesViewBody() {
        assertAcked(indicesAdmin().prepareCreate("tz_source").setMapping("d", "type=date"));
        prepareIndex("tz_source").setSource("d", "2026-01-01T00:30:00Z").setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();
        assertAcked(createView("tz_view", "FROM tz_source | EVAL truncated = DATE_TRUNC(1 day, d) | KEEP truncated"));
        String utc = "2026-01-01T00:00:00.000Z";
        String minusFive = "2025-12-31T00:00:00.000-05:00";

        assertThat(firstValue("FROM tz_view"), equalTo(utc));
        // The caller's SET reaches inside the view.
        assertThat(firstValue("SET time_zone=\"-05:00\"; FROM tz_view"), equalTo(minusFive));

        String key = "esql.query.settings.time_zone";
        updateClusterSettings(Settings.builder().put(key, "-05:00"));
        try {
            // The cluster default reaches inside the view exactly as the caller's SET does, also nested and next to an index.
            assertAcked(createView("tz_outer_view", "FROM tz_view"));
            assertThat(firstValue("FROM tz_view"), equalTo(minusFive));
            assertThat(firstValue("FROM tz_outer_view"), equalTo(minusFive));
            assertThat(
                firstValue("FROM tz_source, tz_view | EVAL truncated = COALESCE(truncated, DATE_TRUNC(1 day, d)) | KEEP truncated"),
                equalTo(minusFive)
            );
            // And the caller's SET still beats it, inside the view too.
            assertThat(firstValue("SET time_zone=\"Z\"; FROM tz_view"), equalTo(utc));
        } finally {
            updateClusterSettings(Settings.builder().putNull(key));
        }
    }

    /**
     * {@code approximation} is not scoped at view boundaries either: a query over a view is approximated, or not, the same way
     * whether the caller's {@code SET} or the cluster default turned it on.
     */
    public void testClusterDefaultOfApproximationActsLikeSetOverView() {
        setupWildcardViewsData();
        assertAcked(createView("approx_view", "FROM wcv_data | STATS c = COUNT(*)"));
        Boolean viaSet;
        try (var resp = run("SET approximation=true; FROM approx_view")) {
            viaSet = resp.approximationApplied();
        }
        String key = "esql.query.settings.approximation";
        updateClusterSettings(Settings.builder().put(key, "true"));
        try (var resp = run("FROM approx_view")) {
            assertThat(resp.approximationApplied(), equalTo(viaSet));
        } finally {
            updateClusterSettings(Settings.builder().putNull(key));
        }
        assertThat("SET approximation applies to a query over a view", viaSet, org.hamcrest.Matchers.notNullValue());
    }

    private String firstValue(String query) {
        try (var resp = run(query)) {
            return getValuesList(resp).get(0).get(0).toString();
        }
    }

    public void testLoadSetInsideViewLoadsFullyUnmappedField() {
        setupUnmappedIndices();
        assertAcked(createView("load_view", """
            SET unmapped_fields="load";
            FROM view_source | KEEP id, extra
            """));
        // The caller set nothing. The view's own SET loads extra from view_source.
        try (var resp = run("FROM load_view | SORT id")) {
            assertThat(getValuesList(resp), equalTo(List.of(List.of(1, "e1"), List.of(2, "e2"))));
        }
        // The caller's nullify does not override the definition.
        try (var resp = run("""
            SET unmapped_fields="nullify";
            FROM load_view | SORT id
            """)) {
            assertThat(getValuesList(resp), equalTo(List.of(List.of(1, "e1"), List.of(2, "e2"))));
        }
    }

    public void testNullifySetInsideViewNullFillsAndIgnoresCallerLoad() {
        setupUnmappedIndices();
        assertAcked(createView("null_view", """
            SET unmapped_fields="nullify";
            FROM view_source | KEEP id, extra
            """));
        // The view's nullify applies on its own, with no caller SET to keep its view branch.
        try (var resp = run("FROM null_view | SORT id")) {
            assertThat(getValuesList(resp), equalTo(List.of(Arrays.asList(1, null), Arrays.asList(2, null))));
        }
        try (var resp = run("""
            SET unmapped_fields="load";
            FROM null_view | SORT id
            """)) {
            assertThat(getValuesList(resp), equalTo(List.of(Arrays.asList(1, null), Arrays.asList(2, null))));
        }
    }

    public void testLoadSetInsideViewDoesNotLeakIntoNestedView() {
        setupUnmappedIndices();
        assertAcked(createView("inner_plain", "FROM view_source | KEEP id, extra"));
        assertAcked(createView("outer_load", """
            SET unmapped_fields="load";
            FROM inner_plain
            """));
        // extra is referenced in inner_plain, which did not SET load. The enclosing SET must not reach it.
        expectThrows(VerificationException.class, containsString("Unknown column [extra]"), () -> run("FROM outer_load").close());
    }

    public void testLoadSetInsideNestedViewAppliesWithoutEnclosingSet() {
        setupUnmappedIndices();
        assertAcked(createView("inner_load", """
            SET unmapped_fields="load";
            FROM view_source | KEEP id, extra
            """));
        assertAcked(createView("outer_plain", "FROM inner_load"));
        try (var resp = run("FROM outer_plain | SORT id")) {
            assertThat(getValuesList(resp), equalTo(List.of(List.of(1, "e1"), List.of(2, "e2"))));
        }
    }

    /**
     * The view body is analysed without the caller's setting, so its RENAME stays as written when the caller's reference is
     * null-filled at the view's output: the renamed column keeps its values and the original name does not come back.
     */
    public void testNullFillAtOutputOfViewEndingInRename() {
        setupUnmappedIndices();
        assertAcked(createView("rename_view", "FROM view_source | RENAME id AS ident"));
        for (String mode : List.of("nullify", "load")) {
            String set = "SET unmapped_fields=\"" + mode + "\"; ";
            try (var resp = run(set + "FROM rename_view | KEEP ident, extra | SORT ident")) {
                assertThat(mode, getValuesList(resp), equalTo(List.of(Arrays.asList(1, null), Arrays.asList(2, null))));
            }
            try (var resp = run(set + "FROM rename_view | EVAL x = other | SORT ident")) {
                assertThat(mode, columnNames(resp), equalTo(List.of("ident", "other", "x")));
                assertThat(mode, getValuesList(resp), equalTo(List.of(Arrays.asList(1, null, null), Arrays.asList(2, null, null))));
            }
        }
    }

    /**
     * The view's own SET loads {@code extra} under a new name, and the caller's setting, whichever mode, only null-fills a
     * further reference at the view's output.
     */
    public void testNullFillAtOutputOfLoadViewEndingInRename() {
        setupUnmappedIndices();
        assertAcked(createView("rename_load", """
            SET unmapped_fields="load";
            FROM view_source | RENAME extra AS e
            """));
        try (var resp = run("FROM rename_load | SORT id")) {
            assertThat(columnNames(resp), equalTo(List.of("id", "e")));
            assertThat(getValuesList(resp), equalTo(List.of(List.of(1, "e1"), List.of(2, "e2"))));
        }
        for (String mode : List.of("nullify", "load")) {
            try (var resp = run("SET unmapped_fields=\"" + mode + "\"; FROM rename_load | KEEP id, e, other | SORT id")) {
                assertThat(mode, getValuesList(resp), equalTo(List.of(Arrays.asList(1, "e1", null), Arrays.asList(2, "e2", null))));
            }
        }
    }

    /**
     * The enclosing view's SET stops at the nested view, so its reference to {@code extra} is null-filled at the nested view's
     * output. The nested view did not SET a mode, so its RENAME must stay as written, whether or not the caller SETs a mode too.
     */
    public void testSetInsideViewNullFillsAtOutputOfNestedViewEndingInRename() {
        setupUnmappedIndices();
        assertAcked(createView("inner_rename", "FROM view_source | RENAME id AS ident"));
        for (String viewMode : List.of("nullify", "load")) {
            assertAcked(createView("outer_set", "SET unmapped_fields=\"" + viewMode + "\";\nFROM inner_rename | KEEP ident, extra"));
            for (String query : List.of(
                "FROM outer_set | SORT ident",
                "SET unmapped_fields=\"nullify\"; FROM outer_set | SORT ident",
                "SET unmapped_fields=\"load\"; FROM outer_set | SORT ident"
            )) {
                try (var resp = run(query)) {
                    assertThat(viewMode + ": " + query, columnNames(resp), equalTo(List.of("ident", "extra")));
                    assertThat(
                        viewMode + ": " + query,
                        getValuesList(resp),
                        equalTo(List.of(Arrays.asList(1, null), Arrays.asList(2, null)))
                    );
                }
            }
        }
    }

    /**
     * {@code foo} is {@code double} in {@code mt_a}, {@code long} in {@code mt_b}, and present only in {@code _source} in
     * {@code mt_c}. The view loads it, so {@code TO_DEGREES(foo)} is a type conflict even when the caller set nothing.
     */
    public void testLoadSetInsideViewReportsMixedTypeConflictWhenCallerUsesDefault() {
        String dynamicOff = "{\"dynamic\": false, \"properties\": {\"id\": {\"type\": \"integer\"}}}";
        assertAcked(indicesAdmin().prepareCreate("mt_a").setMapping("""
            {"dynamic": false, "properties": {"id": {"type": "integer"}, "foo": {"type": "double"}}}
            """));
        assertAcked(indicesAdmin().prepareCreate("mt_b").setMapping("""
            {"dynamic": false, "properties": {"id": {"type": "integer"}, "foo": {"type": "long"}}}
            """));
        assertAcked(indicesAdmin().prepareCreate("mt_c").setMapping(dynamicOff));
        prepareIndex("mt_a").setSource("id", 1, "foo", 1.5).setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();
        prepareIndex("mt_b").setSource("id", 2, "foo", 2).setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();
        prepareIndex("mt_c").setSource("id", 3, "foo", "3").setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();
        assertAcked(createView("mt_view", """
            SET unmapped_fields="load";
            FROM mt_a, mt_b, mt_c | EVAL x = TO_DEGREES(foo) | KEEP id, x
            """));
        for (String query : List.of(
            "FROM mt_view",
            "SET unmapped_fields=\"nullify\"; FROM mt_view",
            "SET unmapped_fields=\"load\"; FROM mt_view"
        )) {
            expectThrows(
                VerificationException.class,
                containsString("Cannot use field [foo] due to ambiguities"),
                () -> run(query).close()
            );
        }
    }

    /**
     * Loading a subfield of a flattened field is rejected under {@code load}, and a view that SETs {@code load} must reject it
     * the same way when the caller set nothing. {@code foo} is flattened in {@code flat_a}; {@code foo.bar} is a keyword in
     * {@code flat_b}, so {@code foo} is type-conflicted over both; {@code flat_c} maps neither, so over {@code flat_a, flat_c}
     * {@code foo} is flattened in only one index.
     */
    public void testLoadSetInsideViewRejectsFlattenedSubfield() {
        assertAcked(indicesAdmin().prepareCreate("flat_a").setMapping("""
            {"dynamic": false, "properties": {"id": {"type": "integer"}, "foo": {"type": "flattened"}}}
            """));
        assertAcked(indicesAdmin().prepareCreate("flat_b").setMapping("""
            {"dynamic": false, "properties": {"id": {"type": "integer"}, "foo.bar": {"type": "keyword"}}}
            """));
        assertAcked(indicesAdmin().prepareCreate("flat_c").setMapping("""
            {"dynamic": false, "properties": {"id": {"type": "integer"}}}
            """));
        List<List<String>> cases = List.of(
            List.of("FROM flat_a | KEEP foo.a", "Loading subfield [foo.a] when parent [foo]"),
            List.of("FROM flat_a, flat_b | WHERE foo.bar == \"x\"", "Loading subfield [foo.bar] when parent [foo]"),
            List.of("FROM flat_a, flat_c | KEEP foo.a", "Loading subfield [foo.a] when parent [foo]")
        );
        for (int i = 0; i < cases.size(); i++) {
            String body = cases.get(i).get(0);
            String error = cases.get(i).get(1) + " is of flattened field type is not supported with unmapped_fields=\"load\"";
            String viewName = "flat_view_" + i;
            expectThrows(VerificationException.class, containsString(error), () -> run("SET unmapped_fields=\"load\"; " + body).close());
            assertAcked(createView(viewName, "SET unmapped_fields=\"load\"; " + body));
            expectThrows(VerificationException.class, containsString(error), () -> run("FROM " + viewName).close());
        }
    }

    /**
     * {@code t} is {@code text} in {@code tp_a} and only in {@code _source} in {@code tp_b}. Loading it nulls the unmapped
     * value and warns. The warning follows the view's SET, not the caller's.
     */
    public void testLoadSetInsideViewWarnsAboutPartiallyUnmappedText() throws Exception {
        assertAcked(indicesAdmin().prepareCreate("tp_a").setMapping("""
            {"dynamic": false, "properties": {"id": {"type": "integer"}, "t": {"type": "text"}}}
            """));
        assertAcked(indicesAdmin().prepareCreate("tp_b").setMapping("""
            {"dynamic": false, "properties": {"id": {"type": "integer"}}}
            """));
        prepareIndex("tp_a").setSource("id", 1, "t", "a1").setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();
        prepareIndex("tp_b").setSource("id", 2, "t", "b2").setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();
        assertAcked(createView("tp_view", """
            SET unmapped_fields="load";
            FROM tp_a, tp_b | KEEP id, t
            """));
        try (var resp = run("FROM tp_view | SORT id")) {
            assertThat(getValuesList(resp), equalTo(List.of(Arrays.asList(1, "a1"), Arrays.asList(2, null))));
        }
        for (String query : List.of(
            "FROM tp_view | SORT id",
            "SET unmapped_fields=\"nullify\"; FROM tp_view | SORT id",
            "SET unmapped_fields=\"load\"; FROM tp_view | SORT id"
        )) {
            assertThat(warningHeaders(query), hasItem(containsString("Field [t] is unmapped in some indices")));
        }
    }

    /**
     * A caller who knows a view's contents may expect their {@code SET unmapped_fields="load"} to load from the view's indices.
     * When an outer reference is null-filled at the view's output for that reason, the response says so, naming the view.
     */
    public void testLoadWarnsWhenViewBoundaryNullFillsReference() throws Exception {
        setupUnmappedIndices();
        assertAcked(createView("pass_through_view", "FROM view_source"));
        assertAcked(createView("filtered_view", "FROM view_source | WHERE id > 0"));
        assertAcked(createView("stats_view", "FROM view_source | STATS c = COUNT(*) BY id"));
        assertAcked(createView("keep_view", "FROM view_source | KEEP id"));
        assertAcked(createView("load_view", """
            SET unmapped_fields="load";
            FROM view_source
            """));
        assertAcked(createView("inner_plain", "FROM view_source | WHERE id > 0"));
        assertAcked(createView("outer_load", """
            SET unmapped_fields="load";
            FROM inner_plain | KEEP id, extra
            """));
        assertAcked(createView("multi_plain", "FROM pass_through_view, filtered_view"));
        assertAcked(createView("top_plain", "FROM multi_plain, caller_idx"));
        assertAcked(createView("union_keep_view", "FROM (FROM caller_idx | KEEP id), (FROM view_source | KEEP id) | WHERE id > 0"));

        assertThat(
            warningHeaders("SET unmapped_fields=\"load\"; FROM pass_through_view | KEEP id, extra"),
            hasItem(viewBoundaryWarning("load", "pass_through_view", "field [extra] is"))
        );
        List<String> mixed = warningHeaders("SET unmapped_fields=\"load\"; FROM caller_idx, filtered_view | KEEP id, extra, other");
        assertThat(mixed, hasItem(viewBoundaryWarning("load", "filtered_view", "fields [extra], [other] are")));
        assertThat(mixed, not(hasItem(containsString("caller_idx"))));
        // A field referenced only by a later command is null-filled in a later pass, under the alignment the earlier fill caused.
        assertThat(
            warningHeaders("SET unmapped_fields=\"load\"; FROM caller_idx, filtered_view | EVAL a = extra | KEEP id, a, other"),
            hasItem(viewBoundaryWarning("load", "filtered_view", "fields [extra], [other] are"))
        );
        assertThat(
            warningHeaders("SET unmapped_fields=\"load\"; FROM filtered_view | DROP extra | KEEP id, other"),
            hasItem(viewBoundaryWarning("load", "filtered_view", "field [other] is"))
        );
        if (EsqlCapabilities.Cap.OPTIONAL_FIELDS_LOAD_ALL_V2.isEnabled()) {
            assertThat(
                warningHeaders("SET unmapped_fields=\"load_all\"; FROM filtered_view | KEEP id, extra"),
                hasItem(viewBoundaryWarning("load_all", "filtered_view", "field [extra] is"))
            );
        }
        // The enclosing view's SET stops at the nested view in the same way, and the warning names the view that SETs it.
        assertThat(
            warningHeaders("FROM outer_load"),
            hasItem(viewBoundaryWarning("load", "outer_load", "inner_plain", null, "field [extra] is"))
        );
        // The view already SETs load, so it only has to reference the field.
        assertThat(
            warningHeaders("SET unmapped_fields=\"load\"; FROM load_view | KEEP id, extra"),
            hasItem(loadingViewBoundaryWarning("load_view", "field [extra] is"))
        );
        // A nested view keeps its own name only while its rows stay apart from the enclosing view's: the rows of a pass-through
        // view merge into the enclosing view's, so those are reported under the enclosing view.
        List<String> nested = warningHeaders("SET unmapped_fields=\"load\"; FROM multi_plain | KEEP id, extra");
        assertThat(nested, hasItem(viewBoundaryWarning("load", null, "filtered_view", "multi_plain", "field [extra] is")));
        assertThat(nested, hasItem(viewBoundaryWarning("load", "multi_plain", "field [extra] is")));
        // Lifted through two levels, a nested view is named with the view that reads it, not the outermost one.
        List<String> twoLevels = warningHeaders("SET unmapped_fields=\"load\"; FROM top_plain | KEEP id, extra");
        assertThat(twoLevels, hasItem(viewBoundaryWarning("load", null, "filtered_view", "multi_plain", "field [extra] is")));
        assertThat(twoLevels, hasItem(viewBoundaryWarning("load", null, "multi_plain", "top_plain", "field [extra] is")));
        assertThat(twoLevels, hasItem(viewBoundaryWarning("load", "top_plain", "field [extra] is")));

        for (String query : List.of(
            // No loading mode at the caller: nothing would have been loaded.
            "SET unmapped_fields=\"nullify\"; FROM filtered_view | KEEP id, extra",
            // The view aggregates the field away, so it would be null whatever the mode inside.
            "SET unmapped_fields=\"load\"; FROM stats_view | KEEP id, extra",
            // The view's KEEP hides the field, whether alone or under the alignment of a union with the caller's index.
            "SET unmapped_fields=\"load\"; FROM keep_view | KEEP id, extra",
            "SET unmapped_fields=\"load\"; FROM caller_idx, keep_view | KEEP id, extra",
            "SET unmapped_fields=\"load\"; FROM caller_idx, keep_view | EVAL a = extra | KEEP id, a, other",
            // Every branch of the view's union hides the field.
            "SET unmapped_fields=\"load\"; FROM union_keep_view | KEEP id, extra",
            // The caller references nothing the view lacks.
            "SET unmapped_fields=\"load\"; FROM filtered_view | KEEP id",
            // The caller drops the field, so it is not in the result either way.
            "SET unmapped_fields=\"load\"; FROM caller_idx, filtered_view | DROP extra"
        )) {
            assertThat(query, warningHeaders(query), not(hasItem(containsString("does not affect the contents of view"))));
        }
    }

    private static Matcher<String> viewBoundaryWarning(String mode, String viewName, String fields) {
        return viewBoundaryWarning(mode, null, viewName, null, fields);
    }

    private static Matcher<String> viewBoundaryWarning(
        String mode,
        @Nullable String settingView,
        String viewName,
        @Nullable String enclosingView,
        String fields
    ) {
        return viewBoundaryWarning(mode, settingView, viewName, enclosingView, fields, "SET unmapped_fields=\"load\" and reference");
    }

    private static Matcher<String> loadingViewBoundaryWarning(String viewName, String fields) {
        return viewBoundaryWarning("load", null, viewName, null, fields, "reference");
    }

    private static Matcher<String> viewBoundaryWarning(
        String mode,
        @Nullable String settingView,
        String viewName,
        @Nullable String enclosingView,
        String fields,
        String remedy
    ) {
        return equalTo(
            "SET unmapped_fields=\""
                + mode
                + "\""
                + (settingView == null ? "" : " in view [" + settingView + "]")
                + " does not affect the contents of view ["
                + viewName
                + "]"
                + (enclosingView == null ? "" : " nested in view [" + enclosingView + "]")
                + ", so "
                + fields
                + " null in its rows; to load such fields, the view definition must "
                + remedy
                + " them itself"
        );
    }

    public void testLoadSetInsideViewLoadsPartiallyUnmappedField() {
        setupPunkIndices();
        assertAcked(createView("punk_load", """
            SET unmapped_fields="load";
            FROM punk_a, punk_b | KEEP id, n
            """));
        try (var resp = run("FROM punk_load | SORT id")) {
            assertThat(getValuesList(resp), equalTo(List.of(List.of(1, 10), List.of(2, 20))));
        }
    }

    public void testLoadSetInsideViewDoesNotLoadPartiallyUnmappedFieldOfNestedView() {
        setupPunkIndices();
        assertAcked(createView("punk_inner", "FROM punk_a, punk_b | KEEP id, n"));
        assertAcked(createView("punk_outer", """
            SET unmapped_fields="load";
            FROM punk_inner
            """));
        // n is mapped in punk_a and only in _source in punk_b. The enclosing LOAD must not load punk_b's n.
        try (var resp = run("FROM punk_outer | SORT id")) {
            assertThat(getValuesList(resp), equalTo(List.of(Arrays.asList(1, 10), Arrays.asList(2, null))));
        }
    }

    /**
     * {@code FROM a*} sees index {@code abc} and, once the caller SETs {@code wildcards_match_views}, view {@code a_view}.
     * {@code a_view}'s {@code FROM b*} sees index {@code bcd} and, only once {@code a_view} itself SETs the flag, view
     * {@code b_view}. {@code b_view}'s {@code FROM c*} likewise sees {@code c_view} only when {@code b_view} SETs the flag.
     * Each definition stands alone.
     */
    public void testWildcardSettingInsideViewResolvesExactlyOneMoreLevel() {
        String mapping = """
            {"dynamic": false, "properties": {"id": {"type": "integer"}}}
            """;
        for (String index : List.of("abc", "bcd", "cde")) {
            assertAcked(indicesAdmin().prepareCreate(index).setMapping(mapping));
        }
        prepareIndex("abc").setSource("id", 1).setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();
        prepareIndex("bcd").setSource("id", 2).setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();
        prepareIndex("cde").setSource("id", 3).setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();
        // 1003 is the marker that c_view was resolved. 3 is cde itself.
        assertAcked(createView("c_view", "FROM cde | EVAL id = id + 1000 | KEEP id"));
        assertAcked(createView("b_view", "FROM c* | KEEP id"));
        assertAcked(createView("a_view", "FROM b* | KEEP id"));

        try (var resp = run("FROM a* | KEEP id | SORT id")) {
            assertThat(getValuesList(resp), equalTo(List.of(List.of(1))));
        }

        // Outer SET opens a_view. a_view did not opt in, so b* matches bcd (id 2) and not b_view (which would add cde's 3).
        try (var resp = run("""
            SET wildcards_match_views=true;
            FROM a* | KEEP id | SORT id
            """)) {
            assertThat(getValuesList(resp), equalTo(List.of(List.of(1), List.of(2))));
        }

        assertAcked(createView("a_view", """
            SET wildcards_match_views=true;
            FROM b* | KEEP id
            """));
        // a_view now opens b_view. b_view did not opt in, so c* matches cde (id 3) and not c_view (id 1003).
        try (var resp = run("""
            SET wildcards_match_views=true;
            FROM a* | KEEP id | SORT id
            """)) {
            assertThat(getValuesList(resp), equalTo(List.of(List.of(1), List.of(2), List.of(3))));
        }

        assertAcked(createView("b_view", """
            SET wildcards_match_views=true;
            FROM c* | KEEP id
            """));
        try (var resp = run("""
            SET wildcards_match_views=true;
            FROM a* | KEEP id | SORT id
            """)) {
            assertThat(getValuesList(resp), equalTo(List.of(List.of(1), List.of(2), List.of(3), List.of(1003))));
        }

        // The nested definitions opted in, but the outer query did not, so a_view is not opened.
        try (var resp = run("FROM a* | KEEP id | SORT id")) {
            assertThat(getValuesList(resp), equalTo(List.of(List.of(1))));
        }
    }

    /**
     * Response headers live on the thread context of the node that ran the request, so the query and the header read
     * must use the same node.
     */
    private List<String> warningHeaders(String query) throws InterruptedException {
        String node = internalCluster().getRandomNodeName();
        List<String> warnings = new CopyOnWriteArrayList<>();
        AtomicReference<Exception> failure = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);
        client(node).execute(
            EsqlQueryAction.INSTANCE,
            syncEsqlQueryRequest(query),
            new LatchedActionListener<>(
                ActionListener.wrap(
                    response -> internalCluster().getInstance(TransportService.class, node)
                        .getThreadPool()
                        .getThreadContext()
                        .getResponseHeaders()
                        .getOrDefault("Warning", List.of())
                        .stream()
                        .map(w -> HeaderWarning.decodeAndUnescape(HeaderWarning.extractWarningValueFromWarningHeader(w, false)))
                        .forEach(warnings::add),
                    failure::set
                ),
                latch
            )
        );
        assertTrue("query did not complete", latch.await(30, TimeUnit.SECONDS));
        if (failure.get() != null) {
            throw new AssertionError("query failed: " + query, failure.get());
        }
        return warnings;
    }

    private AcknowledgedResponse createView(String viewName, String query) {
        createdViews.add(viewName);
        return client().execute(
            PutViewAction.INSTANCE,
            new PutViewAction.Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, new View(viewName, query, null, false))
        ).actionGet(30, TimeUnit.SECONDS);
    }

    /**
     * The test cluster does not wipe views between tests, and a view left by one test can match another test's wildcard
     * (for example {@code bad_view} matching {@code b*}).
     */
    @After
    public void deleteCreatedViews() {
        if (createdViews.isEmpty()) {
            return;
        }
        try {
            client().execute(
                DeleteViewAction.INSTANCE,
                new DeleteViewAction.Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, createdViews.toArray(String[]::new))
            ).actionGet(30, TimeUnit.SECONDS);
        } catch (ResourceNotFoundException ignored) {
            // already deleted
        } finally {
            createdViews.clear();
        }
    }
}
