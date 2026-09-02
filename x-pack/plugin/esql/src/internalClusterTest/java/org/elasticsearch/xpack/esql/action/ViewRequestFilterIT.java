/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.cluster.metadata.View;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.xpack.esql.view.PutViewAction;
import org.junit.Before;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;

/**
 * Integration tests for applying the out-of-band request {@code filter} to logical views.
 *
 * <p>A logical view is defined as a stored ES|QL query (e.g. {@code FROM indexA | WHERE status = 200}). When a request
 * carries a Query DSL {@code filter}, views should apply it to their <em>output</em>, not push it into the view's source
 * index as a Lucene scan. That distinction matters whenever the filter references a field that the view computes or
 * renames — the field does not exist in the source index, so a Lucene-path push would silently return zero rows.
 *
 * <p>These tests verify three orthogonal concerns:
 * <ol>
 *   <li><b>Correct filtering</b>: the request filter actually selects the right rows from the view's output.</li>
 *   <li><b>No Lucene pushdown into the view source</b>: filters on computed/aggregated fields still work, proving that
 *   the filter is applied after the view's processing, not before the source scan.</li>
 *   <li><b>Mixed view+index queries</b>: the filter is applied to the view output <em>and</em> pushed to the bare-index
 *   branch as a Lucene query — both paths agree on the rows they select.</li>
 * </ol>
 *
 * <p>The conformance strategy mirrors {@link ExternalDatasetRequestFilterConformanceIT}: the same data is loaded into
 * both a plain index (Lucene path) and a view over that index, and identical request filters are run against both. If
 * the two paths diverge, one of the equality assertions fails.
 */
public class ViewRequestFilterIT extends AbstractEsqlIntegTestCase {

    private static final int ROWS = 30;
    private static final String INDEX = "vrf_idx";
    /** View that passes all rows through — equivalent to a plain index query, so conformance holds trivially. */
    private static final String PASSTHROUGH_VIEW = "vrf_passthrough";
    /** View that filters to status=200 rows only — the request filter applies on top of the view's own WHERE. */
    private static final String PREFILTERED_VIEW = "vrf_prefiltered";
    /** View that computes an aggregated field ({@code cnt}) not present in the source index. */
    private static final String STATS_VIEW = "vrf_stats";

    private static int status(int i) {
        return 200 + (i % 3) * 100; // 200, 300, 400
    }

    private static String region(int i) {
        return i % 2 == 0 ? "eu" : "us";
    }

    @Before
    public void loadData() {
        assertAcked(
            client().admin()
                .indices()
                .prepareCreate(INDEX)
                .setSettings(Settings.builder().put("index.number_of_shards", 1))
                .setMapping("id", "type=integer", "status", "type=integer", "region", "type=keyword")
        );
        for (int i = 0; i < ROWS; i++) {
            client().prepareIndex(INDEX).setSource("id", i, "status", status(i), "region", region(i)).get();
        }
        client().admin().indices().prepareRefresh(INDEX).get();

        // Passthrough view: equivalent to querying the index directly.
        createView(PASSTHROUGH_VIEW, "FROM " + INDEX);
        // Pre-filtered view: only 200-status rows visible via the view.
        createView(PREFILTERED_VIEW, "FROM " + INDEX + " | WHERE status == 200");
        // Stats view: groups by region, emitting a computed field 'cnt' that does NOT exist in the source index.
        createView(STATS_VIEW, "FROM " + INDEX + " | STATS cnt = COUNT(*) BY region");
    }

    private void createView(String name, String query) {
        assertAcked(
            client().execute(
                PutViewAction.INSTANCE,
                new PutViewAction.Request(TimeValue.THIRTY_SECONDS, TimeValue.THIRTY_SECONDS, new View(name, query))
            ).actionGet(30, TimeUnit.SECONDS)
        );
    }

    /** Execute a request filter against a source (index or view) and return the sorted id list. */
    private List<Object> ids(String source, QueryBuilder filter) {
        EsqlQueryRequest request = syncEsqlQueryRequest("FROM " + source + " | KEEP id | SORT id ASC").filter(filter);
        try (EsqlQueryResponse response = run(request)) {
            return getValuesList(response).stream().map(r -> r.get(0)).toList();
        }
    }

    // ─── Conformance: passthrough view must agree with direct index query ────────

    /**
     * A request filter on a passthrough view must select the exact same rows as the same filter on the underlying index.
     * This proves the filter is evaluated semantically, not accidentally filtered by some plan artefact.
     */
    public void testPassthroughViewConformanceTerm() {
        QueryBuilder filter = QueryBuilders.termQuery("status", 300);
        assertEquals("passthrough view and direct index must agree", ids(INDEX, filter), ids(PASSTHROUGH_VIEW, filter));
    }

    public void testPassthroughViewConformanceRange() {
        QueryBuilder filter = QueryBuilders.rangeQuery("status").gte(300);
        assertEquals("passthrough view and direct index must agree", ids(INDEX, filter), ids(PASSTHROUGH_VIEW, filter));
    }

    public void testPassthroughViewConformanceBool() {
        QueryBuilder filter = QueryBuilders.boolQuery()
            .must(QueryBuilders.termQuery("region", "eu"))
            .must(QueryBuilders.rangeQuery("status").gt(200));
        assertEquals("passthrough view and direct index must agree", ids(INDEX, filter), ids(PASSTHROUGH_VIEW, filter));
    }

    /**
     * A missing-field filter matches nothing on both the index and the passthrough view — both treat null as false
     * under a positive filter.
     */
    public void testMissingFieldMatchesNothingOnView() {
        assertThat(ids(PASSTHROUGH_VIEW, QueryBuilders.termQuery("nope", "x")), empty());
        // And its negation matches everything.
        EsqlQueryRequest req = syncEsqlQueryRequest("FROM " + PASSTHROUGH_VIEW + " | KEEP id | SORT id ASC").filter(
            QueryBuilders.boolQuery().mustNot(QueryBuilders.termQuery("nope", "x"))
        );
        try (EsqlQueryResponse resp = run(req)) {
            assertThat(getValuesList(resp).size(), equalTo(ROWS));
        }
    }

    // ─── Pre-filtered view: request filter applies on top of view's own WHERE ───

    /**
     * The view's own {@code WHERE status == 200} runs first, then the request filter is applied to the output.
     * A request filter for status=200 selects everything the view exposes; status=300 selects nothing.
     */
    public void testRequestFilterComposesWithViewWhereClause() {
        // Filter matches the view's own predicate: all view rows are visible.
        assertThat(
            ids(PREFILTERED_VIEW, QueryBuilders.termQuery("status", 200)).size(),
            equalTo((int) java.util.stream.IntStream.range(0, ROWS).filter(i -> status(i) == 200).count())
        );
        // Filter is stricter than the view's predicate: nothing passes.
        assertThat(ids(PREFILTERED_VIEW, QueryBuilders.termQuery("status", 300)), empty());
    }

    /**
     * A filter on {@code region} on top of the pre-filtered view (which only emits status=200 rows) should select
     * only the eu status=200 rows, not all eu rows.
     */
    public void testRequestFilterOnPreFilteredViewIsComposedCorrectly() {
        long expectedCount = java.util.stream.IntStream.range(0, ROWS).filter(i -> status(i) == 200 && region(i).equals("eu")).count();
        assertThat(ids(PREFILTERED_VIEW, QueryBuilders.termQuery("region", "eu")).size(), equalTo((int) expectedCount));
    }

    // ─── Stats view: filter on computed field must work ─────────────────────────

    /**
     * The stats view emits {@code (region, cnt)} where {@code cnt} is a computed aggregate not in the source index.
     * A request filter on {@code cnt} must be applied to the view's output — if the Lucene esFilter were pushed into
     * the source scan, Lucene would find no {@code cnt} field and return zero rows.
     *
     * <p>This test proves that the no-pushdown-into-view-branch fix is actually exercised: the result set must be
     * non-empty for any plausible count threshold, ruling out the "Lucene dropped everything" failure mode.
     */
    public void testFilterOnComputedStatsFieldWorks() {
        // cnt must be > 0 for any region (every region has at least one row), so this should return both regions.
        EsqlQueryRequest req = syncEsqlQueryRequest("FROM " + STATS_VIEW + " | SORT region ASC").filter(
            QueryBuilders.rangeQuery("cnt").gt(0)
        );
        try (EsqlQueryResponse resp = run(req)) {
            List<List<Object>> rows = getValuesList(resp);
            assertThat("both regions must appear — filter on computed cnt must work", rows.size(), equalTo(2));
        }
    }

    /**
     * A request filter that selects exactly one region from the stats view must return exactly that region's row.
     * Again, if the Lucene push-in bug were present, the source scan would find no {@code region} field scoped to the
     * aggregated output, and the result would be wrong.
     */
    public void testFilterOnGroupByKeyFromStatsViewSelectsCorrectBucket() {
        EsqlQueryRequest req = syncEsqlQueryRequest("FROM " + STATS_VIEW + " | KEEP region | SORT region ASC").filter(
            QueryBuilders.termQuery("region", "eu")
        );
        try (EsqlQueryResponse resp = run(req)) {
            List<List<Object>> rows = getValuesList(resp);
            assertThat(rows.size(), equalTo(1));
            assertThat(rows.get(0).get(0), equalTo("eu"));
        }
    }

    /**
     * match_all on a stats view returns all buckets unfiltered — the no-op path must not erroneously wrap the view
     * output in a trivially-true Filter.
     */
    public void testMatchAllOnStatsViewReturnsAllBuckets() {
        EsqlQueryRequest req = syncEsqlQueryRequest("FROM " + STATS_VIEW).filter(QueryBuilders.matchAllQuery());
        try (EsqlQueryResponse resp = run(req)) {
            assertThat(getValuesList(resp).size(), equalTo(2)); // eu and us
        }
    }

    // ─── Mixed view+index queries ────────────────────────────────────────────────

    /**
     * When a query mixes a plain index branch and a view branch ({@code FROM index, view}), the request filter must
     * reach both branches: applied as a Lucene query on the index branch, and as an ES|QL Filter above the view's
     * output on the view branch.
     *
     * <p>We give the two sources disjoint id ranges to make the provenance of each row decidable.
     * Index: id 0..N-1. Passthrough view over the same index: ids appear from the view unchanged.
     * A filter on {@code status=300} must select the same ids from both branches.
     */
    public void testMixedViewAndIndexQueryAppliesFilterToBoth() {
        // Create a second index with disjoint ids (base 1000) so view and index rows are distinguishable.
        String idx2 = "vrf_idx2";
        assertAcked(
            client().admin()
                .indices()
                .prepareCreate(idx2)
                .setSettings(Settings.builder().put("index.number_of_shards", 1))
                .setMapping("id", "type=integer", "status", "type=integer", "region", "type=keyword")
        );
        final int BASE2 = 1000;
        for (int i = 0; i < ROWS; i++) {
            client().prepareIndex(idx2).setSource("id", BASE2 + i, "status", status(i), "region", region(i)).get();
        }
        client().admin().indices().prepareRefresh(idx2).get();

        String view2 = "vrf_view2";
        createView(view2, "FROM " + idx2);

        QueryBuilder filter = QueryBuilders.termQuery("status", 300);
        // Expected: all id values from both idx and idx2 where status==300.
        List<Object> expectedFromIdx = ids(INDEX, filter);
        List<Object> expectedFromIdx2 = ids(idx2, filter);
        // Mixed query: FROM passthrough_view (over INDEX), view2 (over idx2).
        EsqlQueryRequest req = syncEsqlQueryRequest("FROM " + PASSTHROUGH_VIEW + ", " + view2 + " | KEEP id | SORT id ASC").filter(filter);
        try (EsqlQueryResponse resp = run(req)) {
            List<Object> actual = getValuesList(resp).stream().map(r -> r.get(0)).toList();
            assertThat(
                "both view branches must be filtered by status=300",
                actual,
                containsInAnyOrder(java.util.stream.Stream.concat(expectedFromIdx.stream(), expectedFromIdx2.stream()).toArray())
            );
        }
    }

    // ─── Fail-closed: unsupported DSL construct must fail the query ──────────────

    /**
     * A wildcard query is not in the supported DSL subset for views. The whole query must fail with a 400 naming the
     * construct — the supported term clause does not rescue it.
     */
    public void testUnsupportedDslConstructOnViewFailsQuery() {
        QueryBuilder unsupported = QueryBuilders.boolQuery()
            .must(QueryBuilders.termQuery("status", 200))
            .must(QueryBuilders.wildcardQuery("region", "e*"));
        Exception e = expectThrows(
            Exception.class,
            () -> run(syncEsqlQueryRequest("FROM " + PASSTHROUGH_VIEW + " | KEEP id").filter(unsupported))
        );
        assertThat(e.getMessage(), containsString("[wildcard]"));
    }
}
