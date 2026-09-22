/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.apache.http.util.EntityUtils;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.cluster.metadata.View;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.xpack.esql.view.PutViewAction;
import org.junit.Before;

import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.startsWith;

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

    /**
     * Six rows cover every {@code (status, region)} combination exactly once, so each expected result below can be written
     * out as a literal:
     * <pre>
     *   id | status | region
     *    0 |   200  |  eu
     *    1 |   300  |  us
     *    2 |   400  |  eu
     *    3 |   200  |  us
     *    4 |   300  |  eu
     *    5 |   400  |  us
     * </pre>
     */
    private static final int ROWS = 6;
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

    /** A real HTTP transport, so {@link #testUnsupportedDslConstructOnViewIsDroppedWithWarning} can read the {@code Warning} header. */
    @Override
    protected boolean addMockHttpTransport() {
        return false;
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
        indexRows(INDEX, 0);

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
                new PutViewAction.Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, new View(name, query))
            ).actionGet(30, TimeUnit.SECONDS)
        );
    }

    /** Bulk-indexes {@link #ROWS} rows with ids {@code base..base+ROWS-1} and the shared status/region pattern, then refreshes. */
    private static void indexRows(String index, int base) {
        BulkRequestBuilder bulk = client().prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        for (int i = 0; i < ROWS; i++) {
            bulk.add(new IndexRequest(index).source("id", base + i, "status", status(i), "region", region(i)));
        }
        indexDocs(bulk);
    }

    /** Bulk-indexes the given documents and refreshes, so they are immediately visible to the queries under test. */
    private static void indexDocs(IndexRequest... docs) {
        BulkRequestBuilder bulk = client().prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        for (IndexRequest doc : docs) {
            bulk.add(doc);
        }
        indexDocs(bulk);
    }

    private static void indexDocs(BulkRequestBuilder bulk) {
        BulkResponse response = bulk.get();
        assertFalse(response.buildFailureMessage(), response.hasFailures());
    }

    /** Execute a request filter against a source (index or view) and return the sorted id list. */
    private List<Object> ids(String source, QueryBuilder filter) {
        EsqlQueryRequest request = syncEsqlQueryRequest("FROM " + source + " | KEEP id | SORT id ASC").filter(filter);
        try (EsqlQueryResponse response = run(request)) {
            return getValuesList(response).stream().map(r -> r.get(0)).toList();
        }
    }

    /** Runs {@code query} with the request filter and returns every row. */
    private List<List<Object>> rows(String query, QueryBuilder filter) {
        try (EsqlQueryResponse response = run(syncEsqlQueryRequest(query).filter(filter))) {
            return getValuesList(response);
        }
    }

    // ─── Conformance: passthrough view must agree with direct index query ────────

    /**
     * The filter must select exactly {@code expectedIds} both on the index (Lucene path) and on the passthrough view
     * (view-output path). Spelling the ids out, rather than only comparing the two paths to each other, means a failure
     * says which path went wrong and how.
     */
    private void assertIndexAndPassthroughViewSelect(QueryBuilder filter, List<Object> expectedIds) {
        assertThat("direct index query", ids(INDEX, filter), equalTo(expectedIds));
        assertThat("passthrough view query", ids(PASSTHROUGH_VIEW, filter), equalTo(expectedIds));
    }

    /**
     * A request filter on a passthrough view must select the exact same rows as the same filter on the underlying index.
     * This proves the filter is evaluated semantically, not accidentally filtered by some plan artefact.
     */
    public void testPassthroughViewConformanceTerm() {
        assertIndexAndPassthroughViewSelect(QueryBuilders.termQuery("status", 300), List.of(1, 4));
    }

    public void testPassthroughViewConformanceRange() {
        assertIndexAndPassthroughViewSelect(QueryBuilders.rangeQuery("status").gte(300), List.of(1, 2, 4, 5));
    }

    public void testPassthroughViewConformanceBool() {
        QueryBuilder filter = QueryBuilders.boolQuery()
            .must(QueryBuilders.termQuery("region", "eu"))
            .must(QueryBuilders.rangeQuery("status").gt(200));
        assertIndexAndPassthroughViewSelect(filter, List.of(2, 4));
    }

    /**
     * A missing-field filter matches nothing on both the index and the passthrough view — both treat null as false
     * under a positive filter.
     */
    public void testMissingFieldMatchesNothingOnView() {
        assertThat(ids(PASSTHROUGH_VIEW, QueryBuilders.termQuery("nope", "x")), empty());
        // And its negation matches everything.
        QueryBuilder negated = QueryBuilders.boolQuery().mustNot(QueryBuilders.termQuery("nope", "x"));
        assertThat(ids(PASSTHROUGH_VIEW, negated), equalTo(List.of(0, 1, 2, 3, 4, 5)));
    }

    /**
     * A filter on a field the view does not output is not merely evaluated to no rows — the optimizer removes the view branch
     * altogether, so no work is scheduled for it. The rewriter binds the missing field to {@code NULL}, the translated leaf is then
     * a constant, and the standard pipeline takes over: {@code ConstantFolding} folds it to {@code false}, {@code PruneFilters}
     * collapses the filtered branch to an empty {@code LocalRelation}, and {@code PruneEmptyMergeBranches} drops that branch — or
     * the whole union, when every branch is a view. Pinned here so the rewriter never needs its own dead-branch detection.
     */
    public void testFilterOnFieldMissingFromViewPrunesTheViewBranch() {
        assumeTrue("EXPLAIN requires the capability to be enabled", EsqlCapabilities.Cap.EXPLAIN.isEnabled());
        QueryBuilder fake = QueryBuilders.termQuery("fake", 1);
        // View branch alongside a bare index: only the bare-index branch survives.
        String mixed = optimizedLogicalPlan("FROM " + PREFILTERED_VIEW + ", " + INDEX + " | KEEP id", fake);
        assertThat("the view branch is pruned, leaving the bare-index branch", mixed, containsString("ViewUnionAll[[main]]"));
        assertThat(mixed, not(containsString(PREFILTERED_VIEW)));
        // Every branch is a view: the union collapses to an empty local relation.
        String allViews = optimizedLogicalPlan("FROM " + PREFILTERED_VIEW + ", " + STATS_VIEW, fake);
        assertThat("no branch survives", allViews, startsWith("LocalRelation["));
        assertThat(allViews, containsString("EMPTY"));
        assertThat(allViews, not(containsString("ViewUnionAll")));
    }

    /** Runs {@code EXPLAIN} over {@code query} with the given request filter and returns the coordinator's optimized logical plan. */
    private String optimizedLogicalPlan(String query, QueryBuilder filter) {
        try (EsqlQueryResponse response = run(syncEsqlQueryRequest("EXPLAIN (" + query + ")").filter(filter))) {
            List<String> columns = response.columns().stream().map(ColumnInfoImpl::name).toList();
            int role = columns.indexOf("role");
            int type = columns.indexOf("type");
            int plan = columns.indexOf("plan");
            for (List<Object> row : getValuesList(response)) {
                if ("coordinator".equals(row.get(role)) && "optimizedLogicalPlan".equals(row.get(type))) {
                    return (String) row.get(plan);
                }
            }
        }
        throw new AssertionError("EXPLAIN returned no coordinator optimizedLogicalPlan row for [" + query + "]");
    }

    // ─── Pre-filtered view: request filter applies on top of view's own WHERE ───

    /**
     * The view's own {@code WHERE status == 200} runs first, then the request filter is applied to the output.
     * A request filter for status=200 selects everything the view exposes; status=300 selects nothing.
     */
    public void testRequestFilterComposesWithViewWhereClause() {
        // Filter matches the view's own predicate: all view rows are visible.
        assertThat(ids(PREFILTERED_VIEW, QueryBuilders.termQuery("status", 200)), equalTo(List.of(0, 3)));
        // Filter is stricter than the view's predicate: nothing passes.
        assertThat(ids(PREFILTERED_VIEW, QueryBuilders.termQuery("status", 300)), empty());
    }

    /**
     * A filter on {@code region} on top of the pre-filtered view (which only emits status=200 rows) should select
     * only the eu status=200 row (id 0), not all eu rows (0, 2, 4).
     */
    public void testRequestFilterOnPreFilteredViewIsComposedCorrectly() {
        assertThat(ids(PREFILTERED_VIEW, QueryBuilders.termQuery("region", "eu")), equalTo(List.of(0)));
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
        // Both regions have cnt == 3, so a threshold of 2 keeps both buckets; if the filter had been pushed into the
        // source scan there would be no cnt field to match and the result would be empty.
        assertThat(
            "filter on computed cnt must keep both buckets intact",
            rows("FROM " + STATS_VIEW + " | KEEP region, cnt | SORT region ASC", QueryBuilders.rangeQuery("cnt").gt(2)),
            equalTo(List.of(List.of("eu", 3L), List.of("us", 3L)))
        );
        // And a threshold above the actual counts removes both — proving the filter is evaluated against cnt's real value.
        assertThat(rows("FROM " + STATS_VIEW + " | KEEP region, cnt", QueryBuilders.rangeQuery("cnt").gt(3)), empty());
    }

    /**
     * A request filter that selects exactly one region from the stats view must return exactly that region's row.
     * Again, if the Lucene push-in bug were present, the source scan would find no {@code region} field scoped to the
     * aggregated output, and the result would be wrong.
     */
    public void testFilterOnGroupByKeyFromStatsViewSelectsCorrectBucket() {
        assertThat(
            rows("FROM " + STATS_VIEW + " | KEEP region, cnt", QueryBuilders.termQuery("region", "eu")),
            equalTo(List.of(List.of("eu", 3L)))
        );
    }

    /**
     * match_all on a stats view returns all buckets unfiltered — the no-op path must not erroneously wrap the view
     * output in a trivially-true Filter.
     */
    public void testMatchAllOnStatsViewReturnsAllBuckets() {
        assertThat(
            "match_all must leave every bucket visible",
            rows("FROM " + STATS_VIEW + " | KEEP region, cnt | SORT region ASC", QueryBuilders.matchAllQuery()),
            equalTo(List.of(List.of("eu", 3L), List.of("us", 3L)))
        );
    }

    // ─── Mixed view+index queries ────────────────────────────────────────────────

    /**
     * When a query names two views over different indices, the request filter must reach both view branches.
     *
     * <p>We give the two sources disjoint id ranges to make the provenance of each row decidable.
     * Index: id 0..N-1. Second index: ids from 1000. A filter on {@code status=300} must select the matching ids from
     * both branches.
     *
     * <p>For the genuinely mixed view + bare-index shape see
     * {@link #testMixedViewAndBareIndexQueryAppliesFilterToBoth} — the two branch kinds take different filter paths
     * there, which is what makes that case worth covering separately.
     */
    public void testTwoViewBranchesAppliesFilterToBoth() {
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
        indexRows(idx2, BASE2);

        String view2 = "vrf_view2";
        createView(view2, "FROM " + idx2);

        // status=300 is ids 1 and 4 in each index; idx2's copies sit at 1001 and 1004.
        assertThat(
            "both view branches must be filtered by status=300",
            rows("FROM " + PASSTHROUGH_VIEW + ", " + view2 + " | KEEP id | SORT id ASC", QueryBuilders.termQuery("status", 300)),
            equalTo(List.of(List.of(1), List.of(4), List.of(1001), List.of(1004)))
        );
    }

    /**
     * The genuinely mixed shape: {@code FROM view, index}. The two branches take different filter paths — the view
     * branch gets an ES|QL {@code Filter} above its output and is exempted from the Lucene push-in, while the bare
     * index branch gets the raw DSL pushed into its scan — and both must select the same rows their standalone
     * queries would.
     */
    public void testMixedViewAndBareIndexQueryAppliesFilterToBoth() {
        String idx3 = "vrf_idx3";
        assertAcked(
            client().admin()
                .indices()
                .prepareCreate(idx3)
                .setSettings(Settings.builder().put("index.number_of_shards", 1))
                .setMapping("id", "type=integer", "status", "type=integer", "region", "type=keyword")
        );
        final int BASE3 = 2000;
        indexRows(idx3, BASE3);

        // View branch first, bare index second: one ViewUnionAll carrying one view branch and one bare-index branch.
        // status=300 is ids 1 and 4 from the view, 2001 and 2004 from the bare index.
        assertThat(
            "the view branch and the bare-index branch must both be filtered, by different paths",
            rows("FROM " + PASSTHROUGH_VIEW + ", " + idx3 + " | KEEP id | SORT id ASC", QueryBuilders.termQuery("status", 300)),
            equalTo(List.of(List.of(1), List.of(4), List.of(2001), List.of(2004)))
        );
    }

    /**
     * An empty filter must be indistinguishable from sending no filter. Kibana sends one rather than omitting the field when no
     * filtering is wanted, and it would otherwise take the whole view-boundary path — suppressing view compaction — only to install a
     * no-op {@code Filter}. Asserted on the stats view because a computed field makes any stray filtering visible.
     */
    public void testEmptyFilterIsEquivalentToNoFilter() {
        String query = "FROM " + STATS_VIEW + " | KEEP region, cnt | SORT region ASC";
        List<List<Object>> unfiltered;
        try (EsqlQueryResponse resp = run(syncEsqlQueryRequest(query))) {
            unfiltered = getValuesList(resp);
        }
        for (QueryBuilder empty : List.of(
            QueryBuilders.matchAllQuery(),
            QueryBuilders.boolQuery(),
            QueryBuilders.boolQuery().filter(QueryBuilders.matchAllQuery()),
            QueryBuilders.boolQuery().must(QueryBuilders.boolQuery())
        )) {
            try (EsqlQueryResponse resp = run(syncEsqlQueryRequest(query).filter(empty))) {
                assertThat("empty filter [" + empty + "] must match the unfiltered result", getValuesList(resp), equalTo(unfiltered));
            }
        }
    }

    /**
     * A filter on a field the view creates with {@code EVAL} — present in the view's output but absent from the source index's
     * mapping. The request filter is also handed to field-caps as an {@code index_filter} when resolving sources, so a filter naming
     * such a field would resolve no indices at all; analysis retries without it, and the filter must still end up on the view's
     * output rather than in the source scan, where the field does not exist and nothing would match.
     *
     * <p>Complements {@link #testFilterOnComputedStatsFieldWorks}, which covers the same idea for a {@code STATS}-computed field.
     */
    public void testFilterOnEvalComputedFieldInViewWorks() {
        String view = "vrf_eval";
        createView(view, "FROM " + INDEX + " | EVAL region_upper = TO_UPPER(region)");

        assertThat(
            "the EVAL-computed field can only be filtered on the view's output",
            rows("FROM " + view + " | KEEP id, region_upper | SORT id ASC", QueryBuilders.termQuery("region_upper", "EU")),
            equalTo(List.of(List.of(0, "EU"), List.of(2, "EU"), List.of(4, "EU")))
        );
    }

    /**
     * A view that shifts {@code @timestamp}, over sources whose raw timestamps differ. The filter belongs on the view's output, so it
     * must not also be used to prune the view's sources during index resolution.
     *
     * <p>The request filter is handed to field-caps as an {@code index_filter}, which drops any index whose shards cannot match it.
     * {@code vrf_dp_b} holds only a January document, so a {@code range @timestamp >= March} prunes it — even though the view moves
     * that document 100 days forward, past the cutoff, and the filter should keep it. Because {@code vrf_dp_a} survives, the prune is
     * partial: no error, no retry, and the pruned index's row is simply missing from the result.
     *
     * <p>This is the realistic shape: Kibana's time picker is a {@code range} on {@code @timestamp}, and dates are the one field type
     * field-caps prunes by value ({@code DateFieldMapper} is the only override of {@code isFieldWithinQuery}; everything else reports
     * {@code INTERSECTS} and is never pruned). A {@code term}, or a {@code range} on a numeric field, would not show this.
     */
    public void testRequestFilterDoesNotPruneViewSources() {
        String a = "vrf_dp_a";
        String b = "vrf_dp_b";
        for (String index : List.of(a, b)) {
            assertAcked(
                client().admin()
                    .indices()
                    .prepareCreate(index)
                    .setSettings(Settings.builder().put("index.number_of_shards", 1))
                    .setMapping("id", "type=integer", "@timestamp", "type=date")
            );
        }
        indexDocs(
            new IndexRequest(a).source("id", 1, "@timestamp", "2024-01-01T00:00:00Z"),
            new IndexRequest(a).source("id", 2, "@timestamp", "2024-06-01T00:00:00Z"),
            new IndexRequest(b).source("id", 3, "@timestamp", "2024-01-01T00:00:00Z")
        );

        createView("vrf_dp_view", "FROM " + a + "," + b + " | EVAL @timestamp = @timestamp + 100 day | KEEP id, @timestamp");

        EsqlQueryRequest req = syncEsqlQueryRequest("FROM vrf_dp_view | KEEP id | SORT id ASC").filter(
            QueryBuilders.rangeQuery("@timestamp").gte("2024-03-01")
        );
        try (EsqlQueryResponse resp = run(req)) {
            assertThat(
                "every row's shifted timestamp is past the cutoff, including the one whose source the filter would have pruned",
                getValuesList(resp).stream().map(r -> r.get(0)).toList(),
                equalTo(List.of(1, 2, 3))
            );
        }
    }

    // ─── Views ending in KEEP ────────────────────────────────────────────────────

    /**
     * A view whose body ends in {@code KEEP} — the most ordinary view shape there is. Preserving its boundary for the filter used to
     * leave everything above the {@code ViewUnionAll} unresolved: the analyzer's merge alignment saw a branch that was already a
     * {@code Project} over exactly the merge columns, rewrote nothing, and returned the merge with the empty output view resolution
     * gave it. The filter rewriter then marked the tree analyzed, so the optimizer hit {@code UnresolvedException: Invalid call to
     * dataType on an unresolved object ?id} instead of verification reporting anything.
     */
    public void testFilterOnViewEndingInKeep() {
        String view = "vrf_keep";
        createView(view, "FROM " + INDEX + " | EVAL region_upper = TO_UPPER(region) | KEEP id, region_upper");

        assertThat(
            rows("FROM " + view + " | KEEP id, region_upper | SORT id ASC", QueryBuilders.termQuery("region_upper", "EU")),
            equalTo(List.of(List.of(0, "EU"), List.of(2, "EU"), List.of(4, "EU")))
        );
    }

    /**
     * Two views that both end in {@code KEEP} over the same columns. Every branch is already an aligned {@code Project}, so the
     * merge alignment rewrites nothing — the same path as {@link #testFilterOnViewEndingInKeep}, but with a multi-branch
     * {@code ViewUnionAll} that existed before boundaries were ever preserved.
     */
    public void testFilterOnTwoViewsBothEndingInKeep() {
        createView("vrf_keep_a", "FROM " + INDEX + " | KEEP id, status");
        createView("vrf_keep_b", "FROM " + INDEX + " | KEEP id, status");

        // Each view is a separate branch over the same index, so every status=300 id (1 and 4) appears once per view.
        assertThat(
            rows("FROM vrf_keep_a, vrf_keep_b | KEEP id | SORT id ASC", QueryBuilders.termQuery("status", 300)),
            equalTo(List.of(List.of(1), List.of(1), List.of(4), List.of(4)))
        );
    }

    // ─── Views whose body already branches ───────────────────────────────────────

    /**
     * A view whose body contains a subquery already branches, so it must not be given a boundary wrapper: nesting one
     * {@code MergePlan} inside another is unexecutable. Before this was handled, adding a request filter to such a view
     * turned a working query into a 400 ("Nested subqueries are not supported"), and bypassing that check only moved
     * the failure to execution ("ExchangeSourceHandler wasn't provided").
     *
     * <p>Note {@code FROM a, b} is a single multi-pattern relation, not a branch point — only a subquery in the body
     * creates one, which is why the two shapes behave differently here.
     *
     * <p>Such a view falls back to the pre-filter behaviour: the filter takes the index pushdown path. That is why the
     * assertion below is on a <em>mapped</em> field, where pushdown and view-output filtering agree. A filter on a
     * field the view computes still returns nothing for this shape — a known limitation, not covered here because it
     * is the open question of whether to fail loudly instead.
     */
    public void testRequestFilterOnViewWhoseBodyContainsSubqueryDoesNotFail() {
        String a = "vrf_branch_a";
        String b = "vrf_branch_b";
        for (String index : List.of(a, b)) {
            assertAcked(
                client().admin()
                    .indices()
                    .prepareCreate(index)
                    .setSettings(Settings.builder().put("index.number_of_shards", 1))
                    .setMapping("id", "type=integer", "region", "type=keyword")
            );
        }
        indexDocs(
            new IndexRequest(a).source("id", 1, "region", "eu"),
            new IndexRequest(a).source("id", 2, "region", "us"),
            new IndexRequest(b).source("id", 3, "region", "eu"),
            new IndexRequest(b).source("id", 4, "region", "us")
        );

        // A trailing operator after the union is what forces the branch point to stay nested under any wrapper.
        String view = "vrf_branching_view";
        createView(view, "FROM " + a + ", (FROM " + b + ") | EVAL tag = region");

        assertThat(ids(view, QueryBuilders.termQuery("region", "eu")), containsInAnyOrder(1, 3));
    }

    // ─── Views vs user-written subqueries ────────────────────────────────────────

    /**
     * A literal subquery branch is not a view branch: its filter goes down the ordinary Lucene path, while a sibling
     * view branch gets the filter applied above its output. The two must not be conflated — that distinction is the
     * whole point of tracking {@code viewBranchKeys} rather than treating any named branch as a view.
     *
     * <p>The stats view makes the difference observable: {@code cnt} exists only in the view's output, so it can only
     * be filtered there, while the subquery branch is filtered on a real indexed field.
     */
    public void testLiteralSubqueryBranchIsFilteredAtSourceWhileViewBranchIsFilteredAtOutput() {
        // region is a real field on the index (so the subquery branch can be filtered by Lucene) and also a grouping
        // key the stats view emits, so one filter is meaningful on both branches.
        // One (eu, 3) row from the view branch and one from the subquery branch; the us buckets are gone from both.
        assertThat(
            "both branches filtered to eu, by different paths",
            rows(
                "FROM " + STATS_VIEW + ", (FROM " + INDEX + " | STATS cnt = COUNT(*) BY region) | KEEP region, cnt",
                QueryBuilders.termQuery("region", "eu")
            ),
            equalTo(List.of(List.of("eu", 3L), List.of("eu", 3L)))
        );
    }

    // ─── Unsupported DSL construct is dropped with a warning ─────────────────────

    /**
     * A wildcard query is not in the supported DSL subset for views. Matching the dataset policy, the query does not fail:
     * the supported term clause is applied, the wildcard is dropped, and the {@code Warning} response header names the
     * construct and the view. Goes through REST because that header is what a caller actually sees.
     * <p>
     * Uses a non-pass-through view deliberately. A pass-through view ({@code FROM index}) is collapsed to its source
     * index during view compaction, because filtering the index and filtering the view's output are then the same
     * operation — so its filter takes the ordinary Lucene-scan path and never reaches this translation at all.
     */
    public void testUnsupportedDslConstructOnViewIsDroppedWithWarning() throws IOException {
        Request request = new Request("POST", "/_query");
        request.setJsonEntity(String.format(Locale.ROOT, """
            {
              "query": "FROM %s | KEEP region, cnt",
              "filter": {
                "bool": {
                  "must": [
                    { "term": { "region": "eu" } },
                    { "wildcard": { "region": { "value": "e*" } } }
                  ]
                }
              }
            }
            """, STATS_VIEW));
        Response response = getRestClient().performRequest(request);
        assertThat(response.getStatusLine().getStatusCode(), equalTo(200));
        // The term alone selects the eu bucket; had the wildcard narrowed anything further, or the whole filter been
        // abandoned, the values would differ.
        assertThat(EntityUtils.toString(response.getEntity()), containsString("\"values\":[[\"eu\",3]]"));
        List<String> warnings = response.getWarnings();
        assertTrue(
            "expected a warning naming the dropped [wildcard] and the view; got: " + warnings,
            warnings.stream().anyMatch(w -> w.contains("[wildcard] on view [" + STATS_VIEW + "]"))
        );
    }
}
