/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.cluster.metadata.View;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities.Cap;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.view.PutViewAction;
import org.junit.Before;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.equalTo;

/**
 * Pins that union types (same field name, conflicting types across sources) behave <em>identically</em> no matter how
 * the sources are composed: a multi-pattern relation ({@code FROM a,b}), a literal subquery, a pass-through view, a
 * view whose body is not collapsible, and the shapes where a request filter or {@code METADATA} keeps a view branch
 * apart that compaction would otherwise merge.
 *
 * <p>The common contract, established empirically here:
 * <ul>
 *   <li><b>Soft-fail</b>: a conflicting field surfaces as an {@code unsupported} column of nulls — never a query
 *   error — whether the sources resolve as one relation or as union branches ({@code EsqlIndexResolver} marks
 *   ambiguous fields as {@code TypeConflictedField}, and branch alignment Eval-nulls conflicted columns; even
 *   purely computed conflicts like {@code FROM (ROW x=1), (ROW x="a")} soft-fail).</li>
 *   <li><b>Explicit casts resolve the conflict</b> — including a cast <em>outside</em> the subquery or view, which
 *   is the only place a caller can put one when the conflict sits inside a view body they do not own.</li>
 * </ul>
 *
 * <p>These tests are the acceptance suite for the compaction refactor: moving view/subquery
 * compaction later must not change any outcome pinned here. In particular the request-filter and METADATA variants
 * pin that features which keep a view branch apart do not change type-resolution semantics.
 */
public class ViewSubqueryUnionTypeIT extends AbstractEsqlIntegTestCase {

    private static final String INDEX_A = "vut_a";
    private static final String INDEX_B = "vut_b";
    /** View whose body is a bare pass-through of {@link #INDEX_B}; compaction can merge it into sibling patterns. */
    private static final String PASSTHROUGH_VIEW = "vut_pass";
    /** Same data, but the body carries an operator, so the view stays a union branch. */
    private static final String OPAQUE_VIEW = "vut_opaque";

    @Before
    public void loadData() {
        // f: integer in a, keyword in b — the union-type conflict. g exists only in a. id is integer everywhere.
        assertAcked(
            client().admin()
                .indices()
                .prepareCreate(INDEX_A)
                .setSettings(Settings.builder().put("index.number_of_shards", 1))
                .setMapping("id", "type=integer", "f", "type=integer", "g", "type=keyword")
        );
        assertAcked(
            client().admin()
                .indices()
                .prepareCreate(INDEX_B)
                .setSettings(Settings.builder().put("index.number_of_shards", 1))
                .setMapping("id", "type=integer", "f", "type=keyword")
        );
        BulkRequestBuilder bulk = client().prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        bulk.add(new IndexRequest(INDEX_A).source("id", 1, "f", 1, "g", "x"));
        bulk.add(new IndexRequest(INDEX_A).source("id", 2, "f", 2, "g", "y"));
        // The keyword value parses as an integer, so the explicit-cast tests stay warning-free.
        bulk.add(new IndexRequest(INDEX_B).source("id", 3, "f", "3"));
        BulkResponse response = bulk.get();
        assertFalse(response.buildFailureMessage(), response.hasFailures());

        createView(PASSTHROUGH_VIEW, "FROM " + INDEX_B);
        createView(OPAQUE_VIEW, "FROM " + INDEX_B + " | LIMIT 1000000");
    }

    private void createView(String name, String query) {
        assertAcked(
            client().execute(
                PutViewAction.INSTANCE,
                new PutViewAction.Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, new View(name, query))
            ).actionGet(30, TimeUnit.SECONDS)
        );
    }

    // ─── Soft-fail: the conflicting column is unsupported + nulls in every composition ───

    public void testMergedPatternSoftFailsOnConflict() {
        assertSoftFail("FROM " + INDEX_A + "," + INDEX_B, null);
    }

    public void testLiteralSubquerySoftFailsOnConflict() {
        assertSoftFail("FROM " + INDEX_A + ", (FROM " + INDEX_B + ")", null);
    }

    public void testPassThroughViewSoftFailsOnConflict() {
        assertSoftFail("FROM " + INDEX_A + ", " + PASSTHROUGH_VIEW, null);
    }

    public void testNonCollapsibleViewSoftFailsOnConflict() {
        assertSoftFail("FROM " + INDEX_A + ", " + OPAQUE_VIEW, null);
    }

    /**
     * A request filter keeps the view branch apart for the filter rewrite ({@code preserveViewBoundaries}) — and must
     * not change type-resolution semantics while doing so. The filter matches every row, so only its presence matters.
     */
    public void testRequestFilterDoesNotChangeConflictSemantics() {
        assertSoftFail("FROM " + INDEX_A + ", " + PASSTHROUGH_VIEW, QueryBuilders.existsQuery("id"));
    }

    /** As above, but with a view branch the filter genuinely preserves (an opaque body cannot collapse). */
    public void testRequestFilterOnPreservedViewBoundaryDoesNotChangeConflictSemantics() {
        assertSoftFail("FROM " + INDEX_A + ", " + OPAQUE_VIEW, QueryBuilders.existsQuery("id"));
    }

    /** {@code METADATA _name} keeps view identity — and must not change type-resolution semantics while doing so. */
    public void testMetadataNameDoesNotChangeConflictSemantics() {
        assumeTrue("requires OUTER_METADATA_NULL_INJECTION", Cap.OUTER_METADATA_NULL_INJECTION.isEnabled());
        assumeTrue("requires VIEWS_WITH_NO_BRANCHING", Cap.VIEWS_WITH_NO_BRANCHING.isEnabled());
        assertSoftFail("FROM " + INDEX_A + ", " + PASSTHROUGH_VIEW + " METADATA _name", null);
    }

    /** Conflicts between purely computed columns soft-fail too — no field-caps involved, this is union alignment. */
    public void testComputedColumnConflictSoftFails() {
        try (EsqlQueryResponse response = run(syncEsqlQueryRequest("FROM (ROW x = 1), (ROW x = \"a\") | KEEP x"))) {
            assertThat(response.columns().get(0).type(), equalTo(DataType.UNSUPPORTED));
            assertThat(getValuesList(response), equalTo(List.of(Arrays.asList((Object) null), Arrays.asList((Object) null))));
        }
    }

    // ─── The escape hatch: explicit casts resolve the conflict in every composition ───

    public void testCastResolvesConflictOnMergedPattern() {
        assertCastResolves("FROM " + INDEX_A + "," + INDEX_B);
    }

    /**
     * The cast sits <em>outside</em> the subquery. This is the shape that matters for views: when the conflict is
     * inside a view body the caller does not own, an outer cast is the only fix available to them.
     */
    public void testOuterCastResolvesConflictAcrossSubqueryBranches() {
        assertCastResolves("FROM " + INDEX_A + ", (FROM " + INDEX_B + ")");
    }

    public void testOuterCastResolvesConflictAcrossViewBranches() {
        assertCastResolves("FROM " + INDEX_A + ", " + OPAQUE_VIEW);
    }

    /**
     * The branch's top {@code Project} is user-written and itself mints the conflicting column (a trailing RENAME), so
     * the pushed-down conversion cannot be inserted below it. It must go <em>above</em> the user's projection instead,
     * under the alignment projection. Found in review: this shape used to skip the branch's conversion and fail
     * verification with {@code Column [f] has conflicting data types in subqueries}.
     */
    public void testOuterCastResolvesConflictWhenBranchEndsInRename() {
        assertCastResolves("FROM " + INDEX_A + ", (FROM " + INDEX_B + " | RENAME f AS x | RENAME x AS f)");
        assertCastResolves("FROM " + INDEX_A + ", (FROM " + INDEX_B + " | EVAL x = f | KEEP id, x | RENAME x AS f)");
    }

    // ─── A neighbouring case the compositions also agree on ───

    /** A field missing from one source (not conflicting) is null-filled identically in both compositions. */
    public void testMissingFieldIsNullFilledInBothCompositions() {
        List<List<Object>> expected = List.of(Arrays.asList(1, "x"), Arrays.asList(2, "y"), Arrays.asList(3, null));
        assertThat("merged pattern", rows("FROM " + INDEX_A + "," + INDEX_B + " | KEEP id, g | SORT id ASC", null), equalTo(expected));
        assertThat(
            "union branches",
            rows("FROM " + INDEX_A + ", (FROM " + INDEX_B + ") | KEEP id, g | SORT id ASC", null),
            equalTo(expected)
        );
    }

    // ─── helpers ──────────────────────────────────────────────────────────────────

    /** {@code KEEP id, f} over {@code source} returns an {@code unsupported} column f with null values for every row. */
    private void assertSoftFail(String source, QueryBuilder filter) {
        EsqlQueryRequest request = syncEsqlQueryRequest(source + " | KEEP id, f | SORT id ASC");
        if (filter != null) {
            request.filter(filter);
        }
        try (EsqlQueryResponse response = run(request)) {
            assertThat(response.columns().stream().map(ColumnInfoImpl::name).toList(), equalTo(List.of("id", "f")));
            assertThat("the conflicting column soft-fails to unsupported", response.columns().get(1).type(), equalTo(DataType.UNSUPPORTED));
            assertThat(
                "unsupported values are null for every row, including the integer-typed ones",
                getValuesList(response),
                equalTo(List.of(Arrays.asList(1, null), Arrays.asList(2, null), Arrays.asList(3, null)))
            );
        }
    }

    /** An explicit cast resolves the conflict per source: integers pass through, the keyword parses. */
    private void assertCastResolves(String source) {
        try (EsqlQueryResponse response = run(syncEsqlQueryRequest(source + " | EVAL fi = f::integer | KEEP id, fi | SORT id ASC"))) {
            assertThat(getValuesList(response), equalTo(List.of(List.of(1, 1), List.of(2, 2), List.of(3, 3))));
        }
    }

    /** Runs {@code query} (with the request filter when given) and returns every row. */
    private List<List<Object>> rows(String query, QueryBuilder filter) {
        EsqlQueryRequest request = syncEsqlQueryRequest(query);
        if (filter != null) {
            request.filter(filter);
        }
        try (EsqlQueryResponse response = run(request)) {
            return getValuesList(response);
        }
    }
}
