/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.cluster.metadata.View;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.view.DeleteViewAction;
import org.elasticsearch.xpack.esql.view.PutViewAction;

import java.util.List;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;

/**
 * Subqueries in the {@code FROM} command combined with logical views.
 *
 * <p>A {@code FROM} pattern that matches <b>both a view and a real index</b> — e.g. the wildcard {@code airports*} matching the view
 * {@code airports_view} and the index {@code airports} — resolves to a {@code ViewUnionAll} with a view branch and a concrete-index
 * branch. Combined with a sibling subquery that used to nest one merge inside another, which is unexecutable, so the planner rejected
 * it. View compaction now flattens the nesting instead, and these tests pin the resulting unions.
 *
 * <p>Each test is self-contained: it creates its own indices and view and deletes the view afterwards, so this suite shares no fixture
 * with {@link SubqueryIT} or {@link SubqueryFailureIT}.
 */
public class SubqueryViewsIT extends AbstractEsqlIntegTestCase {

    /**
     * A wildcard that matches both a view and a real index, combined with a sibling subquery, used to be rejected: the pattern expanded
     * to a {@code ViewUnionAll} nested under the subquery's {@code UnionAll}, and nested merges are unexecutable.
     *
     * <p>View compaction now flattens that nesting — {@code ViewCompaction.rewriteUnionAllsWithNamedSubqueries} inlines a
     * {@code ViewUnionAll} child's entries into the enclosing union, producing one flat merge over
     * {@code {airports_view, airports, employees}} — so the three arrangements below all execute and return the union.
     *
     * <p>{@code airports} appears twice by design: {@code airports*} matches both the view (whose body reads {@code airports}) and the
     * index itself, and each is a separate branch. {@code FROM airports*} alone likewise returns two rows.
     */
    public void testViewAndIndexInMainQueryWithSubquery() {
        assertWildcardViewUnionWithSubquery("FROM airports*, (FROM employees)");
    }

    /** As {@link #testViewAndIndexInMainQueryWithSubquery}, with the wildcard inside the subquery instead of the main query. */
    public void testViewAndIndexInsideSubquery() {
        assertWildcardViewUnionWithSubquery("FROM employees, (FROM airports*)");
    }

    /** As {@link #testViewAndIndexInMainQueryWithSubquery}, with the wildcard in one of several sibling subqueries. */
    public void testViewAndIndexInOneOfMultipleSubqueries() {
        assertWildcardViewUnionWithSubquery("FROM (FROM airports*), (FROM employees)");
    }

    /**
     * Runs {@code query} — which must union the {@code airports*} wildcard (view + index) with the {@code employees} index in some
     * arrangement — and asserts the flattened union it should now produce.
     */
    private void assertWildcardViewUnionWithSubquery(String query) {
        assumeViewBranchingSupported();
        setupWildcardMatchingViewAndIndices();
        try (var response = run(query + " | SORT name | KEEP id, name")) {
            assertThat(
                "the wildcard's view and index branches plus the subquery branch, flattened into one union",
                EsqlTestUtils.getValuesList(response),
                equalTo(List.of(List.of(1, "a"), List.of(1, "a"), List.of(1, "e")))
            );
        } finally {
            deleteViews("airports_view");
        }
    }

    private static void assumeViewBranchingSupported() {
        assumeTrue("Requires views in cluster state", EsqlCapabilities.Cap.VIEWS_IN_CLUSTER_STATE.isEnabled());
        assumeTrue("Requires views with branching", EsqlCapabilities.Cap.VIEWS_WITH_BRANCHING.isEnabled());
    }

    /**
     * Creates the {@code airports} index and an {@code airports_view} view (both matched by the wildcard {@code airports*}), plus an
     * {@code employees} index used as the sibling relation. The view body carries a processing command ({@code LIMIT}) so it is kept as
     * a named view branch rather than compacted into the concrete index — this is what makes {@code airports*} expand to a branching
     * {@code ViewUnionAll} of the view and the real index. The two indices share the same mapping so the top-level {@code UnionAll} has
     * no column-type conflicts that would fail verification.
     */
    private void setupWildcardMatchingViewAndIndices() {
        client().admin().indices().prepareCreate("airports").setMapping("id", "type=integer", "name", "type=keyword").get();
        client().prepareBulk()
            .add(new IndexRequest("airports").id("1").source("id", 1, "name", "a"))
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();

        client().admin().indices().prepareCreate("employees").setMapping("id", "type=integer", "name", "type=keyword").get();
        client().prepareBulk()
            .add(new IndexRequest("employees").id("1").source("id", 1, "name", "e"))
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();

        ensureYellow("airports", "employees");
        installView("airports_view", "FROM airports | LIMIT 10");
    }

    private static void installView(String name, String query) {
        assertAcked(
            client().execute(
                PutViewAction.INSTANCE,
                new PutViewAction.Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, new View(name, query))
            )
        );
    }

    private static void deleteViews(String... names) {
        for (String name : names) {
            assertAcked(
                client().execute(
                    DeleteViewAction.INSTANCE,
                    new DeleteViewAction.Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, new String[] { name })
                )
            );
        }
    }
}
