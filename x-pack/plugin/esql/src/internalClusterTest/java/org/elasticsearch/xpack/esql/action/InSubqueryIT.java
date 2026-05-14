/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.junit.Before;

import java.util.List;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;

/**
 * Integration tests for WHERE ... IN (subquery) and WHERE ... NOT IN (subquery).
 */
public class InSubqueryIT extends AbstractEsqlIntegTestCase {

    @Before
    public void checkCapability() {
        assumeTrue("Requires IN subquery support", EsqlCapabilities.Cap.WHERE_IN_SUBQUERY_WITHOUT_VIEW.isEnabled());
    }

    @Before
    public void setupIndices() {
        createAndPopulateIndex();
    }

    // ---- basic IN / NOT IN ----

    public void testBasicInSubquery() {
        try (var resp = run("FROM test | WHERE id IN (FROM test | SORT id | LIMIT 3 | KEEP id) | SORT id | KEEP id, color")) {
            assertColumnNames(resp.columns(), List.of("id", "color"));
            assertColumnTypes(resp.columns(), List.of("integer", "keyword"));
            assertValues(resp.values(), List.of(List.of(1, "red"), List.of(2, "blue"), List.of(3, "red")));
        }
    }

    public void testBasicNotInSubquery() {
        try (var resp = run("FROM test | WHERE id NOT IN (FROM test | SORT id | LIMIT 3 | KEEP id) | SORT id | KEEP id, color")) {
            assertColumnNames(resp.columns(), List.of("id", "color"));
            assertValues(resp.values(), List.of(List.of(4, "blue"), List.of(5, "red"), List.of(6, "blue")));
        }
    }

    // ---- empty subquery results ----

    public void testInSubqueryEmptyResults() {
        try (var resp = run("FROM test | WHERE id IN (FROM test | WHERE id > 100 | KEEP id) | SORT id | KEEP id")) {
            assertColumnNames(resp.columns(), List.of("id"));
            assertValues(resp.values(), List.of());
        }
    }

    public void testNotInSubqueryEmptyResults() {
        try (var resp = run("FROM test | WHERE id NOT IN (FROM test | WHERE id > 100 | KEEP id) | SORT id | KEEP id")) {
            assertColumnNames(resp.columns(), List.of("id"));
            assertValues(resp.values(), List.of(List.of(1), List.of(2), List.of(3), List.of(4), List.of(5), List.of(6)));
        }
    }

    // ---- IN subquery with STATS ----

    public void testInSubqueryWithStats() {
        try (var resp = run("FROM test | WHERE id IN (FROM test | STATS max_id = MAX(id) | KEEP max_id) | SORT id | KEEP id, color")) {
            assertColumnNames(resp.columns(), List.of("id", "color"));
            assertValues(resp.values(), List.of(List.of(6, "blue")));
        }
    }

    // ---- IN subquery with filter inside subquery ----

    public void testInSubqueryWithFilterInside() {
        try (var resp = run("FROM test | WHERE id IN (FROM test | WHERE color == \"red\" | KEEP id) | SORT id | KEEP id, color")) {
            assertColumnNames(resp.columns(), List.of("id", "color"));
            assertValues(resp.values(), List.of(List.of(1, "red"), List.of(3, "red"), List.of(5, "red")));
        }
    }

    /**
     * Request-level filter (same JSON {@code filter} object as REST ES|QL {@code POST ... /_query})
     * intersects with IN-subquery execution.
     */
    public void testInSubqueryWithTopLevelFilter() {
        var request = syncEsqlQueryRequest("""
            FROM test
            | WHERE id IN (FROM test | WHERE color == "red" | KEEP id)
            | SORT id
            | KEEP id, color
            """).filter(new RangeQueryBuilder("id").gte(3)).pragmas(getPragmas());
        try (var resp = run(request)) {
            assertColumnNames(resp.columns(), List.of("id", "color"));
            assertValues(resp.values(), List.of(List.of(3, "red"), List.of(5, "red")));
        }
    }

    /**
     * Same request-level filter pattern as {@link #testInSubqueryWithTopLevelFilter}, for {@code NOT IN}.
     */
    public void testNotInSubqueryWithTopLevelFilter() {
        var request = syncEsqlQueryRequest("""
            FROM test
            | WHERE id NOT IN (FROM test | WHERE color == "red" | KEEP id)
            | SORT id
            | KEEP id, color
            """).filter(new RangeQueryBuilder("id").gte(3)).pragmas(getPragmas());
        try (var resp = run(request)) {
            assertColumnNames(resp.columns(), List.of("id", "color"));
            assertValues(resp.values(), List.of(List.of(4, "blue"), List.of(6, "blue")));
        }
    }

    // ---- IN subquery combined with other conditions ----

    public void testInSubqueryWithAdditionalFilter() {
        try (var resp = run("""
            FROM test
            | WHERE id IN (FROM test | SORT id | LIMIT 4 | KEEP id) AND color == "blue"
            | SORT id
            | KEEP id, color
            """)) {
            assertColumnNames(resp.columns(), List.of("id", "color"));
            assertValues(resp.values(), List.of(List.of(2, "blue"), List.of(4, "blue")));
        }
    }

    // ---- multiple IN subqueries ----

    public void testMultipleInSubqueries() {
        try (var resp = run("""
            FROM test
            | WHERE id IN (FROM test | WHERE color == "red" | KEEP id)
                AND id IN (FROM test | WHERE id < 3 | KEEP id)
            | SORT id
            | KEEP id, color
            """)) {
            assertColumnNames(resp.columns(), List.of("id", "color"));
            assertValues(resp.values(), List.of(List.of(1, "red")));
        }
    }

    // ---- mixed IN and NOT IN ----

    public void testMixedInAndNotInSubqueries() {
        try (var resp = run("""
            FROM test
            | WHERE id IN (FROM test | WHERE id <= 4 | KEEP id)
                AND id NOT IN (FROM test | WHERE color == "blue" | KEEP id)
            | SORT id
            | KEEP id, color
            """)) {
            assertColumnNames(resp.columns(), List.of("id", "color"));
            assertValues(resp.values(), List.of(List.of(1, "red"), List.of(3, "red")));
        }
    }

    // ---- IN subquery with EVAL ----

    public void testInSubqueryWithEvalInside() {
        try (var resp = run("""
            FROM test
            | WHERE id IN (FROM test | EVAL doubled = id * 2 | WHERE doubled <= 6 | KEEP id)
            | SORT id
            | KEEP id, color
            """)) {
            assertColumnNames(resp.columns(), List.of("id", "color"));
            assertValues(resp.values(), List.of(List.of(1, "red"), List.of(2, "blue"), List.of(3, "red")));
        }
    }

    // ---- constant left-hand side ----

    public void testConstantInSubquery() {
        try (var resp = run("FROM test | WHERE 3 IN (FROM test | KEEP id) | SORT id | KEEP id, color")) {
            assertColumnNames(resp.columns(), List.of("id", "color"));
            assertValues(
                resp.values(),
                List.of(List.of(1, "red"), List.of(2, "blue"), List.of(3, "red"), List.of(4, "blue"), List.of(5, "red"), List.of(6, "blue"))
            );
        }
    }

    public void testConstantNotInSubquery() {
        try (var resp = run("FROM test | WHERE 999 NOT IN (FROM test | KEEP id) | SORT id | KEEP id, color")) {
            assertColumnNames(resp.columns(), List.of("id", "color"));
            assertValues(
                resp.values(),
                List.of(List.of(1, "red"), List.of(2, "blue"), List.of(3, "red"), List.of(4, "blue"), List.of(5, "red"), List.of(6, "blue"))
            );
        }
    }

    // ---- IN subquery from different index ----

    public void testInSubqueryFromDifferentIndex() {
        assertAcked(
            client().admin()
                .indices()
                .prepareCreate("ids")
                .setSettings(Settings.builder().put("index.number_of_shards", 1))
                .setMapping("val", "type=integer")
        );
        client().prepareBulk()
            .add(new IndexRequest("ids").id("1").source("val", 1))
            .add(new IndexRequest("ids").id("2").source("val", 3))
            .add(new IndexRequest("ids").id("3").source("val", 5))
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();
        ensureYellow("ids");

        try (var resp = run("FROM test | WHERE id IN (FROM ids | KEEP val) | SORT id | KEEP id, color")) {
            assertColumnNames(resp.columns(), List.of("id", "color"));
            assertValues(resp.values(), List.of(List.of(1, "red"), List.of(3, "red"), List.of(5, "red")));
        }
    }

    // ---- nested IN subquery ----

    public void testNestedInSubquery() {
        try (var resp = run("""
            FROM test
            | WHERE id IN (
                FROM test
                | WHERE id IN (FROM test | WHERE color == "red" | KEEP id)
                | WHERE id <= 3
                | KEEP id
              )
            | SORT id
            | KEEP id, color
            """)) {
            assertColumnNames(resp.columns(), List.of("id", "color"));
            assertValues(resp.values(), List.of(List.of(1, "red"), List.of(3, "red")));
        }
    }

    // ---- disjunctive IN subquery with mixed predicates ----

    /**
     * Three-way OR: IN subquery for red, IN subquery for blue, and id &gt; 5.
     * Exclusive branch partitioning ensures no duplicates.
     */
    public void testDisjunctiveInSubqueriesWithMixedPredicates() {
        try (var resp = run("""
            FROM test
            | WHERE id IN (FROM test | WHERE color == "red" | KEEP id)
                OR (id IN (FROM test | WHERE color == "blue" | KEEP id) AND id < 3 )
                OR id > 5
            | SORT id
            | KEEP id, color
            """)) {
            assertColumnNames(resp.columns(), List.of("id", "color"));
            assertColumnTypes(resp.columns(), List.of("integer", "keyword"));
            assertValues(
                resp.values(),
                List.of(List.of(1, "red"), List.of(2, "blue"), List.of(3, "red"), List.of(5, "red"), List.of(6, "blue"))
            );
        }
    }

    // ---- forced hash-join path correctness ----
    //
    // These tests pin {@code in_subquery_hash_join_threshold=0} so SemiJoin.inlineData always
    // routes through the hash-join branch (LEFT-join on sentinel + IS NOT NULL filter) once dedup
    // produces at least one key, instead of folding into Filter(IN(literals...)).
    // They cover the cases the default-threshold tests above cannot — small subquery results that
    // would otherwise be inlined as Filter(IN(literals...)).

    /**
     * Forces the hash-join path even with a small subquery. The {@code test} index has 6 rows, the
     * subquery returns 3 unique ids; without forcing, this would be {@code Filter(IN(1,2,3))}.
     * Verifies the hash-join LEFT-join + IS NOT NULL filter produces the same set of matches.
     */
    public void testInSubqueryHashJoinForced() {
        assumeTrue("requires query pragmas", canUseQueryPragmas());
        try (var resp = run(syncEsqlQueryRequest("""
            FROM test
            | WHERE id IN (FROM test | SORT id | LIMIT 3 | KEEP id)
            | SORT id
            | KEEP id, color""").pragmas(forceHashJoin()))) {
            assertColumnNames(resp.columns(), List.of("id", "color"));
            assertValues(resp.values(), List.of(List.of(1, "red"), List.of(2, "blue"), List.of(3, "red")));
        }
    }

    /**
     * Same as above, but for NOT IN — the sentinel filter flips to IS NULL.
     */
    public void testNotInSubqueryHashJoinForced() {
        assumeTrue("requires query pragmas", canUseQueryPragmas());
        try (var resp = run(syncEsqlQueryRequest("""
            FROM test
            | WHERE id NOT IN (FROM test | SORT id | LIMIT 3 | KEEP id)
            | SORT id
            | KEEP id, color""").pragmas(forceHashJoin()))) {
            assertColumnNames(resp.columns(), List.of("id", "color"));
            assertValues(resp.values(), List.of(List.of(4, "blue"), List.of(5, "red"), List.of(6, "blue")));
        }
    }

    /**
     * Subquery returns duplicate values ({@code color} = "red" appears 3x, "blue" 3x). Without
     * dedup, the LEFT-join would produce a row per duplicate and a single left row matching "red"
     * would appear three times. This pins that BlockHash dedup collapses the right side correctly.
     */
    public void testInSubqueryHashJoinDuplicatesOnRightSide() {
        assumeTrue("requires query pragmas", canUseQueryPragmas());
        try (var resp = run(syncEsqlQueryRequest("""
            FROM test
            | WHERE color IN (FROM test | KEEP color)
            | SORT id
            | KEEP id, color""").pragmas(forceHashJoin()))) {
            assertColumnNames(resp.columns(), List.of("id", "color"));
            // All 6 rows pass — each "red"/"blue" matches exactly one dedup row on the right.
            assertValues(
                resp.values(),
                List.of(List.of(1, "red"), List.of(2, "blue"), List.of(3, "red"), List.of(4, "blue"), List.of(5, "red"), List.of(6, "blue"))
            );
        }
    }

    /**
     * Subquery whose result contains a null does not cause spurious matches on non-null left rows.
     * A subquery emitting {@code {red, null}} must match left rows whose color is "red" but NOT
     * match left rows whose color is "blue" (the null on the right side has no effect on non-null
     * left lookups).
     */
    public void testInSubqueryHashJoinNullOnBothSide() {
        assumeTrue("requires query pragmas", canUseQueryPragmas());
        // Add a row with id=7 and no color so the subquery can emit a null in its output.
        client().prepareBulk()
            .add(new IndexRequest("test").id("7").source("id", 7))
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();
        try (var resp = run(syncEsqlQueryRequest("""
            FROM test
            | WHERE color IN (FROM test | WHERE id == 1 OR id == 7 | KEEP color)
            | SORT id
            | KEEP id, color""").pragmas(forceHashJoin()))) {
            assertColumnNames(resp.columns(), List.of("id", "color"));
            // Subquery dedups to {red}; only the "red" rows on the left match.
            assertValues(resp.values(), List.of(List.of(1, "red"), List.of(3, "red"), List.of(5, "red")));
        }
    }

    /**
     * SQL semantics for {@code NOT IN} with a NULL on the right: the predicate is FALSE for
     * matches and NULL for non-matches (because {@code x = NULL} is NULL), so every row is
     * filtered out. {@code SemiJoin#inlineData} short-circuits this to {@code Filter(FALSE)}
     * as soon as it sees a NULL in the subquery output, without ever running BlockHash or
     * building the LEFT join.
     */
    public void testNotInSubqueryHashJoinNullOnBothSides() {
        assumeTrue("requires query pragmas", canUseQueryPragmas());
        client().prepareBulk()
            .add(new IndexRequest("test").id("7").source("id", 7))
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();
        try (var resp = run(syncEsqlQueryRequest("""
            FROM test
            | WHERE color NOT IN (FROM test | WHERE id == 1 OR id == 7 | KEEP color)
            | SORT id
            | KEEP id""").pragmas(forceHashJoin()))) {
            assertColumnNames(resp.columns(), List.of("id"));
            assertValues(resp.values(), List.of());
        }
    }

    /**
     * Empty subquery under the forced hash-join threshold still falls through the empty-result
     * fast path inside {@code inlineData} (before BlockHash). SEMI -> Filter(FALSE) -> 0 rows.
     */
    public void testInSubqueryHashJoinEmptyResult() {
        assumeTrue("requires query pragmas", canUseQueryPragmas());
        try (var resp = run(syncEsqlQueryRequest("""
            FROM test
            | WHERE id IN (FROM test | WHERE id > 100 | KEEP id)
            | SORT id
            | KEEP id""").pragmas(forceHashJoin()))) {
            assertColumnNames(resp.columns(), List.of("id"));
            assertValues(resp.values(), List.of());
        }
    }

    /**
     * Empty subquery + NOT IN under forced hash-join: ANTI semantics with empty right side returns
     * all left rows (Filter(TRUE)).
     */
    public void testNotInSubqueryHashJoinEmptyResult() {
        assumeTrue("requires query pragmas", canUseQueryPragmas());
        try (var resp = run(syncEsqlQueryRequest("""
            FROM test
            | WHERE id NOT IN (FROM test | WHERE id > 100 | KEEP id)
            | SORT id
            | KEEP id""").pragmas(forceHashJoin()))) {
            assertColumnNames(resp.columns(), List.of("id"));
            assertValues(resp.values(), List.of(List.of(1), List.of(2), List.of(3), List.of(4), List.of(5), List.of(6)));
        }
    }

    /**
     * Hash-join correctness across multiple shards. We create an index with 3 shards, populate it
     * with 30 rows, run the subquery (which fans out across shards and gathers on the coordinator
     * before dedup), and verify the result matches.
     */
    public void testInSubqueryHashJoinMultiShardCorrectness() {
        assumeTrue("requires query pragmas", canUseQueryPragmas());
        assertAcked(
            client().admin()
                .indices()
                .prepareCreate("multi")
                .setSettings(Settings.builder().put("index.number_of_shards", 3))
                .setMapping("id", "type=integer", "tag", "type=keyword")
        );
        var bulk = client().prepareBulk();
        for (int i = 0; i < 30; i++) {
            bulk.add(new IndexRequest("multi").id(String.valueOf(i)).source("id", i, "tag", i % 5 == 0 ? "match" : "other"));
        }
        bulk.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();
        ensureYellow("multi");

        try (var resp = run(syncEsqlQueryRequest("""
            FROM multi
            | WHERE id IN (FROM multi | WHERE tag == "match" | KEEP id)
            | STATS cnt = COUNT(*)
            """).pragmas(forceHashJoin()))) {
            assertColumnNames(resp.columns(), List.of("cnt"));
            // ids 0, 5, 10, 15, 20, 25 match.
            assertValues(resp.values(), List.of(List.of(6L)));
        }
    }

    /**
     * Nested IN subqueries under forced hash-join: both the inner and outer subqueries take the
     * hash-join path. Verifies that successive {@code inlineData} invocations don't interfere
     * (each manages its own dedup page and circuit-breaker accounting).
     */
    public void testNestedInSubqueryHashJoin() {
        assumeTrue("requires query pragmas", canUseQueryPragmas());
        try (var resp = run(syncEsqlQueryRequest("""
            FROM test
            | WHERE id IN (
                FROM test
                | WHERE id IN (FROM test | WHERE color == "red" | KEEP id)
                | WHERE id <= 3
                | KEEP id
              )
            | SORT id
            | KEEP id, color
            """).pragmas(forceHashJoin()))) {
            assertColumnNames(resp.columns(), List.of("id", "color"));
            assertValues(resp.values(), List.of(List.of(1, "red"), List.of(3, "red")));
        }
    }

    /**
     * Parity check: the same query routed through the filter path (default threshold) and the
     * hash-join path (forced) must return identical results. This is the strongest form of
     * regression protection — any divergence between paths shows up as a test failure.
     */
    public void testInSubqueryHashJoinAndFilterPathsAgree() {
        assumeTrue("requires query pragmas", canUseQueryPragmas());
        String query = """
            FROM test
            | WHERE id IN (FROM test | WHERE color == "red" | KEEP id)
            | SORT id
            | KEEP id, color
            """;
        try (
            var defaultResp = run(syncEsqlQueryRequest(query).pragmas(QueryPragmas.EMPTY));
            var forcedResp = run(syncEsqlQueryRequest(query).pragmas(forceHashJoin()))
        ) {
            assertEquals(getValuesList(defaultResp), getValuesList(forcedResp));
        }
    }

    /**
     * Parity with NULLs on both the left side (id=7 has no color) and inside the subquery output
     * (the subquery's {@code KEEP color} emits the NULL for id=7 alongside "red"). Both paths
     * must agree for {@code IN} <em>and</em> {@code NOT IN}: the filter path inherits SQL NULL
     * semantics from the {@code In} operator, the hash-join path closes the same gap via the
     * left-side {@code IsNotNull(leftField)} filter, right-side NULL stripping in dedup, and the
     * ANTI-with-null-right short-circuit.
     */
    public void testInSubqueryHashJoinAndFilterPathsAgreeWithNulls() {
        assumeTrue("requires query pragmas", canUseQueryPragmas());
        client().prepareBulk()
            .add(new IndexRequest("test").id("7").source("id", 7))
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();
        String inQuery = """
            FROM test
            | WHERE color IN (FROM test | WHERE id == 1 OR id == 7 | KEEP color)
            | SORT id
            | KEEP id
            """;
        try (
            var defaultResp = run(syncEsqlQueryRequest(inQuery).pragmas(QueryPragmas.EMPTY));
            var forcedResp = run(syncEsqlQueryRequest(inQuery).pragmas(forceHashJoin()))
        ) {
            assertEquals(getValuesList(defaultResp), getValuesList(forcedResp));
        }
        String notInQuery = """
            FROM test
            | WHERE color NOT IN (FROM test | WHERE id == 1 OR id == 7 | KEEP color)
            | SORT id
            | KEEP id
            """;
        try (
            var defaultResp = run(syncEsqlQueryRequest(notInQuery).pragmas(QueryPragmas.EMPTY));
            var forcedResp = run(syncEsqlQueryRequest(notInQuery).pragmas(forceHashJoin()))
        ) {
            assertEquals(getValuesList(defaultResp), getValuesList(forcedResp));
        }
    }

    private static QueryPragmas forceHashJoin() {
        // pragma value 0 means "always use hash-join, never filter-of-literals"
        return new QueryPragmas(Settings.builder().put("in_subquery_hash_join_threshold", 0).build());
    }

    // ---- helpers ----

    private void createAndPopulateIndex() {
        assertAcked(
            client().admin()
                .indices()
                .prepareCreate("test")
                .setSettings(Settings.builder().put("index.number_of_shards", randomIntBetween(1, 5)))
                .setMapping("id", "type=integer", "color", "type=keyword")
        );
        client().prepareBulk()
            .add(new IndexRequest("test").id("1").source("id", 1, "color", "red"))
            .add(new IndexRequest("test").id("2").source("id", 2, "color", "blue"))
            .add(new IndexRequest("test").id("3").source("id", 3, "color", "red"))
            .add(new IndexRequest("test").id("4").source("id", 4, "color", "blue"))
            .add(new IndexRequest("test").id("5").source("id", 5, "color", "red"))
            .add(new IndexRequest("test").id("6").source("id", 6, "color", "blue"))
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();
        ensureYellow("test");
    }
}
