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
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.elasticsearch.xpack.esql.view.DeleteViewAction;
import org.elasticsearch.xpack.esql.view.PutViewAction;
import org.junit.Before;

import java.util.List;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;

/**
 * Integration tests for WHERE IN (subquery) and WHERE NOT IN (subquery).
 */
@TestLogging(value = "org.elasticsearch.xpack.esql:TRACE", reason = "debug")
public class InSubqueryIT extends AbstractEsqlIntegTestCase {

    @Before
    public void checkCapability() {
        assumeTrue("Requires IN subquery support", EsqlCapabilities.Cap.WHERE_IN_SUBQUERY_WITHOUT_VIEW.isEnabled());
    }

    @Before
    public void setupIndices() {
        createAndPopulateIndex();
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

    // ---- IN subquery with ROW as the source command ----

    public void testWhereInSubqueryWithRowAsSourceWithProcessingCommand() {
        assumeTrue("Requires ROW subquery support", EsqlCapabilities.Cap.SUBQUERY_WITH_ROW.isEnabled());
        try (var resp = run("FROM test | WHERE id IN (ROW x = 1, y = 2 | WHERE x > 0 | DROP y) | SORT id | KEEP id, color")) {
            assertColumnNames(resp.columns(), List.of("id", "color"));
            assertColumnTypes(resp.columns(), List.of("integer", "keyword"));
            assertValues(resp.values(), List.of(List.of(1, "red")));
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

    // ---- forced hash-join path correctness ----
    //
    // These tests pin {@code in_subquery_hash_join_threshold=0} so AbstractSubqueryJoin.inlineData always
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

    // ---- multi-valued left-hand-side of disjunctive IN subquery (MarkJoin) ----
    //
    // For an {@code IN (subquery)} that is nested under {@code OR} (so the rewrite produces a
    // {@link org.elasticsearch.xpack.esql.plan.logical.join.MarkJoin MarkJoin}), the
    // operator must produce a boolean <em>mark</em> attribute that captures the three-valued
    // result of {@code leftField IN (subquery)} for every left row.
    //
    // For a multi-valued left key, the SQL/ES|QL convention used by the {@code In} operator is to
    // emit {@code NULL} and a "single-value function encountered multi-value" warning. The
    // {@code MarkJoin} filter-path inherits that semantics because it materializes the mark
    // through the same {@code In} evaluator. The hash-join path, however, builds the mark from a
    // sentinel attribute populated by a LEFT join and an {@code IsNull(leftField)} check — neither
    // of which models the single-value-function/MV warning. As a result the two paths can disagree
    // on rows whose left key is multi-valued. The tests below pin the filter-path semantics and
    // exercise both paths so the divergence is observable from the integration layer.

    /**
     * Disjunctive IN subquery (MarkJoin) over a multi-valued left key on the filter path.
     * Indexes a row with {@code color=["red","green"]} alongside the existing single-valued rows,
     * then runs {@code color IN (subq) OR id > N} for two values of {@code N} so the OR's
     * non-IN side is, respectively, {@code FALSE} and {@code TRUE} for the multi-valued row.
     * Documents the SQL-3VL outcome: the MV row's mark is {@code NULL}, so the row survives iff
     * the other OR branch is {@code TRUE}.
     */
    public void testDisjunctiveInSubqueryMultiValueLeftKeyFilterPath() {
        indexMultiValuedColorRow();
        // OR's right-hand side is FALSE for every row (id never exceeds 100). The MV row's mark
        // is NULL, NULL OR FALSE = NULL → row dropped. Single-valued rows behave as usual:
        // color="red" → mark=TRUE → kept; color="blue" → mark=FALSE → dropped.
        try (var resp = run("""
            FROM test
            | WHERE color IN (FROM test | WHERE id == 1 | KEEP color) OR id > 100
            | SORT id
            | KEEP id
            """)) {
            assertColumnNames(resp.columns(), List.of("id"));
            assertValues(resp.values(), List.of(List.of(1), List.of(3), List.of(5)));
        }
        // OR's right-hand side is TRUE for id=6 and id=7. The MV row (id=7) now survives because
        // NULL OR TRUE = TRUE, even though its IN mark is still NULL.
        try (var resp = run("""
            FROM test
            | WHERE color IN (FROM test | WHERE id == 1 | KEEP color) OR id > 5
            | SORT id
            | KEEP id
            """)) {
            assertColumnNames(resp.columns(), List.of("id"));
            assertValues(resp.values(), List.of(List.of(1), List.of(3), List.of(5), List.of(6), List.of(7)));
        }
    }

    /**
     * Same query as {@link #testDisjunctiveInSubqueryMultiValueLeftKeyFilterPath} but with the
     * hash-join threshold pinned to 0 so the rewrite always takes the hash-join path. Both paths
     * must agree on the set of surviving rows; any divergence indicates that the hash-join
     * implementation of {@code MarkJoin} fails to honor the {@code In} operator's MV
     * NULL-with-warning semantics.
     * <p>
     * Currently fails because the hash-join path's mark expression (CASE based on
     * {@code IsNull(leftField)} and {@code IsNotNull(sentinel)}) does not encode the
     * "single-value function encountered multi-value" NULL outcome that the filter path's
     * {@code In} evaluator produces for a multi-valued left key. A multi-valued row whose values
     * include a subquery match collapses to {@code mark=TRUE}, so the row is kept under an OR
     * even when the filter path would drop it.
     */
    public void testDisjunctiveInSubqueryMultiValueLeftKeyParity() {
        assumeTrue("requires query pragmas", canUseQueryPragmas());
        indexMultiValuedColorRow();
        String[] queries = new String[] {
            // OR's other side is FALSE for the MV row → filter path drops it (mark=NULL OR FALSE).
            """
                FROM test
                | WHERE color IN (FROM test | WHERE id == 1 | KEEP color) OR id > 100
                | SORT id
                | KEEP id
                """,
            // OR's other side is TRUE for the MV row → both paths must keep it.
            """
                FROM test
                | WHERE color IN (FROM test | WHERE id == 1 | KEEP color) OR id > 5
                | SORT id
                | KEEP id
                """,
            // NOT IN under OR. For an MV row, NOT IN should also yield NULL (¬NULL = NULL); the
            // other OR side is FALSE → row dropped on the filter path.
            """
                FROM test
                | WHERE color NOT IN (FROM test | WHERE id == 1 | KEEP color) OR id > 100
                | SORT id
                | KEEP id
                """ };
        for (String query : queries) {
            try (
                var defaultResp = run(syncEsqlQueryRequest(query).pragmas(QueryPragmas.EMPTY));
                var forcedResp = run(syncEsqlQueryRequest(query).pragmas(forceHashJoin()))
            ) {
                assertEquals(
                    "filter and hash-join paths disagree on MV left key for query:\n" + query,
                    getValuesList(defaultResp),
                    getValuesList(forcedResp)
                );
            }
        }
    }

    /**
     * Top-level conjunctive IN / NOT IN (SEMI / ANTI {@code SemiJoin}, not {@code MarkJoin})
     * over a multi-valued left key. The filter path materializes {@code Filter(In(...))} /
     * {@code Filter(Not(In(...)))} which, by {@code In}'s three-valued semantics, drops MV-LHS
     * rows (mark=NULL → treated as FALSE under WHERE). The hash-join path is forced via
     * {@code in_subquery_hash_join_threshold=0}; it must agree by piping the left key through
     * {@code MvSingleValueOrNull} (centralized in {@code SemiJoin#inlineAsHashJoin}) so MV
     * positions become NULL before the LEFT join — both NULL/MV rows are then pre-filtered by
     * {@code IsNotNull(svKey)}, never reach BlockHash, and cannot leak through the sentinel
     * filter as a spurious match (or be duplicated by the LEFT join). Equivalent to
     * {@link #testDisjunctiveInSubqueryMultiValueLeftKeyParity} but for SEMI/ANTI shapes.
     */
    public void testTopLevelInSubqueryMultiValueLeftKeyParity() {
        assumeTrue("requires query pragmas", canUseQueryPragmas());
        indexMultiValuedColorRow();
        String[] queries = new String[] {
            // SEMI: filter path drops the MV row (In(MV, ["red"]) = NULL → FALSE under WHERE).
            // Hash-join path must too — the SV guard turns MV → NULL → no match → no row.
            """
                FROM test
                | WHERE color IN (FROM test | WHERE id == 1 | KEEP color)
                | SORT id
                | KEEP id
                """,
            // ANTI: filter path drops the MV row (Not(In(MV, ["red"])) = Not(NULL) = NULL →
            // FALSE under WHERE). Hash-join path must too — the SV guard drops the MV row via
            // the IsNotNull(svKey) pre-filter, so it never reaches the IS NULL sentinel branch.
            """
                FROM test
                | WHERE color NOT IN (FROM test | WHERE id == 1 | KEEP color)
                | SORT id
                | KEEP id
                """ };
        for (String query : queries) {
            try (
                var defaultResp = run(syncEsqlQueryRequest(query).pragmas(QueryPragmas.EMPTY));
                var forcedResp = run(syncEsqlQueryRequest(query).pragmas(forceHashJoin()))
            ) {
                assertEquals(
                    "filter and hash-join paths disagree on MV left key for query:\n" + query,
                    getValuesList(defaultResp),
                    getValuesList(forcedResp)
                );
            }
        }
    }

    /**
     * Self-referencing IN subquery over a multi-valued left key. Both the left side and the
     * subquery source are {@code FROM test}; row id=7 has {@code color=["red","green"]}, and a
     * separate {@code color_dup} index also has an MV row at id=7 (set up but not queried here —
     * it serves as a fixture that can be reused in follow-up tests).
     * <p>
     * The subquery output, after BlockHash dedup, contains {@code {"red","blue","green"}} because
     * BlockHash expands MV right-side positions into their individual values. Without the SV
     * guard on the left side, id=7's MV key would match against {@code "red"} (or {@code "green"})
     * in the right side and the row would survive. With the guard,
     * {@code MvSingleValueOrNull(color)} for id=7 yields {@code NULL} and the
     * {@code IsNotNull(svKey)} pre-filter (SEMI) drops it before the LEFT join — so id=7 must
     * NOT appear in the result.
     */
    /**
     * Subquery side {@code color_dup} contains only a single multi-valued row. Under the right-
     * side MV-as-NULL canonicalization in {@code SemiJoin#inlineData} (see
     * {@code collectSingleValueNonNullPositions}), that MV position is stripped from the
     * BlockHash input — leaving the dedup output empty. The empty-after-strip case is treated
     * as "every right value is NULL", which for {@code SemiJoin} (top-level {@code IN}) short-
     * circuits to {@code Filter(FALSE)} (no candidate match key remains). The result is therefore
     * empty even though the outer relation has rows whose colors textually appear inside the MV
     * value on the right — those values are unreachable because the MV row was canonicalized to
     * NULL, mirroring the left-side {@code MvSingleValueOrNull} guard.
     */
    public void testInSubqueryMultiValueLeftKeySelfReferencing() {
        indexMultiValuedColorRow();
        createColorDupIndexWithMultiValuedRow();
        try (var resp = run("""
            FROM test
            | WHERE color IN (FROM color_dup | KEEP color)
            | SORT id
            | KEEP id
            """)) {
            assertColumnNames(resp.columns(), List.of("id"));
            assertValues(resp.values(), List.of());
        }
    }

    // ---- right-side MV / NULL canonicalization in the IN-subquery output ----
    //
    // SemiJoin#inlineData converts every multi-valued right-side position to NULL via
    // {@code convertMvPositionsToNull} before BlockHash, mirroring the left-side
    // {@code MvSingleValueOrNull} guard. BlockHash then collapses all NULLs (original or
    // MV-derived) into its reserved group 0, surfacing as a single NULL position at index 0 of
    // the dedup output. The post-dedup logic derives {@code rightHadNulls} from that position
    // and the short-circuit decisions (ANTI: any NULL → empty; SEMI: only all-NULL → empty;
    // MARK: all-NULL → mark=NULL). The tests below pin that contract end-to-end and
    // ensure filter and hash-join paths agree.

    /**
     * SEMI subquery contains a multi-valued row whose element values are textually
     * representable ("green") but do not appear anywhere else in the subquery output. The
     * right-side MV-to-NULL canonicalization must prevent those element values from leaking
     * into the dedup keys, so the only matchable values on the right are the genuine SV reds
     * — "green" must <strong>not</strong> match the left-side {@code id=8} row even though it
     * appears as one element of {@code id=7}'s multi-valued color.
     */
    public void testInSubqueryRightSideMvElementValuesAreNotMatchable() {
        indexMultiValuedColorRow();
        client().prepareBulk()
            .add(new IndexRequest("test").id("8").source("id", 8, "color", "green"))
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();
        // Subquery: id=1 emits "red", id=7 emits ["red", "green"]. After MV→NULL the dedup
        // is {NULL, "red"} → left-side matches are only "red" rows.
        try (var resp = run("""
            FROM test
            | WHERE color IN (FROM test | WHERE id == 1 OR id == 7 | KEEP color)
            | SORT id
            | KEEP id
            """)) {
            assertColumnNames(resp.columns(), List.of("id"));
            // id=1,3,5 match "red". id=8 ("green") does NOT match because "green" was an MV
            // element on the right and was canonicalized to NULL, not preserved as a value.
            // id=7 itself is dropped because its MV color folds to NULL on the left as well.
            assertValues(resp.values(), List.of(List.of(1), List.of(3), List.of(5)));
        }
    }

    /**
     * NOT IN with a multi-valued row in the subquery output: ANTI's
     * {@code shortCircuitOnAnyRightNull()} triggers as soon as {@code SemiJoin#inlineData}
     * sees a NULL position in the dedup output (which it always does once any MV row is
     * canonicalized to NULL). The whole plan collapses to {@code Filter(FALSE)} → empty.
     * This mirrors {@code x NOT IN (..., NULL, ...)} SQL semantics: never TRUE for any row.
     */
    public void testNotInSubqueryRightSideMvShortCircuitsToEmpty() {
        indexMultiValuedColorRow();
        try (var resp = run("""
            FROM test
            | WHERE color NOT IN (FROM test | KEEP color)
            | SORT id
            | KEEP id
            """)) {
            assertColumnNames(resp.columns(), List.of("id"));
            assertValues(resp.values(), List.of());
        }
    }

    /**
     * Parity check: with a multi-valued row in the subquery output, the filter path and the
     * forced hash-join path must produce identical results. The filter path keeps the
     * MV-derived NULL as a NULL literal in the {@code In} list; the hash-join path strips it
     * from the {@link org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation
     * LocalRelation} (so the runtime BlockHash can't match {@code null = null}) but still
     * forwards {@code rightHadNulls} for ANTI's short-circuit. Any divergence here would
     * indicate {@code convertMvPositionsToNull} / {@code stripFirstPosition} / the
     * {@code rightHadNulls} derivation in {@code SemiJoin#inlineData} drifted apart.
     */
    public void testInSubqueryRightSideMvFilterAndHashJoinPathsAgree() {
        assumeTrue("requires query pragmas", canUseQueryPragmas());
        indexMultiValuedColorRow();
        String[] queries = new String[] {
            // Mix of SV ("red") and MV ([red,green]) on the right. Both paths must keep only
            // the "red" matches and drop everything else (including the MV left row).
            """
                FROM test
                | WHERE color IN (FROM test | WHERE id IN (1, 7) | KEEP color)
                | SORT id
                | KEEP id
                """,
            // Right side has only the MV row → all-NULL after canonicalization → SEMI
            // short-circuits to Filter(FALSE). Both paths must return empty.
            """
                FROM test
                | WHERE color IN (FROM test | WHERE id == 7 | KEEP color)
                | SORT id
                | KEEP id
                """,
            // NOT IN with the MV row in the subquery → ANTI short-circuit → empty. Both
            // paths must return empty.
            """
                FROM test
                | WHERE color NOT IN (FROM test | WHERE id == 7 | KEEP color)
                | SORT id
                | KEEP id
                """,
            // NOT IN where the subquery has SV + MV. Any MV → NULL on the right is fatal for
            // ANTI, so result is empty on both paths.
            """
                FROM test
                | WHERE color NOT IN (FROM test | WHERE id IN (1, 7) | KEEP color)
                | SORT id
                | KEEP id
                """ };
        for (String query : queries) {
            try (
                var defaultResp = run(syncEsqlQueryRequest(query).pragmas(QueryPragmas.EMPTY));
                var forcedResp = run(syncEsqlQueryRequest(query).pragmas(forceHashJoin()))
            ) {
                assertEquals(
                    "filter and hash-join paths disagree on MV-on-right for query:\n" + query,
                    getValuesList(defaultResp),
                    getValuesList(forcedResp)
                );
            }
        }
    }

    /**
     * Disjunctive IN subquery ({@link org.elasticsearch.xpack.esql.plan.logical.join.MarkJoin
     * MarkJoin}) with a multi-valued row in the subquery output. The MV row canonicalizes to
     * NULL on the right, so {@code In}'s three-valued mark is {@code NULL} for left rows whose
     * key doesn't match any SV right value. Combined with the outer OR clause, this surfaces
     * the {@code NULL OR TRUE = TRUE} vs {@code NULL OR FALSE = NULL} distinction — and both
     * the filter and hash-join paths must agree.
     */
    public void testDisjunctiveInSubqueryRightSideMvFilterAndHashJoinPathsAgree() {
        assumeTrue("requires query pragmas", canUseQueryPragmas());
        indexMultiValuedColorRow();
        String[] queries = new String[] {
            // Subquery has only the MV row → dedup is {NULL} after canonicalization → mark is
            // NULL for every left row. Outer OR with `id > 100` (FALSE for every row) gives
            // NULL OR FALSE = NULL → row dropped on both paths.
            """
                FROM test
                | WHERE color IN (FROM test | WHERE id == 7 | KEEP color) OR id > 100
                | SORT id
                | KEEP id
                """,
            // Same subquery as above but the OR's other side is TRUE for some rows → mark is
            // NULL, NULL OR TRUE = TRUE → those rows are kept; otherwise NULL OR FALSE = NULL
            // → dropped. The MV left row at id=7 is also dropped because its color folds to
            // NULL through the left-side MvSingleValueOrNull guard.
            """
                FROM test
                | WHERE color IN (FROM test | WHERE id == 7 | KEEP color) OR id > 4
                | SORT id
                | KEEP id
                """,
            // Subquery has SV "red" + the MV row. Dedup is {NULL, "red"}; mark is TRUE for
            // "red" rows, NULL for "blue"/MV rows. Combined with OR `id > 100`: only the
            // "red" rows survive on both paths.
            """
                FROM test
                | WHERE color IN (FROM test | WHERE id IN (1, 7) | KEEP color) OR id > 100
                | SORT id
                | KEEP id
                """ };
        for (String query : queries) {
            try (
                var defaultResp = run(syncEsqlQueryRequest(query).pragmas(QueryPragmas.EMPTY));
                var forcedResp = run(syncEsqlQueryRequest(query).pragmas(forceHashJoin()))
            ) {
                assertEquals(
                    "MarkJoin filter and hash-join paths disagree on MV-on-right for query:\n" + query,
                    getValuesList(defaultResp),
                    getValuesList(forcedResp)
                );
            }
        }
    }

    // ---- views referenced from inside an IN subquery ----

    public void testInSubqueryReferencingSimpleView() {
        assumeTrue("Requires views in cluster state", EsqlCapabilities.Cap.VIEWS_IN_CLUSTER_STATE.isEnabled());
        assumeTrue("Requires IN subquery view support", EsqlCapabilities.Cap.WHERE_IN_SUBQUERY_WITH_VIEW.isEnabled());

        try {
            installView("red_ids", "FROM test | WHERE color == \"red\" | KEEP id");

            try (var resp = run("FROM test | WHERE id IN (FROM red_ids) | SORT id | KEEP id, color")) {
                assertColumnNames(resp.columns(), List.of("id", "color"));
                assertValues(resp.values(), List.of(List.of(1, "red"), List.of(3, "red"), List.of(5, "red")));
            }
        } finally {
            deleteViews("red_ids");
        }
    }

    /**
     * Regression test: a request-level {@code filter} makes {@code EsqlSession.analyzeWithRetry}
     * re-run {@code Analyzer.analyze} on the same parsed plan after a {@code VerificationException},
     * revisiting the {@code ViewUnionAll} built for the two unioned views -- whose
     * {@code namedSubqueries} map {@code asSubqueryMap} already drained on the first pass (see
     * {@link org.elasticsearch.xpack.esql.plan.logical.ViewUnionAllTests#testReplaceChildrenDoesNotMutateOriginalInstance}).
     * <p>
     * Before we corrected a bug this was failing with {@link java.util.NoSuchElementException}.
     */
    public void testTwoViewsUnionedInInSubqueryRetryDoesNotDrainViewUnionAll() {
        assumeTrue("Requires views in cluster state", EsqlCapabilities.Cap.VIEWS_IN_CLUSTER_STATE.isEnabled());
        assumeTrue("Requires IN subquery view support", EsqlCapabilities.Cap.WHERE_IN_SUBQUERY_WITH_VIEW.isEnabled());
        try {
            installView("red_ids", "FROM test | WHERE color == \"red\" | KEEP id");
            installView("blue_ids", "FROM test | WHERE color == \"blue\" | KEEP id");

            var request = syncEsqlQueryRequest("""
                FROM test
                | EVAL bogus = totally_bogus_column_xyz + 1
                | WHERE id IN (FROM red_ids, blue_ids | KEEP id)
                | KEEP id
                """).filter(new RangeQueryBuilder("id").gte(0));
            expectThrows(VerificationException.class, () -> run(request).close());
        } finally {
            deleteViews("red_ids", "blue_ids");
        }
    }

    public void testViewReferencedFromInSubquery() {
        assumeTrue("Requires views in cluster state", EsqlCapabilities.Cap.VIEWS_IN_CLUSTER_STATE.isEnabled());
        assumeTrue("Requires IN subquery view support", EsqlCapabilities.Cap.WHERE_IN_SUBQUERY_WITH_VIEW.isEnabled());
        try {
            installView("in_layer_1", "FROM test | WHERE id IN (FROM test | WHERE color == \"red\" | KEEP id) | KEEP id");
            installView("in_layer_2", "FROM test | WHERE id IN (FROM in_layer_1 | KEEP id) | KEEP id");
            installView("in_layer_3", "FROM test | WHERE id IN (FROM in_layer_2 | KEEP id) | KEEP id, color");

            try (var resp = run("FROM test | WHERE id IN (FROM in_layer_3 | WHERE id > 1 | KEEP id) | SORT id | KEEP id, color")) {
                assertColumnNames(resp.columns(), List.of("id", "color"));
                assertValues(resp.values(), List.of(List.of(3, "red"), List.of(5, "red")));
            }
        } finally {
            deleteViews("in_layer_1", "in_layer_2", "in_layer_3");
        }
    }

    private void indexMultiValuedColorRow() {
        client().prepareBulk()
            .add(new IndexRequest("test").id("7").source("id", 7, "color", List.of("red", "green")))
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();
    }

    private void createColorDupIndexWithMultiValuedRow() {
        assertAcked(
            client().admin()
                .indices()
                .prepareCreate("color_dup")
                .setSettings(Settings.builder().put("index.number_of_shards", randomIntBetween(1, 5)))
                .setMapping("id", "type=integer", "color", "type=keyword")
        );
        client().prepareBulk()
            .add(new IndexRequest("color_dup").id("7").source("id", 7, "color", List.of("red", "green")))
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();
        ensureYellow("color_dup");
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

        assertAcked(
            client().admin()
                .indices()
                .prepareCreate("ids")
                .setSettings(Settings.builder().put("index.number_of_shards", 1))
                .setMapping("id", "type=integer")
        );
        client().prepareBulk()
            .add(new IndexRequest("ids").id("1").source("id", 1))
            .add(new IndexRequest("ids").id("2").source("id", 3))
            .add(new IndexRequest("ids").id("3").source("id", 5))
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();
        ensureYellow("ids");
    }

    public static void installView(String name, String query) {
        assertAcked(
            client().execute(
                PutViewAction.INSTANCE,
                new PutViewAction.Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, new View(name, query))
            )
        );
    }

    public static void deleteViews(String... names) {
        for (String name : names) {
            try {
                assertAcked(
                    client().execute(
                        DeleteViewAction.INSTANCE,
                        new DeleteViewAction.Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, new String[] { name })
                    )
                );
            } catch (RuntimeException e) {
                // View may be absent if the test body failed before creating it.
            }
        }
    }
}
