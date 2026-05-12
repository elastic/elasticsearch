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
import org.junit.Before;

import java.util.List;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;

/**
 * Integration tests for WHERE ... IN (subquery) and WHERE ... NOT IN (subquery).
 */
public class InSubqueryIT extends AbstractEsqlIntegTestCase {

    @Before
    public void checkCapability() {
        assumeTrue("Requires IN subquery support", EsqlCapabilities.Cap.WHERE_IN_SUBQUERY_WITH_VIEW.isEnabled());
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
