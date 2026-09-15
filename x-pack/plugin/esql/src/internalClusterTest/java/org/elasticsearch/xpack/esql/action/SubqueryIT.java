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
import org.elasticsearch.compute.operator.DriverProfile;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.elasticsearch.xpack.esql.view.DeleteViewAction;
import org.elasticsearch.xpack.esql.view.PutViewAction;
import org.junit.Before;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

/**
 * Tests for subquery batch execution in ComputeService.
 * Verifies that limiting concurrent subqueries via the {@code subquery_batch_size} pragma
 * produces correct results across different batch sizes and query shapes.
 */
public class SubqueryIT extends AbstractEsqlIntegTestCase {

    @Before
    public void checkPragma() {
        assumeTrue("requires query pragmas", canUseQueryPragmas());
    }

    @Before
    public void setupIndex() {
        createAndPopulateIndex();
    }

    public void testSubqueryBatchSizeOne() {
        var query = """
            FROM
               ( FROM test | WHERE content:"fox" ),
               ( FROM test | WHERE content:"dog" ),
               ( FROM test | WHERE content:"cat" )
            | KEEP id, content
            | SORT id
            """;
        var pragmas = new QueryPragmas(Settings.builder().put(QueryPragmas.BRANCH_PARALLEL_DEGREE.getKey(), 1).build());
        try (var resp = run(syncEsqlQueryRequest(query).pragmas(pragmas))) {
            assertColumnNames(resp.columns(), List.of("id", "content"));
            assertColumnTypes(resp.columns(), List.of("integer", "text"));
            Iterable<Iterable<Object>> expectedValues = List.of(
                List.of(1, "This is a brown fox"),
                List.of(2, "This is a brown dog"),
                List.of(3, "This dog is really brown"),
                List.of(4, "The dog is brown but this document is very very long"),
                List.of(5, "There is also a white cat"),
                List.of(6, "The quick brown fox jumps over the lazy dog"),
                List.of(6, "The quick brown fox jumps over the lazy dog")
            );
            assertValues(resp.values(), expectedValues);
        }
    }

    public void testSubqueryBatchSizeTwo() {
        var query = """
            FROM
               ( FROM test | WHERE id == 6 ),
               ( FROM test | WHERE id == 2 ),
               ( FROM test | WHERE id == 5 ),
               ( FROM test | WHERE id == 1 ),
               ( FROM test | WHERE id == 3 )
            | SORT id
            | KEEP id, content
            """;
        var pragmas = new QueryPragmas(Settings.builder().put(QueryPragmas.BRANCH_PARALLEL_DEGREE.getKey(), 2).build());
        try (var resp = run(syncEsqlQueryRequest(query).pragmas(pragmas))) {
            assertColumnNames(resp.columns(), List.of("id", "content"));
            assertColumnTypes(resp.columns(), List.of("integer", "text"));
            Iterable<Iterable<Object>> expectedValues = List.of(
                List.of(1, "This is a brown fox"),
                List.of(2, "This is a brown dog"),
                List.of(3, "This dog is really brown"),
                List.of(5, "There is also a white cat"),
                List.of(6, "The quick brown fox jumps over the lazy dog")
            );
            assertValues(resp.values(), expectedValues);
        }
    }

    public void testSubqueryBatchSizeWithStatsAndBatchSizeOne() {
        var query = """
            FROM
               (FROM test | STATS x=COUNT(*), y=MV_SORT(VALUES(id)) ),
               (FROM test | WHERE id == 2 )
            | KEEP x, y, id
            | SORT x NULLS LAST
            """;
        var pragmas = new QueryPragmas(Settings.builder().put(QueryPragmas.BRANCH_PARALLEL_DEGREE.getKey(), 1).build());
        try (var resp = run(syncEsqlQueryRequest(query).pragmas(pragmas))) {
            assertColumnNames(resp.columns(), List.of("x", "y", "id"));
            Iterable<Iterable<Object>> expectedValues = List.of(
                Arrays.stream(new Object[] { 6L, List.of(1, 2, 3, 4, 5, 6), null }).toList(),
                Arrays.stream(new Object[] { null, null, 2 }).toList()
            );
            assertValues(resp.values(), expectedValues);
        }
    }

    public void testSubqueryBatchSizeWithEmptyBranches() {
        var query = """
            FROM
               ( FROM test | WHERE content:"rabbit" ),
               ( FROM test | WHERE content:"dog" ),
               ( FROM test | WHERE content:"lion" ),
               ( FROM test | WHERE content:"cat" )
            | KEEP id, content
            | SORT id
            """;
        var pragmas = new QueryPragmas(Settings.builder().put(QueryPragmas.BRANCH_PARALLEL_DEGREE.getKey(), 2).build());
        try (var resp = run(syncEsqlQueryRequest(query).pragmas(pragmas))) {
            assertColumnNames(resp.columns(), List.of("id", "content"));
            assertColumnTypes(resp.columns(), List.of("integer", "text"));
            Iterable<Iterable<Object>> expectedValues = List.of(
                List.of(2, "This is a brown dog"),
                List.of(3, "This dog is really brown"),
                List.of(4, "The dog is brown but this document is very very long"),
                List.of(5, "There is also a white cat"),
                List.of(6, "The quick brown fox jumps over the lazy dog")
            );
            assertValues(resp.values(), expectedValues);
        }
    }

    /**
     * Batch size larger than number of subqueries - all subqueries should execute in a single batch.
     */
    public void testBatchSizeLargerThanSubqueryCount() {
        var query = """
            FROM
               ( FROM test | WHERE id == 1 ),
               ( FROM test | WHERE id == 2 )
            | KEEP id, content
            | SORT id
            """;
        var pragmas = new QueryPragmas(Settings.builder().put(QueryPragmas.BRANCH_PARALLEL_DEGREE.getKey(), 8).build());
        try (var resp = run(syncEsqlQueryRequest(query).pragmas(pragmas))) {
            assertColumnNames(resp.columns(), List.of("id", "content"));
            assertColumnTypes(resp.columns(), List.of("integer", "text"));
            Iterable<Iterable<Object>> expectedValues = List.of(List.of(1, "This is a brown fox"), List.of(2, "This is a brown dog"));
            assertValues(resp.values(), expectedValues);
        }
    }

    /**
     * Batch size equals number of subqueries - exactly one batch, no recursion.
     */
    public void testBatchSizeEqualsSubqueryCount() {
        var query = """
            FROM
               ( FROM test | WHERE content:"fox" ),
               ( FROM test | WHERE content:"cat" ),
               ( FROM test | WHERE id == 2 )
            | KEEP id, content
            | SORT id
            """;
        var pragmas = new QueryPragmas(Settings.builder().put(QueryPragmas.BRANCH_PARALLEL_DEGREE.getKey(), 3).build());
        try (var resp = run(syncEsqlQueryRequest(query).pragmas(pragmas))) {
            assertColumnNames(resp.columns(), List.of("id", "content"));
            assertColumnTypes(resp.columns(), List.of("integer", "text"));
            Iterable<Iterable<Object>> expectedValues = List.of(
                List.of(1, "This is a brown fox"),
                List.of(2, "This is a brown dog"),
                List.of(5, "There is also a white cat"),
                List.of(6, "The quick brown fox jumps over the lazy dog")
            );
            assertValues(resp.values(), expectedValues);
        }
    }

    /**
     * Single subquery with batch size 1 - minimal edge case.
     */
    public void testSingleSubqueryWithBatchSizeOne() {
        var query = """
            FROM
               ( FROM test | WHERE content:"fox" )
            | KEEP id, content
            | SORT id
            """;
        var pragmas = new QueryPragmas(Settings.builder().put(QueryPragmas.BRANCH_PARALLEL_DEGREE.getKey(), 1).build());
        try (var resp = run(syncEsqlQueryRequest(query).pragmas(pragmas))) {
            assertColumnNames(resp.columns(), List.of("id", "content"));
            assertColumnTypes(resp.columns(), List.of("integer", "text"));
            Iterable<Iterable<Object>> expectedValues = List.of(
                List.of(1, "This is a brown fox"),
                List.of(6, "The quick brown fox jumps over the lazy dog")
            );
            assertValues(resp.values(), expectedValues);
        }
    }

    /**
     * Different schemas across subqueries in different batches.
     * One subquery returns STATS columns, another returns raw columns.
     * Missing columns should be null.
     */
    public void testDifferentSchemasAcrossBatches() {
        var query = """
            FROM
               ( FROM test | STATS cnt = COUNT(*) ),
               ( FROM test | WHERE id == 1 | KEEP id ),
               ( FROM test | STATS mx = MAX(id) )
            | KEEP cnt, id, mx
            | SORT cnt NULLS LAST, id NULLS LAST
            """;
        var pragmas = new QueryPragmas(Settings.builder().put(QueryPragmas.BRANCH_PARALLEL_DEGREE.getKey(), 1).build());
        try (var resp = run(syncEsqlQueryRequest(query).pragmas(pragmas))) {
            assertColumnNames(resp.columns(), List.of("cnt", "id", "mx"));
            Iterable<Iterable<Object>> expectedValues = List.of(
                Arrays.stream(new Object[] { 6L, null, null }).toList(),
                Arrays.stream(new Object[] { null, 1, null }).toList(),
                Arrays.stream(new Object[] { null, null, 6 }).toList()
            );
            assertValues(resp.values(), expectedValues);
        }
    }

    /**
     * All subqueries return empty results across multiple batches.
     */
    public void testAllEmptyBranches() {
        var query = """
            FROM
               ( FROM test | WHERE id == 999 ),
               ( FROM test | WHERE id == 888 ),
               ( FROM test | WHERE id == 777 )
            | KEEP id, content
            | SORT id
            """;
        var pragmas = new QueryPragmas(Settings.builder().put(QueryPragmas.BRANCH_PARALLEL_DEGREE.getKey(), 1).build());
        try (var resp = run(syncEsqlQueryRequest(query).pragmas(pragmas))) {
            assertColumnNames(resp.columns(), List.of("id", "content"));
            assertColumnTypes(resp.columns(), List.of("integer", "text"));
            Iterable<Iterable<Object>> expectedValues = List.of();
            assertValues(resp.values(), expectedValues);
        }
    }

    /**
     * Duplicate rows across subqueries - UNION ALL semantics should preserve all duplicates,
     * even when subqueries are in different batches.
     */
    public void testDuplicateRowsAcrossBatches() {
        var query = """
            FROM
               ( FROM test | WHERE id == 1 ),
               ( FROM test | WHERE id == 1 ),
               ( FROM test | WHERE id == 1 )
            | KEEP id, content
            | SORT id
            """;
        var pragmas = new QueryPragmas(Settings.builder().put(QueryPragmas.BRANCH_PARALLEL_DEGREE.getKey(), 1).build());
        try (var resp = run(syncEsqlQueryRequest(query).pragmas(pragmas))) {
            assertColumnNames(resp.columns(), List.of("id", "content"));
            assertColumnTypes(resp.columns(), List.of("integer", "text"));
            Iterable<Iterable<Object>> expectedValues = List.of(
                List.of(1, "This is a brown fox"),
                List.of(1, "This is a brown fox"),
                List.of(1, "This is a brown fox")
            );
            assertValues(resp.values(), expectedValues);
        }
    }

    /**
     * No explicit batch size pragma - uses the default (processor count).
     * Verifies the default path works correctly.
     */
    public void testDefaultBatchSize() {
        var query = """
            FROM
               ( FROM test | WHERE content:"fox" ),
               ( FROM test | WHERE content:"dog" ),
               ( FROM test | WHERE content:"cat" )
            | KEEP id, content
            | SORT id
            """;
        try (var resp = run(query)) {
            assertColumnNames(resp.columns(), List.of("id", "content"));
            assertColumnTypes(resp.columns(), List.of("integer", "text"));
            Iterable<Iterable<Object>> expectedValues = List.of(
                List.of(1, "This is a brown fox"),
                List.of(2, "This is a brown dog"),
                List.of(3, "This dog is really brown"),
                List.of(4, "The dog is brown but this document is very very long"),
                List.of(5, "There is also a white cat"),
                List.of(6, "The quick brown fox jumps over the lazy dog"),
                List.of(6, "The quick brown fox jumps over the lazy dog")
            );
            assertValues(resp.values(), expectedValues);
        }
    }

    /**
     * Empty batch followed by non-empty batch - first batch produces no rows,
     * second batch produces rows. Verifies batch transitions from empty to non-empty.
     */
    public void testEmptyBatchFollowedByNonEmptyBatch() {
        var query = """
            FROM
               ( FROM test | WHERE id == 999 ),
               ( FROM test | WHERE id == 888 ),
               ( FROM test | WHERE id == 1 ),
               ( FROM test | WHERE id == 2 )
            | KEEP id, content
            | SORT id
            """;
        var pragmas = new QueryPragmas(Settings.builder().put(QueryPragmas.BRANCH_PARALLEL_DEGREE.getKey(), 2).build());
        try (var resp = run(syncEsqlQueryRequest(query).pragmas(pragmas))) {
            assertColumnNames(resp.columns(), List.of("id", "content"));
            assertColumnTypes(resp.columns(), List.of("integer", "text"));
            Iterable<Iterable<Object>> expectedValues = List.of(List.of(1, "This is a brown fox"), List.of(2, "This is a brown dog"));
            assertValues(resp.values(), expectedValues);
        }
    }

    /**
     * Non-empty batch followed by empty batch - first batch produces rows,
     * second batch produces nothing. Verifies final empty batch is handled cleanly.
     */
    public void testNonEmptyBatchFollowedByEmptyBatch() {
        var query = """
            FROM
               ( FROM test | WHERE id == 1 ),
               ( FROM test | WHERE id == 2 ),
               ( FROM test | WHERE id == 999 ),
               ( FROM test | WHERE id == 888 )
            | KEEP id, content
            | SORT id
            """;
        var pragmas = new QueryPragmas(Settings.builder().put(QueryPragmas.BRANCH_PARALLEL_DEGREE.getKey(), 2).build());
        try (var resp = run(syncEsqlQueryRequest(query).pragmas(pragmas))) {
            assertColumnNames(resp.columns(), List.of("id", "content"));
            assertColumnTypes(resp.columns(), List.of("integer", "text"));
            Iterable<Iterable<Object>> expectedValues = List.of(List.of(1, "This is a brown fox"), List.of(2, "This is a brown dog"));
            assertValues(resp.values(), expectedValues);
        }
    }

    /**
     * Many subqueries with batch size 1 - maximal sequential batching with many recursive calls.
     */
    public void testManySubqueriesWithBatchSizeOne() {
        var query = """
            FROM
               ( FROM test | WHERE id == 1 ),
               ( FROM test | WHERE id == 2 ),
               ( FROM test | WHERE id == 3 ),
               ( FROM test | WHERE id == 4 ),
               ( FROM test | WHERE id == 5 ),
               ( FROM test | WHERE id == 6 ),
               ( FROM test | WHERE id == 1 )
            | KEEP id, content
            | SORT id
            """;
        var pragmas = new QueryPragmas(Settings.builder().put(QueryPragmas.BRANCH_PARALLEL_DEGREE.getKey(), 1).build());
        try (var resp = run(syncEsqlQueryRequest(query).pragmas(pragmas))) {
            assertColumnNames(resp.columns(), List.of("id", "content"));
            assertColumnTypes(resp.columns(), List.of("integer", "text"));
            Iterable<Iterable<Object>> expectedValues = List.of(
                List.of(1, "This is a brown fox"),
                List.of(1, "This is a brown fox"),
                List.of(2, "This is a brown dog"),
                List.of(3, "This dog is really brown"),
                List.of(4, "The dog is brown but this document is very very long"),
                List.of(5, "There is also a white cat"),
                List.of(6, "The quick brown fox jumps over the lazy dog")
            );
            assertValues(resp.values(), expectedValues);
        }
    }

    public void testThreeLevelNestedSubqueriesAtDifferentParallelDegrees() {
        var query = """
            FROM
               ( FROM test | WHERE id == 1 ),
               ( FROM
                    ( FROM test | WHERE id == 2 ),
                    ( FROM
                         ( FROM test | WHERE id == 3 ),
                         ( FROM test | WHERE id == 4 )
                    )
               ),
               ( FROM test | WHERE id == 5 )
            | KEEP id
            | SORT id
            """;
        for (int degree : List.of(1, 2, 8)) {
            try (var resp = run(syncEsqlQueryRequest(query).pragmas(branchPragmas(degree)))) {
                assertColumnNames(resp.columns(), List.of("id"));
                assertValues(resp.values(), List.of(List.of(1), List.of(2), List.of(3), List.of(4), List.of(5)));
            }
        }
    }

    public void testNestedSubqueryProfileUsesHierarchicalNames() {
        var query = """
            FROM
               ( FROM test | WHERE id == 1 ),
               ( FROM
                    ( FROM test | WHERE id == 2 ),
                    ( FROM
                         ( FROM test | WHERE id == 3 ),
                         ( FROM test | WHERE id == 4 )
                    )
               )
            | SORT id
            | KEEP id
            """;
        try (var resp = run(syncEsqlQueryRequest(query).pragmas(branchPragmas(1)).profile(true))) {
            assertNotNull(resp.profile());
            Set<String> descriptions = resp.profile().drivers().stream().map(DriverProfile::description).collect(Collectors.toSet());
            assertTrue(descriptions.contains("main.final"));
            assertTrue(descriptions.contains("subplan-0.final"));
            assertTrue(descriptions.contains("subplan-1.merge"));
            assertTrue(descriptions.contains("subplan-1.subplan-0.final"));
            assertTrue(descriptions.contains("subplan-1.subplan-1.merge"));
            assertTrue(descriptions.contains("subplan-1.subplan-1.subplan-0.final"));
            assertTrue(descriptions.contains("subplan-1.subplan-1.subplan-1.final"));
        }
    }

    public void testNestedSubqueryOuterLimitWithQueuedLeaves() {
        var query = """
            FROM
               ( FROM test ),
               ( FROM
                    ( FROM test ),
                    ( FROM
                         ( FROM test ),
                         ( FROM test )
                    )
               )
            | LIMIT 1
            """;
        try (var resp = run(syncEsqlQueryRequest(query).pragmas(branchPragmas(1)))) {
            var values = resp.values();
            assertTrue(values.hasNext());
            values.next();
            assertFalse(values.hasNext());
        }
    }

    /**
     * The main {@code FROM} pattern {@code airports*} matches both the {@code airports_view} view and the {@code airports} index, so it
     * expands to a view union. The sibling {@code (FROM employees)} adds an enclosing subquery union.
     */
    public void testViewAndIndexInMainQueryWithSubquery() {
        setupWildcardMatchingViewAndIndices();
        try {
            assertAirportViewAndEmployeeRows("FROM airports*, (FROM employees)");
        } finally {
            deleteViews("airports_view");
        }
    }

    /** A wildcard view union inside a subquery can be nested below the top-level source union. */
    public void testViewAndIndexInsideSubquery() {
        setupWildcardMatchingViewAndIndices();
        try {
            assertAirportViewAndEmployeeRows("FROM employees, (FROM airports*)");
        } finally {
            deleteViews("airports_view");
        }
    }

    /** A wildcard view union can also occur inside one of several sibling subqueries. */
    public void testViewAndIndexInOneOfMultipleSubqueries() {
        setupWildcardMatchingViewAndIndices();
        try {
            assertAirportViewAndEmployeeRows("FROM (FROM airports*), (FROM employees)");
        } finally {
            deleteViews("airports_view");
        }
    }

    public void testUnionAllWithForkInsideSubqueries() {
        var query = """
            FROM (FROM test | FORK (WHERE id > 4) (WHERE id <= 4)),
                 (FROM test | FORK (WHERE id > 2) (WHERE id <= 2))
            | KEEP _fork, id
            | SORT _fork, id
            """;
        try (var resp = run(syncEsqlQueryRequest(query))) {
            List<List<Object>> rows = getValuesList(resp);
            // fork1: id 3, 4, 5, 5, 6, 6
            assertThat(rows.get(0).get(0).toString(), equalTo("fork1"));
            assertThat(rows.get(0).get(1), equalTo(3));
            assertThat(rows.get(1).get(0).toString(), equalTo("fork1"));
            assertThat(rows.get(1).get(1), equalTo(4));
            assertThat(rows.get(2).get(0).toString(), equalTo("fork1"));
            assertThat(rows.get(2).get(1), equalTo(5));
            assertThat(rows.get(3).get(0).toString(), equalTo("fork1"));
            assertThat(rows.get(3).get(1), equalTo(5));
            assertThat(rows.get(4).get(0).toString(), equalTo("fork1"));
            assertThat(rows.get(4).get(1), equalTo(6));
            assertThat(rows.get(5).get(0).toString(), equalTo("fork1"));
            assertThat(rows.get(5).get(1), equalTo(6));

            // fork2: id 1, 1, 2, 2, 3, 4
            assertThat(rows.get(6).get(0).toString(), equalTo("fork2"));
            assertThat(rows.get(6).get(1), equalTo(1));
            assertThat(rows.get(7).get(0).toString(), equalTo("fork2"));
            assertThat(rows.get(7).get(1), equalTo(1));
            assertThat(rows.get(8).get(0).toString(), equalTo("fork2"));
            assertThat(rows.get(8).get(1), equalTo(2));
            assertThat(rows.get(9).get(0).toString(), equalTo("fork2"));
            assertThat(rows.get(9).get(1), equalTo(2));
            assertThat(rows.get(10).get(0).toString(), equalTo("fork2"));
            assertThat(rows.get(10).get(1), equalTo(3));
            assertThat(rows.get(11).get(0).toString(), equalTo("fork2"));
            assertThat(rows.get(11).get(1), equalTo(4));
        }
    }

    public void testForkAfterUnionAllSubqueries() {
        var query = """
            FROM (FROM test | WHERE id > 4),
                 (FROM test | WHERE id <= 2)
            | FORK (WHERE id > 3) (WHERE id <= 3)
            | KEEP _fork, id
            | SORT _fork, id
            """;
        try (var resp = run(syncEsqlQueryRequest(query))) {
            List<List<Object>> rows = getValuesList(resp);
            assertThat(rows, hasSize(4));
            // fork1: id 5, 6
            assertThat(rows.get(0).get(0).toString(), equalTo("fork1"));
            assertThat(rows.get(0).get(1), equalTo(5));
            assertThat(rows.get(1).get(0).toString(), equalTo("fork1"));
            assertThat(rows.get(1).get(1), equalTo(6));

            // fork2: id 1, 2
            assertThat(rows.get(2).get(0).toString(), equalTo("fork2"));
            assertThat(rows.get(2).get(1), equalTo(1));
            assertThat(rows.get(3).get(0).toString(), equalTo("fork2"));
            assertThat(rows.get(3).get(1), equalTo(2));
        }
    }

    public void testForkInsideAndAfterUnionAllSubqueries() {
        var query = """
            FROM (FROM test | FORK (WHERE id > 4) (WHERE id <= 4)),
                 (FROM test | WHERE id <= 2 | EVAL _fork = "fork1")
            | FORK (WHERE id > 3) (WHERE id <= 3)
            | KEEP _fork, id
            | SORT _fork, id
            """;
        try (var resp = run(syncEsqlQueryRequest(query))) {
            List<List<Object>> rows = getValuesList(resp);
            assertThat(rows, hasSize(8));

            // fork1: id 4, 5, 6
            assertThat(rows.get(0).get(0).toString(), equalTo("fork1"));
            assertThat(rows.get(0).get(1), equalTo(4));
            assertThat(rows.get(1).get(0).toString(), equalTo("fork1"));
            assertThat(rows.get(1).get(1), equalTo(5));
            assertThat(rows.get(2).get(0).toString(), equalTo("fork1"));
            assertThat(rows.get(2).get(1), equalTo(6));

            // fork2: id 1, 1, 2, 2, 3
            assertThat(rows.get(3).get(0).toString(), equalTo("fork2"));
            assertThat(rows.get(3).get(1), equalTo(1));
            assertThat(rows.get(4).get(0).toString(), equalTo("fork2"));
            assertThat(rows.get(4).get(1), equalTo(1));
            assertThat(rows.get(5).get(0).toString(), equalTo("fork2"));
            assertThat(rows.get(5).get(1), equalTo(2));
            assertThat(rows.get(6).get(0).toString(), equalTo("fork2"));
            assertThat(rows.get(6).get(1), equalTo(2));
            assertThat(rows.get(7).get(0).toString(), equalTo("fork2"));
            assertThat(rows.get(7).get(1), equalTo(3));
        }
    }

    public void testUnionAllOfForkingViewAndSubquery() {
        assertAcked(
            client().execute(
                PutViewAction.INSTANCE,
                new PutViewAction.Request(
                    TEST_REQUEST_TIMEOUT,
                    TEST_REQUEST_TIMEOUT,
                    new View("fork_view", "FROM test | FORK (WHERE id > 4) (WHERE id <= 4)")
                )
            )
        );
        try {
            var query = """
                FROM (FROM fork_view),
                     (FROM test | WHERE id <= 2 | EVAL _fork = "fork1")
                | KEEP _fork, id
                | SORT _fork, id
                """;
            try (var resp = run(syncEsqlQueryRequest(query))) {
                List<List<Object>> rows = getValuesList(resp);
                assertThat(rows, hasSize(8));

                // fork1: id 1, 2, 5, 6
                assertThat(rows.get(0).get(0).toString(), equalTo("fork1"));
                assertThat(rows.get(0).get(1), equalTo(1));
                assertThat(rows.get(1).get(0).toString(), equalTo("fork1"));
                assertThat(rows.get(1).get(1), equalTo(2));
                assertThat(rows.get(2).get(0).toString(), equalTo("fork1"));
                assertThat(rows.get(2).get(1), equalTo(5));
                assertThat(rows.get(3).get(0).toString(), equalTo("fork1"));
                assertThat(rows.get(3).get(1), equalTo(6));

                // fork2: id 1, 2, 3, 4
                assertThat(rows.get(4).get(0).toString(), equalTo("fork2"));
                assertThat(rows.get(4).get(1), equalTo(1));
                assertThat(rows.get(5).get(0).toString(), equalTo("fork2"));
                assertThat(rows.get(5).get(1), equalTo(2));
                assertThat(rows.get(6).get(0).toString(), equalTo("fork2"));
                assertThat(rows.get(6).get(1), equalTo(3));
                assertThat(rows.get(7).get(0).toString(), equalTo("fork2"));
                assertThat(rows.get(7).get(1), equalTo(4));
            }
        } finally {
            deleteViews("fork_view");
        }
    }

    public void testUnionAllOfForkingViewAndForkingSubquery() {
        assertAcked(
            client().execute(
                PutViewAction.INSTANCE,
                new PutViewAction.Request(
                    TEST_REQUEST_TIMEOUT,
                    TEST_REQUEST_TIMEOUT,
                    new View("fork_view", "FROM test | FORK (WHERE id > 4) (WHERE id <= 4)")
                )
            )
        );
        try {
            var query = """
                FROM (FROM fork_view),
                     (FROM test | FORK (WHERE id > 2) (WHERE id <= 2))
                | KEEP _fork, id
                | SORT _fork, id
                """;
            try (var resp = run(syncEsqlQueryRequest(query))) {
                List<List<Object>> rows = getValuesList(resp);
                assertThat(rows, hasSize(12));

                // fork1: id 3, 4, 5, 5, 6, 6
                assertThat(rows.get(0).get(0).toString(), equalTo("fork1"));
                assertThat(rows.get(0).get(1), equalTo(3));
                assertThat(rows.get(1).get(0).toString(), equalTo("fork1"));
                assertThat(rows.get(1).get(1), equalTo(4));
                assertThat(rows.get(2).get(0).toString(), equalTo("fork1"));
                assertThat(rows.get(2).get(1), equalTo(5));
                assertThat(rows.get(3).get(0).toString(), equalTo("fork1"));
                assertThat(rows.get(3).get(1), equalTo(5));
                assertThat(rows.get(4).get(0).toString(), equalTo("fork1"));
                assertThat(rows.get(4).get(1), equalTo(6));
                assertThat(rows.get(5).get(0).toString(), equalTo("fork1"));
                assertThat(rows.get(5).get(1), equalTo(6));

                // fork2: id 1, 1, 2, 2, 3, 4
                assertThat(rows.get(6).get(0).toString(), equalTo("fork2"));
                assertThat(rows.get(6).get(1), equalTo(1));
                assertThat(rows.get(7).get(0).toString(), equalTo("fork2"));
                assertThat(rows.get(7).get(1), equalTo(1));
                assertThat(rows.get(8).get(0).toString(), equalTo("fork2"));
                assertThat(rows.get(8).get(1), equalTo(2));
                assertThat(rows.get(9).get(0).toString(), equalTo("fork2"));
                assertThat(rows.get(9).get(1), equalTo(2));
                assertThat(rows.get(10).get(0).toString(), equalTo("fork2"));
                assertThat(rows.get(10).get(1), equalTo(3));
                assertThat(rows.get(11).get(0).toString(), equalTo("fork2"));
                assertThat(rows.get(11).get(1), equalTo(4));
            }
        } finally {
            deleteViews("fork_view");
        }
    }

    private void assertAirportViewAndEmployeeRows(String query) {
        try (var resp = run(query + " | KEEP id, name | SORT name")) {
            assertColumnNames(resp.columns(), List.of("id", "name"));
            assertColumnTypes(resp.columns(), List.of("integer", "keyword"));
            // The concrete airport index and airports_view each return the airport row; the employee subquery returns the employee row.
            assertValues(resp.values(), List.of(List.of(1, "a"), List.of(1, "a"), List.of(1, "e")));
        }
    }

    /** Creates a wildcard match containing one concrete index and one non-compactable view, plus a sibling index. */
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
        assertAcked(
            client().execute(
                PutViewAction.INSTANCE,
                new PutViewAction.Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, new View("airports_view", "FROM airports | LIMIT 10"))
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

    private static QueryPragmas branchPragmas(int degree) {
        return new QueryPragmas(Settings.builder().put(QueryPragmas.BRANCH_PARALLEL_DEGREE.getKey(), degree).build());
    }

    private void createAndPopulateIndex() {
        var indexName = "test";
        var client = client().admin().indices();
        var createRequest = client.prepareCreate(indexName)
            .setSettings(Settings.builder().put("index.number_of_shards", randomIntBetween(1, 6)))
            .setMapping("id", "type=integer", "content", "type=text");
        assertAcked(createRequest);
        client().prepareBulk()
            .add(new IndexRequest(indexName).id("1").source("id", 1, "content", "This is a brown fox"))
            .add(new IndexRequest(indexName).id("2").source("id", 2, "content", "This is a brown dog"))
            .add(new IndexRequest(indexName).id("3").source("id", 3, "content", "This dog is really brown"))
            .add(new IndexRequest(indexName).id("4").source("id", 4, "content", "The dog is brown but this document is very very long"))
            .add(new IndexRequest(indexName).id("5").source("id", 5, "content", "There is also a white cat"))
            .add(new IndexRequest(indexName).id("6").source("id", 6, "content", "The quick brown fox jumps over the lazy dog"))
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();
        ensureYellow(indexName);
    }
}
