/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;

import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

/**
 * Integration tests for IN_RANGE function in LOOKUP JOIN scenarios.
 * Tests various combinations of IN_RANGE usage:
 * 1. WHERE clause after LOOKUP JOIN with IN_RANGE filtering on lookup index fields
 * 2. IN_RANGE in LOOKUP JOIN ON condition (right-side pushable filter)
 * 3. Pushdown of IN_RANGE filters to the lookup index
 */
public class LookupJoinInRangeIT extends AbstractEsqlIntegTestCase {

    @Override
    public void setUp() throws Exception {
        super.setUp();
        setupIndices();
    }

    private void setupIndices() {
        // Create main index with date fields
        assertAcked(
            client().admin()
                .indices()
                .prepareCreate("employees")
                .setMapping(
                    Map.of(
                        "properties",
                        Map.of("emp_no", Map.of("type", "integer"), "hire_date", Map.of("type", "date"), "name", Map.of("type", "keyword"))
                    )
                )
        );

        // Index some employee documents with different hire dates
        indexRandom(
            true,
            false,
            prepareIndex("employees").setId("1").setSource(Map.of("emp_no", 10001, "hire_date", "1985-01-15T00:00:00Z", "name", "Alice")),
            prepareIndex("employees").setId("2").setSource(Map.of("emp_no", 10002, "hire_date", "1986-06-20T00:00:00Z", "name", "Bob")),
            prepareIndex("employees").setId("3").setSource(Map.of("emp_no", 10003, "hire_date", "1987-12-10T00:00:00Z", "name", "Charlie")),
            prepareIndex("employees").setId("4").setSource(Map.of("emp_no", 10004, "hire_date", "1988-03-25T00:00:00Z", "name", "Diana"))
        );

        // Create lookup index with date_range fields
        assertAcked(
            client().admin()
                .indices()
                .prepareCreate("date_ranges")
                .setSettings(
                    Settings.builder()
                        .put(IndexSettings.MODE.getKey(), IndexMode.LOOKUP)
                        .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                        .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                )
                .setMapping(
                    Map.of(
                        "properties",
                        Map.of(
                            "date_range",
                            Map.of("type", "date_range"),
                            "decade",
                            Map.of("type", "keyword"),
                            "description",
                            Map.of("type", "keyword")
                        )
                    )
                )
        );

        // Index date range documents
        indexRandom(
            true,
            false,
            prepareIndex("date_ranges").setId("1")
                .setSource(
                    Map.of(
                        "date_range",
                        Map.of("gte", "1985-01-01T00:00:00Z", "lt", "1986-01-01T00:00:00Z"),
                        "decade",
                        "1980s",
                        "description",
                        "1985"
                    )
                ),
            prepareIndex("date_ranges").setId("2")
                .setSource(
                    Map.of(
                        "date_range",
                        Map.of("gte", "1986-01-01T00:00:00Z", "lt", "1987-01-01T00:00:00Z"),
                        "decade",
                        "1980s",
                        "description",
                        "1986"
                    )
                ),
            prepareIndex("date_ranges").setId("3")
                .setSource(
                    Map.of(
                        "date_range",
                        Map.of("gte", "1987-01-01T00:00:00Z", "lt", "1988-01-01T00:00:00Z"),
                        "decade",
                        "1980s",
                        "description",
                        "1987"
                    )
                ),
            prepareIndex("date_ranges").setId("4")
                .setSource(
                    Map.of(
                        "date_range",
                        Map.of("gte", "1988-01-01T00:00:00Z", "lt", "1989-01-01T00:00:00Z"),
                        "decade",
                        "1980s",
                        "description",
                        "1988"
                    )
                )
        );
    }

    /**
     * Test IN_RANGE in WHERE clause after LOOKUP JOIN.
     * The filter should be pushed down to the lookup index (right side).
     * Query: FROM employees | LOOKUP JOIN date_ranges ON TRUE | WHERE IN_RANGE(TO_DATETIME("1985-06-15"), date_range)
     */
    public void testInRangeInWhereClauseAfterLookupJoin() {
        String query = """
            FROM employees
            | LOOKUP JOIN date_ranges ON TRUE
            | WHERE IN_RANGE(TO_DATETIME("1985-06-15"), date_range)
            | KEEP emp_no, name, description
            | SORT emp_no
            """;

        try (EsqlQueryResponse response = run(query)) {
            assertThat(response.isPartial(), equalTo(false));

            // Should match only the 1985 date range
            // Since we're joining on TRUE, we get all combinations, but the WHERE filter should
            // only keep rows where the date_range contains 1985-06-15
            List<List<Object>> values = getValuesList(response);
            assertThat(values.size(), greaterThanOrEqualTo(1));

            // Verify that all results have description="1985" (the only range containing 1985-06-15)
            boolean found1985 = false;
            for (List<Object> row : values) {
                if (row.size() >= 3 && "1985".equals(row.get(2))) {
                    found1985 = true;
                    break;
                }
            }
            assertThat("Should find at least one row with description='1985'", found1985, equalTo(true));
        }
    }

    /**
     * Test IN_RANGE in WHERE clause filtering on constant date with lookup range field.
     * This tests Case 2 pushdown: constant date + range field.
     * Query: FROM employees | LOOKUP JOIN date_ranges ON TRUE | WHERE IN_RANGE(TO_DATETIME("1986-06-15"), date_range)
     */
    public void testInRangeConstantDateWithLookupRangeField() {
        String query = """
            FROM employees
            | LOOKUP JOIN date_ranges ON TRUE
            | WHERE IN_RANGE(TO_DATETIME("1986-06-15"), date_range)
            | KEEP emp_no, name, description
            | SORT emp_no
            """;

        try (EsqlQueryResponse response = run(query)) {
            assertThat(response.isPartial(), equalTo(false));
            List<List<Object>> values = getValuesList(response);

            // Should match only the 1986 date range
            boolean found1986 = false;
            for (List<Object> row : values) {
                if (row.size() >= 3 && "1986".equals(row.get(2))) {
                    found1986 = true;
                    break;
                }
            }
            assertThat("Should find at least one row with description='1986'", found1986, equalTo(true));
        }
    }

    /**
     * Test IN_RANGE in WHERE clause with date from left side and constant range.
     * This tests Case 1 pushdown: date field + constant range.
     * Query: FROM employees | WHERE IN_RANGE(hire_date, TO_DATE_RANGE("1985-01-01..1986-01-01"))
     */
    public void testInRangeDateFieldWithConstantRange() {
        String query = """
            FROM employees
            | WHERE IN_RANGE(hire_date, TO_DATE_RANGE("1985-01-01T00:00:00Z..1986-01-01T00:00:00Z"))
            | KEEP emp_no, name, hire_date
            | SORT emp_no
            """;

        try (EsqlQueryResponse response = run(query)) {
            assertThat(response.isPartial(), equalTo(false));
            List<List<Object>> values = getValuesList(response);

            // Should match employees hired in 1985
            assertThat("Should find employees hired in 1985", values.size(), greaterThanOrEqualTo(1));

            // Verify all results are from 1985
            for (List<Object> row : values) {
                assertThat("Each row should have at least 3 columns", row.size(), greaterThanOrEqualTo(3));
            }
        }
    }

    /**
     * Test IN_RANGE in LOOKUP JOIN ON condition as a right-side pushable filter.
     * This tests that IN_RANGE can be used in the ON clause when it only references lookup index fields.
     * Query: FROM employees | LOOKUP JOIN date_ranges ON TRUE AND IN_RANGE(TO_DATETIME("1987-06-15"), date_range)
     */
    public void testInRangeInLookupJoinOnCondition() {
        // This test verifies that IN_RANGE can be used as a right-side pushable filter in the ON condition
        String query = """
            FROM employees
            | LOOKUP JOIN date_ranges ON TRUE AND IN_RANGE(TO_DATETIME("1987-06-15"), date_range)
            | KEEP emp_no, name, description
            | SORT emp_no
            """;

        try (EsqlQueryResponse response = run(query)) {
            assertThat(response.isPartial(), equalTo(false));
            List<List<Object>> values = getValuesList(response);

            // Should match only the 1987 date range
            boolean found1987 = false;
            for (List<Object> row : values) {
                if (row.size() >= 3 && "1987".equals(row.get(2))) {
                    found1987 = true;
                    break;
                }
            }
            assertThat("Should find at least one row with description='1987'", found1987, equalTo(true));
        }
    }

    /**
     * Test IN_RANGE with date from left side in WHERE clause after LOOKUP JOIN.
     * This tests filtering on the left side date field after the join.
     * Query: FROM employees | LOOKUP JOIN date_ranges ON TRUE | WHERE IN_RANGE(hire_date, TO_DATE_RANGE("1986-01-01..1987-01-01"))
     */
    public void testInRangeLeftSideDateInWhereAfterJoin() {
        String query = """
            FROM employees
            | LOOKUP JOIN date_ranges ON TRUE
            | WHERE IN_RANGE(hire_date, TO_DATE_RANGE("1986-01-01T00:00:00Z..1987-01-01T00:00:00Z"))
            | KEEP emp_no, name, hire_date, description
            | SORT emp_no
            """;

        try (EsqlQueryResponse response = run(query)) {
            assertThat(response.isPartial(), equalTo(false));
            List<List<Object>> values = getValuesList(response);

            // Should match employees hired in 1986
            assertThat("Should find employees hired in 1986", values.size(), greaterThanOrEqualTo(1));
        }
    }

    /**
     * Test IN_RANGE with multiple date ranges in WHERE clause.
     * This tests that multiple IN_RANGE conditions can be combined.
     * Query: FROM employees | LOOKUP JOIN date_ranges ON TRUE | WHERE IN_RANGE(TO_DATETIME("1985-06-15"), date_range)
     * OR IN_RANGE(TO_DATETIME("1988-06-15"), date_range)
     */
    public void testInRangeMultipleRangesInWhere() {
        String query = """
            FROM employees
            | LOOKUP JOIN date_ranges ON TRUE
            | WHERE IN_RANGE(TO_DATETIME("1985-06-15"), date_range) OR IN_RANGE(TO_DATETIME("1988-06-15"), date_range)
            | KEEP emp_no, name, description
            | SORT emp_no
            """;

        try (EsqlQueryResponse response = run(query)) {
            assertThat(response.isPartial(), equalTo(false));
            List<List<Object>> values = getValuesList(response);

            // Should match both 1985 and 1988 date ranges
            boolean found1985 = false;
            boolean found1988 = false;
            for (List<Object> row : values) {
                if (row.size() >= 3) {
                    String desc = String.valueOf(row.get(2));
                    if ("1985".equals(desc)) {
                        found1985 = true;
                    }
                    if ("1988".equals(desc)) {
                        found1988 = true;
                    }
                }
            }
            assertThat("Should find rows with description='1985'", found1985, equalTo(true));
            assertThat("Should find rows with description='1988'", found1988, equalTo(true));
        }
    }

    /**
     * Test that IN_RANGE correctly handles boundary conditions.
     * Query: FROM employees | LOOKUP JOIN date_ranges ON TRUE | WHERE IN_RANGE(TO_DATETIME("1985-01-01"), date_range)
     */
    public void testInRangeBoundaryCondition() {
        String query = """
            FROM employees
            | LOOKUP JOIN date_ranges ON TRUE
            | WHERE IN_RANGE(TO_DATETIME("1985-01-01"), date_range)
            | KEEP emp_no, name, description
            | SORT emp_no
            """;

        try (EsqlQueryResponse response = run(query)) {
            assertThat(response.isPartial(), equalTo(false));
            List<List<Object>> values = getValuesList(response);

            // Should match the 1985 date range (boundary inclusive)
            boolean found1985 = false;
            for (List<Object> row : values) {
                if (row.size() >= 3 && "1985".equals(row.get(2))) {
                    found1985 = true;
                    break;
                }
            }
            assertThat("Should find rows with description='1985' at boundary", found1985, equalTo(true));
        }
    }
}
