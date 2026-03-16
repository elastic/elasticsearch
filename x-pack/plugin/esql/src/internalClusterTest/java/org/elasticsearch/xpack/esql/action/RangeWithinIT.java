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
import org.junit.Before;

import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.junit.Assume.assumeTrue;

/**
 * Integration tests for RANGE_WITHIN (date ranges).
 * Covers various layouts: date/range from main index and from joined indices.
 */
public class RangeWithinIT extends AbstractEsqlIntegTestCase {

    @Before
    public void setupIndices() {
        assumeTrue("requires RANGE_WITHIN capability", EsqlCapabilities.Cap.RANGE_WITHIN.isEnabled());
        // Create main index with date fields and join_key for LOOKUP JOIN (same value so every row joins every lookup row)
        assertAcked(
            client().admin()
                .indices()
                .prepareCreate("employees")
                .setMapping(
                    Map.of(
                        "properties",
                        Map.of(
                            "emp_no",
                            Map.of("type", "integer"),
                            "hire_date",
                            Map.of("type", "date"),
                            "name",
                            Map.of("type", "keyword"),
                            "join_key",
                            Map.of("type", "integer")
                        )
                    )
                )
        );

        // Index some employee documents with different hire dates (join_key=1 so all match all lookup rows)
        indexRandom(
            true,
            false,
            prepareIndex("employees").setId("1")
                .setSource(Map.of("emp_no", 10001, "hire_date", "1985-01-15T00:00:00Z", "name", "Alice", "join_key", 1)),
            prepareIndex("employees").setId("2")
                .setSource(Map.of("emp_no", 10002, "hire_date", "1986-06-20T00:00:00Z", "name", "Bob", "join_key", 1)),
            prepareIndex("employees").setId("3")
                .setSource(Map.of("emp_no", 10003, "hire_date", "1987-12-10T00:00:00Z", "name", "Charlie", "join_key", 1)),
            prepareIndex("employees").setId("4")
                .setSource(Map.of("emp_no", 10004, "hire_date", "1988-03-25T00:00:00Z", "name", "Diana", "join_key", 1))
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
                            Map.of("type", "keyword"),
                            "join_key",
                            Map.of("type", "integer")
                        )
                    )
                )
        );

        // Index date range documents (join_key=1 so all match all employee rows)
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
                        "1985",
                        "join_key",
                        1
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
                        "1986",
                        "join_key",
                        1
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
                        "1987",
                        "join_key",
                        1
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
                        "1988",
                        "join_key",
                        1
                    )
                )
        );

        // Main index with date_range (for "date from LOOKUP, range from FROM" test)
        assertAcked(
            client().admin()
                .indices()
                .prepareCreate("decades_main")
                .setMapping(
                    Map.of(
                        "properties",
                        Map.of(
                            "date_range",
                            Map.of("type", "date_range"),
                            "decade",
                            Map.of("type", "keyword"),
                            "join_key",
                            Map.of("type", "integer")
                        )
                    )
                )
        );
        indexRandom(
            true,
            false,
            prepareIndex("decades_main").setId("1")
                .setSource(
                    Map.of(
                        "date_range",
                        Map.of("gte", "1985-01-01T00:00:00Z", "lt", "1990-01-01T00:00:00Z"),
                        "decade",
                        "1980s",
                        "join_key",
                        1
                    )
                )
        );

        // Lookup index with a date field (for "date from LOOKUP, range from FROM" test)
        assertAcked(
            client().admin()
                .indices()
                .prepareCreate("lookup_with_date")
                .setSettings(
                    Settings.builder()
                        .put(IndexSettings.MODE.getKey(), IndexMode.LOOKUP)
                        .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                        .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                )
                .setMapping(Map.of("properties", Map.of("some_date", Map.of("type", "date"), "join_key", Map.of("type", "integer"))))
        );
        indexRandom(
            true,
            false,
            prepareIndex("lookup_with_date").setId("1").setSource(Map.of("some_date", "1985-06-15T00:00:00Z", "join_key", 1))
        );
    }

    /**
     * Test RANGE_WITHIN in WHERE clause after LOOKUP JOIN.
     * Query: FROM employees | LOOKUP JOIN date_ranges ON join_key | WHERE RANGE_WITHIN(TO_DATETIME("1985-06-15"), date_range)
     */
    public void testRangeContainsInWhereClauseAfterLookupJoin() {
        String query = """
            FROM employees
            | LOOKUP JOIN date_ranges ON join_key
            | WHERE RANGE_WITHIN(TO_DATETIME("1985-06-15"), date_range)
            | KEEP emp_no, name, description
            | SORT emp_no
            """;

        try (EsqlQueryResponse response = run(query)) {
            assertThat(response.isPartial(), equalTo(false));

            List<List<Object>> values = getValuesList(response);
            assertThat(values.size(), greaterThanOrEqualTo(1));

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
     * Test RANGE_WITHIN in WHERE clause filtering on constant date with lookup range field.
     * Query: FROM employees | LOOKUP JOIN date_ranges ON join_key | WHERE RANGE_WITHIN(TO_DATETIME("1986-06-15"), date_range)
     */
    public void testRangeContainsConstantDateWithLookupRangeField() {
        String query = """
            FROM employees
            | LOOKUP JOIN date_ranges ON join_key
            | WHERE RANGE_WITHIN(TO_DATETIME("1986-06-15"), date_range)
            | KEEP emp_no, name, description
            | SORT emp_no
            """;

        try (EsqlQueryResponse response = run(query)) {
            assertThat(response.isPartial(), equalTo(false));
            List<List<Object>> values = getValuesList(response);

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
     * Test RANGE_WITHIN in WHERE clause after LOOKUP JOIN with constant date.
     * Query: FROM employees | LOOKUP JOIN date_ranges ON join_key | WHERE RANGE_WITHIN(TO_DATETIME("1987-06-15"), date_range)
     */
    public void testRangeContainsInLookupJoinWhere() {
        String query = """
            FROM employees
            | LOOKUP JOIN date_ranges ON join_key
            | WHERE RANGE_WITHIN(TO_DATETIME("1987-06-15"), date_range)
            | KEEP emp_no, name, description
            | SORT emp_no
            """;

        try (EsqlQueryResponse response = run(query)) {
            assertThat(response.isPartial(), equalTo(false));
            List<List<Object>> values = getValuesList(response);

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
     * Test RANGE_WITHIN with date from left side in WHERE clause after LOOKUP JOIN.
     * Query: FROM employees | LOOKUP JOIN date_ranges ON join_key
     * | WHERE RANGE_WITHIN(hire_date, TO_DATE_RANGE("1986-01-01..1987-01-01"))
     */
    public void testRangeContainsLeftSideDateInWhereAfterJoin() {
        String query = """
            FROM employees
            | LOOKUP JOIN date_ranges ON join_key
            | WHERE RANGE_WITHIN(hire_date, TO_DATE_RANGE("1986-01-01T00:00:00Z..1987-01-01T00:00:00Z"))
            | KEEP emp_no, name, hire_date, description
            | SORT emp_no
            """;

        try (EsqlQueryResponse response = run(query)) {
            assertThat(response.isPartial(), equalTo(false));
            List<List<Object>> values = getValuesList(response);

            assertThat("Should find employees hired in 1986", values.size(), greaterThanOrEqualTo(1));
        }
    }

    /**
     * Test RANGE_WITHIN with date from FROM index and date_range from LOOKUP.
     * Query: FROM employees | LOOKUP JOIN date_ranges ON join_key | WHERE RANGE_WITHIN(hire_date, date_range)
     */
    public void testRangeWithinDateFromFromRangeFromLookup() {
        String query = """
            FROM employees
            | LOOKUP JOIN date_ranges ON join_key
            | WHERE RANGE_WITHIN(hire_date, date_range)
            | KEEP emp_no, hire_date, description
            | SORT emp_no
            """;

        try (EsqlQueryResponse response = run(query)) {
            assertThat(response.isPartial(), equalTo(false));
            List<List<Object>> values = getValuesList(response);
            assertThat("Should find rows where employee hire_date is within joined date_range", values.size(), greaterThanOrEqualTo(1));
        }
    }

    /**
     * Test RANGE_WITHIN with date from LOOKUP and date_range from FROM index.
     * Query: FROM decades_main | LOOKUP JOIN lookup_with_date ON join_key | WHERE RANGE_WITHIN(some_date, date_range)
     */
    public void testRangeWithinDateFromLookupRangeFromFrom() {
        String query = """
            FROM decades_main
            | LOOKUP JOIN lookup_with_date ON join_key
            | WHERE RANGE_WITHIN(some_date, date_range)
            | KEEP date_range, some_date
            """;

        try (EsqlQueryResponse response = run(query)) {
            assertThat(response.isPartial(), equalTo(false));
            List<List<Object>> values = getValuesList(response);
            assertThat("Should find row where lookup some_date is within FROM date_range", values.size(), equalTo(1));
        }
    }

    /**
     * Test RANGE_WITHIN with multiple date ranges in WHERE clause.
     * Query: FROM employees | LOOKUP JOIN date_ranges ON join_key | WHERE RANGE_WITHIN(TO_DATETIME("1985-06-15"), date_range)
     * OR RANGE_WITHIN(TO_DATETIME("1988-06-15"), date_range)
     */
    public void testRangeContainsMultipleRangesInWhere() {
        String query = """
            FROM employees
            | LOOKUP JOIN date_ranges ON join_key
            | WHERE RANGE_WITHIN(TO_DATETIME("1985-06-15"), date_range) OR RANGE_WITHIN(TO_DATETIME("1988-06-15"), date_range)
            | KEEP emp_no, name, description
            | SORT emp_no
            """;

        try (EsqlQueryResponse response = run(query)) {
            assertThat(response.isPartial(), equalTo(false));
            List<List<Object>> values = getValuesList(response);

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
     * Test that RANGE_WITHIN correctly handles boundary conditions.
     * Query: FROM employees | LOOKUP JOIN date_ranges ON join_key | WHERE RANGE_WITHIN(TO_DATETIME("1985-01-01"), date_range)
     */
    public void testRangeContainsBoundaryCondition() {
        String query = """
            FROM employees
            | LOOKUP JOIN date_ranges ON join_key
            | WHERE RANGE_WITHIN(TO_DATETIME("1985-01-01"), date_range)
            | KEEP emp_no, name, description
            | SORT emp_no
            """;

        try (EsqlQueryResponse response = run(query)) {
            assertThat(response.isPartial(), equalTo(false));
            List<List<Object>> values = getValuesList(response);

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
