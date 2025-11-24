/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.mapper.extras.MapperExtrasPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.esql.CsvTestsDataLoader;
import org.elasticsearch.xpack.esql.plugin.EsqlPlugin;

import java.io.IOException;
import java.net.URL;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Objects;

import static org.elasticsearch.test.ESIntegTestCase.Scope.SUITE;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;

@ClusterScope(scope = SUITE, numDataNodes = 1, numClientNodes = 0, supportsDedicatedMasters = false)
@TestLogging(
    value = "org.elasticsearch.xpack.esql:TRACE,org.elasticsearch.compute:TRACE",
    reason = "debug lookup join expression resolution"
)
public class LookupJoinGeneralExpressionIT extends AbstractEsqlIntegTestCase {

    private static final String MAIN_INDEX = "main_index";
    private static final String LOOKUP_INDEX = "lookup_index";
    private static final String MULTI_COL_MAIN_INDEX = "multi_column_joinable";
    private static final String MULTI_COL_LOOKUP_INDEX = "multi_column_joinable_lookup";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(EsqlPlugin.class, MapperExtrasPlugin.class);
    }

    private void ensureIndicesAndData() {
        assumeTrue(
            "LOOKUP JOIN ON general expressions requires capability",
            EsqlCapabilities.Cap.LOOKUP_JOIN_ON_BOOLEAN_EXPRESSION.isEnabled()
        );
        if (indexExists(MAIN_INDEX) == false) {
            createIndices();
            indexData();
        }
    }

    private void ensureMultiColumnIndicesAndData() {
        assumeTrue(
            "LOOKUP JOIN ON general expressions requires capability",
            EsqlCapabilities.Cap.LOOKUP_JOIN_ON_BOOLEAN_EXPRESSION.isEnabled()
        );
        if (indexExists(MULTI_COL_MAIN_INDEX) == false) {
            loadMultiColumnDataFromCsv();
        }
    }

    private void createIndices() {
        // Create main index
        CreateIndexRequestBuilder mainIndexRequest = prepareCreate(MAIN_INDEX).setSettings(
            Settings.builder().put("index.number_of_shards", 1).put("index.number_of_replicas", 0)
        ).setMapping("""
            {
              "properties": {
                "id": {"type": "integer"},
                "name": {"type": "keyword"},
                "value": {"type": "integer"},
                "score": {"type": "double"},
                "description": {"type": "text"},
                "category": {"type": "keyword"},
                "active": {"type": "boolean"}
              }
            }
            """);

        assertAcked(mainIndexRequest);

        // Create lookup index with lookup mode
        CreateIndexRequestBuilder lookupIndexRequest = prepareCreate(LOOKUP_INDEX).setSettings(
            Settings.builder()
                .put("index.number_of_shards", 1)
                .put("index.number_of_replicas", 0)
                .put("index.mode", IndexMode.LOOKUP.getName())
        ).setMapping("""
            {
              "properties": {
                "lookup_id": {"type": "integer"},
                "lookup_name": {"type": "keyword"},
                "lookup_value": {"type": "integer"},
                "lookup_score": {"type": "double"},
                "lookup_description": {"type": "text"},
                "lookup_category": {"type": "keyword"},
                "lookup_active": {"type": "boolean"},
                "priority": {"type": "integer"}
              }
            }
            """);

        assertAcked(lookupIndexRequest);
    }

    private void indexData() {
        // Index main index data
        client().prepareIndex(MAIN_INDEX).setId("1").setSource("""
            {"id": 1, "name": "Alice", "value": 10, "score": 85.5, "description": "test description", "category": "A", "active": true}
            """, XContentType.JSON).get();
        client().prepareIndex(MAIN_INDEX).setId("2").setSource("""
            {"id": 2, "name": "Bob", "value": 20, "score": 90.0, "description": "some text", "category": "B", "active": true}
            """, XContentType.JSON).get();
        client().prepareIndex(MAIN_INDEX).setId("3").setSource("""
            {"id": 3, "name": "Charlie", "value": 30, "score": 75.5, "description": "different text", "category": "A", "active": false}
            """, XContentType.JSON).get();
        client().prepareIndex(MAIN_INDEX).setId("4").setSource("""
            {"id": 4, "name": "David", "value": 40, "score": 95.0, "description": "description", "category": "C", "active": true}
            """, XContentType.JSON).get();

        // Index lookup index data
        client().prepareIndex(LOOKUP_INDEX).setId("1").setSource("""
            {"lookup_id": 1, "lookup_name": "Alice", "lookup_value": 10, "lookup_score": 85.5,
             "lookup_description": "test description", "lookup_category": "A",
              "lookup_active": true, "priority": 1}
            """, XContentType.JSON).get();
        client().prepareIndex(LOOKUP_INDEX).setId("2").setSource("""
            {"lookup_id": 2, "lookup_name": "Bob", "lookup_value": 20, "lookup_score": 90.0,
             "lookup_description": "another test", "lookup_category": "B", "lookup_active": true, "priority": 2}
            """, XContentType.JSON).get();
        client().prepareIndex(LOOKUP_INDEX).setId("3").setSource("""
            {"lookup_id": 3, "lookup_name": "Eve", "lookup_value": 50, "lookup_score": 80.0,
             "lookup_description": "different", "lookup_category": "A", "lookup_active": false, "priority": 3}
            """, XContentType.JSON).get();
        client().prepareIndex(LOOKUP_INDEX).setId("4").setSource("""
            {"lookup_id": 4, "lookup_name": "Frank", "lookup_value": 60, "lookup_score": 70.0,
             "lookup_description": "test", "lookup_category": "B", "lookup_active": true, "priority": 4}
            """, XContentType.JSON).get();

        refresh(MAIN_INDEX, LOOKUP_INDEX);
    }

    @Override
    protected boolean addMockHttpTransport() {
        return false;  // Need real HTTP transport for RestClient
    }

    /**
     * Loads multi-column indices and data using the existing CsvTestsDataLoader infrastructure.
     * Loads only the two datasets needed for multi-column join tests.
     */
    private void loadMultiColumnDataFromCsv() {
        try {
            RestClient restClient = getRestClient();
            org.elasticsearch.logging.Logger logger = org.elasticsearch.logging.LogManager.getLogger(CsvTestsDataLoader.class);

            // Load multi_column_joinable
            loadSingleDataset(
                restClient,
                logger,
                MULTI_COL_MAIN_INDEX,
                "mapping-multi_column_joinable.json",
                "multi_column_joinable.csv",
                Settings.builder().put("index.number_of_shards", 1).put("index.number_of_replicas", 0).build()
            );

            // Load multi_column_joinable_lookup
            Settings lookupSettings = Settings.builder()
                .put("index.number_of_shards", 1)
                .put("index.number_of_replicas", 0)
                .put("index.mode", IndexMode.LOOKUP.getName())
                .build();

            loadSingleDataset(
                restClient,
                logger,
                MULTI_COL_LOOKUP_INDEX,
                "mapping-multi_column_joinable_lookup.json",
                "multi_column_joinable_lookup.csv",
                lookupSettings
            );

            // Force refresh to make data available immediately
            refresh(MULTI_COL_MAIN_INDEX, MULTI_COL_LOOKUP_INDEX);
        } catch (IOException e) {
            throw new AssertionError("Failed to load CSV data", e);
        }
    }

    /**
     * Loads a single dataset following the CsvTestsDataLoader.load() pattern.
     * Creates the index and loads CSV data using the public loadCsvData() method.
     */
    private void loadSingleDataset(
        RestClient restClient,
        org.elasticsearch.logging.Logger logger,
        String indexName,
        String mappingFileName,
        String csvFileName,
        Settings indexSettings
    ) throws IOException {
        URL mappingResource = CsvTestsDataLoader.class.getResource("/" + mappingFileName);
        URL csvResource = CsvTestsDataLoader.class.getResource("/data/" + csvFileName);
        if (mappingResource == null || csvResource == null) {
            throw new IllegalArgumentException("Cannot find resources for " + indexName);
        }

        String mappingContent = CsvTestsDataLoader.readTextFile(mappingResource);
        ESRestTestCase.createIndex(restClient, indexName, indexSettings, mappingContent, null);

        // Use the public loadCsvData() method to reuse existing CSV parsing logic
        CsvTestsDataLoader.loadCsvData(restClient, indexName, csvResource, false, logger);
    }

    private EsqlQueryResponse runQuery(String query) {
        return run(syncEsqlQueryRequest(query));
    }

    /**
     * Verifies that the query results match the expected data.
     * @param actualValues The actual results from the query
     * @param expectedRows Expected rows, where each row is an array of expected values in column order
     */
    private void verifyResults(List<List<Object>> actualValues, Object[]... expectedRows) {
        if (actualValues.size() != expectedRows.length) {
            fail(
                String.format(
                    Locale.ROOT,
                    "Result count mismatch.%nExpected: %d rows%nActual: %d rows%n%nExpected results:%n%s%n%nActual results:%n%s",
                    expectedRows.length,
                    actualValues.size(),
                    formatRows(expectedRows),
                    formatRows(actualValues)
                )
            );
        }
        for (int i = 0; i < expectedRows.length; i++) {
            List<Object> actualRow = actualValues.get(i);
            Object[] expectedRow = expectedRows[i];
            if (actualRow.size() != expectedRow.length) {
                fail(
                    String.format(
                        Locale.ROOT,
                        "Row %d column count mismatch.%nExpected: %d columns%nActual: %d "
                            + "columns%n%nExpected row %d: %s%nActual row %d: %s%n%nAll "
                            + "expected results:%n%s%n%nAll actual results:%n%s",
                        i,
                        expectedRow.length,
                        actualRow.size(),
                        i,
                        formatRow(expectedRow),
                        i,
                        formatRow(actualRow),
                        formatRows(expectedRows),
                        formatRows(actualValues)
                    )
                );
            }
            for (int j = 0; j < expectedRow.length; j++) {
                if (Objects.equals(actualRow.get(j), expectedRow[j]) == false) {
                    fail(
                        String.format(
                            Locale.ROOT,
                            "Row %d, column %d mismatch.%nExpected: %s%nActual: %s%n%n"
                                + "Expected row %d: %s%nActual row %d: %s%n%nAll expected results:"
                                + "%n%s%n%nAll actual results:%n%s",
                            i,
                            j,
                            expectedRow[j],
                            actualRow.get(j),
                            i,
                            formatRow(expectedRow),
                            i,
                            formatRow(actualRow),
                            formatRows(expectedRows),
                            formatRows(actualValues)
                        )
                    );
                }
            }
        }
    }

    private String formatRows(Object[]... rows) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < rows.length; i++) {
            sb.append("Row ").append(i).append(": ").append(formatRow(rows[i])).append("\n");
        }
        return sb.toString();
    }

    private String formatRows(List<List<Object>> rows) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < rows.size(); i++) {
            sb.append("Row ").append(i).append(": ").append(formatRow(rows.get(i))).append("\n");
        }
        return sb.toString();
    }

    private String formatRow(Object[] row) {
        return java.util.Arrays.toString(row);
    }

    private String formatRow(List<Object> row) {
        return row.toString();
    }

    // Test cases for general expressions in LOOKUP JOIN ON conditions

    public void testFilterOnRightSideLucenePushableMatch() {
        ensureIndicesAndData();
        String query = String.format(Locale.ROOT, """
            FROM %s
            | LOOKUP JOIN %s ON id == lookup_id AND lookup_name == "Alice"
            | WHERE lookup_id IS NOT NULL
            | KEEP id, name, lookup_name, lookup_value
            | SORT id
            """, MAIN_INDEX, LOOKUP_INDEX);

        try (EsqlQueryResponse response = runQuery(query)) {
            List<List<Object>> values = getValuesList(response);
            verifyResults(
                values,
                new Object[] { 1, "Alice", "Alice", 10 }  // id, name, lookup_name, lookup_value
            );
        }
    }

    public void testFilterOnRightSideNonPushable() {
        ensureIndicesAndData();
        String query = String.format(Locale.ROOT, """
            FROM %s
            | LOOKUP JOIN %s ON id == lookup_id AND ABS(lookup_value) > 15
            | WHERE lookup_id IS NOT NULL
            | KEEP id, name, lookup_name, lookup_value
            | SORT id, lookup_name, lookup_value
            """, MAIN_INDEX, LOOKUP_INDEX);

        try (EsqlQueryResponse response = runQuery(query)) {
            List<List<Object>> values = getValuesList(response);
            // Expected: id=2,3,4 (ABS(lookup_value) > 15); id=1 excluded (ABS(10) <= 15)
            verifyResults(
                values,
                new Object[] { 2, "Bob", "Bob", 20 },      // id, name, lookup_name, lookup_value
                new Object[] { 3, "Charlie", "Eve", 50 },
                new Object[] { 4, "David", "Frank", 60 }
            );
        }
    }

    public void testFilterOnLeftSideLucenePushableRange() {
        ensureIndicesAndData();
        String query = String.format(Locale.ROOT, """
            FROM %s
            | LOOKUP JOIN %s ON id == lookup_id AND name == lookup_name AND value >= 20
            | WHERE lookup_id IS NOT NULL
            | KEEP id, name, value, lookup_name, lookup_value
            | SORT id
            """, MAIN_INDEX, LOOKUP_INDEX);

        try (EsqlQueryResponse response = runQuery(query)) {
            List<List<Object>> values = getValuesList(response);
            // Expected: id=2 (value>=20 and name matches); id=3,4 excluded (name doesn't match)
            verifyResults(
                values,
                new Object[] { 2, "Bob", 20, "Bob", 20 }  // id, name, value, lookup_name, lookup_value
            );
        }
    }

    public void testFilterOnLeftSideNonPushable() {
        ensureIndicesAndData();
        String query = String.format(Locale.ROOT, """
            FROM %s
            | LOOKUP JOIN %s ON id == lookup_id AND name == lookup_name AND ABS(value) > 15
            | WHERE lookup_id IS NOT NULL
            | KEEP id, name, value, lookup_name, lookup_value
            | SORT id
            """, MAIN_INDEX, LOOKUP_INDEX);

        try (EsqlQueryResponse response = runQuery(query)) {
            List<List<Object>> values = getValuesList(response);
            // The JOIN ON condition evaluates: id == lookup_id AND name == lookup_name AND ABS(value) > 15
            // For each left row, we check if it matches any right row:
            // id=1: ABS(10)=10 <= 15, condition fails → excluded
            // id=2: ABS(20)=20 > 15, id==lookup_id(2), name=="Bob"==lookup_name → MATCH
            // id=3: ABS(30)=30 > 15, id==lookup_id(3), but name=="Charlie" != lookup_name=="Eve" → excluded
            // id=4: ABS(40)=40 > 15, id==lookup_id(4), but name=="David" != lookup_name=="Frank" → excluded
            // Expected: only id=2 matches
            verifyResults(
                values,
                new Object[] { 2, "Bob", 20, "Bob", 20 }  // id, name, value, lookup_name, lookup_value
            );
        }
    }

    public void testFilterOnBothSidesBothLucenePushable() {
        ensureIndicesAndData();
        String query = String.format(Locale.ROOT, """
            FROM %s
            | LOOKUP JOIN %s ON id == lookup_id AND lookup_category == "A" AND value >= 10 AND value <= 30
            | WHERE lookup_id IS NOT NULL
            | KEEP id, name, value, lookup_name, lookup_category
            | SORT id, value, lookup_name
            """, MAIN_INDEX, LOOKUP_INDEX);

        try (EsqlQueryResponse response = runQuery(query)) {
            List<List<Object>> values = getValuesList(response);
            // Expected: id=1,3 (value in range and lookup_category="A"); id=2 excluded (category="B"), id=4 excluded (value>30)
            verifyResults(
                values,
                new Object[] { 1, "Alice", 10, "Alice", "A" },   // id, name, value, lookup_name, lookup_category
                new Object[] { 3, "Charlie", 30, "Eve", "A" }
            );
        }
    }

    public void testFilterOnBothSidesBothNonPushable() {
        ensureIndicesAndData();
        String query = String.format(Locale.ROOT, """
            FROM %s
            | LOOKUP JOIN %s ON id == lookup_id AND ABS(lookup_value) > 15 AND ABS(value) > 15
            | WHERE lookup_id IS NOT NULL
            | KEEP id, name, value, lookup_name, lookup_value
            | SORT id, value, lookup_name, lookup_value
            """, MAIN_INDEX, LOOKUP_INDEX);

        try (EsqlQueryResponse response = runQuery(query)) {
            List<List<Object>> values = getValuesList(response);
            // Expected: id=2,3,4 (both ABS conditions > 15); id=1 excluded (ABS(10) <= 15)
            verifyResults(
                values,
                new Object[] { 2, "Bob", 20, "Bob", 20 },      // id, name, value, lookup_name, lookup_value
                new Object[] { 3, "Charlie", 30, "Eve", 50 },
                new Object[] { 4, "David", 40, "Frank", 60 }
            );
        }
    }

    public void testFilterOnBothSidesMixed() {
        ensureIndicesAndData();
        String query = String.format(Locale.ROOT, """
            FROM %s
            | LOOKUP JOIN %s ON id == lookup_id AND ABS(lookup_value) > 15 AND value >= 20
            | WHERE lookup_id IS NOT NULL
            | KEEP id, name, value, lookup_name, lookup_value
            | SORT id, value, lookup_name, lookup_value
            """, MAIN_INDEX, LOOKUP_INDEX);

        try (EsqlQueryResponse response = runQuery(query)) {
            List<List<Object>> values = getValuesList(response);
            // Expected: id=2,3,4 (value>=20 and ABS(lookup_value)>15)
            verifyResults(
                values,
                new Object[] { 2, "Bob", 20, "Bob", 20 },      // id, name, value, lookup_name, lookup_value
                new Object[] { 3, "Charlie", 30, "Eve", 50 },
                new Object[] { 4, "David", 40, "Frank", 60 }
            );
        }
    }

    public void testGeneralExpressionJoinWithRightSideFilter() {
        ensureIndicesAndData();
        String query = String.format(Locale.ROOT, """
            FROM %s
            | LOOKUP JOIN %s ON id == lookup_id AND (lookup_name == "Alice" OR lookup_name == "Bob")
            | WHERE lookup_id IS NOT NULL
            | KEEP id, name, lookup_name, lookup_value
            | SORT id, lookup_name, lookup_value
            """, MAIN_INDEX, LOOKUP_INDEX);

        try (EsqlQueryResponse response = runQuery(query)) {
            List<List<Object>> values = getValuesList(response);
            // Expected: id=1,2 (lookup_name is "Alice" or "Bob")
            verifyResults(
                values,
                new Object[] { 1, "Alice", "Alice", 10 },  // id, name, lookup_name, lookup_value
                new Object[] { 2, "Bob", "Bob", 20 }
            );
        }
    }

    public void testGeneralExpressionJoinWithLeftSideFilter() {
        ensureIndicesAndData();
        String query = String.format(Locale.ROOT, """
            FROM %s
            | LOOKUP JOIN %s ON id == lookup_id AND name == lookup_name AND ((value >= 10 AND value <= 20) OR name == "Charlie")
            | WHERE lookup_id IS NOT NULL
            | KEEP id, name, value, lookup_name, lookup_value
            | SORT id, value, lookup_name, lookup_value
            """, MAIN_INDEX, LOOKUP_INDEX);

        try (EsqlQueryResponse response = runQuery(query)) {
            List<List<Object>> values = getValuesList(response);
            // Expected: id=1,2 (value in range and name matches); id=3 excluded (name doesn't match)
            verifyResults(
                values,
                new Object[] { 1, "Alice", 10, "Alice", 10 },  // id, name, value, lookup_name, lookup_value
                new Object[] { 2, "Bob", 20, "Bob", 20 }
            );
        }
    }

    public void testGeneralExpressionJoinComplexCondition() {
        ensureIndicesAndData();
        String query = String.format(Locale.ROOT, """
            FROM %s
            | LOOKUP JOIN %s ON id == lookup_id AND (lookup_name == name OR (lookup_value == value AND lookup_category == category))
            | WHERE lookup_id IS NOT NULL
            | KEEP id, name, lookup_name, lookup_value, lookup_category
            | SORT id, lookup_name, lookup_value
            """, MAIN_INDEX, LOOKUP_INDEX);

        try (EsqlQueryResponse response = runQuery(query)) {
            List<List<Object>> values = getValuesList(response);
            // Expected: id=1,2 (lookup_name matches name); id=3,4 excluded (neither condition matches)
            verifyResults(
                values,
                new Object[] { 1, "Alice", "Alice", 10, "A" },  // id, name, lookup_name, lookup_value, lookup_category
                new Object[] { 2, "Bob", "Bob", 20, "B" }
            );
        }
    }

    public void testFilterOnRightSideNonPushableNotInOutput() {
        ensureIndicesAndData();
        // Test with a non-pushable filter on the right side (ABS(lookup_value) > 15)
        // The lookup_value field is used in the filter but NOT in KEEP, ensuring it's not selected to output
        String query = String.format(Locale.ROOT, """
            FROM %s
            | LOOKUP JOIN %s ON id == lookup_id AND ABS(lookup_value) > 15
            | WHERE lookup_id IS NOT NULL
            | KEEP id, name, lookup_name, lookup_category
            | SORT id, lookup_name, lookup_category
            """, MAIN_INDEX, LOOKUP_INDEX);

        try (EsqlQueryResponse response = runQuery(query)) {
            List<List<Object>> values = getValuesList(response);
            verifyResults(
                values,
                new Object[] { 2, "Bob", "Bob", "B" },      // id, name, lookup_name, lookup_category
                new Object[] { 3, "Charlie", "Eve", "A" },   // id=3 matches lookup_id=3, ABS(50)=50 > 15
                new Object[] { 4, "David", "Frank", "B" }   // id=4 matches lookup_id=4, ABS(60)=60 > 15
            );
        }
    }

    /*
    public void testMatchOnLeftSideOrRightSideFilter() {
        ensureIndicesAndData();
        String query = String.format(Locale.ROOT, """
            FROM %s
            | LOOKUP JOIN %s ON id == lookup_id AND (MATCH(description, "test") OR lookup_value > 20)
            | WHERE lookup_id IS NOT NULL
            | KEEP id, name, description, lookup_name, lookup_value
            | SORT id
            """, MAIN_INDEX, LOOKUP_INDEX);

        try (EsqlQueryResponse response = runQuery(query)) {
            List<List<Object>> values = getValuesList(response);
            // Expected: 3 rows match
            // id=1: MATCH("test description", "test") = true OR lookup_value=10 > 20 = false → true → JOIN succeeds (via MATCH only)
            // id=2: MATCH("some text", "test") = false OR lookup_value=20 > 20 = false → false → JOIN fails
            // id=3: MATCH("different text", "test") = false OR lookup_value=50 > 20 = true → true → JOIN succeeds (via lookup_value > 20)
            // id=4: MATCH("description", "test") = false OR lookup_value=60 > 20 = true → true → JOIN succeeds (via lookup_value > 20)
            verifyResults(
                values,
                new Object[] { 1, "Alice", "test description", "Alice", 10 },  // id, name, description, lookup_name, lookup_value
                new Object[] { 3, "Charlie", "different text", "Eve", 50 },
                new Object[] { 4, "David", "description", "Frank", 60 }
            );
        }
    }

    public void testMatchOnLeftSideOrLeftSidePushable() {
        ensureIndicesAndData();
        String query = String.format(Locale.ROOT, """
            FROM %s
            | LOOKUP JOIN %s ON id == lookup_id AND (MATCH(description, "test") OR value >= 25)
            | WHERE lookup_id IS NOT NULL
            | KEEP id, name, description, value, lookup_name, lookup_value
            | SORT id
            """, MAIN_INDEX, LOOKUP_INDEX);

        try (EsqlQueryResponse response = runQuery(query)) {
            List<List<Object>> values = getValuesList(response);
            // Expected: 3 rows match
            // id=1: MATCH("test description", "test") = true OR value=10 >= 25 = false → true → JOIN succeeds (via MATCH only)
            // id=2: MATCH("some text", "test") = false OR value=20 >= 25 = false → false → JOIN fails
            // id=3: MATCH("different text", "test") = false OR value=30 >= 25 = true → true → JOIN succeeds (via value >= 25)
            // id=4: MATCH("description", "test") = false OR value=40 >= 25 = true → true → JOIN succeeds (via value >= 25)
            verifyResults(
                values,
                new Object[] { 1, "Alice", "test description", 10, "Alice", 10 },  // id, name, description, value, lookup_name,
                                                                                   // lookup_value
                new Object[] { 3, "Charlie", "different text", 30, "Eve", 50 },
                new Object[] { 4, "David", "description", 40, "Frank", 60 }
            );
        }
    }

    public void testMatchOnLeftSideOrLeftSideNonPushable() {
        ensureIndicesAndData();
        String query = String.format(Locale.ROOT, """
            FROM %s
            | LOOKUP JOIN %s ON id == lookup_id AND (MATCH(description, "test") OR ABS(value) > 25)
            | WHERE lookup_id IS NOT NULL
            | KEEP id, name, description, value, lookup_name, lookup_value
            | SORT id
            """, MAIN_INDEX, LOOKUP_INDEX);

        try (EsqlQueryResponse response = runQuery(query)) {
            List<List<Object>> values = getValuesList(response);
            // Expected: 3 rows match
            // id=1: MATCH("test description", "test") = true OR ABS(10)=10 > 25 = false → true → JOIN succeeds (via MATCH only)
            // id=2: MATCH("some text", "test") = false OR ABS(20)=20 > 25 = false → false → JOIN fails
            // id=3: MATCH("different text", "test") = false OR ABS(30)=30 > 25 = true → true → JOIN succeeds (via ABS(value) > 25)
            // id=4: MATCH("description", "test") = false OR ABS(40)=40 > 25 = true → true → JOIN succeeds (via ABS(value) > 25)
            verifyResults(
                values,
                new Object[] { 1, "Alice", "test description", 10, "Alice", 10 },  // id, name, description, value, lookup_name,
                                                                                   // lookup_value
                new Object[] { 3, "Charlie", "different text", 30, "Eve", 50 },
                new Object[] { 4, "David", "description", 40, "Frank", 60 }
            );
        }
    } */

    public void testMatchOnRightSideOrLeftSideCondition() {
        ensureIndicesAndData();
        String query = String.format(Locale.ROOT, """
            FROM %s
            | LOOKUP JOIN %s ON id == lookup_id AND (MATCH(lookup_description, "description") OR value >= 30)
            | WHERE lookup_id IS NOT NULL
            | KEEP id, name, description, value, lookup_name, lookup_value, lookup_description
            | SORT id, value, lookup_name, lookup_value
            """, MAIN_INDEX, LOOKUP_INDEX);

        try (EsqlQueryResponse response = runQuery(query)) {
            List<List<Object>> values = getValuesList(response);
            // Expected: 3 rows match
            // id=1: MATCH("test description", "description") = true OR value=10 >= 30 = false → true → JOIN succeeds (via MATCH)
            // id=2: MATCH("another test", "description") = false OR value=20 >= 30 = false → false → JOIN fails
            // id=3: MATCH("different", "description") = false OR value=30 >= 30 = true → true → JOIN succeeds (via value >= 30)
            // id=4: MATCH("test", "description") = false OR value=40 >= 30 = true → true → JOIN succeeds (via value >= 30)
            verifyResults(
                values,
                new Object[] { 1, "Alice", "test description", 10, "Alice", 10, "test description" },  // id, name, description, value,
                                                                                                       // lookup_name, lookup_value,
                                                                                                       // lookup_description
                new Object[] { 3, "Charlie", "different text", 30, "Eve", 50, "different" },
                new Object[] { 4, "David", "description", 40, "Frank", 60, "test" }
            );
        }
    }

    public void testArithmeticExpressionInJoinCondition() {
        ensureIndicesAndData();
        String query = String.format(Locale.ROOT, """
            FROM %s
            | LOOKUP JOIN %s ON id == lookup_id + 1
            | WHERE lookup_id IS NOT NULL
            | KEEP id, name, lookup_id, lookup_name, lookup_value
            | SORT id, lookup_id, lookup_name, lookup_value
            """, MAIN_INDEX, LOOKUP_INDEX);

        try (EsqlQueryResponse response = runQuery(query)) {
            List<List<Object>> values = getValuesList(response);
            // Expected: 3 rows match
            // id=2: matches lookup_id=1 (Alice)
            // id=3: matches lookup_id=2 (Bob)
            // id=4: matches lookup_id=3 (Eve)
            verifyResults(
                values,
                new Object[] { 2, "Bob", 1, "Alice", 10 },  // id, name, lookup_id, lookup_name, lookup_value
                new Object[] { 3, "Charlie", 2, "Bob", 20 },
                new Object[] { 4, "David", 3, "Eve", 50 }
            );
        }
    }

    public void testMultiColMixedGtEqSwapLeftRight() {
        ensureMultiColumnIndicesAndData();
        // Test similar to lookupMultiColMixedGtEqSwapLeftRight from CSV spec
        // Uses swapped comparison: id_int < left_id (right field < left field) AND is_active_left == is_active_bool
        String query = String.format(Locale.ROOT, """
            FROM %s
            | RENAME id_int AS left_id, name_str AS left_name, is_active_bool AS is_active_left
            | LOOKUP JOIN %s ON id_int < left_id AND is_active_left == is_active_bool
            | KEEP left_id, left_name, id_int, name_str, is_active_left, is_active_bool
            | SORT left_id, left_name, id_int, name_str, is_active_left, is_active_bool
            | LIMIT 20
            """, MULTI_COL_MAIN_INDEX, MULTI_COL_LOOKUP_INDEX);

        try (EsqlQueryResponse response = runQuery(query)) {
            List<List<Object>> values = getValuesList(response);
            verifyResults(
                values,
                new Object[] { 1, "Alice", null, null, true, null },  // left_id=1: no matches (id_int < 1 means no id_int exists)
                new Object[] { List.of(1, 19, 21), "Sophia", null, null, true, null },  // left_id=[1,19,21]: no matches
                new Object[] { 2, "Bob", null, null, false, null },  // left_id=2: no matches (id_int=1 has is_active_bool=true !=
                                                                     // is_active_left=false)
                new Object[] { 3, "Charlie", 1, "Alice", true, true },  // left_id=3: matches id_int=1 (duplicate - first occurrence)
                new Object[] { 3, "Charlie", 1, "Alice", true, true },  // left_id=3: matches id_int=1 (duplicate - second occurrence)
                new Object[] { 4, "David", 2, "Bob", false, false },  // left_id=4: matches id_int=2
                new Object[] { 4, "David", 3, "Charlie", false, false },  // left_id=4: matches id_int=3 with is_active_bool=false
                new Object[] { 5, "Eve", 1, "Alice", true, true },  // left_id=5: matches id_int=1 (duplicate - first occurrence)
                new Object[] { 5, "Eve", 1, "Alice", true, true },  // left_id=5: matches id_int=1 (duplicate - second occurrence)
                new Object[] { 5, "Eve", 3, "Charlie", true, true },  // left_id=5: matches id_int=3 with is_active_bool=true
                new Object[] { 6, null, 1, "Alice", true, true },  // left_id=6: matches id_int=1 (duplicate - first occurrence)
                new Object[] { 6, null, 1, "Alice", true, true },  // left_id=6: matches id_int=1 (duplicate - second occurrence)
                new Object[] { 6, null, 3, "Charlie", true, true },  // left_id=6: matches id_int=3 with is_active_bool=true
                new Object[] { 6, null, 5, "Eve", true, true },  // left_id=6: matches id_int=5 (duplicate - first occurrence)
                new Object[] { 6, null, 5, "Eve", true, true },  // left_id=6: matches id_int=5 (duplicate - second occurrence)
                new Object[] { 7, "Grace", 2, "Bob", false, false },  // left_id=7: matches id_int=2
                new Object[] { 7, "Grace", 3, "Charlie", false, false },  // left_id=7: matches id_int=3 with is_active_bool=false
                new Object[] { 7, "Grace", 4, "David", false, false },  // left_id=7: matches id_int=4
                new Object[] { 8, "Hank", 1, "Alice", true, true },  // left_id=8: matches id_int=1 (duplicate - first occurrence)
                new Object[] { 8, "Hank", 1, "Alice", true, true }  // left_id=8: matches id_int=1 (duplicate - second occurrence)
                // Note: left_id=8 also matches id_int=3 and id_int=5, but the LIMIT 20 cuts off some results
            );
        }
    }

    public void testComplexExpressionRelatingLeftAndRight() {
        ensureIndicesAndData();
        // Test with complex expression relating left and right: (id + lookup_value) == (value + lookup_id)
        // This creates a relationship between left and right fields
        String query = String.format(Locale.ROOT, """
            FROM %s
            | LOOKUP JOIN %s ON (id + lookup_value) == (value + lookup_id)
            | WHERE lookup_id IS NOT NULL
            | KEEP id, name, value, lookup_id, lookup_name, lookup_value
            | SORT id, value, lookup_id, lookup_value
            """, MAIN_INDEX, LOOKUP_INDEX);

        try (EsqlQueryResponse response = runQuery(query)) {
            List<List<Object>> values = getValuesList(response);
            // Check: (id + lookup_value) == (value + lookup_id)
            // id=1: (1 + 10) == (10 + 1) → 11 == 11 → MATCH
            // id=2: (2 + 20) == (20 + 2) → 22 == 22 → MATCH
            // id=3: (3 + 50) == (30 + 3) → 53 == 33 → NO MATCH
            // id=4: (4 + 60) == (40 + 4) → 64 == 44 → NO MATCH
            verifyResults(values, new Object[] { 1, "Alice", 10, 1, "Alice", 10 }, new Object[] { 2, "Bob", 20, 2, "Bob", 20 });
        }
    }

    public void testNoRelationshipBetweenLeftAndRight() {
        ensureIndicesAndData();
        String query = String.format(Locale.ROOT, """
            FROM %s
            | LOOKUP JOIN %s ON value > 15 AND lookup_value > 15
            | WHERE lookup_id IS NOT NULL
            | KEEP id, name, value, lookup_id, lookup_name, lookup_value
            | SORT id, lookup_id
            """, MAIN_INDEX, LOOKUP_INDEX);

        try (EsqlQueryResponse response = runQuery(query)) {
            List<List<Object>> values = getValuesList(response);
            verifyResults(
                values,
                new Object[] { 2, "Bob", 20, 2, "Bob", 20 },
                new Object[] { 2, "Bob", 20, 3, "Eve", 50 },
                new Object[] { 2, "Bob", 20, 4, "Frank", 60 },
                new Object[] { 3, "Charlie", 30, 2, "Bob", 20 },
                new Object[] { 3, "Charlie", 30, 3, "Eve", 50 },
                new Object[] { 3, "Charlie", 30, 4, "Frank", 60 },
                new Object[] { 4, "David", 40, 2, "Bob", 20 },
                new Object[] { 4, "David", 40, 3, "Eve", 50 },
                new Object[] { 4, "David", 40, 4, "Frank", 60 }
            );
        }
    }

    public void testFilterOnLeftWithLimit() {
        ensureIndicesAndData();
        String query = String.format(Locale.ROOT, """
            FROM %s
            | LOOKUP JOIN %s ON id == lookup_id AND value >= 20
            | WHERE lookup_id IS NOT NULL
            | KEEP id, name, value, lookup_name, lookup_value
            | SORT id
            | LIMIT 2
            """, MAIN_INDEX, LOOKUP_INDEX);

        try (EsqlQueryResponse response = runQuery(query)) {
            List<List<Object>> values = getValuesList(response);
            // Expected: id=2,3 (value >= 20), limited to 2 rows
            verifyResults(values, new Object[] { 2, "Bob", 20, "Bob", 20 }, new Object[] { 3, "Charlie", 30, "Eve", 50 });
        }
    }

    public void testFilterOnRightWithLimit() {
        ensureIndicesAndData();
        // Test filter on right side with LIMIT
        // Filter: lookup_value >= 20
        // Limit: 2
        String query = String.format(Locale.ROOT, """
            FROM %s
            | LOOKUP JOIN %s ON id == lookup_id AND lookup_value >= 20
            | WHERE lookup_id IS NOT NULL
            | KEEP id, name, value, lookup_name, lookup_value
            | SORT id
            | LIMIT 2
            """, MAIN_INDEX, LOOKUP_INDEX);

        try (EsqlQueryResponse response = runQuery(query)) {
            List<List<Object>> values = getValuesList(response);
            // Expected: id=2,3,4 (lookup_value >= 20), limited to 2 rows
            verifyResults(values, new Object[] { 2, "Bob", 20, "Bob", 20 }, new Object[] { 3, "Charlie", 30, "Eve", 50 });
        }
    }

    public void testFilterOnLeftAndRightWithAnd() {
        ensureIndicesAndData();
        String query = String.format(Locale.ROOT, """
            FROM %s
            | LOOKUP JOIN %s ON id == lookup_id AND value >= 20 AND lookup_value >= 20
            | WHERE lookup_id IS NOT NULL
            | KEEP id, name, value, lookup_name, lookup_value
            | SORT id, value, lookup_name, lookup_value
            """, MAIN_INDEX, LOOKUP_INDEX);

        try (EsqlQueryResponse response = runQuery(query)) {
            List<List<Object>> values = getValuesList(response);
            // Expected: id=2,3,4 (both value >= 20 AND lookup_value >= 20)
            verifyResults(
                values,
                new Object[] { 2, "Bob", 20, "Bob", 20 },
                new Object[] { 3, "Charlie", 30, "Eve", 50 },
                new Object[] { 4, "David", 40, "Frank", 60 }
            );
        }
    }

    public void testFilterOnLeftAndRightWithOr() {
        ensureIndicesAndData();
        String query = String.format(Locale.ROOT, """
            FROM %s
            | LOOKUP JOIN %s ON id == lookup_id AND (value >= 30 OR lookup_value >= 50)
            | WHERE lookup_id IS NOT NULL
            | KEEP id, name, value, lookup_name, lookup_value
            | SORT id, value, lookup_name, lookup_value
            """, MAIN_INDEX, LOOKUP_INDEX);

        try (EsqlQueryResponse response = runQuery(query)) {
            List<List<Object>> values = getValuesList(response);
            // Expected:
            // id=1: value=10 < 30 AND lookup_value=10 < 50 → NO MATCH
            // id=2: value=20 < 30 AND lookup_value=20 < 50 → NO MATCH
            // id=3: value=30 >= 30 OR lookup_value=50 >= 50 → MATCH
            // id=4: value=40 >= 30 OR lookup_value=60 >= 50 → MATCH
            verifyResults(values, new Object[] { 3, "Charlie", 30, "Eve", 50 }, new Object[] { 4, "David", 40, "Frank", 60 });
        }
    }

    public void testFilterOnLeftAndRightComplexAndOr() {
        ensureIndicesAndData();
        String query = String.format(Locale.ROOT, """
            FROM %s
            | LOOKUP JOIN %s ON id == lookup_id AND ((value >= 20 AND lookup_value >= 20) OR (value < 15 AND lookup_value < 15))
            | WHERE lookup_id IS NOT NULL
            | KEEP id, name, value, lookup_name, lookup_value
            | SORT id, value, lookup_name, lookup_value
            """, MAIN_INDEX, LOOKUP_INDEX);

        try (EsqlQueryResponse response = runQuery(query)) {
            List<List<Object>> values = getValuesList(response);
            // Expected:
            // id=1: value=10 < 15 AND lookup_value=10 < 15 → MATCH (second OR condition)
            // id=2: value=20 >= 20 AND lookup_value=20 >= 20 → MATCH (first OR condition)
            // id=3: value=30 >= 20 AND lookup_value=50 >= 20 → MATCH (first OR condition)
            // id=4: value=40 >= 20 AND lookup_value=60 >= 20 → MATCH (first OR condition)
            verifyResults(
                values,
                new Object[] { 1, "Alice", 10, "Alice", 10 },
                new Object[] { 2, "Bob", 20, "Bob", 20 },
                new Object[] { 3, "Charlie", 30, "Eve", 50 },
                new Object[] { 4, "David", 40, "Frank", 60 }
            );
        }
    }

    public void testFilterOnConditionAndTrue() {
        ensureIndicesAndData();
        // Test with just TRUE as filter - should match all rows
        String query = String.format(Locale.ROOT, """
            FROM %s
            | LOOKUP JOIN %s ON id == lookup_id AND TRUE
            | WHERE lookup_id IS NOT NULL
            | KEEP id, name, value, lookup_name, lookup_value
            | SORT id, value, lookup_name, lookup_value
            """, MAIN_INDEX, LOOKUP_INDEX);

        try (EsqlQueryResponse response = runQuery(query)) {
            List<List<Object>> values = getValuesList(response);
            // Expected: All rows where id == lookup_id (TRUE always evaluates to true)
            verifyResults(
                values,
                new Object[] { 1, "Alice", 10, "Alice", 10 },
                new Object[] { 2, "Bob", 20, "Bob", 20 },
                new Object[] { 3, "Charlie", 30, "Eve", 50 },
                new Object[] { 4, "David", 40, "Frank", 60 }
            );
        }
    }

    public void testComplexExpressionWithArithmeticAndComparison() {
        ensureIndicesAndData();
        String query = String.format(Locale.ROOT, """
            FROM %s
            | LOOKUP JOIN %s ON (id * 2) + lookup_value == value + lookup_id
            | WHERE lookup_id IS NOT NULL
            | KEEP id, name, value, lookup_id, lookup_name, lookup_value
            | SORT id
            """, MAIN_INDEX, LOOKUP_INDEX);

        try (EsqlQueryResponse response = runQuery(query)) {
            List<List<Object>> values = getValuesList(response);
            // Check: (id * 2) + lookup_value == value + lookup_id
            // id=1: (1*2) + 10 == 10 + 1 → 2 + 10 == 11 → 12 == 11 → NO MATCH
            // id=2: (2*2) + 20 == 20 + 2 → 4 + 20 == 22 → 24 == 22 → NO MATCH
            // id=3: (3*2) + 50 == 30 + 3 → 6 + 50 == 33 → 56 == 33 → NO MATCH
            // id=4: (4*2) + 60 == 40 + 4 → 8 + 60 == 44 → 68 == 44 → NO MATCH
            // No matches expected
            verifyResults(values);
        }
    }

    public void testComplexExpressionWithMultipleOperations() {
        ensureIndicesAndData();
        String query = String.format(Locale.ROOT, """
            FROM %s
            | LOOKUP JOIN %s ON ABS(value - lookup_value) < 25
            | KEEP id, name, value, lookup_name, lookup_value
            | SORT id, value, lookup_name, lookup_value
            """, MAIN_INDEX, LOOKUP_INDEX);

        try (EsqlQueryResponse response = runQuery(query)) {
            List<List<Object>> values = getValuesList(response);
            // Results sorted by id, value, lookup_name, lookup_value for consistent ordering
            verifyResults(
                values,
                new Object[] { 1, "Alice", 10, "Alice", 10 },
                new Object[] { 1, "Alice", 10, "Bob", 20 },
                new Object[] { 2, "Bob", 20, "Alice", 10 },
                new Object[] { 2, "Bob", 20, "Bob", 20 },
                new Object[] { 3, "Charlie", 30, "Alice", 10 },
                new Object[] { 3, "Charlie", 30, "Bob", 20 },
                new Object[] { 3, "Charlie", 30, "Eve", 50 },
                new Object[] { 4, "David", 40, "Bob", 20 },
                new Object[] { 4, "David", 40, "Eve", 50 },
                new Object[] { 4, "David", 40, "Frank", 60 }
            );
        }
    }

    public void testComplexExpressionRelatingLeftAndRightMultiColumn() {
        ensureMultiColumnIndicesAndData();
        String query = String.format(Locale.ROOT, """
            FROM %s
            | RENAME id_int AS id_left, name_str AS name_left
            | LOOKUP JOIN %s ON (id_left + id_int) == (id_left * 2)
            | KEEP id_left, name_left, id_int, name_str
            | SORT id_left, name_left, id_int, name_str
            | LIMIT 20
            """, MULTI_COL_MAIN_INDEX, MULTI_COL_LOOKUP_INDEX);

        try (EsqlQueryResponse response = runQuery(query)) {
            List<List<Object>> values = getValuesList(response);
            verifyResults(
                values,
                new Object[] { 1, "Alice", 1, "Alice" },  // id_left=1 matches id_int=1 (first occurrence)
                new Object[] { 1, "Alice", 1, "Alice" },  // id_left=1 matches id_int=1 (second occurrence - duplicate)
                new Object[] { List.of(1, 19, 21), "Sophia", null, null },  // id_left=[1,19,21] doesn't match
                new Object[] { 2, "Bob", 2, "Bob" },
                new Object[] { 3, "Charlie", 3, "Charlie" },  // id_left=3 matches id_int=3 (first occurrence)
                new Object[] { 3, "Charlie", 3, "Charlie" },  // id_left=3 matches id_int=3 (second occurrence)
                new Object[] { 4, "David", 4, "David" },
                new Object[] { 5, "Eve", 5, "Eve" },  // id_left=5 matches id_int=5 (first occurrence)
                new Object[] { 5, "Eve", 5, "Eve" },  // id_left=5 matches id_int=5 (second occurrence - duplicate)
                new Object[] { 6, null, 6, null },  // id_left=6 matches id_int=6 (empty name in lookup)
                new Object[] { 7, "Grace", 7, "Grace" },
                new Object[] { 8, "Hank", 8, "Hank" },
                new Object[] { 9, "Ivy", null, null },  // id_left=9 doesn't match
                new Object[] { 10, "John", null, null },  // id_left=10 doesn't match
                new Object[] { 12, "Liam", 12, "Liam" },  // id_left=12 matches id_int=12
                new Object[] { 13, "Mia", 13, "Mia" },  // id_left=13 matches id_int=13
                new Object[] { 14, "Nina", 14, "Nina" },  // id_left=14 matches id_int=14 (CSV has id_int=14, not [14])
                new Object[] { 15, "Oscar", null, null },  // id_left=15 doesn't match
                new Object[] { List.of(17, 18), "Olivia", null, null },  // id_left=[17,18] doesn't match
                new Object[] { null, "Kate", null, null }  // id_left=null doesn't match
            );
        }
    }
}
