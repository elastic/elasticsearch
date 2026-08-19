/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.single_node;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.elasticsearch.Build;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.test.TestClustersThreadFilter;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.esql.EsqlStreamTestUtils;
import org.elasticsearch.xpack.esql.qa.rest.EsqlSpecTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlStreamTestUtils.parseNdjson;
import static org.elasticsearch.xpack.esql.EsqlStreamTestUtils.tolerateDefaultLimitWarning;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.startsWith;

@ThreadLeakFilters(filters = TestClustersThreadFilter.class)
public class EsqlStreamQueryIT extends ESRestTestCase {

    @ClassRule
    public static ElasticsearchCluster cluster = Clusters.testCluster();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @Before
    @After
    public void assertRequestBreakerEmpty() throws Exception {
        EsqlSpecTestCase.assertRequestBreakerEmpty();
    }

    private static final String INDEX = "stream-test";
    private static final int AGREEMENT_PAGE_SIZE = 2;

    @Before
    public void initIndex() throws IOException {
        Request createIndex = new Request("PUT", "/" + INDEX);
        createIndex.setJsonEntity("""
            {
              "mappings": {
                "properties": {
                  "value":        { "type": "integer" },
                  "description":  { "type": "keyword" },
                  "sparse_field": { "type": "keyword" },
                  "noidx_field":  { "type": "keyword", "index": false, "doc_values": false },
                  "agg_field":    { "type": "aggregate_metric_double", "metrics": ["min", "max", "sum", "value_count"] }
                }
              }
            }
            """);
        assertOK(client().performRequest(createIndex));

        Request bulk = new Request("POST", "/_bulk?index=" + INDEX + "&refresh=true");
        bulk.setJsonEntity("""
            {"index": {"_id": "1"}}
            {"value": 1, "description": "number one", "noidx_field": "a", \
            "agg_field": {"min": 1.0, "max": 3.0, "sum": 10.0, "value_count": 5}}
            {"index": {"_id": "2"}}
            {"value": 2, "description": "number two", "noidx_field": "b", \
            "agg_field": {"min": 2.0, "max": 6.0, "sum": 20.0, "value_count": 6}}
            {"index": {"_id": "3"}}
            {"value": 3, "noidx_field": "c", \
            "agg_field": {"min": 3.0, "max": 9.0, "sum": 30.0, "value_count": 7}}
            {"index": {"_id": "4"}}
            {"value": 4, "description": "number four", "noidx_field": "d", \
            "agg_field": {"min": 4.0, "max": 12.0, "sum": 40.0, "value_count": 8}}
            """);
        assertOK(client().performRequest(bulk));
    }

    public void testFramingAndFooter() throws IOException {
        List<Map<String, Object>> lines = stream("""
            {"query": "FROM stream-test | SORT value | LIMIT 100 | KEEP value", "page_size": 1}
            """);

        Map<String, Object> columnsLine = lines.get(0);
        assertThat(columnsLine, hasKey("columns"));
        assertThat(columnsLine, not(hasKey("values")));
        assertThat(columnsLine, not(hasKey("took")));
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> columns = (List<Map<String, Object>>) columnsLine.get("columns");
        assertThat(columns, hasSize(1));
        assertThat(columns.get(0).get("name"), equalTo("value"));
        assertThat(columns.get(0).get("type"), equalTo("integer"));

        List<Map<String, Object>> valueLines = lines.subList(1, lines.size() - 1);
        assertThat("expected multiple page lines with page_size=1", valueLines.size(), greaterThan(1));
        int totalRows = 0;
        for (Map<String, Object> valueLine : valueLines) {
            assertThat(valueLine, hasKey("values"));
            assertThat(valueLine, not(hasKey("columns")));
            assertThat(valueLine, not(hasKey("took")));
            @SuppressWarnings("unchecked")
            List<List<Object>> rows = (List<List<Object>>) valueLine.get("values");
            totalRows += rows.size();
        }
        assertThat("total row count across all pages should equal doc count", totalRows, equalTo(4));

        Map<String, Object> footer = lines.get(lines.size() - 1);
        assertThat(footer, hasKey("took"));
        assertThat(footer.get("took"), instanceOf(Number.class));
        assertThat(footer, hasKey("is_partial"));
        assertThat(footer.get("is_partial"), equalTo(false));
        assertThat(footer, not(hasKey("columns")));
        assertThat(footer, not(hasKey("values")));
        assertThat(footer, not(hasKey("error")));
    }

    public void testDropNullColumns() throws IOException {
        List<Map<String, Object>> lines = stream("""
            {"query": "FROM stream-test | SORT value | LIMIT 100 | KEEP value, description, sparse_field", "page_size": 2}
            """, "drop_null_columns=true");

        Map<String, Object> header = lines.get(0);
        assertThat(header, hasKey("all_columns"));
        assertThat(header, hasKey("columns"));
        assertThat(header, not(hasKey("values")));

        List<Map<String, Object>> allColumns = columnList(header, "all_columns");
        List<Map<String, Object>> trimmedColumns = columnList(header, "columns");

        assertThat("all_columns should list all three fields", allColumns, hasSize(3));
        assertThat("sparse_field should be dropped, leaving two columns", trimmedColumns, hasSize(2));
        assertFalse("sparse_field must not appear in trimmed columns", columnNames(header, "columns").contains("sparse_field"));

        for (Map<String, Object> line : lines.subList(1, lines.size() - 1)) {
            if (line.containsKey("values") == false) {
                continue;
            }
            for (List<Object> row : rows(line)) {
                assertThat("row width must match trimmed column count", row.size(), equalTo(trimmedColumns.size()));
            }
        }
    }

    public void testDropNullColumnsNoIndexFields() throws IOException {
        assertNoColumnTrimmed("{\"query\": \"FROM stream-test | STATS c = COUNT(*)\", \"page_size\": 10}", "c");
        assertNoColumnTrimmed("{\"query\": \"ROW x = 1\", \"page_size\": 10}", "x");
    }

    public void testErrorFraming() throws IOException {
        ResponseException re = expectThrows(ResponseException.class, () -> EsqlStreamTestUtils.rawStream(client(), """
            {"query": "FROM stream-test | EVAL x = unknown_function(value)", "page_size": 1}
            """));

        String contentType = re.getResponse().getEntity().getContentType().getValue();
        assertThat(contentType, containsString("application/x-ndjson"));

        List<Map<String, Object>> lines = parseNdjson(re.getResponse());
        assertThat("error response should be a single NDJSON line", lines, hasSize(1));

        Map<String, Object> errorLine = lines.get(0);
        assertThat(errorLine, hasKey("error"));
        assertThat(errorLine, hasKey("status"));
        @SuppressWarnings("unchecked")
        Map<String, Object> error = (Map<String, Object>) errorLine.get("error");
        assertThat(error, hasKey("type"));
        assertThat(error.get("type"), equalTo("verification_exception"));
        assertThat(error, hasKey("reason"));
        assertThat(error.get("reason"), notNullValue());
        assertThat(
            "status in error line must match HTTP response status",
            errorLine.get("status"),
            equalTo(re.getResponse().getStatusLine().getStatusCode())
        );
    }

    public void testMissingPageSize() {
        ResponseException re = expectThrows(ResponseException.class, () -> EsqlStreamTestUtils.rawStream(client(), """
            {"query": "FROM stream-test | LIMIT 1"}
            """));
        assertThat(re.getResponse().getStatusLine().getStatusCode(), equalTo(400));
        assertThat(re.getMessage(), containsString("page_size"));
    }

    public void testInvalidPageSize() {
        ResponseException re = expectThrows(ResponseException.class, () -> EsqlStreamTestUtils.rawStream(client(), """
            {"query": "FROM stream-test | LIMIT 1", "page_size": 0}
            """));
        assertThat(re.getResponse().getStatusLine().getStatusCode(), equalTo(400));
        assertThat(re.getMessage(), containsString("page_size"));
    }

    public void testWarningsInFooter() throws IOException {
        List<Map<String, Object>> lines = stream("""
            {"query": "FROM stream-test | EVAL n = to_int(description) | SORT value | KEEP value, n | LIMIT 100", "page_size": 10}
            """);

        Map<String, Object> footer = lines.get(lines.size() - 1);
        assertThat("footer should be present", footer, hasKey("took"));
        assertThat("footer must contain warnings from failed conversions", footer, hasKey("warnings"));
        @SuppressWarnings("unchecked")
        List<String> warnings = (List<String>) footer.get("warnings");
        assertFalse("warnings list must not be empty", warnings.isEmpty());
        assertThat(warnings, everyItem(not(startsWith("299 Elasticsearch-"))));
        assertThat("no warning entry should be wrapped in quotes", warnings, everyItem(not(startsWith("\""))));
        assertTrue(
            "at least one warning must be the to_int conversion message with a Line position prefix",
            warnings.stream().anyMatch(w -> w.startsWith("Line 1:") && w.contains("evaluation of [to_int(description)] failed"))
        );
    }

    public void testDropNullColumnsDoesNotDropSourceOnlyField() throws IOException {
        List<Map<String, Object>> lines = stream("""
            {"query": "FROM stream-test | SORT value | LIMIT 100 | KEEP value, noidx_field, sparse_field", "page_size": 2}
            """, "drop_null_columns=true");

        Map<String, Object> header = lines.get(0);
        assertThat(header, hasKey("all_columns"));
        assertThat(header, hasKey("columns"));
        assertThat("all_columns must list all three requested fields", columnList(header, "all_columns"), hasSize(3));

        List<String> trimmedNames = columnNames(header, "columns");
        assertTrue("noidx_field is populated and must not be dropped", trimmedNames.contains("noidx_field"));
        assertFalse("sparse_field is empty and must be dropped", trimmedNames.contains("sparse_field"));

        assertColumnPopulatedInEveryRow(lines, trimmedNames.indexOf("noidx_field"), "noidx_field");
    }

    public void testDropNullColumnsAgreesWithQueryEndpoint() throws IOException {
        assertStreamAgreesWithQuery("FROM stream-test | SORT value | LIMIT 100 | RENAME sparse_field AS s | KEEP value, s");
        assertStreamAgreesWithQuery("FROM stream-test | SORT value | LIMIT 100 | EVAL s = sparse_field | KEEP value, s");
        assertStreamAgreesWithQueryIgnoringRowOrder("FROM stream-test | STATS c = COUNT(*) BY s = sparse_field");
        assertStreamAgreesWithQuery("FROM stream-test | SORT value | LIMIT 100 | EVAL s = sparse_field | EVAL t = s | KEEP value, t");
        assertStreamAgreesWithQueryIgnoringRowOrder("FROM stream-test | STATS c = COUNT(*) BY sparse_field");
        assertStreamAgreesWithQuery("FROM stream-test | SORT value | LIMIT 100 | KEEP value, description");
        assertStreamAgreesWithQuery("FROM stream-test | SORT value | LIMIT 100 | KEEP value, noidx_field, sparse_field");
    }

    public void testDropNullColumnsIsIndexWideNotResultWide() throws IOException {
        Map<String, Object> queryResponse = query("FROM stream-test | WHERE value == 3 | KEEP value, description");
        List<String> queryColumnNames = columnNames(queryResponse, "columns");
        assertFalse("/_query must drop description when WHERE excludes the only doc with it", queryColumnNames.contains("description"));

        List<Map<String, Object>> streamLines = stream(
            streamBody("FROM stream-test | WHERE value == 3 | KEEP value, description", 10),
            "drop_null_columns=true"
        );
        List<String> streamColumnNames = columnNames(streamLines.get(0), "columns");
        assertTrue("/_query/stream must keep description because it is populated index-wide", streamColumnNames.contains("description"));
    }

    public void testDropNullColumnsAliasOfPopulatedFieldIsKept() throws IOException {
        List<Map<String, Object>> lines = stream("""
            {"query": "FROM stream-test | SORT value | LIMIT 100 | EVAL d = description | KEEP value, d", "page_size": 10}
            """, "drop_null_columns=true");

        assertTrue("alias of a populated field must not be dropped", columnNames(lines.get(0), "columns").contains("d"));
    }

    public void testDropNullColumnsAcrossCommaSeparatedIndices() throws IOException {
        Request createIndex2 = new Request("PUT", "/stream-test-2");
        createIndex2.setJsonEntity("""
            {
              "mappings": {
                "properties": {
                  "value":        { "type": "integer" },
                  "description":  { "type": "keyword" },
                  "sparse_field": { "type": "keyword" }
                }
              }
            }
            """);
        assertOK(client().performRequest(createIndex2));

        Request bulk2 = new Request("POST", "/_bulk?index=stream-test-2&refresh=true");
        bulk2.setJsonEntity("""
            {"index": {"_id": "5"}}
            {"value": 5, "description": "number five"}
            {"index": {"_id": "6"}}
            {"value": 6, "description": "number six"}
            """);
        assertOK(client().performRequest(bulk2));

        String query = "FROM stream-test,stream-test-2 | SORT value | LIMIT 10 | KEEP value, description, sparse_field";
        List<Map<String, Object>> lines = stream(streamBody(query, 2), "drop_null_columns=true");

        Map<String, Object> header = lines.get(0);
        assertThat("header must contain all_columns", header, hasKey("all_columns"));
        assertThat("header must contain columns", header, hasKey("columns"));

        assertThat("all_columns must list all three fields", columnList(header, "all_columns"), hasSize(3));

        List<String> trimmedNames = columnNames(header, "columns");
        assertTrue("value must survive: it is populated in both indices", trimmedNames.contains("value"));
        assertTrue("description must survive: it is populated in both indices", trimmedNames.contains("description"));
        assertFalse("sparse_field must be dropped: it is empty in both indices", trimmedNames.contains("sparse_field"));

        assertStreamAgreesWithQuery(query);
    }

    public void testDropNullColumnsAggregateMetricDoubleIsKept() throws IOException {
        String query = "FROM stream-test | SORT value | LIMIT 100 | KEEP value, agg_field, sparse_field";
        List<Map<String, Object>> lines = stream(streamBody(query, 2), "drop_null_columns=true");

        List<String> trimmedNames = columnNames(lines.get(0), "columns");
        assertTrue("a populated aggregate_metric_double must not be dropped", trimmedNames.contains("agg_field"));
        assertFalse("sparse_field is empty and must still be dropped", trimmedNames.contains("sparse_field"));

        assertColumnPopulatedInEveryRow(lines, trimmedNames.indexOf("agg_field"), "agg_field");
        assertStreamAgreesWithQuery(query);
    }

    public void testInlineStats() throws IOException {
        String esql = "FROM stream-test | INLINE STATS avg_val = AVG(value) | SORT value | LIMIT 10";
        assertStreamAgreesWithQueryIgnoringRowOrder(esql);

        List<Map<String, Object>> lines = stream(streamBody(esql, AGREEMENT_PAGE_SIZE), "drop_null_columns=true");
        long headerFrameCount = lines.stream().filter(l -> l.containsKey("columns")).count();
        assertEquals("stream must emit exactly one columns header frame for an INLINE STATS query", 1L, headerFrameCount);
    }

    public void testSubqueryIn() throws IOException {
        String esql = "FROM stream-test | WHERE value IN (FROM stream-test | KEEP value | WHERE value > 2) | SORT value";
        assertStreamAgreesWithQuery(esql);

        List<Map<String, Object>> lines = stream(streamBody(esql, AGREEMENT_PAGE_SIZE), "drop_null_columns=true");
        long headerFrameCount = lines.stream().filter(l -> l.containsKey("columns")).count();
        assertEquals("stream must emit exactly one columns header frame for an IN-subquery query", 1L, headerFrameCount);
    }

    public void testExplain() throws IOException {
        assumeTrue("EXPLAIN is snapshot only", Build.current().isSnapshot());
        String esql = "EXPLAIN (FROM stream-test | SORT value | LIMIT 10 | KEEP value)";

        List<Map<String, Object>> lines = stream(streamBody(esql, AGREEMENT_PAGE_SIZE));
        long headerFrameCount = lines.stream().filter(l -> l.containsKey("columns")).count();
        assertEquals("EXPLAIN stream must emit exactly one columns header frame", 1L, headerFrameCount);
        List<String> explainColumnNames = columnNames(lines.get(0), "columns");
        assertEquals(
            "EXPLAIN columns must be cluster, node, role, type, plan",
            List.of("cluster", "node", "role", "type", "plan"),
            explainColumnNames
        );
    }

    public void testBucketColumnMetadataAgreesWithQueryEndpoint() throws IOException {
        initBucketIndex();
        String esql = "SET column_metadata=true; FROM stream-bucket-test | STATS c = COUNT(*) BY b = BUCKET(date, 1 month) | SORT b";

        // Guard: confirm /_query actually emits _meta for the BUCKET column before comparing with the stream
        Map<String, Object> queryResponse = query(esql);
        List<Map<String, Object>> queryColumns = columnList(queryResponse, "columns");
        Map<String, Object> bColumn = queryColumns.stream()
            .filter(col -> "b".equals(col.get("name")))
            .findFirst()
            .orElseThrow(() -> new AssertionError("column 'b' not found in /_query response"));
        assertNotNull("/_query must emit _meta.bucket for BUCKET column 'b'", bColumn.get("_meta"));

        assertStreamAgreesWithQuery(esql, true);
    }

    public void testApproximationExtraColumnsAgreeWithQueryEndpoint() throws IOException {
        initBucketIndex();
        String esql = "SET approximation=true; FROM stream-bucket-test | STATS count = COUNT(*)";

        Map<String, Object> queryResponse = query(esql);
        List<Map<String, Object>> queryColumns = columnList(queryResponse, "columns");
        assertEquals("/_query with approximation=true must return 3 columns", 3, queryColumns.size());
        assertEquals("second column must be the CI column", "_approximation_confidence_interval(count)", queryColumns.get(1).get("name"));
        assertEquals("third column must be the certified column", "_approximation_certified(count)", queryColumns.get(2).get("name"));
        assertNotNull("CI column must carry _meta.approximation from /_query", queryColumns.get(1).get("_meta"));
        assertNotNull("certified column must carry _meta.approximation from /_query", queryColumns.get(2).get("_meta"));

        assertStreamAgreesWithQuery(esql, true);
    }

    private void initBucketIndex() throws IOException {
        Request createIndex = new Request("PUT", "/stream-bucket-test");
        createIndex.setJsonEntity("""
            {
              "mappings": {
                "properties": {
                  "date":  { "type": "date" },
                  "value": { "type": "integer" }
                }
              }
            }
            """);
        assertOK(client().performRequest(createIndex));

        Request bulk = new Request("POST", "/_bulk?index=stream-bucket-test&refresh=true");
        bulk.setJsonEntity("""
            {"index": {}}
            {"date": "1985-01-15", "value": 10}
            {"index": {}}
            {"date": "1985-01-20", "value": 20}
            {"index": {}}
            {"date": "1985-02-10", "value": 30}
            {"index": {}}
            {"date": "1985-02-25", "value": 40}
            {"index": {}}
            {"date": "1985-03-05", "value": 50}
            {"index": {}}
            {"date": "1985-03-15", "value": 60}
            {"index": {}}
            {"date": "1985-04-01", "value": 70}
            {"index": {}}
            {"date": "1985-04-20", "value": 80}
            """);
        assertOK(client().performRequest(bulk));
    }

    private void assertStreamAgreesWithQuery(String esql) throws IOException {
        assertStreamAgreesWithQuery(esql, true);
    }

    private void assertStreamAgreesWithQueryIgnoringRowOrder(String esql) throws IOException {
        assertStreamAgreesWithQuery(esql, false);
    }

    private void assertStreamAgreesWithQuery(String esql, boolean orderedRows) throws IOException {
        Map<String, Object> queryResponse = query(esql);
        List<Map<String, Object>> queryAllColumns = columnList(queryResponse, "all_columns");
        List<Map<String, Object>> queryColumns = columnList(queryResponse, "columns");

        List<Map<String, Object>> lines = stream(streamBody(esql, AGREEMENT_PAGE_SIZE), "drop_null_columns=true");
        Map<String, Object> streamHeader = lines.get(0);
        List<Map<String, Object>> streamAllColumns = columnList(streamHeader, "all_columns");
        List<Map<String, Object>> streamColumns = columnList(streamHeader, "columns");

        assertEquals(
            "all_columns count must agree between /_query and /_query/stream for: " + esql,
            queryAllColumns.size(),
            streamAllColumns.size()
        );
        assertEquals("all_columns must agree between /_query and /_query/stream for: " + esql, queryAllColumns, streamAllColumns);
        assertEquals("columns count must agree between /_query and /_query/stream for: " + esql, queryColumns.size(), streamColumns.size());
        assertEquals("columns must agree between /_query and /_query/stream for: " + esql, queryColumns, streamColumns);

        List<List<Object>> queryRows = rows(queryResponse);
        List<List<Object>> streamRows = streamRows(lines);

        assertThat("query returned no rows, so the data comparison would be vacuous: " + esql, queryRows, not(empty()));
        for (List<Object> row : streamRows) {
            assertThat("stream row width must match the trimmed column count for: " + esql, row.size(), equalTo(streamColumns.size()));
        }
        if (orderedRows) {
            assertEquals("values must agree between /_query and /_query/stream for: " + esql, queryRows, streamRows);
        } else {
            assertEquals(
                "values must agree (ignoring row order) between /_query and /_query/stream for: " + esql,
                canonicalRows(queryRows),
                canonicalRows(streamRows)
            );
        }
    }

    private static List<List<Object>> streamRows(List<Map<String, Object>> lines) {
        List<List<Object>> all = new ArrayList<>();
        for (Map<String, Object> line : lines) {
            if (line.containsKey("values")) {
                all.addAll(rows(line));
            }
        }
        return all;
    }

    private static List<String> canonicalRows(List<List<Object>> rows) {
        return rows.stream().map(String::valueOf).sorted().toList();
    }

    private void assertNoColumnTrimmed(String queryBody, String expectedColumnName) throws IOException {
        List<Map<String, Object>> lines = stream(queryBody, "drop_null_columns=true");
        Map<String, Object> header = lines.get(0);
        assertThat(header, hasKey("all_columns"));
        assertThat(header, hasKey("columns"));
        assertThat(columnNames(header, "all_columns"), hasSize(1));
        assertThat(columnNames(header, "columns"), hasSize(1));
        assertThat(columnNames(header, "all_columns").get(0), equalTo(expectedColumnName));
        assertThat(columnNames(header, "columns").get(0), equalTo(expectedColumnName));
    }

    private void assertColumnPopulatedInEveryRow(List<Map<String, Object>> lines, int position, String fieldName) {
        for (Map<String, Object> line : lines.subList(1, lines.size() - 1)) {
            if (line.containsKey("values") == false) {
                continue;
            }
            for (List<Object> row : rows(line)) {
                assertThat(fieldName + " must carry a non-null value in every row", row.get(position), notNullValue());
            }
        }
    }

    @SuppressWarnings("unchecked")
    private static List<String> columnNames(Map<String, Object> line, String key) {
        return ((List<Map<String, Object>>) line.get(key)).stream().map(c -> (String) c.get("name")).toList();
    }

    @SuppressWarnings("unchecked")
    private static List<Map<String, Object>> columnList(Map<String, Object> line, String key) {
        return (List<Map<String, Object>>) line.get(key);
    }

    @SuppressWarnings("unchecked")
    private static List<List<Object>> rows(Map<String, Object> line) {
        return (List<List<Object>>) line.get("values");
    }

    private Map<String, Object> query(String esql) throws IOException {
        Request request = new Request("POST", "/_query?drop_null_columns=true");
        request.setJsonEntity("{\"query\":\"" + esql.replace("\"", "\\\"") + "\"}");
        tolerateDefaultLimitWarning(request);
        return XContentHelper.convertToMap(XContentType.JSON.xContent(), client().performRequest(request).getEntity().getContent(), false);
    }

    private static String streamBody(String esql, int pageSize) {
        return "{\"query\":\"" + esql.replace("\"", "\\\"") + "\",\"page_size\":" + pageSize + "}";
    }

    private List<Map<String, Object>> stream(String bodyJson, String... queryParams) throws IOException {
        Response response = EsqlStreamTestUtils.rawStream(client(), bodyJson, queryParams);
        assertThat(
            "/_query/stream must respond with application/x-ndjson",
            response.getEntity().getContentType().getValue(),
            containsString("application/x-ndjson")
        );
        return parseNdjson(response);
    }
}
