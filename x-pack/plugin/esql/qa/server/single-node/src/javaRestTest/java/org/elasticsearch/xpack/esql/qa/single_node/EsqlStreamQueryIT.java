/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.single_node;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.test.TestClustersThreadFilter;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.esql.qa.rest.EsqlSpecTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;

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

    @Before
    public void initIndex() throws IOException {
        Request createIndex = new Request("PUT", "/" + INDEX);
        createIndex.setJsonEntity("""
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
        assertEquals(200, client().performRequest(createIndex).getStatusLine().getStatusCode());

        Request bulk = new Request("POST", "/_bulk?index=" + INDEX + "&refresh=true");
        bulk.setJsonEntity("""
            {"index": {"_id": "1"}}
            {"value": 1, "description": "number one"}
            {"index": {"_id": "2"}}
            {"value": 2, "description": "number two"}
            {"index": {"_id": "3"}}
            {"value": 3}
            {"index": {"_id": "4"}}
            {"value": 4, "description": "number four"}
            """);
        assertEquals(200, client().performRequest(bulk).getStatusLine().getStatusCode());
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

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> allColumns = (List<Map<String, Object>>) header.get("all_columns");
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> trimmedColumns = (List<Map<String, Object>>) header.get("columns");

        assertThat("all_columns should list all three fields", allColumns, hasSize(3));
        assertThat("sparse_field should be dropped, leaving two columns", trimmedColumns, hasSize(2));

        List<String> trimmedNames = trimmedColumns.stream().map(c -> (String) c.get("name")).toList();
        assertFalse("sparse_field must not appear in trimmed columns", trimmedNames.contains("sparse_field"));

        for (Map<String, Object> line : lines.subList(1, lines.size() - 1)) {
            if (line.containsKey("values") == false) {
                continue;
            }
            @SuppressWarnings("unchecked")
            List<List<Object>> rows = (List<List<Object>>) line.get("values");
            for (List<Object> row : rows) {
                assertThat("row width must match trimmed column count", row.size(), equalTo(trimmedColumns.size()));
            }
        }
    }

    public void testErrorFraming() throws IOException {
        ResponseException re = expectThrows(ResponseException.class, () -> rawStream("""
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
        assertThat(error.get("type"), notNullValue());
        assertThat(error, hasKey("reason"));
        assertThat(error.get("reason"), notNullValue());
        assertThat(
            "status in error line must match HTTP response status",
            errorLine.get("status"),
            equalTo(re.getResponse().getStatusLine().getStatusCode())
        );
    }

    public void testMissingPageSize() {
        ResponseException re = expectThrows(ResponseException.class, () -> rawStream("""
            {"query": "FROM stream-test | LIMIT 1"}
            """));
        assertThat(re.getResponse().getStatusLine().getStatusCode(), equalTo(400));
        assertThat(re.getMessage(), containsString("page_size"));
    }

    public void testInvalidPageSize() {
        ResponseException re = expectThrows(ResponseException.class, () -> rawStream("""
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
    }

    private List<Map<String, Object>> stream(String bodyJson, String... queryParams) throws IOException {
        Response response = rawStream(bodyJson, queryParams);
        assertThat(
            "/_query/stream must respond with application/x-ndjson",
            response.getEntity().getContentType().getValue(),
            containsString("application/x-ndjson")
        );
        return parseNdjson(response);
    }

    private Response rawStream(String bodyJson, String... queryParams) throws IOException {
        String path = "/_query/stream";
        if (queryParams.length > 0) {
            path += "?" + String.join("&", queryParams);
        }
        Request request = new Request("POST", path);
        request.setJsonEntity(bodyJson);
        return client().performRequest(request);
    }

    private static List<Map<String, Object>> parseNdjson(Response response) throws IOException {
        String body = EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8);
        List<Map<String, Object>> lines = new ArrayList<>();
        for (String line : body.split("\n")) {
            if (line.isBlank()) {
                continue;
            }
            lines.add(XContentHelper.convertToMap(XContentType.JSON.xContent(), line, false));
        }
        return lines;
    }
}
