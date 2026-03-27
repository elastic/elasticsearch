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
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.test.TestClustersThreadFilter;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xcontent.XContentType;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;

/**
 * Integration tests for the {@code POST /_query/stream} endpoint.
 * <p>
 * The streaming endpoint returns results as NDJSON (newline-delimited JSON). Each line
 * is a complete JSON object:
 * <ol>
 *   <li>A columns line: {@code {"columns":[{"name":"...","type":"..."},...]}}
 *   <li>One or more values lines: {@code {"values":[[...],[...],...]}}
 *   <li>A completion line: {@code {"completion":{"status":"ok","total_rows":N}}}
 * </ol>
 */
@ThreadLeakFilters(filters = TestClustersThreadFilter.class)
public class EsqlStreamingIT extends ESRestTestCase {

    @ClassRule
    public static ElasticsearchCluster cluster = Clusters.testCluster();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    private void ensureTestData() throws IOException {
        Request createIndex = new Request("PUT", "/streaming_test");
        createIndex.setJsonEntity("""
            {
              "settings": {"number_of_shards": 1},
              "mappings": {
                "properties": {
                  "value": {"type": "long"},
                  "name": {"type": "keyword"}
                }
              }
            }""");
        assertOK(client().performRequest(createIndex));

        StringBuilder bulk = new StringBuilder();
        String[] names = { "a", "b", "c", "d", "e" };
        for (int i = 0; i < 5; i++) {
            bulk.append("{\"index\":{}}\n");
            bulk.append("{\"value\":").append(i + 1).append(",\"name\":\"").append(names[i]).append("\"}\n");
        }
        Request bulkRequest = new Request("POST", "/streaming_test/_bulk");
        bulkRequest.addParameter("refresh", "true");
        bulkRequest.setJsonEntity(bulk.toString());
        assertOK(client().performRequest(bulkRequest));
    }

    public void testBasicStreamingQuery() throws IOException {
        ensureTestData();

        Response response = streamQuery("FROM streaming_test | SORT value | LIMIT 5", 2);
        List<Map<String, Object>> lines = parseNdjsonResponse(response);

        assertThat(lines.size(), greaterThanOrEqualTo(3));

        Map<String, Object> columnsLine = lines.get(0);
        assertThat(columnsLine.containsKey("columns"), equalTo(true));
        @SuppressWarnings("unchecked")
        List<Map<String, String>> columns = (List<Map<String, String>>) columnsLine.get("columns");
        assertThat(columns, hasSize(2));

        List<List<Object>> allValues = new ArrayList<>();
        for (int i = 1; i < lines.size() - 1; i++) {
            Map<String, Object> valuesLine = lines.get(i);
            assertThat("line " + i + " should have values", valuesLine.containsKey("values"), equalTo(true));
            @SuppressWarnings("unchecked")
            List<List<Object>> values = (List<List<Object>>) valuesLine.get("values");
            allValues.addAll(values);
        }
        assertThat("should have all 5 rows across chunks", allValues, hasSize(5));

        for (int i = 0; i < 5; i++) {
            assertThat("row " + i + " value", ((Number) allValues.get(i).get(1)).intValue(), equalTo(i + 1));
        }

        Map<String, Object> completionLine = lines.get(lines.size() - 1);
        assertThat(completionLine.containsKey("completion"), equalTo(true));
        @SuppressWarnings("unchecked")
        Map<String, Object> completion = (Map<String, Object>) completionLine.get("completion");
        assertThat(completion.get("status"), equalTo("ok"));
        assertThat(((Number) completion.get("total_rows")).intValue(), equalTo(5));
    }

    public void testAllResultsFitInOnePage() throws IOException {
        ensureTestData();

        Response response = streamQuery("FROM streaming_test | SORT value | LIMIT 5", 100);
        List<Map<String, Object>> lines = parseNdjsonResponse(response);

        assertThat(lines, hasSize(3));

        Map<String, Object> columnsLine = lines.get(0);
        assertThat(columnsLine.containsKey("columns"), equalTo(true));

        Map<String, Object> valuesLine = lines.get(1);
        @SuppressWarnings("unchecked")
        List<List<Object>> values = (List<List<Object>>) valuesLine.get("values");
        assertThat(values, hasSize(5));

        Map<String, Object> completionLine = lines.get(2);
        @SuppressWarnings("unchecked")
        Map<String, Object> completion = (Map<String, Object>) completionLine.get("completion");
        assertThat(completion.get("status"), equalTo("ok"));
        assertThat(((Number) completion.get("total_rows")).intValue(), equalTo(5));
    }

    public void testEmptyResultSet() throws IOException {
        ensureTestData();

        Response response = streamQuery("FROM streaming_test | WHERE value > 9999 | SORT value | LIMIT 5", 2);
        List<Map<String, Object>> lines = parseNdjsonResponse(response);

        assertThat(lines.size(), greaterThanOrEqualTo(1));

        Map<String, Object> lastLine = lines.get(lines.size() - 1);
        assertThat(lastLine.containsKey("completion"), equalTo(true));
        @SuppressWarnings("unchecked")
        Map<String, Object> completion = (Map<String, Object>) lastLine.get("completion");
        assertThat(completion.get("status"), equalTo("ok"));
        assertThat(((Number) completion.get("total_rows")).intValue(), equalTo(0));
    }

    public void testDefaultPageSize() throws IOException {
        ensureTestData();

        Response response = streamQuery("FROM streaming_test | SORT value | LIMIT 5", null);
        List<Map<String, Object>> lines = parseNdjsonResponse(response);

        assertThat(lines, hasSize(3));

        Map<String, Object> valuesLine = lines.get(1);
        @SuppressWarnings("unchecked")
        List<List<Object>> values = (List<List<Object>>) valuesLine.get("values");
        assertThat(values, hasSize(5));
    }

    private Response streamQuery(String query, Integer pageSize) throws IOException {
        Request request = new Request("POST", "/_query/stream");
        StringBuilder body = new StringBuilder();
        body.append("{\"query\":\"").append(query).append("\"");
        if (pageSize != null) {
            body.append(",\"page_size\":").append(pageSize);
        }
        body.append("}");
        request.setJsonEntity(body.toString());
        return client().performRequest(request);
    }

    private static List<Map<String, Object>> parseNdjsonResponse(Response response) throws IOException {
        String body = EntityUtils.toString(response.getEntity());
        String[] rawLines = body.split("\n");
        List<Map<String, Object>> parsed = new ArrayList<>();
        for (String line : rawLines) {
            String trimmed = line.trim();
            if (trimmed.isEmpty()) {
                continue;
            }
            parsed.add(XContentHelper.convertToMap(XContentType.JSON.xContent(), trimmed, false));
        }
        return parsed;
    }
}
