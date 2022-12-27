/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.test.eql;

import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.junit.After;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.elasticsearch.xpack.ql.TestUtils.assertNoSearchContexts;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

public abstract class EqlRestTestCase extends RemoteClusterAwareEqlRestTestCase {

    private static final String defaultValidationIndexName = "eql_search_validation_test";
    private static final String validQuery = "process where user = \\\"SYSTEM\\\"";

    @After
    public void checkSearchContent() throws Exception {
        assertNoSearchContexts(client());
    }

    private static final String[][] testBadRequests = {
        { null, "request body or source parameter is required" },
        { "{}", "query is null or empty" },
        { """
            {"query": ""}""", "query is null or empty" },
        { String.format(Locale.ROOT, """
            {"query": "%s", "timestamp_field": ""}
            """, validQuery), "timestamp field is null or empty" },
        { String.format(Locale.ROOT, """
            {"query": "%s", "event_category_field": ""}
            """, validQuery), "event category field is null or empty" },
        { String.format(Locale.ROOT, """
            {"query": "%s", "size": -1}
            """, validQuery), "size must be greater than or equal to 0" },
        { String.format(Locale.ROOT, """
            {"query": "%s", "fetch_size": 1}
            """, validQuery), "fetch size must be greater than 1" },
        { String.format(Locale.ROOT, """
            {"query": "%s", "filter": null}
            """, validQuery), "filter doesn't support values of type: VALUE_NULL" },
        { String.format(Locale.ROOT, """
            {"query": "%s", "filter": {}}
            """, validQuery), "query malformed, empty clause found" },
        { String.format(Locale.ROOT, """
            {"query": "%s", "max_samples_per_key": 0}
            """, validQuery), "max_samples_per_key must be greater than 0" } };

    public void testBadRequests() throws Exception {
        createIndex(defaultValidationIndexName, (String) null);

        for (String[] test : testBadRequests) {
            assertBadRequest(test[0], test[1], 400);
        }

        bulkIndex("""
            {"index": {"_index": "%s", "_id": 1}}
            {"event":{"category":"process"},"@timestamp":"2020-01-01T12:34:56Z"}
            """.formatted(defaultValidationIndexName));
        assertBadRequest("""
            {"query": "sample by event.category [any where true] [any where true]",
             "fetch_size": 1001}
            """, "Fetch size cannot be greater than [1000]", 500);

        deleteIndexWithProvisioningClient(defaultValidationIndexName);
    }

    private void assertBadRequest(String query, String errorMessage, int errorCode) throws IOException {
        final String endpoint = "/" + indexPattern(defaultValidationIndexName) + "/_eql/search";
        Request request = new Request("GET", endpoint);
        request.setJsonEntity(query);

        ResponseException e = expectThrows(ResponseException.class, () -> client().performRequest(request));
        Response response = e.getResponse();

        assertThat(response.getHeader("Content-Type"), containsString("application/json"));
        assertThat(EntityUtils.toString(response.getEntity()), containsString(errorMessage));
        assertThat(response.getStatusLine().getStatusCode(), is(errorCode));
    }

    @SuppressWarnings("unchecked")
    public void testIndexWildcardPatterns() throws Exception {
        createIndex("test1", """
            "my_alias" : {}, "test_alias" : {}""");
        createIndex("test2", """
            "my_alias" : {}""");

        bulkIndex("""
            {"index": {"_index": "test1", "_id": 1}}
            {"event":{"category":"process"},"@timestamp":"2020-09-04T12:34:56Z"}
            {"index": {"_index": "test2", "_id": 2}}
            {"event":{"category":"process"},"@timestamp":"2020-09-05T12:34:56Z"}
            """);

        String[] wildcardRequests = {
            "test1,test2",
            "test1*,test2",
            "test1,test2*",
            "test1*,test2*",
            "test*",
            "test1,test2,inexistent",
            "my_alias",
            "my_alias,test*",
            "test2,my_alias,test1",
            "my_al*" };

        for (String indexPattern : wildcardRequests) {
            String endpoint = "/" + indexPattern(indexPattern) + "/_eql/search";
            Request request = new Request("GET", endpoint);
            request.setJsonEntity("""
                {"query":"process where true"}""");
            Response response = client().performRequest(request);

            Map<String, Object> responseMap;
            try (InputStream content = response.getEntity().getContent()) {
                responseMap = XContentHelper.convertToMap(JsonXContent.jsonXContent, content, false);
            }
            Map<String, Object> hits = (Map<String, Object>) responseMap.get("hits");
            List<Map<String, Object>> events = (List<Map<String, Object>>) hits.get("events");
            assertEquals(2, events.size());
            assertEquals("1", events.get(0).get("_id"));
            assertEquals("2", events.get(1).get("_id"));
        }

        deleteIndexWithProvisioningClient("test1");
        deleteIndexWithProvisioningClient("test2");
    }

    @SuppressWarnings("unchecked")
    public void testUnicodeChars() throws Exception {
        createIndex("test", (String) null);

        String bulk = """
            {"index": {"_index": "test", "_id": 1}}
            {"event":{"category":"process"},"@timestamp":"2020-09-04T12:34:56Z","log" : "prefix_ë_suffix"}
            {"index": {"_index": "test", "_id": 2}}
            {"event":{"category":"process"},"@timestamp":"2020-09-05T12:34:57Z","log" : "prefix_𖠋_suffix"}
            """;
        bulkIndex(bulk);

        String endpoint = "/" + indexPattern("test") + "/_eql/search";
        Request request = new Request("GET", endpoint);
        request.setJsonEntity("""
            {"query":"process where log==\\"prefix_\\\\u{0eb}_suffix\\""}""");
        Response response = client().performRequest(request);

        Map<String, Object> responseMap;
        try (InputStream content = response.getEntity().getContent()) {
            responseMap = XContentHelper.convertToMap(JsonXContent.jsonXContent, content, false);
        }
        Map<String, Object> hits = (Map<String, Object>) responseMap.get("hits");
        List<Map<String, Object>> events = (List<Map<String, Object>>) hits.get("events");
        assertEquals(1, events.size());
        assertEquals("1", events.get(0).get("_id"));

        request.setJsonEntity("""
            {"query":"process where log==\\"prefix_\\\\u{01680b}_suffix\\""}""");
        response = client().performRequest(request);

        try (InputStream content = response.getEntity().getContent()) {
            responseMap = XContentHelper.convertToMap(JsonXContent.jsonXContent, content, false);
        }
        hits = (Map<String, Object>) responseMap.get("hits");
        events = (List<Map<String, Object>>) hits.get("events");
        assertEquals(1, events.size());
        assertEquals("2", events.get(0).get("_id"));

        deleteIndexWithProvisioningClient("test");
    }

    private void bulkIndex(String bulk) throws IOException {
        Request bulkRequest = new Request("POST", "/_bulk");
        bulkRequest.setJsonEntity(bulk);
        bulkRequest.addParameter("refresh", "true");

        Response response = provisioningClient().performRequest(bulkRequest);
        assertThat(response.getStatusLine().getStatusCode(), equalTo(200));
        String bulkResponse = EntityUtils.toString(response.getEntity());
        assertThat(bulkResponse, not(containsString("\"errors\": true")));
    }

    protected String indexPattern(String pattern) {
        return pattern;
    }
}
