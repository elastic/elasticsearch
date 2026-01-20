/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.textstructure.rest;

import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;

public class TextStructureNestedJsonIT extends ESRestTestCase {

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .module("x-pack-text-structure")
        .setting("xpack.security.enabled", "false")
        .build();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    public void testJsonObjectDetection() throws IOException {
        String nestedJsonSample = """
            {"timestamp": "1478261151445", "id": 1, "message": "Connection established"}
            {"timestamp": "1478261151446", "id": 2, "message": "Request processed"}
            {"timestamp": "1478261151447", "id": 3, "message": "Data written"}
            """;

        Map<String, Object> responseMap = executeAndVerifyRequest(nestedJsonSample);

        assertKeyValue("timestamp", "date", responseMap);
        assertKeyValue("id", "long", responseMap);
        assertKeyValue("message", "keyword", responseMap);
    }

    public void testJsonObjectWithArrayDetection() throws IOException {
        String nestedJsonSample = """
            {"timestamp": "1478261151445", "id": [1, 2, 3], "message": "Connection established"}
            {"timestamp": "1478261151446", "id": [1, 2, 3], "message": "Request processed"}
            {"timestamp": "1478261151447", "id": [1, 2, 3.1], "message": "Data written"}
            """;

        Map<String, Object> responseMap = executeAndVerifyRequest(nestedJsonSample);

        assertKeyValue("timestamp", "date", responseMap);
        assertKeyValue("id", "double", responseMap);
        assertKeyValue("message", "keyword", responseMap);
    }

    public void testJsonObjectWithMixedArrayOfObjectsAndPrimitives() throws IOException {
        String nestedJsonSample = """
            {"timestamp": "1478261151445", "host": [1, {"id": 1}], "message": "Connection established"}
            {"timestamp": "1478261151446", "host": [2, {"id": 2}], "message": "Request processed"}
            {"timestamp": "1478261151447", "host": [3, {"id": 3}], "message": "Data written"}
            """;

        ResponseException e = expectThrows(ResponseException.class, () -> executeAndVerifyRequest(nestedJsonSample));
        assertThat(e.getResponse().getStatusLine().getStatusCode(), equalTo(400));
        assertThat(e.getMessage(), containsString("[host] has both object and non-object values"));
    }

    /**
     * Test that nested JSON objects are properly detected and mapped with hierarchical structure
     */
    public void testNestedJsonObjectDetection() throws IOException {
        String nestedJsonSample = """
            {"host": {"id": 1, "category": "NETWORKING DEVICE"}, "timestamp": "1478261151445", "message": "Connection established"}
            {"host": {"id": 2, "category": "COMPUTE NODE"}, "timestamp": "1478261151446", "message": "Request processed"}
            {"host": {"id": 3, "category": "STORAGE DEVICE"}, "timestamp": "1478261151447", "message": "Data written"}
            """;

        Map<String, Object> responseMap = executeAndVerifyRequest(nestedJsonSample);

        assertKeyValue("host.id", "long", responseMap);
        assertKeyValue("host.category", "keyword", responseMap);
        assertKeyValue("timestamp", "date", responseMap);
        assertKeyValue("message", "keyword", responseMap);
    }

    public void testNestedJsonObjectWithArrayOfObjects() throws IOException {
        String nestedJsonSample = """
            {"hosts": [{"id": 1, "name": "host1"}, {"id": 2, "name": "host2"}, {"id": 3, "name": "host3"}], "timestamp": "1478261151445"}
            {"hosts": [{"id": 4, "name": "host1"}, {"id": 5, "name": "host5"}], "timestamp": "1478261151446"}
            {"hosts": [{"id": 6, "name": "host6"}], "timestamp": "1478261151446"}
            """;

        Map<String, Object> responseMap = executeAndVerifyRequest(nestedJsonSample);

        assertKeyValue("hosts.id", "long", responseMap);
        assertKeyValue("hosts.name", "keyword", responseMap);
        assertKeyValue("timestamp", "date", responseMap);
    }

    public void testNestedJsonObjectEmptyObjectsMappedToObject() throws IOException {
        String nestedJsonSample = """
            {"host": {}, "timestamp": "1478261151445", "message": { "id" : 1, "message" : {}}}
            {"host": {}, "timestamp": "1478261151446", "message": { "id" : 2, "message" : {}}}
            {"host": {}, "timestamp": "1478261151446", "message": { "id" : 3, "message" : {}}}
            """;

        Map<String, Object> responseMap = executeAndVerifyRequest(nestedJsonSample);

        assertKeyValue("host", "object", responseMap);
    }

    /**
     * Test with the explain parameter to see the reasoning
     */
    public void testNestedJsonWithExplainParameter() throws IOException {
        String sample = """
            {"data": {"value": 42}, "timestamp": "2024-01-01T10:00:00Z"}
            {"data": {"value": 43}, "timestamp": "2024-01-01T10:01:00Z"}
            """;

        Request request = new Request("POST", "/_text_structure/find_structure");
        request.addParameter("explain", "true");
        request.setEntity(new StringEntity(sample, ContentType.APPLICATION_JSON));
        Response response = client().performRequest(request);
        assertOK(response);

        Map<String, Object> responseMap = entityAsMap(response);

        // Verify explanation is present
        assertThat(responseMap, hasKey("explanation"));
        @SuppressWarnings("unchecked")
        var explanation = responseMap.get("explanation");
        assertThat(explanation, notNullValue());
    }

    public void testNestedJsonDepthLimit() throws IOException {
        int maxDepth = 10;
        int testDepthBeyondLimit = maxDepth + 3;

        // Generate deeply nested JSON samples
        StringBuilder sample = new StringBuilder();
        for (int i = 1; i <= 3; i++) {
            sample.append(generateDeeplyNestedJson(testDepthBeyondLimit, i)).append("\n");
        }

        Map<String, Object> responseMap = executeAndVerifyRequest(sample.toString());

        @SuppressWarnings("unchecked")
        Map<String, Object> mappings = (Map<String, Object>) responseMap.get("mappings");
        @SuppressWarnings("unchecked")
        Map<String, Object> properties = (Map<String, Object>) mappings.get("properties");

        String withinLimitKey = buildNestedKey(maxDepth);
        String beyondLimitKey = buildNestedKey(maxDepth + 1);
        assertThat("Key within depth limit should exist", properties, hasKey(withinLimitKey));
        assertThat("Key beyond depth limit should not exist", properties, not(hasKey(beyondLimitKey)));
        assertKeyValue(withinLimitKey, "object", responseMap);

    }

    /**
     * Generates a deeply nested JSON object.
     * Example for depth=3, id=1: {"level1": {"level2": {"level3": {"value": 1}}}}
     */
    private String generateDeeplyNestedJson(int depth, int id) {
        StringBuilder json = new StringBuilder();
        for (int i = 1; i <= depth; i++) {
            json.append("{\"level").append(i).append("\": ");
        }
        json.append("{\"value\": ").append(id).append("}");
        for (int i = 0; i < depth; i++) {
            json.append("}");
        }
        return json.toString();
    }

    /**
     * Builds a dot-notation key for nested levels.
     * Example for depth=3: "level1.level2.level3"
     */
    private String buildNestedKey(int depth) {
        StringBuilder key = new StringBuilder();
        for (int i = 1; i <= depth; i++) {
            if (i > 1) {
                key.append(".");
            }
            key.append("level").append(i);
        }
        return key.toString();
    }

    private static Map<String, Object> executeAndVerifyRequest(String sample) throws IOException {
        Request request = new Request("POST", "/_text_structure/find_structure");
        request.setEntity(new StringEntity(sample, ContentType.APPLICATION_JSON));
        Response response = client().performRequest(request);
        assertOK(response);
        return entityAsMap(response);
    }

    private void assertKeyValue(String expectedKey, String expectedType, Map<String, Object> responseMap) {
        assertThat(responseMap.get("format"), equalTo("ndjson"));

        @SuppressWarnings("unchecked")
        Map<String, Object> mappings = (Map<String, Object>) responseMap.get("mappings");
        assertThat(mappings, hasKey("properties"));

        @SuppressWarnings("unchecked")
        Map<String, Object> properties = (Map<String, Object>) mappings.get("properties");

        assertThat(properties, hasKey(expectedKey));
        @SuppressWarnings("unchecked")
        Map<String, Object> timestampMapping = (Map<String, Object>) properties.get(expectedKey);
        assertThat(timestampMapping.get("type"), equalTo(expectedType));
    }
}
