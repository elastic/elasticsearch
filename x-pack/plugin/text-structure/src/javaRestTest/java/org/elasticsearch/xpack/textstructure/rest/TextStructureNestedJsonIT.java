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
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.instanceOf;

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

    public void testJsonObjectDetectionBasicRequest() throws IOException {
        String jsonSample = """
            {"timestamp": "1478261151445", "id": 1, "message": "Connection established"}
            {"timestamp": "1478261151446", "id": 2, "message": "Request processed"}
            {"timestamp": "1478261151447", "id": 3, "message": "Data written"}
            """;

        Map<String, Object> responseMap = executeAndVerifyRequest(jsonSample, null);

        assertKeyValue("timestamp", "date", responseMap);
        assertKeyValue("id", "long", responseMap);
        assertKeyValue("message", "keyword", responseMap);
    }

    public void testNestedJsonObjectDetectionDefaultBehavior() throws IOException {
        String nestedJsonSample = """
            {"host": {"id": 1, "category": "NETWORKING DEVICE"}, "timestamp": "1478261151445"}
            {"host": {"id": 2, "category": "COMPUTE NODE"}, "timestamp": "1478261151446"}
            {"host": {"id": 3, "category": "STORAGE DEVICE"}, "timestamp": "1478261151447"}
            """;

        Map<String, Object> responseMap = executeAndVerifyRequest(nestedJsonSample, null);

        assertKeyValue("host", "object", responseMap);
        assertKeyValue("timestamp", "date", responseMap);
    }

    public void testNestedJsonObjectDetectionRecursive() throws IOException {
        String nestedJsonSample = """
            {"host": {"id": 1, "category": "NETWORKING DEVICE"}, "timestamp": "1478261151445"}
            {"host": {"id": 2, "category": "COMPUTE NODE"}, "timestamp": "1478261151446"}
            {"host": {"id": 3, "category": "STORAGE DEVICE"}, "timestamp": "1478261151447"}
            """;

        Map<String, Object> responseMap = executeAndVerifyRequest(nestedJsonSample, Boolean.TRUE);

        assertKeyValue("host.id", "long", responseMap);
        assertKeyValue("host.category", "keyword", responseMap);
        assertKeyValue("timestamp", "date", responseMap);
    }

    public void testNestedJsonObjectDetectionNoParseRecursivelyArgumentDefaultsToFalse() throws IOException {
        String nestedJsonSample = """
            {"host": {"id": 1, "category": "NETWORKING DEVICE"}, "timestamp": "1478261151445"}
            {"host": {"id": 2, "category": "COMPUTE NODE"}, "timestamp": "1478261151446"}
            {"host": {"id": 3, "category": "STORAGE DEVICE"}, "timestamp": "1478261151447"}
            """;

        Map<String, Object> responseMap1 = executeAndVerifyRequest(nestedJsonSample, Boolean.FALSE);
        Map<String, Object> responseMap2 = executeAndVerifyRequest(nestedJsonSample, null);

        assertThat(
            "Setting `should_parse_recursively=false` is equivalent to not setting this argument at all",
            responseMap1,
            equalTo(responseMap2)
        );
    }

    private static Map<String, Object> executeAndVerifyRequest(String sample, Boolean shouldParseRecursively) throws IOException {
        Request request = new Request("POST", "/_text_structure/find_structure");
        request.setEntity(new StringEntity(sample, ContentType.APPLICATION_JSON));

        if (shouldParseRecursively != null) {
            request.addParameter("should_parse_recursively", Boolean.toString(shouldParseRecursively));
        }
        Response response = client().performRequest(request);
        assertOK(response);
        return entityAsMap(response);
    }

    private void assertKeyValue(String expectedKey, String expectedType, Map<String, Object> responseMap) {
        assertThat(responseMap.get("format"), equalTo("ndjson"));
        assertThat(responseMap, hasKey("mappings"));

        @SuppressWarnings("unchecked")
        Map<String, Object> mappings = (Map<String, Object>) responseMap.get("mappings");
        assertThat(mappings, instanceOf(Map.class));
        assertThat(mappings, hasKey("properties"));

        @SuppressWarnings("unchecked")
        Map<String, Object> properties = (Map<String, Object>) mappings.get("properties");
        assertThat(properties, instanceOf(Map.class));
        assertThat(properties, hasKey(expectedKey));

        @SuppressWarnings("unchecked")
        Map<String, Object> typeMapping = (Map<String, Object>) properties.get(expectedKey);
        assertThat(typeMapping.get("type"), equalTo(expectedType));
        assertThat(typeMapping, instanceOf(Map.class));
    }
}
