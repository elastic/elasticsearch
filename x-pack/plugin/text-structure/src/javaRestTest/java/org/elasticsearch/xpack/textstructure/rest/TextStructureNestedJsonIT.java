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

        @SuppressWarnings("unchecked")
        Map<String, Object> mappings = (Map<String, Object>) responseMap.get("mappings");
        assertThat(mappings, hasKey("properties"));

        @SuppressWarnings("unchecked")
        Map<String, Object> properties = (Map<String, Object>) mappings.get("properties");
        assertThat(properties, hasKey("timestamp"));
        assertThat(properties, hasKey("id"));
        assertThat(properties, hasKey("message"));
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

        // Verify format is detected as NDJSON
        assertThat(responseMap.get("format"), equalTo("ndjson"));

        // Verify mappings contain nested structure
        @SuppressWarnings("unchecked")
        Map<String, Object> mappings = (Map<String, Object>) responseMap.get("mappings");
        assertThat(mappings, hasKey("properties"));

        @SuppressWarnings("unchecked")
        Map<String, Object> properties = (Map<String, Object>) mappings.get("properties");

        // Verify 'host' is detected as an object type (nested)
        assertThat(properties, hasKey("host"));
        @SuppressWarnings("unchecked")
        Map<String, Object> hostMapping = (Map<String, Object>) properties.get("host");
        assertThat(hostMapping, notNullValue());
        assertThat(hostMapping.get("type"), equalTo("object"));

        // Verify 'host' has nested properties
        assertThat(hostMapping, hasKey("properties"));
        @SuppressWarnings("unchecked")
        Map<String, Object> hostProperties = (Map<String, Object>) hostMapping.get("properties");
        assertThat(hostProperties, hasKey("id"));
        assertThat(hostProperties, hasKey("category"));

        // Verify nested field types
        @SuppressWarnings("unchecked")
        Map<String, Object> idMapping = (Map<String, Object>) hostProperties.get("id");
        assertThat(idMapping.get("type"), equalTo("long"));

        @SuppressWarnings("unchecked")
        Map<String, Object> categoryMapping = (Map<String, Object>) hostProperties.get("category");
        assertThat(categoryMapping.get("type"), equalTo("keyword"));

        // Verify flat fields are also present
        assertThat(properties, hasKey("message"));
        assertThat(properties, hasKey("timestamp"));
    }

    /**
     * Test deeply nested JSON structures (3+ levels)
     */
    public void testDeeplyNestedJsonStructure() throws IOException {
        String deeplyNestedSample = """
            {"server": {"location": {"datacenter": {"name": "DC1", "region": "US-WEST"}, "rack": "A1"}, "hostname": "srv001"}, "status": "online"}
            {"server": {"location": {"datacenter": {"name": "DC2", "region": "US-EAST"}, "rack": "B2"}, "hostname": "srv002"}, "status": "online"}
            """;

        Map<String, Object> responseMap = executeAndVerifyRequest(deeplyNestedSample);

        @SuppressWarnings("unchecked")
        Map<String, Object> mappings = (Map<String, Object>) responseMap.get("mappings");
        @SuppressWarnings("unchecked")
        Map<String, Object> properties = (Map<String, Object>) mappings.get("properties");

        // Navigate through nested structure: server -> location -> datacenter
        assertThat(properties, hasKey("server"));
        @SuppressWarnings("unchecked")
        Map<String, Object> serverMapping = (Map<String, Object>) properties.get("server");
        assertThat(serverMapping.get("type"), equalTo("object"));

        @SuppressWarnings("unchecked")
        Map<String, Object> serverProps = (Map<String, Object>) serverMapping.get("properties");
        assertThat(serverProps, hasKey("location"));
        assertThat(serverProps, hasKey("hostname"));

        @SuppressWarnings("unchecked")
        Map<String, Object> locationMapping = (Map<String, Object>) serverProps.get("location");
        assertThat(locationMapping.get("type"), equalTo("object"));

        @SuppressWarnings("unchecked")
        Map<String, Object> locationProps = (Map<String, Object>) locationMapping.get("properties");
        assertThat(locationProps, hasKey("datacenter"));
        assertThat(locationProps, hasKey("rack"));

        // Verify deepest level
        @SuppressWarnings("unchecked")
        Map<String, Object> datacenterMapping = (Map<String, Object>) locationProps.get("datacenter");
        assertThat(datacenterMapping.get("type"), equalTo("object"));

        @SuppressWarnings("unchecked")
        Map<String, Object> datacenterProps = (Map<String, Object>) datacenterMapping.get("properties");
        assertThat(datacenterProps, hasKey("name"));
        assertThat(datacenterProps, hasKey("region"));
    }

    /**
     * Test mixed flat and nested fields in the same document
     */
    public void testMixedFlatAndNestedFields() throws IOException {
        String mixedSample = """
            {"id": 1, "user": {"name": "Alice", "email": "alice@example.com"}, "action": "login", "timestamp": "2024-01-01T10:00:00Z"}
            {"id": 2, "user": {"name": "Bob", "email": "bob@example.com"}, "action": "logout", "timestamp": "2024-01-01T10:05:00Z"}
            """;

        Map<String, Object> responseMap = executeAndVerifyRequest(mixedSample);

        @SuppressWarnings("unchecked")
        Map<String, Object> mappings = (Map<String, Object>) responseMap.get("mappings");
        @SuppressWarnings("unchecked")
        Map<String, Object> properties = (Map<String, Object>) mappings.get("properties");

        // Verify flat fields
        assertThat(properties, hasKey("id"));
        assertThat(properties, hasKey("action"));
        assertThat(properties, hasKey("timestamp"));

        // Verify nested field
        assertThat(properties, hasKey("user"));
        @SuppressWarnings("unchecked")
        Map<String, Object> userMapping = (Map<String, Object>) properties.get("user");
        assertThat(userMapping.get("type"), equalTo("object"));

        @SuppressWarnings("unchecked")
        Map<String, Object> userProps = (Map<String, Object>) userMapping.get("properties");
        assertThat(userProps, hasKey("name"));
        assertThat(userProps, hasKey("email"));
    }

    /**
     * Test that field stats are calculated for nested fields
     */
    public void testNestedFieldStats() throws IOException {
        String sample = """
            {"host": {"id": 1, "name": "server1"}, "count": 100}
            {"host": {"id": 2, "name": "server2"}, "count": 200}
            {"host": {"id": 3, "name": "server3"}, "count": 300}
            """;

        Map<String, Object> responseMap = executeAndVerifyRequest(sample);

        // Verify field_stats are present
        assertThat(responseMap, hasKey("field_stats"));
        @SuppressWarnings("unchecked")
        Map<String, Object> fieldStats = (Map<String, Object>) responseMap.get("field_stats");

        // Field stats should exist for both flat and nested fields
        assertThat(fieldStats, hasKey("count"));
        assertThat(fieldStats, hasKey("host.id"));
        assertThat(fieldStats, hasKey("host.name"));

        // Verify sample stats for a numeric field
        @SuppressWarnings("unchecked")
        Map<String, Object> countStats = (Map<String, Object>) fieldStats.get("count");
        assertThat(countStats, hasKey("count"));
        assertThat(countStats.get("count"), equalTo(3));
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

    private static Map<String, Object> executeAndVerifyRequest(String sample) throws IOException {
        Request request = new Request("POST", "/_text_structure/find_structure");
        request.setEntity(new StringEntity(sample, ContentType.APPLICATION_JSON));
        Response response = client().performRequest(request);
        assertOK(response);
        return entityAsMap(response);
    }
}

