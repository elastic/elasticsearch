/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.tools;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class ProfileParserTests extends ESTestCase {
    public void testThreadDurationUsesDriverTime() throws IOException {
        JsonNode event = parseDriverEvent("\"driver_nanos\": 7000,");
        assertEquals(7.0, event.get("tdur").doubleValue(), 0.0);
        assertEquals(1000L, event.get("args").get("cpu_nanos").longValue());
        assertEquals(7000L, event.get("args").get("driver_nanos").longValue());
    }

    public void testThreadDurationUsesLegacyCpuTime() throws IOException {
        JsonNode event = parseDriverEvent("");
        assertEquals(1.0, event.get("tdur").doubleValue(), 0.0);
        assertEquals(1000L, event.get("args").get("cpu_nanos").longValue());
        assertFalse(event.get("args").has("driver_nanos"));
    }

    public void testZeroDriverTimeDoesNotFallBackToCpuTime() throws IOException {
        JsonNode event = parseDriverEvent("\"driver_nanos\": 0,");
        assertEquals(0.0, event.get("tdur").doubleValue(), 0.0);
        assertEquals(0L, event.get("args").get("driver_nanos").longValue());
    }

    private JsonNode parseDriverEvent(String driverNanosField) throws IOException {
        String response = """
            {
              "profile": {
                "drivers": [{
                  "description": "data",
                  "cluster_name": "test",
                  "node_name": "node",
                  "start_millis": 1,
                  "stop_millis": 2,
                  "took_nanos": 10000,
                  "cpu_nanos": 1000,
                  %s
                  "iterations": 1,
                  "operators": [],
                  "sleeps": {"counts": {}}
                }]
              }
            }
            """.formatted(driverNanosField);
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        try (
            ByteArrayInputStream input = new ByteArrayInputStream(response.getBytes(StandardCharsets.UTF_8));
            XContentBuilder builder = new XContentBuilder(JsonXContent.jsonXContent, output)
        ) {
            ProfileParser.parseProfile(ProfileParser.readProfileFromResponse(input), builder);
        }
        JsonNode events = new ObjectMapper().readTree(output.toByteArray()).get("traceEvents");
        for (JsonNode event : events) {
            if ("X".equals(event.get("ph").textValue())) {
                return event;
            }
        }
        throw new AssertionError("missing driver event");
    }
}
