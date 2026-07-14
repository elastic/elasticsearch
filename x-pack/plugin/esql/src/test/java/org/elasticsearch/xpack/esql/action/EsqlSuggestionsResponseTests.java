/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.common.Strings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.esql.action.EsqlSuggestionsResponse.Warning;
import org.elasticsearch.xpack.esql.action.FieldSuggestion.RangeSuggestion;
import org.elasticsearch.xpack.esql.action.FieldSuggestion.ValueSuggestion;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class EsqlSuggestionsResponseTests extends ESTestCase {

    public void testToXContentShape() {
        Map<String, FieldSuggestion> fields = new LinkedHashMap<>();
        fields.put("agent.keyword", new FieldSuggestion("keyword", List.of(new ValueSuggestion("ask", 650)), null));
        fields.put("@timestamp", new FieldSuggestion("date", null, new RangeSuggestion("2024-01-01T00:00:00Z", "2024-12-31T23:59:59Z")));
        fields.put("response_time", new FieldSuggestion("long", null, new RangeSuggestion(0, 50000)));
        fields.put("body", FieldSuggestion.ofType("text"));

        EsqlSuggestionsResponse response = new EsqlSuggestionsResponse(fields, List.of(), 5L, 1000L);

        String json = toJson(response);
        assertEquals("""
            {
              "took" : 5,
              "sampled_doc_count" : 1000,
              "fields" : {
                "agent.keyword" : {
                  "type" : "keyword",
                  "values" : [
                    {
                      "value" : "ask",
                      "doc_count" : 650
                    }
                  ]
                },
                "@timestamp" : {
                  "type" : "date",
                  "range" : {
                    "min" : "2024-01-01T00:00:00Z",
                    "max" : "2024-12-31T23:59:59Z"
                  }
                },
                "response_time" : {
                  "type" : "long",
                  "range" : {
                    "min" : 0,
                    "max" : 50000
                  }
                },
                "body" : {
                  "type" : "text"
                }
              },
              "warnings" : [ ]
            }""", json);
    }

    public void testToXContentOmitsSampledDocCountWhenAbsent() {
        EsqlSuggestionsResponse response = new EsqlSuggestionsResponse(Map.of("body", FieldSuggestion.ofType("text")), List.of());
        String json = toJson(response);
        assertFalse(json, json.contains("sampled_doc_count"));
        assertTrue(json, json.contains("\"took\" : 0"));
    }

    public void testWarningsWireNames() {
        EsqlSuggestionsResponse response = new EsqlSuggestionsResponse(
            Map.of("body", FieldSuggestion.ofType("text")),
            List.of(Warning.DLS_ACTIVE, Warning.SKIPPED_COLD, Warning.SHARDS_SKIPPED, Warning.FALSE_POSITIVES_POSSIBLE, Warning.TIMED_OUT)
        );
        String json = toJson(response);
        assertTrue(json, json.contains("\"dls_active\""));
        assertTrue(json, json.contains("\"skipped_cold\""));
        assertTrue(json, json.contains("\"shards_skipped\""));
        assertTrue(json, json.contains("\"false_positives_possible\""));
        assertTrue(json, json.contains("\"timed_out\""));
    }

    private static String toJson(EsqlSuggestionsResponse response) {
        try {
            var builder = XContentFactory.jsonBuilder().prettyPrint();
            response.toXContent(builder, org.elasticsearch.xcontent.ToXContent.EMPTY_PARAMS);
            return Strings.toString(builder);
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }
}
