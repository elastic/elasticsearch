/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.prometheus;

import org.apache.http.message.BasicNameValuePair;
import org.elasticsearch.client.Request;
import org.elasticsearch.test.rest.ObjectPath;

import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

/** Checks late label projection against real shard mappings, including aliases with no predictable prefix. */
public class PromqlLabelProjectionRestIT extends AbstractPrometheusRestIT {
    public void testWithoutAfterRankingResolvesSourceAliases() throws Exception {
        String index = "promql-label-projection";
        readApiKey = createPrometheusReadApiKey("projection-read", index);
        Request create = new Request("PUT", "/" + index);
        create.setJsonEntity("""
            {
              "settings": {
                "index.mode": "time_series",
                "index.routing_path": ["attributes.*"],
                "index.time_series.start_time": "2026-01-01T00:00:00Z",
                "index.time_series.end_time": "2026-01-02T00:00:00Z"
              },
              "mappings": {
                "properties": {
                  "@timestamp": {"type": "date"},
                  "attributes": {
                    "type": "passthrough", "priority": 10, "time_series_dimension": true,
                    "properties": {"cpu": {"type": "keyword"}, "zone": {"type": "keyword"}}
                  },
                  "processor": {"type": "alias", "path": "attributes.cpu"},
                  "load": {"type": "double", "time_series_metric": "gauge"}
                }
              }
            }
            """);
        assertOK(client().performRequest(create));
        String[] cpus = { "a", "b", "c" };
        String[] zones = { "prod", "prod", "qa" };
        int[] values = { 10, 30, 12 };
        for (int i = 0; i < cpus.length; i++) {
            Request doc = new Request("POST", "/" + index + "/_doc");
            doc.setJsonEntity(String.format(Locale.ROOT, """
                {"@timestamp":"2026-01-01T00:04:00Z","attributes":{"cpu":"%s","zone":"%s"},"load":%d}
                """, cpus[i], zones[i], values[i]));
            assertOK(client().performRequest(doc));
        }
        assertOK(client().performRequest(new Request("POST", "/" + index + "/_refresh")));

        for (String excluded : List.of("cpu", "processor", "attributes.cpu")) {
            assertGroups(index, "sum without (" + excluded + ") (load)", Map.of("prod", 40.0, "qa", 12.0));
            assertGroups(index, "sum without (" + excluded + ") (topk(2, load))", Map.of("prod", 30.0, "qa", 12.0));
            assertGroups(index, "sum without (" + excluded + ") (topk(1, topk(2, load)))", Map.of("prod", 30.0));
        }
    }

    private void assertGroups(String index, String query, Map<String, Double> expected) throws Exception {
        Request request = prometheusReadRequest(
            "/_prometheus/" + index + "/api/v1/query",
            new BasicNameValuePair("query", query),
            new BasicNameValuePair("time", "2026-01-01T00:05:00Z")
        );
        ObjectPath response = ObjectPath.createFromResponse(client().performRequest(request));
        assertThat(response.evaluate("status"), equalTo("success"));
        List<Map<String, Object>> rows = response.evaluate("data.result");
        Map<String, Double> actual = new HashMap<>();
        for (int i = 0; i < rows.size(); i++) {
            Map<String, String> labels = response.evaluate("data.result." + i + ".metric");
            assertEquals(query + ": " + labels, 1, labels.size());
            String zone = labels.get("attributes.zone");
            assertNotNull(query + ": " + labels, zone);
            String value = response.evaluate("data.result." + i + ".value.1");
            assertNull(query, actual.put(zone, Double.valueOf(value)));
        }
        assertThat(query, actual, equalTo(expected));
    }
}
