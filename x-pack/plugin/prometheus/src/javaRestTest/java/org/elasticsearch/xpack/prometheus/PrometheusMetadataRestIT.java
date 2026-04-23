/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.prometheus;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.test.rest.ObjectPath;

import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

/**
 * Integration tests for the Prometheus {@code GET /_prometheus/api/v1/metadata} endpoint.
 *
 * <p>Tests focus on high-level HTTP concerns: routing, request/response format, status codes.
 * Detailed plan-building and response-parsing logic is covered by unit tests.
 */
public class PrometheusMetadataRestIT extends AbstractPrometheusRestIT {

    /**
     * Covers: HTTP envelope (status 200, JSON content type, status=="success"), prefix stripping,
     * and entry shape (type, help="", unit="") in a single round-trip.
     */
    public void testMetadataResponse() throws Exception {
        writeMetric("shape_test_metric", Map.of("job", "test"));

        Response response = client().performRequest(metadataRequest());

        assertThat(response.getStatusLine().getStatusCode(), equalTo(200));
        assertThat(response.getEntity().getContentType().getValue(), containsString("application/json"));

        ObjectPath op = ObjectPath.createFromResponse(response);
        assertThat(op.evaluate("status"), equalTo("success"));

        // Stored as "metrics.shape_test_metric" — the prefix must be stripped in the response
        assertThat(op.evaluate("data.shape_test_metric"), notNullValue());
        assertThat(op.evaluate("data.metrics\\.shape_test_metric"), nullValue());

        // Entry shape
        assertThat(op.evaluate("data.shape_test_metric.0.type"), equalTo("gauge"));
        assertThat(op.evaluate("data.shape_test_metric.0.help"), equalTo(""));
        assertThat(op.evaluate("data.shape_test_metric.0.unit"), equalTo(""));
    }

    public void testLimitTruncatesAndEmitsWarning() throws Exception {
        writeMetric("limit_metric_alpha", Map.of("job", "test"));
        writeMetric("limit_metric_beta", Map.of("job", "test"));

        Request request = metadataRequest();
        request.addParameter("limit", "1");

        ObjectPath op = ObjectPath.createFromResponse(client().performRequest(request));
        assertThat(op.evaluate("status"), equalTo("success"));
        assertThat(op.evaluate("warnings.0"), containsString("truncated"));
        @SuppressWarnings("unchecked")
        Map<String, Object> data = (Map<String, Object>) op.evaluate("data");
        assertThat(data.size(), equalTo(1));
        Map.Entry<String, Object> returnedMetric = data.entrySet().iterator().next();
        assertThat(returnedMetric.getKey(), anyOf(equalTo("limit_metric_alpha"), equalTo("limit_metric_beta")));

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> entries = (List<Map<String, Object>>) returnedMetric.getValue();
        assertThat(entries, hasSize(1));
        assertThat(entries.get(0).get("type"), equalTo("gauge"));
        assertThat(entries.get(0).get("help"), equalTo(""));
        assertThat(entries.get(0).get("unit"), equalTo(""));
    }

    public void testCustomIndexVariantRoutes() throws Exception {
        // Write to two different data streams
        writeMetricTo("testapp", "default", "scoped_in_testapp", Map.of("job", "test"));
        writeMetric("generic_only_metric", Map.of("job", "test"));

        // Scope the request to just the testapp data stream
        Request request = new Request("GET", "/_prometheus/metrics-testapp.prometheus-default/api/v1/metadata");
        addReadAuth(request);
        Response response = client().performRequest(request);

        assertThat(response.getStatusLine().getStatusCode(), equalTo(200));
        ObjectPath op = ObjectPath.createFromResponse(response);
        assertThat(op.evaluate("status"), equalTo("success"));
        assertThat(op.evaluate("data.scoped_in_testapp"), notNullValue());
        @SuppressWarnings("unchecked")
        Map<String, Object> data = (Map<String, Object>) op.evaluate("data");
        assertThat(data, not(hasKey("generic_only_metric")));
    }

    public void testMetricParameterFiltersToSingleMetricName() throws Exception {
        writeMetric("filtered_metric", Map.of("job", "test"));
        writeMetric("other_metric", Map.of("job", "test"));

        Request request = metadataRequest();
        request.addParameter("metric", "filtered_metric");

        ObjectPath op = ObjectPath.createFromResponse(client().performRequest(request));
        assertThat(op.evaluate("status"), equalTo("success"));
        assertThat(op.evaluate("data.filtered_metric"), notNullValue());

        @SuppressWarnings("unchecked")
        Map<String, Object> data = (Map<String, Object>) op.evaluate("data");
        assertThat(data.size(), equalTo(1));
        assertThat(data, not(hasKey("other_metric")));
        assertThat(op.evaluate("data.filtered_metric.0.type"), equalTo("gauge"));
        assertThat(op.evaluate("data.filtered_metric.0.help"), equalTo(""));
        assertThat(op.evaluate("data.filtered_metric.0.unit"), equalTo(""));
    }

    /**
     * The same metric stored in two data streams with different mappings (gauge vs. counter) must
     * surface as two distinct entries under the same metric name in the global metadata response.
     */
    public void testMultipleEntriesForSameMetricAcrossStreams() throws Exception {
        // Write first — the default stream's backing index is created with gauge mapping.
        writeMetric("multi_type_metric", Map.of("job", "test"));

        // Custom template maps all metrics.* as counters; only new backing indices pick this up.
        Request putCustomTemplate = new Request("PUT", "/_component_template/metrics-prometheus@custom");
        putCustomTemplate.setJsonEntity("""
            {
              "template": {
                "mappings": {
                  "dynamic_templates": [
                    {
                      "counter": {
                        "path_match": "metrics.*",
                        "mapping": {
                          "type": "double",
                          "time_series_metric": "counter"
                        }
                      }
                    }
                  ]
                }
              }
            }
            """);
        client().performRequest(putCustomTemplate);

        writeMetricTo("generic", "custom", "multi_type_metric", Map.of("job", "test"));

        ObjectPath op = ObjectPath.createFromResponse(client().performRequest(metadataRequest()));
        assertThat(op.evaluate("status"), equalTo("success"));

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> entries = (List<Map<String, Object>>) op.evaluate("data.multi_type_metric");
        assertThat(entries, hasSize(2));
        assertThat(entries.stream().map(e -> (String) e.get("type")).toList(), containsInAnyOrder("gauge", "counter"));
    }

    public void testLimitPerMetricCapsEntriesForSingleMetricName() throws Exception {
        writeMetric("limited_multi_type_metric", Map.of("job", "test"));

        Request putCustomTemplate = new Request("PUT", "/_component_template/metrics-prometheus@custom");
        putCustomTemplate.setJsonEntity("""
            {
              "template": {
                "mappings": {
                  "dynamic_templates": [
                    {
                      "counter": {
                        "path_match": "metrics.*",
                        "mapping": {
                          "type": "double",
                          "time_series_metric": "counter"
                        }
                      }
                    }
                  ]
                }
              }
            }
            """);
        client().performRequest(putCustomTemplate);

        writeMetricTo("generic", "limited", "limited_multi_type_metric", Map.of("job", "test"));

        Request request = metadataRequest();
        request.addParameter("limit_per_metric", "1");

        ObjectPath op = ObjectPath.createFromResponse(client().performRequest(request));
        assertThat(op.evaluate("status"), equalTo("success"));

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> entries = (List<Map<String, Object>>) op.evaluate("data.limited_multi_type_metric");
        assertThat(entries, hasSize(1));
        assertThat(entries.get(0).get("type"), anyOf(equalTo("gauge"), equalTo("counter")));
        assertThat(entries.get(0).get("help"), equalTo(""));
        assertThat(entries.get(0).get("unit"), equalTo(""));
    }

    /**
     * A {@code meta.unit} value set in the field mapping must be surfaced in the {@code unit} field
     * of the metadata response. The custom component template is applied to a fresh namespace so
     * its backing index is created with the explicit property mapping.
     */
    public void testUnitFromFieldMetadata() throws Exception {
        Request putCustomTemplate = new Request("PUT", "/_component_template/metrics-prometheus@custom");
        putCustomTemplate.setJsonEntity("""
            {
              "template": {
                "mappings": {
                  "properties": {
                    "metrics": {
                      "properties": {
                        "http_request_duration_seconds": {
                          "type": "double",
                          "time_series_metric": "gauge",
                          "meta": {
                            "unit": "seconds"
                          }
                        }
                      }
                    }
                  }
                }
              }
            }
            """);
        client().performRequest(putCustomTemplate);

        writeMetricTo("generic", "unitns", "http_request_duration_seconds", Map.of("job", "test"));

        ObjectPath op = ObjectPath.createFromResponse(client().performRequest(metadataRequest()));
        assertThat(op.evaluate("status"), equalTo("success"));
        assertThat(op.evaluate("data.http_request_duration_seconds.0.type"), equalTo("gauge"));
        assertThat(op.evaluate("data.http_request_duration_seconds.0.help"), equalTo(""));
        assertThat(op.evaluate("data.http_request_duration_seconds.0.unit"), equalTo("seconds"));
    }

    private Request metadataRequest() {
        Request request = new Request("GET", "/_prometheus/api/v1/metadata");
        addReadAuth(request);
        return request;
    }
}
