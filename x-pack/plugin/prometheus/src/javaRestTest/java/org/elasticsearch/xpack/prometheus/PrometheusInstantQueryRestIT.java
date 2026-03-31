/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.prometheus;

import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.test.rest.ObjectPath;
import org.elasticsearch.xpack.prometheus.proto.RemoteWrite;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;

/**
 * Integration tests for the Prometheus {@code /api/v1/query} instant query endpoint.
 */
public class PrometheusInstantQueryRestIT extends ESRestTestCase {

    private static final String USER = "test_admin";
    private static final String PASS = "x-pack-test-password";

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .user(USER, PASS, "superuser", false)
        .setting("xpack.security.enabled", "true")
        .setting("xpack.security.autoconfiguration.enabled", "false")
        .setting("xpack.license.self_generated.type", "trial")
        .setting("xpack.ml.enabled", "false")
        .setting("xpack.watcher.enabled", "false")
        .build();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @Override
    protected Settings restClientSettings() {
        String token = basicAuthHeaderValue(USER, new SecureString(PASS.toCharArray()));
        return Settings.builder().put(super.restClientSettings()).put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

    /**
     * Verifies that querying when no Prometheus indices exist returns an empty result instead of an error.
     */
    public void testInstantQueryWithNoPrometheusIndicesReturnsEmptyResult() throws Exception {
        Request request = new Request("GET", "/_prometheus/api/v1/query");
        request.addParameter("query", "nonexistent_metric");
        request.addParameter("time", "2026-01-01T00:05:00Z");

        Response response = client().performRequest(request);
        assertThat(response.getStatusLine().getStatusCode(), equalTo(200));

        ObjectPath responsePath = ObjectPath.createFromResponse(response);
        assertThat(responsePath.evaluate("status"), equalTo("success"));
        assertThat(responsePath.evaluate("data.resultType"), equalTo("vector"));
        assertThat(responsePath.evaluate("data.result"), empty());
    }

    public void testInstantQueryWithIngestedData() throws Exception {
        ingestTestData();

        ObjectPath responsePath = executeInstantQuery(null);
        assertMetricResult(responsePath);
    }

    public void testInstantQueryWithIndexPattern() throws Exception {
        ingestTestData();

        ObjectPath responsePath = executeInstantQuery("metrics-generic.prometheus-*");
        assertMetricResult(responsePath);
    }

    /**
     * Verifies that omitting the {@code time} parameter defaults to current server time without error.
     * Since test data is in the past, the result will be empty — but the request must succeed.
     */
    public void testInstantQueryWithoutTimeDefaultsToNow() throws Exception {
        ingestTestData();

        Request request = new Request("GET", "/_prometheus/api/v1/query");
        request.addParameter("query", "test_gauge_iq{job=\"test_job\"}");

        Response response = client().performRequest(request);
        assertThat(response.getStatusLine().getStatusCode(), equalTo(200));

        ObjectPath responsePath = ObjectPath.createFromResponse(response);
        assertThat(responsePath.evaluate("status"), equalTo("success"));
        assertThat(responsePath.evaluate("data.resultType"), equalTo("vector"));
        // Test data is in the past, so current-time lookback returns no results — that's expected.
        assertThat(responsePath.evaluate("data.result"), empty());
    }

    private static void assertMetricResult(ObjectPath responsePath) throws IOException {
        assertThat(responsePath.evaluate("data.result"), hasSize(1));
        assertThat(responsePath.evaluate("data.result.0.metric.job"), equalTo("test_job"));
        assertThat(responsePath.evaluate("data.result.0.metric.instance"), equalTo("localhost:9090"));

        // Instant query returns a single "value" pair, not a "values" array
        List<Object> value = responsePath.evaluate("data.result.0.value");
        assertThat(value, hasSize(2));
        assertThat(value.get(0), instanceOf(Number.class));
        assertThat(value.get(1), instanceOf(String.class));
    }

    private ObjectPath executeInstantQuery(String index) throws Exception {
        String path = index == null ? "/_prometheus/api/v1/query" : "/_prometheus/" + index + "/api/v1/query";
        Request request = new Request("GET", path);
        request.addParameter("query", "test_gauge_iq{job=\"test_job\"}");
        request.addParameter("time", "2026-01-01T00:05:00Z");

        Response response = client().performRequest(request);
        assertThat(response.getStatusLine().getStatusCode(), equalTo(200));

        ObjectPath responsePath = ObjectPath.createFromResponse(response);
        assertThat(responsePath.evaluate("status"), equalTo("success"));
        assertThat(responsePath.evaluate("data.resultType"), equalTo("vector"));
        return responsePath;
    }

    private void ingestTestData() throws IOException {
        long baseTimestamp = 1767225600000L; // 2026-01-01T00:00:00Z

        Request putCustomTemplate = new Request("PUT", "/_component_template/metrics-prometheus@custom");
        putCustomTemplate.setJsonEntity("""
            {
              "template": {
                "settings": {
                  "index": {
                    "time_series": {
                      "start_time": "2026-01-01T00:00:00Z"
                    }
                  }
                }
              }
            }
            """);
        client().performRequest(putCustomTemplate);

        RemoteWrite.WriteRequest.Builder writeRequestBuilder = RemoteWrite.WriteRequest.newBuilder();
        for (int i = 0; i < 5; i++) {
            writeRequestBuilder.addTimeseries(
                RemoteWrite.TimeSeries.newBuilder()
                    .addLabels(RemoteWrite.Label.newBuilder().setName("__name__").setValue("test_gauge_iq").build())
                    .addLabels(RemoteWrite.Label.newBuilder().setName("job").setValue("test_job").build())
                    .addLabels(RemoteWrite.Label.newBuilder().setName("instance").setValue("localhost:9090").build())
                    .addSamples(RemoteWrite.Sample.newBuilder().setValue(i * 10.0).setTimestamp(baseTimestamp + i * 60_000L).build())
                    .build()
            );
        }

        Request writeRequest = new Request("POST", "/_prometheus/api/v1/write");
        writeRequest.setEntity(
            new ByteArrayEntity(writeRequestBuilder.build().toByteArray(), ContentType.create("application/x-protobuf"))
        );
        Response writeResponse = client().performRequest(writeRequest);
        assertThat(writeResponse.getStatusLine().getStatusCode(), equalTo(204));
        if (writeResponse.getEntity() != null) {
            assertThat(EntityUtils.toString(writeResponse.getEntity()), equalTo(""));
        }

        Request refresh = new Request("POST", "/metrics-generic.prometheus-default/_refresh");
        client().performRequest(refresh);

        Request searchFailures = new Request("GET", "/metrics-generic.prometheus-default::failures/_search");
        searchFailures.setJsonEntity("""
            {
              "track_total_hits": true,
              "size": 0
            }
            """);
        ObjectPath failuresSearchPath = ObjectPath.createFromResponse(client().performRequest(searchFailures));
        Number totalFailures = failuresSearchPath.evaluate("hits.total.value");
        assertThat(totalFailures.intValue(), equalTo(0));
    }
}
