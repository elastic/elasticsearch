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
import org.elasticsearch.test.cluster.FeatureFlag;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.test.rest.ObjectPath;
import org.elasticsearch.xpack.prometheus.proto.RemoteWrite;
import org.junit.ClassRule;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

/**
 * Integration tests for the Prometheus {@code /api/v1/query_range} endpoint.
 */
public class PrometheusQueryRangeRestIT extends ESRestTestCase {

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
        .feature(FeatureFlag.PROMETHEUS_FEATURE_FLAG)
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

    public void testQueryRangeWithIngestedData() throws Exception {
        ingestTestData();

        ObjectPath responsePath = executeQueryRange();
        assertMetricResults(responsePath);
    }

    private static void assertMetricResults(ObjectPath responsePath) throws IOException {
        assertThat(responsePath.evaluate("data.result"), hasSize(1));
        assertThat(responsePath.evaluate("data.result.0.metric.job"), equalTo("test_job"));
        assertThat(responsePath.evaluate("data.result.0.metric.instance"), equalTo("localhost:9090"));
        assertThat(responsePath.evaluate("data.result.0.values"), hasSize(5));
    }

    private ObjectPath executeQueryRange() throws Exception {
        Request request = new Request("GET", "/_prometheus/api/v1/query_range");
        request.addParameter("query", "test_gauge_qr{job=\"test_job\"}");
        request.addParameter("start", "2026-01-01T00:00:00Z");
        request.addParameter("end", "2026-01-01T00:05:00Z");
        request.addParameter("step", "60s");

        Response response = client().performRequest(request);
        assertThat(response.getStatusLine().getStatusCode(), equalTo(200));

        ObjectPath responsePath = ObjectPath.createFromResponse(response);
        assertThat(responsePath.evaluate("status"), equalTo("success"));
        assertThat(responsePath.evaluate("data.resultType"), equalTo("matrix"));
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
                    .addLabels(RemoteWrite.Label.newBuilder().setName("__name__").setValue("test_gauge_qr").build())
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
            // A non-empty body would contain partial failure details from the underlying bulk request.
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
