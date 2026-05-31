/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.prometheus;

import org.apache.http.message.BasicNameValuePair;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.test.rest.ObjectPath;
import org.elasticsearch.xpack.prometheus.proto.RemoteWrite;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;

/**
 * Integration tests for PromQL {@code histogram_quantile()} over classic histograms ingested via remote write.
 */
public class PrometheusHistogramQuantileRestIT extends AbstractPrometheusRestIT {

    private static final String METRIC = "request_duration_seconds_bucket";
    private static final String[] LE_BUCKETS = { "0.5", "1.0", "2.0", "+Inf" };
    private static final long BASE_TIMESTAMP = 1767225600000L; // 2026-01-01T00:00:00Z

    public void testHistogramQuantileRateDropsLeLabel() throws Exception {
        ingestClassicHistogram();

        ObjectPath response = executeQueryRange("histogram_quantile(0.5, rate(" + METRIC + "[1m]))");
        List<Map<String, Object>> results = response.evaluate("data.result");
        assertThat("unexpected series: " + results, results, hasSize(1));
        // histogram_quantile merges the `le` buckets into a single series: the `le` label must be gone while the
        // remaining identifying labels survive.
        Map<String, Object> metric = response.evaluate("data.result.0.metric");
        assertThat(metric, hasKey("job"));
        assertThat(metric, hasKey("instance"));
        assertThat(metric, not(hasKey("le")));
        // TODO: `rate()` should also drop the `__name__` label to match Prometheus semantics.
        // Tracked in https://github.com/elastic/elasticsearch/issues/150318.
    }

    public void testSumHistogramQuantileByIntegration() throws Exception {
        ingestClassicHistogramWithIntegration();

        ObjectPath response = executeQueryRange("sum(histogram_quantile(0.9, rate(" + METRIC + "[5m]))) by (integration)");
        assertThat(response.evaluate("data.result"), hasSize(1));
        Map<String, Object> metric = response.evaluate("data.result.0.metric");
        assertThat(metric.get("integration"), equalTo("api"));
        assertThat(metric, not(hasKey("le")));
    }

    public void testHistogramQuantileWithoutDataReturnsEmpty() throws Exception {
        ObjectPath response = executeQueryRangeOnDefaultIndex("histogram_quantile(0.5, rate(" + METRIC + "[1m]))");
        assertThat(response.evaluate("data.result"), empty());
    }

    private ObjectPath executeQueryRange(String query) throws IOException {
        return executeQueryRangeOnIndex("/_prometheus/" + DEFAULT_DATA_STREAM + "/api/v1/query_range", query);
    }

    private ObjectPath executeQueryRangeOnDefaultIndex(String query) throws IOException {
        return executeQueryRangeOnIndex("/_prometheus/api/v1/query_range", query);
    }

    private ObjectPath executeQueryRangeOnIndex(String path, String query) throws IOException {
        Request request = prometheusReadRequest(
            path,
            new BasicNameValuePair("query", query),
            new BasicNameValuePair("start", "2026-01-01T00:00:00Z"),
            new BasicNameValuePair("end", "2026-01-01T00:05:00Z"),
            new BasicNameValuePair("step", "60s")
        );

        Response response = client().performRequest(request);
        assertThat(response.getStatusLine().getStatusCode(), equalTo(200));

        ObjectPath responsePath = ObjectPath.createFromResponse(response);
        assertThat(responsePath.evaluate("status"), equalTo("success"));
        assertThat(responsePath.evaluate("data.resultType"), equalTo("matrix"));
        return responsePath;
    }

    private void ingestClassicHistogram() throws IOException {
        ingestClassicHistogram(Map.of("job", "test_job", "instance", "localhost:9090"));
    }

    private void ingestClassicHistogramWithIntegration() throws IOException {
        ingestClassicHistogram(Map.of("job", "test_job", "instance", "localhost:9090", "integration", "api"));
    }

    private void ingestClassicHistogram(Map<String, String> commonLabels) throws IOException {
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
        for (int step = 0; step < 5; step++) {
            long timestamp = BASE_TIMESTAMP + step * 60_000L;
            long cumulativeBase = 100L + step * 10L;
            long[] cumulativeCounts = { cumulativeBase / 4, cumulativeBase / 2, cumulativeBase * 3 / 4, cumulativeBase };
            for (int bucket = 0; bucket < LE_BUCKETS.length; bucket++) {
                RemoteWrite.TimeSeries.Builder series = RemoteWrite.TimeSeries.newBuilder()
                    .addLabels(label("__name__", METRIC))
                    .addLabels(label("le", LE_BUCKETS[bucket]));
                commonLabels.forEach((name, value) -> series.addLabels(label(name, value)));
                series.addSamples(sample(cumulativeCounts[bucket], timestamp));
                writeRequestBuilder.addTimeseries(series.build());
            }
        }

        Request writeRequest = new Request("POST", "/_prometheus/api/v1/write");
        writeRequest.setEntity(
            new org.apache.http.entity.ByteArrayEntity(
                writeRequestBuilder.build().toByteArray(),
                org.apache.http.entity.ContentType.create("application/x-protobuf")
            )
        );
        addWriteAuth(writeRequest);
        Response writeResponse = client().performRequest(writeRequest);
        assertThat(writeResponse.getStatusLine().getStatusCode(), equalTo(204));

        client().performRequest(new Request("POST", "/" + DEFAULT_DATA_STREAM + "/_refresh"));
    }
}
