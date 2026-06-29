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

import java.util.List;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

/**
 * Integration tests asserting that the Prometheus {@code /api/v1/query} endpoint returns series whose values evaluate to
 * the non-finite IEEE-754 specials ({@code NaN}, {@code +Inf}, {@code -Inf}) instead of dropping them, matching
 * Prometheus. This exercises the full path: PromQL translation, per-series evaluation, and the Prometheus JSON encoding
 * of non-finite values (see {@code PrometheusQueryResponseListener#formatSampleValue}). See issue #151972.
 */
public class PrometheusNonFiniteMathRestIT extends AbstractPrometheusRestIT {

    private static final String METRIC = "test_gauge_nf{job=\"test_job\"}";
    // Evaluation time T = 00:08:00; default lookback = 5m so the latest in-window sample value is 40.0.
    private static final String EVAL_TIME = "2026-01-01T00:08:00Z";
    private static final double EVAL_TIMESTAMP = 1767226080.0; // = 2026-01-01T00:08:00Z

    public void testMetricTimesPositiveInfinity() throws Exception {
        ingestTestData("test_gauge_nf");
        assertSingleValue(METRIC + " * Inf", "+Inf");
    }

    public void testMetricTimesNegativeInfinity() throws Exception {
        ingestTestData("test_gauge_nf");
        assertSingleValue(METRIC + " * -Inf", "-Inf");
    }

    public void testMetricTimesNaN() throws Exception {
        ingestTestData("test_gauge_nf");
        assertSingleValue(METRIC + " * NaN", "NaN");
    }

    public void testDivisionByZeroIsPositiveInfinity() throws Exception {
        ingestTestData("test_gauge_nf");
        assertSingleValue(METRIC + " / 0", "+Inf");
    }

    public void testModuloByZeroIsNaN() throws Exception {
        ingestTestData("test_gauge_nf");
        assertSingleValue(METRIC + " % 0", "NaN");
    }

    public void testSqrtOfNegativeIsNaN() throws Exception {
        ingestTestData("test_gauge_nf");
        assertSingleValue("sqrt(" + METRIC + " * -1)", "NaN");
    }

    public void testLnOfNegativeIsNaN() throws Exception {
        ingestTestData("test_gauge_nf");
        assertSingleValue("ln(" + METRIC + " * -1)", "NaN");
    }

    /**
     * Prometheus drops every series when {@code min > max} (clamp returns an empty result), see promql/functions.go.
     */
    public void testClampWithMinGreaterThanMaxDropsSeries() throws Exception {
        ingestTestData("test_gauge_nf");
        ObjectPath response = executeInstantQuery("clamp(" + METRIC + ", 100, 0)");
        assertThat(response.evaluate("data.result"), empty());
    }

    private void assertSingleValue(String query, String expectedValue) throws Exception {
        ObjectPath response = executeInstantQuery(query);
        assertThat(response.evaluate("data.result"), hasSize(1));
        assertThat(response.evaluate("data.result.0.value"), equalTo(List.of(EVAL_TIMESTAMP, expectedValue)));
    }

    private ObjectPath executeInstantQuery(String query) throws Exception {
        Request request = prometheusReadRequest(
            "/_prometheus/api/v1/query",
            new BasicNameValuePair("query", query),
            new BasicNameValuePair("time", EVAL_TIME)
        );
        Response response = client().performRequest(request);
        assertThat(response.getStatusLine().getStatusCode(), equalTo(200));

        ObjectPath responsePath = ObjectPath.createFromResponse(response);
        assertThat(responsePath.evaluate("status"), equalTo("success"));
        assertThat(responsePath.evaluate("data.resultType"), equalTo("vector"));
        return responsePath;
    }
}
