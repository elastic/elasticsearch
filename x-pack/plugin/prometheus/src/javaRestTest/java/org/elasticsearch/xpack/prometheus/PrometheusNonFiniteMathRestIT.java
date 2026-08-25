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

    public void testLog10OfNegativeIsNaN() throws Exception {
        ingestTestData("test_gauge_nf");
        assertSingleValue("log10(" + METRIC + " * -1)", "NaN");
    }

    public void testLog2OfNegativeIsNaN() throws Exception {
        ingestTestData("test_gauge_nf");
        assertSingleValue("log2(" + METRIC + " * -1)", "NaN");
    }

    /** {@code asin}/{@code acos} of an out-of-range input ({@code |x|>1}; the metric evaluates to 40) yields {@code NaN}. */
    public void testAsinOutOfRangeIsNaN() throws Exception {
        ingestTestData("test_gauge_nf");
        assertSingleValue("asin(" + METRIC + ")", "NaN");
    }

    public void testAcosOutOfRangeIsNaN() throws Exception {
        ingestTestData("test_gauge_nf");
        assertSingleValue("acos(" + METRIC + ")", "NaN");
    }

    /** {@code acosh} of an input below 1 ({@code metric * 0 == 0}) yields {@code NaN}. */
    public void testAcoshBelowOneIsNaN() throws Exception {
        ingestTestData("test_gauge_nf");
        assertSingleValue("acosh(" + METRIC + " * 0)", "NaN");
    }

    /** {@code atanh(±1)} yields {@code ±Inf} and {@code atanh(|x|>1)} yields {@code NaN} (IEEE-754). */
    public void testAtanhOfOneIsPositiveInfinity() throws Exception {
        ingestTestData("test_gauge_nf");
        assertSingleValue("atanh(" + METRIC + " / " + METRIC + ")", "+Inf");
    }

    public void testAtanhOfNegativeOneIsNegativeInfinity() throws Exception {
        ingestTestData("test_gauge_nf");
        assertSingleValue("atanh(" + METRIC + " / " + METRIC + " * -1)", "-Inf");
    }

    public void testAtanhOutOfRangeIsNaN() throws Exception {
        ingestTestData("test_gauge_nf");
        assertSingleValue("atanh(" + METRIC + ")", "NaN");
    }

    /** {@code sinh}/{@code cosh} overflow to {@code ±Inf}/{@code +Inf} instead of being dropped (metric evaluates to 40). */
    public void testSinhOverflowIsPositiveInfinity() throws Exception {
        ingestTestData("test_gauge_nf");
        assertSingleValue("sinh(" + METRIC + " * 20)", "+Inf");
    }

    public void testSinhOverflowNegativeIsNegativeInfinity() throws Exception {
        ingestTestData("test_gauge_nf");
        assertSingleValue("sinh(" + METRIC + " * -20)", "-Inf");
    }

    public void testCoshOverflowIsPositiveInfinity() throws Exception {
        ingestTestData("test_gauge_nf");
        assertSingleValue("cosh(" + METRIC + " * 20)", "+Inf");
    }

    /**
     * Prometheus {@code round(NaN)} returns {@code NaN}; the single-argument {@code round} preserves the non-finite input
     * rather than rounding it to {@code 0}.
     */
    public void testRoundOfNaNIsNaN() throws Exception {
        ingestTestData("test_gauge_nf");
        assertSingleValue("round(" + METRIC + " * NaN)", "NaN");
    }

    /**
     * Prometheus drops every series when {@code min > max} (clamp returns an empty result), see promql/functions.go.
     */
    public void testClampWithMinGreaterThanMaxDropsSeries() throws Exception {
        ingestTestData("test_gauge_nf");
        ObjectPath response = executeInstantQuery("clamp(" + METRIC + ", 100, 0)");
        assertThat(response.evaluate("data.result"), empty());
    }

    // ---------------------------------------------------------------------------------------------------------------
    // Across-series aggregations honor Prometheus/IEEE-754 non-finite semantics via the PromQL-only lenient aggregator
    // path (sum, avg, max, min, stddev, stdvar). See the matching csv-spec blocks (nonfinite_avg_propagates_infinity,
    // etc.).
    // ---------------------------------------------------------------------------------------------------------------

    /**
     * The {@code sum} aggregator propagates non-finite inputs: two positive series multiplied by {@code Inf} both become
     * {@code +Inf}, so their sum is {@code +Inf} rather than a dropped result.
     */
    public void testSumPropagatesPositiveInfinity() throws Exception {
        ingestTwoSeries("agg_sum_pos", 5.0, 7.0);
        assertSingleValue("sum(agg_sum_pos * Inf)", "+Inf");
    }

    /**
     * The {@code sum} aggregator over a {@code +Inf} and a {@code -Inf} contribution yields {@code NaN} (IEEE-754
     * {@code +Inf + -Inf}), matching Prometheus, rather than dropping the result.
     */
    public void testSumOfPositiveAndNegativeInfinityIsNaN() throws Exception {
        ingestTwoSeries("agg_sum_mixed", 5.0, -7.0);
        assertSingleValue("sum(agg_sum_mixed * Inf)", "NaN");
    }

    /** Prometheus {@code avg} propagates non-finite: the average of two {@code +Inf} series is {@code +Inf}. */
    public void testAvgPropagatesPositiveInfinity() throws Exception {
        ingestTwoSeries("agg_avg_pos", 5.0, 7.0);
        assertSingleValue("avg(agg_avg_pos * Inf)", "+Inf");
    }

    /** Prometheus {@code max} treats {@code -Inf} as an ordinary (smallest) value, so all-{@code -Inf} is {@code -Inf}. */
    public void testMaxOfAllNegativeInfinityIsNegativeInfinity() throws Exception {
        ingestTwoSeries("agg_max_neg", 5.0, 7.0);
        assertSingleValue("max(agg_max_neg * -Inf)", "-Inf");
    }

    /** Prometheus {@code max} treats {@code +Inf} as an ordinary (largest) value, so all-{@code +Inf} is {@code +Inf}. */
    public void testMaxOfAllPositiveInfinityIsPositiveInfinity() throws Exception {
        ingestTwoSeries("agg_max_pos", 5.0, 7.0);
        assertSingleValue("max(agg_max_pos * Inf)", "+Inf");
    }

    /**
     * Prometheus {@code max} skips {@code NaN} when a non-{@code NaN} value is present. {@code sqrt} of {4, -9, 16} is
     * {2, NaN, 4}, so the max is the finite {@code 4}, not {@code NaN}.
     */
    public void testMaxSkipsNaNWhenFinitePresent() throws Exception {
        ingestSeries("agg_max_skipnan", 4.0, -9.0, 16.0);
        assertSingleValue("max(sqrt(agg_max_skipnan))", "4.0");
    }

    /** Prometheus {@code max} is {@code NaN} only when every input is {@code NaN} ({@code sqrt} of two negatives). */
    public void testMaxOfAllNaNIsNaN() throws Exception {
        ingestTwoSeries("agg_max_allnan", -1.0, -4.0);
        assertSingleValue("max(sqrt(agg_max_allnan))", "NaN");
    }

    /** Prometheus {@code min} over an all-{@code -Inf} set is {@code -Inf} (mirrors the max case). */
    public void testMinOfAllNegativeInfinityIsNegativeInfinity() throws Exception {
        ingestTwoSeries("agg_min_neg", 5.0, 7.0);
        assertSingleValue("min(agg_min_neg * -Inf)", "-Inf");
    }

    /**
     * Prometheus {@code min} treats {@code +Inf} as an ordinary (largest) value: {@code min{+Inf, x} = x}. Here
     * {@code 8 / {0, 4}} is {@code {+Inf, 2}}, so the minimum is the finite {@code 2}.
     */
    public void testMinWithPositiveInfinityReturnsFinite() throws Exception {
        ingestTwoSeries("agg_min_posinf", 0.0, 4.0);
        assertSingleValue("min(8 / agg_min_posinf)", "2.0");
    }

    /**
     * Prometheus {@code min} skips {@code NaN} when a non-{@code NaN} value is present. {@code sqrt} of {4, -9, 16} is
     * {2, NaN, 4}, so the min is the finite {@code 2}, not {@code NaN}.
     */
    public void testMinSkipsNaNWhenFinitePresent() throws Exception {
        ingestSeries("agg_min_skipnan", 4.0, -9.0, 16.0);
        assertSingleValue("min(sqrt(agg_min_skipnan))", "2.0");
    }

    /** Prometheus {@code min} is {@code NaN} only when every input is {@code NaN} ({@code sqrt} of two negatives). */
    public void testMinOfAllNaNIsNaN() throws Exception {
        ingestTwoSeries("agg_min_allnan", -1.0, -4.0);
        assertSingleValue("min(sqrt(agg_min_allnan))", "NaN");
    }

    /**
     * Prometheus {@code stddev} over a set containing non-finite values is {@code NaN}: the lenient (PromQL) path
     * reports a non-finite {@code m2} as {@code NaN} rather than dropping the result to null.
     */
    public void testStddevOfNonFiniteIsNaN() throws Exception {
        ingestTwoSeries("agg_stddev", 5.0, 7.0);
        assertSingleValue("stddev(agg_stddev * Inf)", "NaN");
    }

    /**
     * Prometheus {@code stdvar} over a set containing non-finite values is {@code NaN}: the lenient (PromQL) path
     * reports a non-finite {@code m2} as {@code NaN} rather than dropping the result to null.
     */
    public void testStdvarOfNonFiniteIsNaN() throws Exception {
        ingestTwoSeries("agg_stdvar", 5.0, 7.0);
        assertSingleValue("stdvar(agg_stdvar * Inf)", "NaN");
    }

    /**
     * Writes two single-sample series ({@code instance=a} and {@code instance=b}) for {@code metricName}, both sampled
     * at the evaluation time so they fall inside the default 5m lookback window and are live for an aggregation query.
     */
    private void ingestTwoSeries(String metricName, double first, double second) throws Exception {
        ingestSeries(metricName, first, second);
    }

    /**
     * Writes one single-sample series per value (labelled {@code instance=series_<i>}) for {@code metricName}, all
     * sampled at the evaluation time so they fall inside the default 5m lookback window and are live for an aggregation
     * query. Used to build across-series aggregation inputs, including mixed finite / non-finite sets.
     */
    private void ingestSeries(String metricName, double... values) throws Exception {
        long timestampMillis = 1767226080000L; // 2026-01-01T00:08:00Z == EVAL_TIME
        RemoteWrite.WriteRequest.Builder writeRequestBuilder = RemoteWrite.WriteRequest.newBuilder();
        for (int i = 0; i < values.length; i++) {
            writeRequestBuilder.addTimeseries(
                RemoteWrite.TimeSeries.newBuilder()
                    .addLabels(label("__name__", metricName))
                    .addLabels(label("instance", "series_" + i))
                    .addSamples(sample(values[i], timestampMillis))
                    .build()
            );
        }
        ingestTestData(writeRequestBuilder.build());
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
