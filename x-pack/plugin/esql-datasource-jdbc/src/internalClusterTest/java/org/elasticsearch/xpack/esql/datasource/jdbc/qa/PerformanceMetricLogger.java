/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.jdbc.qa;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Emits the JDBC connector's performance-baseline measurements as fixed-shape, grep-friendly
 * {@code INFO} log lines. It exists so the {@link PostgresPerformanceIT} suite can <b>log</b> throughput / latency /
 * memory / concurrency / pushdown numbers for offline trend analysis <b>without ever asserting them</b>:
 * performance thresholds are brittle in shared CI and are therefore never turned into test assertions.
 * <p>
 * <b>Line shape.</b> Every line starts with a constant, easily-greppable prefix and a small set of leading keys, then
 * appends the measurement-specific key/value pairs in the exact order the caller supplies them:
 *
 * <pre>{@code
 * jdbc.perf db=postgres query=full_scan rows=100000 elapsed_ms=1234 throughput_rows_per_sec=81037.10
 * jdbc.perf db=postgres query=filtered_latency queries=1000 p50_ms=3.412 p95_ms=8.771 p99_ms=15.004
 * }</pre>
 *
 * A downstream parser (a Vega dashboard, an Elasticsearch ingest pipeline, or a plain {@code grep 'jdbc.perf'}) sees
 * one uniform {@code key=value} shape across every measurement. Values that could not be taken (e.g. an unreachable
 * circuit-breaker API) are logged verbatim as {@code n/a} rather than omitted, so a field never silently disappears.
 * <p>
 * The logger is deliberately tiny and framework-light (log4j2 only, like {@code ESTestCase#logger}) so a later PR can
 * lift it into a {@code qa/server} module alongside the rest of the harness. {@code PostgresPerformanceIT} enables
 * this class's logger at {@code INFO} via {@code @TestLogging} so the lines are captured in the test output.
 */
public final class PerformanceMetricLogger {

    /** Constant leading token on every emitted line; grep for this to collect a run's metrics. */
    public static final String PREFIX = "jdbc.perf";

    /** Sentinel value logged for a metric that could not be measured (never omit a declared key). */
    public static final String NOT_AVAILABLE = "n/a";

    private static final Logger logger = LogManager.getLogger(PerformanceMetricLogger.class);

    private final String db;

    /**
     * @param db the database label that tags every line (e.g. {@code postgres}); appears as {@code db=<db>}.
     */
    public PerformanceMetricLogger(String db) {
        this.db = db;
    }

    /**
     * Logs one measurement as {@code jdbc.perf db=<db> query=<queryId> <k>=<v> ...}. The {@code metrics} map's
     * iteration order is preserved in the line, so callers should pass a {@link LinkedHashMap} (see {@link #metrics()}).
     */
    public void log(String queryId, Map<String, Object> metrics) {
        StringBuilder sb = new StringBuilder(PREFIX);
        sb.append(" db=").append(db);
        sb.append(" query=").append(queryId);
        for (Map.Entry<String, Object> e : metrics.entrySet()) {
            sb.append(' ').append(e.getKey()).append('=').append(render(e.getValue()));
        }
        logger.info(sb.toString());
    }

    /** A fresh, order-preserving builder for a measurement's key/value pairs. */
    public static MetricsBuilder metrics() {
        return new MetricsBuilder();
    }

    private static String render(Object value) {
        if (value == null) {
            return NOT_AVAILABLE;
        }
        if (value instanceof Double d) {
            if (d.isNaN() || d.isInfinite()) {
                return NOT_AVAILABLE;
            }
            // Fixed 2-3 dp keeps the lines stable and parseable regardless of locale.
            return String.format(Locale.ROOT, "%.3f", d);
        }
        return String.valueOf(value);
    }

    /**
     * Nearest-rank percentile over {@code samples} (nanoseconds), returned in milliseconds. {@code p} is in
     * {@code [0, 100]}. Returns {@link Double#NaN} for an empty sample set (rendered as {@code n/a}). The input array
     * is sorted in place by the caller before repeated calls; this method assumes {@code sorted} is already ascending.
     */
    public static double percentileMillis(long[] sortedNanos, double p) {
        if (sortedNanos.length == 0) {
            return Double.NaN;
        }
        int rank = (int) Math.ceil(p / 100.0 * sortedNanos.length);
        int idx = Math.min(Math.max(rank - 1, 0), sortedNanos.length - 1);
        return sortedNanos[idx] / 1_000_000.0;
    }

    /** rows / (elapsedMillis / 1000), guarding against a zero elapsed. */
    public static double throughputPerSec(long count, long elapsedMillis) {
        double seconds = elapsedMillis / 1000.0;
        if (seconds <= 0) {
            return Double.NaN;
        }
        return count / seconds;
    }

    /** A tiny order-preserving key/value accumulator so a measurement reads as a fluent chain. */
    public static final class MetricsBuilder {
        private final Map<String, Object> values = new LinkedHashMap<>();

        private MetricsBuilder() {}

        public MetricsBuilder put(String key, Object value) {
            values.put(key, value);
            return this;
        }

        public Map<String, Object> build() {
            return values;
        }

        /** Keys accumulated so far, for a caller that wants to log the same field set on multiple lines. */
        public List<String> keys() {
            return new ArrayList<>(values.keySet());
        }
    }
}
