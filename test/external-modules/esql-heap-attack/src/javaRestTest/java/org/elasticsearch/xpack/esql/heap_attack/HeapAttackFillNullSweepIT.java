/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.heap_attack;

import com.carrotsearch.randomizedtesting.annotations.TimeoutSuite;

import org.apache.lucene.tests.util.TimeUnits;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.core.Booleans;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;

import static org.elasticsearch.common.xcontent.support.XContentMapValues.extractValue;

/**
 * Manual benchmark, run with {@code -Dtests.heap_attack.fillnull.sweep=true}: logs a {@code FILLNULL_SWEEP} line per query while sweeping
 * the column count N of {@code FROM idx | FILLNULL 0 ON *}, lifting {@code esql.query.max_fillnull_fields} meanwhile. Keep each query
 * under 5 minutes: past that, {@link #query} triggers this suite's out-of-memory backstop, which kills a node.
 */
@TimeoutSuite(millis = 180 * TimeUnits.MINUTE)
public class HeapAttackFillNullSweepIT extends HeapAttackFillNullTestCase {
    private static final String SWEEP = "tests.heap_attack.fillnull.sweep";
    private static final String SWEEP_FIELDS = SWEEP + ".fields";
    private static final String SWEEP_DOCS = SWEEP + ".docs";
    private static final String SWEEP_SHAPES = SWEEP + ".shapes";
    private static final String SWEEP_MAX_SECONDS = SWEEP + ".max_seconds";
    private static final String SWEEP_MIXED_MAX_FIELDS = SWEEP + ".mixed_max_fields";
    /** Mixed first: it stops at the lower {@code mixed_max_fields}, so it finishes before the all-null run is allowed to hit the wall. */
    private static final String DEFAULT_SHAPES = "MIXED_NULL,ALL_NULL";

    private static final String PROFILE_FILTER = String.join(
        ",",
        "took",
        "profile.query.took_nanos",
        "profile.planning.took_nanos",
        "profile.planning.stop_millis",
        "profile.analysis.took_nanos",
        "profile.plans.description",
        "profile.plans.logical_optimization_nanos",
        "profile.plans.physical_optimization_nanos",
        "profile.drivers.description",
        "profile.drivers.start_millis",
        "profile.drivers.took_nanos",
        "profile.drivers.cpu_nanos",
        "profile.drivers.operators.operator",
        "profile.drivers.operators.status.pages_emitted",
        "profile.drivers.operators.status.rows_emitted"
    );

    /** Set once a node stops answering: every later number would come from a different cluster. */
    private boolean aborted;

    public void testFieldCountSweep() throws Exception {
        assumeTrue("set -D" + SWEEP + "=true to run the FILLNULL sweep", Booleans.parseBoolean(System.getProperty(SWEEP, "false")));
        int docs = Integer.parseInt(System.getProperty(SWEEP_DOCS, "1000"));
        long maxMillis = Long.parseLong(System.getProperty(SWEEP_MAX_SECONDS, "120")) * 1000;
        int mixedMaxFields = Integer.parseInt(System.getProperty(SWEEP_MIXED_MAX_FIELDS, "8000"));
        List<Integer> fieldCounts = Arrays.stream(
            System.getProperty(SWEEP_FIELDS, "10,30,100,250,500,1000,2000,4000,8000,16000").split(",")
        ).map(s -> Integer.parseInt(s.trim())).toList();
        List<Shape> shapes = Arrays.stream(System.getProperty(SWEEP_SHAPES, DEFAULT_SHAPES).split(","))
            .map(s -> Shape.valueOf(s.trim()))
            .toList();

        allowFillNullOnAll(Collections.max(fieldCounts));
        for (Shape shape : shapes) {
            for (int fields : fieldCounts) {
                if (aborted || (shape == Shape.MIXED_NULL && fields > mixedMaxFields)) {
                    break;
                }
                if (sweepOne(shape, fields, docs, maxMillis) == false) {
                    break;
                }
            }
        }
    }

    /** Runs every variant at one N; returns whether the sweep may continue to a larger N. */
    private boolean sweepOne(Shape shape, int fields, int docs, long maxMillis) throws IOException {
        try {
            long start = System.nanoTime();
            try {
                initWideIndex(fields, docs, shape);
            } catch (ResponseException e) {
                log(shape, fields, docs, "index", -1, "failed:" + errorType(e), Map.of());
                return false;
            } catch (IOException e) {
                aborted = true;
                log(shape, fields, docs, "index", -1, "io_error:" + describe(e), Map.of());
                return false;
            }
            log(shape, fields, docs, "index", (System.nanoTime() - start) / 1_000_000, "ok", Map.of());
            StringBuilder control = startQuery().append("FROM ").append(INDEX).append(" | LIMIT 0\"}");
            if (measure(shape, fields, docs, "control", control.toString(), false, maxMillis) == false) {
                return false;
            }
            if (measure(shape, fields, docs, "baseline", wideQuery(false, docs, false), false, maxMillis)) {
                measure(shape, fields, docs, "baseline_profile", wideQuery(false, docs, true), true, maxMillis);
            }
            if (measure(shape, fields, docs, "fillnull", wideQuery(true, docs, false), false, maxMillis) == false) {
                return false;
            }
            if (measure(shape, fields, docs, "fillnull_profile", wideQuery(true, docs, true), true, maxMillis) == false) {
                return false;
            }
            return measure(shape, fields, docs, "fillnull_sort_profile", sortedFillNullQuery("*", "sort_key", docs, true), true, maxMillis);
        } finally {
            if (aborted == false) {
                try {
                    deleteIndex(INDEX);
                } catch (IOException e) {
                    logger.warn("could not delete [{}] after N={}", INDEX, fields, e);
                }
            }
        }
    }

    /** Runs one query under a heap/breaker sampler and logs it; returns whether it succeeded within {@code maxMillis}. */
    private boolean measure(Shape shape, int fields, int docs, String variant, String body, boolean profile, long maxMillis)
        throws IOException {
        Map<String, Object> metrics = new LinkedHashMap<>();
        String outcome;
        long wallMillis;
        try (Sampler sampler = new Sampler()) {
            long start = System.nanoTime();
            try {
                Response response = query(body, profile ? PROFILE_FILTER : "took");
                wallMillis = (System.nanoTime() - start) / 1_000_000;
                Map<String, Object> map = responseAsMap(response);
                metrics.put("took_ms", map.get("took"));
                if (profile) {
                    profileMetrics(map, metrics);
                }
                outcome = wallMillis > maxMillis ? "over_budget" : "ok";
            } catch (ResponseException e) {
                wallMillis = (System.nanoTime() - start) / 1_000_000;
                outcome = "failed:" + errorType(e);
            }
            sampler.stop();
            metrics.put("heap_peak_mb", sampler.maxHeapBytes / (1024 * 1024));
            metrics.put("breaker_peak_mb", sampler.maxBreakerBytes / (1024 * 1024));
            metrics.put("gc_ms", sampler.gcMillisAfter - sampler.gcMillisBefore);
            if (sampler.failure != null) {
                metrics.put("sampler_error", describe(sampler.failure));
            }
        } catch (IOException e) {
            aborted = true;
            log(shape, fields, docs, variant, -1, "io_error:" + describe(e), metrics);
            return false;
        }
        log(shape, fields, docs, variant, wallMillis, outcome, metrics);
        return outcome.equals("ok");
    }

    /**
     * Splits the profile into coordinator planning, the time from the end of planning until the {@code final} driver is built (physical
     * optimization plus local planning) and one entry per driver kind. With {@code FROM | ... | LIMIT} the limit is pushed below the
     * {@code COALESCE}s, so they run in the {@code final} driver rather than on the data node.
     */
    @SuppressWarnings("unchecked")
    private static void profileMetrics(Map<String, Object> map, Map<String, Object> metrics) {
        long planningStop = number(extractValue(map, "profile", "planning", "stop_millis"));
        metrics.put("query_ms", number(extractValue(map, "profile", "query", "took_nanos")) / 1_000_000);
        metrics.put("planning_ms", number(extractValue(map, "profile", "planning", "took_nanos")) / 1_000_000);
        metrics.put("analysis_ms", number(extractValue(map, "profile", "analysis", "took_nanos")) / 1_000_000);
        List<Map<String, Object>> plans = (List<Map<String, Object>>) extractValue(map, "profile", "plans");
        for (Map<String, Object> plan : plans == null ? List.<Map<String, Object>>of() : plans) {
            String description = String.valueOf(plan.get("description"));
            metrics.put(description + "_logical_opt_ms", number(plan.get("logical_optimization_nanos")) / 1_000_000);
            metrics.put(description + "_physical_opt_ms", number(plan.get("physical_optimization_nanos")) / 1_000_000);
        }

        Map<String, long[]> byDescription = new LinkedHashMap<>();
        long finalStart = Long.MAX_VALUE;
        long rowsPerPage = -1;
        List<Map<String, Object>> drivers = (List<Map<String, Object>>) extractValue(map, "profile", "drivers");
        for (Map<String, Object> driver : drivers == null ? List.<Map<String, Object>>of() : drivers) {
            String description = String.valueOf(driver.get("description"));
            List<Map<String, Object>> operators = (List<Map<String, Object>>) driver.getOrDefault("operators", List.of());
            // max wall time, total cpu, operator count
            long[] totals = byDescription.computeIfAbsent(description, k -> new long[3]);
            totals[0] = Math.max(totals[0], number(driver.get("took_nanos")));
            totals[1] += number(driver.get("cpu_nanos"));
            totals[2] = Math.max(totals[2], operators.size());
            if (description.equals("final")) {
                finalStart = Math.min(finalStart, number(driver.get("start_millis")));
            }
            if (description.equals("data") && rowsPerPage < 0 && operators.isEmpty() == false) {
                long pages = number(extractValue(operators.get(0), "status", "pages_emitted"));
                if (pages > 0) {
                    rowsPerPage = number(extractValue(operators.get(0), "status", "rows_emitted")) / pages;
                }
            }
        }
        metrics.put("final_driver_ready_ms", finalStart == Long.MAX_VALUE || planningStop == 0 ? -1 : finalStart - planningStop);
        byDescription.forEach((description, totals) -> {
            metrics.put(description + "_driver_ms", totals[0] / 1_000_000);
            metrics.put(description + "_cpu_ms", totals[1] / 1_000_000);
            metrics.put(description + "_operators", totals[2]);
        });
        metrics.put("data_rows_per_page", rowsPerPage);
    }

    private static long number(Object value) {
        return value instanceof Number n ? n.longValue() : 0;
    }

    private void log(Shape shape, int fields, int docs, String variant, long wallMillis, String outcome, Map<String, Object> metrics) {
        StringJoiner line = new StringJoiner(" ");
        line.add("shape=" + shape).add("fields=" + fields).add("docs=" + docs).add("variant=" + variant);
        line.add("wall_ms=" + wallMillis).add("outcome=" + outcome);
        metrics.forEach((k, v) -> line.add(k + "=" + v));
        logger.info("FILLNULL_SWEEP {}", line);
    }

    private static String errorType(ResponseException e) {
        try {
            return String.valueOf(extractValue(responseAsMap(e.getResponse()), "error", "type"));
        } catch (IOException ioe) {
            return "status_" + e.getResponse().getStatusLine().getStatusCode();
        }
    }

    private static String describe(Exception e) {
        String message = String.valueOf(e.getMessage());
        return e.getClass().getSimpleName() + "(" + (message.length() > 160 ? message.substring(0, 160) : message) + ")";
    }

    /** Samples request-breaker and heap usage on every node while a query runs; breakers expose no peak of their own. */
    private static final class Sampler implements AutoCloseable {
        private static final String STATS = "/_nodes/stats/jvm,breaker";
        private static final String STATS_FILTER =
            "nodes.*.jvm.mem.heap_used_in_bytes,nodes.*.jvm.gc.collectors.*.collection_time_in_millis,"
                + "nodes.*.breakers.request.estimated_size_in_bytes";

        private final Thread thread;
        private volatile boolean stopped;
        private volatile long maxHeapBytes;
        private volatile long maxBreakerBytes;
        private volatile Exception failure;
        private final long gcMillisBefore;
        private long gcMillisAfter;

        Sampler() throws IOException {
            gcMillisBefore = sample();
            thread = new Thread(() -> {
                while (stopped == false) {
                    try {
                        sample();
                        Thread.sleep(200);
                    } catch (IOException | RuntimeException | InterruptedException e) {
                        failure = e;
                        return;
                    }
                }
            }, "fillnull-sweep-sampler");
            thread.setDaemon(true);
            thread.start();
        }

        /** Waits for the sampling thread so the final sample does not race it, then takes that sample. */
        void stop() throws IOException {
            close();
            gcMillisAfter = sample();
        }

        /** Records one sample and returns the total GC time across all nodes and collectors. */
        @SuppressWarnings("unchecked")
        private synchronized long sample() throws IOException {
            Request request = new Request("GET", STATS);
            request.addParameter("filter_path", STATS_FILTER);
            Map<String, Object> nodes = (Map<String, Object>) responseAsMap(adminClient().performRequest(request)).get("nodes");
            long gcMillis = 0;
            for (Object node : nodes.values()) {
                Map<?, ?> stats = (Map<?, ?>) node;
                maxHeapBytes = Math.max(maxHeapBytes, number(extractValue(stats, "jvm", "mem", "heap_used_in_bytes")));
                maxBreakerBytes = Math.max(maxBreakerBytes, number(extractValue(stats, "breakers", "request", "estimated_size_in_bytes")));
                if (extractValue(stats, "jvm", "gc", "collectors") instanceof Map<?, ?> collectors) {
                    for (Object collector : collectors.values()) {
                        gcMillis += number(((Map<?, ?>) collector).get("collection_time_in_millis"));
                    }
                }
            }
            return gcMillis;
        }

        @Override
        public void close() {
            stopped = true;
            try {
                thread.join();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
