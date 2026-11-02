/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.grpc;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.WarningsHandler;
import org.elasticsearch.test.TestClustersThreadFilter;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xpack.esql.datasources.Federation;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

/**
 * Runs queries against a running satellite (a Flight server that implements the satellite protocol and serves the acceptance
 * suite's {@code employees} table) with the pushdown on and off, and expects the same result. Skipped unless
 * {@code -Dtests.satellite.endpoint=localhost:PORT} names the satellite; the satellite's own log shows what it received.
 */
@ThreadLeakFilters(filters = TestClustersThreadFilter.class)
public class SatellitePushdownIT extends ESRestTestCase {

    private static final String ENDPOINT = System.getProperty("tests.satellite.endpoint");

    /** The directory the satellite serves ({@code -Dtests.satellite.data=DIR}): ES reads its CSV files natively to compare. */
    private static final String DATA_DIR = System.getProperty("tests.satellite.data");

    @ClassRule
    public static ElasticsearchCluster cluster = cluster();

    private static ElasticsearchCluster cluster() {
        var builder = ElasticsearchCluster.local()
            .distribution(DistributionType.DEFAULT)
            .nodes(2)
            .setting("xpack.security.enabled", "false")
            .setting("xpack.license.self_generated.type", "trial")
            .setting(Federation.FEDERATION_ENABLED.getKey(), "true")
            .setting("xpack.ml.enabled", "false")
            .jvmArg("--add-opens=java.base/java.nio=ALL-UNNAMED")
            .jvmArg("-Darrow.allocation.manager.type=Unsafe");
        if (DATA_DIR != null) {
            builder.setting("path.repo", DATA_DIR).setting("esql.external.local_allowed_paths", DATA_DIR);
        }
        return builder.build();
    }

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    private static final List<String> ORDERED = List.of(
        "WHERE salary > 50000 | KEEP emp_no, salary | SORT emp_no | LIMIT 10",
        "WHERE salary > 50000 | SORT salary DESC, emp_no | LIMIT 5",
        "WHERE first_name LIKE \"G*\" | KEEP emp_no, first_name | SORT emp_no",
        "WHERE salary > 50000 | EVAL x = salary * 2 | WHERE x > 130000 | KEEP emp_no, x | SORT emp_no | LIMIT 8",
        "WHERE languages IN (1, 2) AND still_hired | KEEP emp_no | SORT emp_no | LIMIT 20",
        "WHERE height > 1.8 | KEEP emp_no, height | SORT emp_no | LIMIT 10",
        "WHERE salary > 50000 AND MV_COUNT(first_name) > 0 | KEEP emp_no, salary | SORT emp_no | LIMIT 5",
        "KEEP emp_no, salary | SORT emp_no | LIMIT 7"
    );

    public void testPushdownOnAndOffGiveTheSameRows() throws IOException {
        assumeTrue("set -Dtests.satellite.endpoint=host:port to run against a satellite", ENDPOINT != null);
        for (String q : ORDERED) {
            String query = "EXTERNAL \"flight://" + ENDPOINT + "/employees\" | " + q;
            Map<String, Object> off = query(query, false);
            Map<String, Object> on = query(query, true);
            assertThat(q, on.get("columns"), equalTo(off.get("columns")));
            assertThat(q, on.get("values"), equalTo(off.get("values")));
            assertFalse("pushed although the pragma is off: " + q, pushed(off));
            assertTrue("not pushed: " + q, pushed(on));
        }
    }

    public void testDatasetFormGivesTheSameRowsAndPushesDown() throws IOException {
        assumeTrue("set -Dtests.satellite.endpoint=host:port to run against a satellite", ENDPOINT != null);
        Request source = new Request("PUT", "/_query/data_source/sat_ds");
        source.setJsonEntity("{\"type\": \"flight\"}");
        client().performRequest(source);
        Request dataset = new Request("PUT", "/_query/dataset/employees_ds");
        dataset.setJsonEntity("{\"data_source\": \"sat_ds\", \"resource\": \"flight://" + ENDPOINT + "/employees\"}");
        client().performRequest(dataset);
        try {
            for (String q : ORDERED) {
                String query = "FROM employees_ds | " + q;
                Map<String, Object> off = query(query, false);
                Map<String, Object> on = query(query, true);
                assertThat(q, on.get("columns"), equalTo(off.get("columns")));
                assertThat(q, on.get("values"), equalTo(off.get("values")));
                assertTrue("not pushed: " + q, pushed(on));
            }
        } finally {
            client().performRequest(new Request("DELETE", "/_query/dataset/employees_ds"));
            client().performRequest(new Request("DELETE", "/_query/data_source/sat_ds"));
        }
    }

    /**
     * The satellite and ES must agree on the nasty values of the {@code edge} table: nulls, int32 and int64 extremes, doubles that
     * do not add up, non-ASCII text and wildcard characters. ES's own evaluation, with the pushdown off, is the oracle: a
     * difference is a bug in the translator or the satellite, or a spec rule that does not match ES|QL. Every query must also
     * really be pushed (the profile names the fragment), or the comparison would prove nothing.
     */
    public void testEdgeTableDifferential() throws IOException {
        assumeTrue("set -Dtests.satellite.endpoint=host:port to run against a satellite", ENDPOINT != null);
        List<String> queries = List.of(
            "WHERE n > 0 | KEEP id | SORT id",
            "WHERE n + 1 > 0 | KEEP id | SORT id",
            "WHERE n - 1 < 0 | KEEP id | SORT id",
            "WHERE n * 2 > 0 | KEEP id | SORT id",
            "WHERE m + 1 > 0 | KEEP id | SORT id",
            "WHERE m * 2 > 0 | KEEP id | SORT id",
            "EVAL x = n + 1 | KEEP id, x | SORT id",
            "EVAL x = m * 3 | KEEP id, x | SORT id",
            "EVAL x = -n | KEEP id, x | SORT id",
            "EVAL x = ABS(n) | KEEP id, x | SORT id",
            "EVAL x = ABS(m) | KEEP id, x | SORT id",
            "EVAL x = COALESCE(n, 0) | KEEP id, x | SORT id",
            "EVAL x = COALESCE(s, \"none\") | KEEP id, x | SORT id",
            "WHERE n IS NULL | KEEP id | SORT id",
            "WHERE n IS NOT NULL AND s IS NOT NULL | KEEP id | SORT id",
            "WHERE n IN (1, 2, 3) | KEEP id | SORT id",
            "WHERE s IN (\"a*b\", \"x\") | KEEP id | SORT id",
            "WHERE flag | KEEP id | SORT id",
            "WHERE NOT flag | KEEP id | SORT id",
            "WHERE flag AND n > 0 | KEEP id | SORT id",
            "WHERE flag OR n > 0 | KEEP id | SORT id",
            "WHERE d > 0.1 + 0.2 | KEEP id | SORT id",
            "WHERE d == 0.30000000000000004 | KEEP id | SORT id",
            "WHERE d >= 1.0E308 | KEEP id | SORT id",
            "EVAL x = d * 2 | KEEP id, x | SORT id",
            "EVAL x = d + d | KEEP id, x | SORT id",
            "WHERE s == \"a*b\" | KEEP id | SORT id",
            "WHERE s LIKE \"a\\\\*b\" | KEEP id | SORT id",
            "WHERE s LIKE \"*\u00e9*\" | KEEP id | SORT id",
            "WHERE s LIKE \"?*\" | KEEP id | SORT id",
            "WHERE STARTS_WITH(s, \"a\") | KEEP id | SORT id",
            "WHERE ENDS_WITH(s, \"b\") | KEEP id | SORT id",
            "WHERE CONTAINS(s, \"*\") | KEEP id | SORT id",
            "WHERE s > \"B\" | KEEP id | SORT id",
            "WHERE s < \"a\" | KEEP id | SORT id",
            "WHERE w > \"2000-01-01T00:00:00Z\"::datetime | KEEP id | SORT id",
            "WHERE ns > \"2000-01-01T00:00:00Z\"::date_nanos | KEEP id | SORT id",
            "KEEP id, s | SORT s DESC NULLS FIRST, id | LIMIT 5",
            "KEEP id, s | SORT s ASC NULLS LAST, id | LIMIT 5",
            "KEEP id, n | SORT n DESC, id | LIMIT 4",
            "KEEP id, m | SORT m ASC, id | LIMIT 4",
            "KEEP id, w | SORT w DESC, id | LIMIT 3",
            "WHERE n > 0 | KEEP id, n, m | SORT n DESC, id | LIMIT 3"
        );
        differential("edge", queries);
    }

    /**
     * NaN, the infinities, null and the two zeros in a double column. ES's own evaluation is the oracle again, and these are the
     * cases where it is not what one would guess: the comparisons are the primitive IEEE operators (a NaN equals nothing, itself
     * included), but {@code IN} uses {@code Double.compare} (NaN equals NaN, and 0.0 and -0.0 differ); an arithmetic result that is
     * not finite is null, while {@code -} and {@code ABS} pass non-finite values through.
     */
    public void testNonFiniteDoublesDifferential() throws IOException {
        assumeTrue("set -Dtests.satellite.endpoint=host:port to run against a satellite", ENDPOINT != null);
        differential(
            "nonfinite",
            List.of(
                "KEEP id, d | SORT id",
                "WHERE d > 0 | KEEP id | SORT id",
                "WHERE d < 0 | KEEP id | SORT id",
                "WHERE d >= 0 | KEEP id | SORT id",
                "WHERE d <= 0 | KEEP id | SORT id",
                "WHERE d == 0 | KEEP id | SORT id",
                "WHERE d != 0 | KEEP id | SORT id",
                "WHERE d == d | KEEP id | SORT id",
                "WHERE d != d | KEEP id | SORT id",
                "WHERE NOT (d > 0) | KEEP id | SORT id",
                "WHERE d IS NULL | KEEP id | SORT id",
                "WHERE d IS NOT NULL | KEEP id | SORT id",
                "WHERE d IN (0.0) | KEEP id | SORT id",
                "WHERE d IN (1.5, 0.0) | KEEP id | SORT id",
                "WHERE d IN (d) | KEEP id | SORT id",
                "EVAL x = d + 1.0 | KEEP id, x | SORT id",
                "EVAL x = d - 1.0 | KEEP id, x | SORT id",
                "EVAL x = d * 0.0 | KEEP id, x | SORT id",
                "EVAL x = -d | KEEP id, x | SORT id",
                "EVAL x = ABS(d) | KEEP id, x | SORT id",
                "EVAL x = COALESCE(d, 9.5) | KEEP id, x | SORT id"
            )
        );
    }

    /** Runs each query over {@code table} with ES's own evaluation (the oracle) and with the pushdown, and compares. */
    private void differential(String table, List<String> queries) throws IOException {
        StringBuilder mismatches = new StringBuilder();
        StringBuilder notPushed = new StringBuilder();
        for (String q : queries) {
            String esql = "EXTERNAL \"flight://" + ENDPOINT + "/" + table + "\" | " + q;
            Map<String, Object> off = query(esql, false);
            Map<String, Object> on = profiled(esql);
            if (on.get("columns").equals(off.get("columns")) == false || on.get("values").equals(off.get("values")) == false) {
                mismatches.append("\n")
                    .append(q)
                    .append("\n   ES (oracle): ")
                    .append(off.get("values"))
                    .append("\n   satellite  : ")
                    .append(on.get("values"));
            }
            if (pushed(on) == false) {
                notPushed.append("\n").append(q);
            }
        }
        assertEquals("rows differ between ES and the satellite:" + mismatches, "", mismatches.toString());
        assertEquals("queries that were not pushed:" + notPushed, "", notPushed.toString());
    }

    /** Aggregations that version 1 pushes, then some it does not: each must give the same rows pushed, not pushed, and read natively. */
    private static final List<String> AGGREGATES = List.of(
        "STATS c = COUNT(*), n = COUNT(languages), s = SUM(salary), mn = MIN(salary), mx = MAX(salary) | LIMIT 5",
        "STATS c = COUNT(*), s = SUM(salary), a = AVG(salary) BY gender | SORT gender | LIMIT 10",
        "STATS c = COUNT(*) BY languages | SORT languages | LIMIT 20",
        "STATS mn = MIN(hire_date), mx = MAX(hire_date) BY still_hired | SORT still_hired | LIMIT 5",
        "STATS mn = MIN(first_name), mx = MAX(first_name) BY languages | SORT languages | LIMIT 10",
        "WHERE salary > 50000 | STATS c = COUNT(*), s = SUM(salary) BY languages, still_hired | SORT languages, still_hired | LIMIT 50",
        "WHERE emp_no < 0 | STATS c = COUNT(*), s = SUM(salary), mx = MAX(salary) | LIMIT 5",
        "WHERE emp_no < 0 | STATS c = COUNT(*) BY gender | LIMIT 5",
        "EVAL d = salary * 2 | STATS s = SUM(d), n = COUNT(d) BY languages | SORT languages | LIMIT 10",
        "STATS c = COUNT(*) BY gender, languages, still_hired | SORT gender, languages, still_hired | LIMIT 100",
        "STATS BY languages | SORT languages | LIMIT 20",
        "STATS mn = MIN(still_hired), mx = MAX(still_hired) BY gender | SORT gender | LIMIT 5"
    );

    /** What stays in ES: the answer is the same, and nothing is pushed beyond the filter and the scan. */
    private static final List<String> NOT_PUSHED_AGGREGATES = List.of(
        "STATS m = MAX(height) | LIMIT 5",
        "STATS c = COUNT(*) BY height | SORT height | LIMIT 5",
        "STATS c = COUNT_DISTINCT(languages) | LIMIT 5",
        "STATS a = AVG(height) | LIMIT 5"
    );

    private void registerAggregateDatasets() throws IOException {
        for (String[] r : new String[][] { { "agg_sat_src", "{\"type\": \"flight\"}" }, { "agg_native_src", "{\"type\": \"local\"}" } }) {
            Request source = new Request("PUT", "/_query/data_source/" + r[0]);
            source.setJsonEntity(r[1]);
            client().performRequest(source);
        }
        Request sat = new Request("PUT", "/_query/dataset/agg_sat_ds");
        sat.setJsonEntity("{\"data_source\": \"agg_sat_src\", \"resource\": \"flight://" + ENDPOINT + "/employees\"}");
        client().performRequest(sat);
        Request nat = new Request("PUT", "/_query/dataset/agg_native_ds");
        nat.setJsonEntity(
            "{\"data_source\": \"agg_native_src\", \"resource\": \"file://"
                + DATA_DIR
                + "/employees.csv\", \"settings\": {\"null_value\": \"\"}}"
        );
        client().performRequest(nat);
    }

    private void dropAggregateDatasets() throws IOException {
        for (String path : new String[] {
            "/_query/dataset/agg_sat_ds",
            "/_query/dataset/agg_native_ds",
            "/_query/data_source/agg_sat_src",
            "/_query/data_source/agg_native_src" }) {
            try {
                client().performRequest(new Request("DELETE", path));
            } catch (IOException e) {
                // already gone
            }
        }
    }

    public void testAggregatesAreTheSamePushedNotPushedAndNative() throws IOException {
        assumeTrue("set -Dtests.satellite.endpoint and -Dtests.satellite.data", ENDPOINT != null && DATA_DIR != null);
        registerAggregateDatasets();
        try {
            for (String q : AGGREGATES) {
                Map<String, Object> natural = query("FROM agg_native_ds | " + q, true);
                Map<String, Object> off = query("FROM agg_sat_ds | " + q, false);
                Map<String, Object> on = query("FROM agg_sat_ds | " + q, true);
                assertThat("columns " + q, on.get("columns"), equalTo(natural.get("columns")));
                assertThat("pushed vs native " + q, on.get("values"), equalTo(natural.get("values")));
                assertThat("not pushed vs native " + q, off.get("values"), equalTo(natural.get("values")));
                assertTrue("the aggregate was not pushed: " + q, aggregatePushed(on));
                assertFalse("pushed although the pragma is off: " + q, aggregatePushed(off));
            }
            for (String q : NOT_PUSHED_AGGREGATES) {
                Map<String, Object> natural = query("FROM agg_native_ds | " + q, true);
                Map<String, Object> on = query("FROM agg_sat_ds | " + q, true);
                assertThat("values " + q, on.get("values"), equalTo(natural.get("values")));
                assertFalse("an aggregate version 1 does not push was pushed: " + q, aggregatePushed(on));
            }
        } finally {
            dropAggregateDatasets();
        }
    }

    /**
     * An aggregation over an index and a satellite gives the merge of the two parts computed apart. The aggregation is not pushed to
     * the satellite in this shape: ES plans a dataset beside an index as a subquery branch, which its decomposition of an aggregate
     * over a union does not cover, so ES aggregates the satellite's rows (still filtered and projected by the v0 pushdown).
     */
    public void testAggregateOverAnIndexAndASatellite() throws IOException {
        assumeTrue("set -Dtests.satellite.endpoint and -Dtests.satellite.data", ENDPOINT != null && DATA_DIR != null);
        registerAggregateDatasets();
        Request create = new Request("PUT", "/agg_idx");
        create.setJsonEntity(
            "{\"mappings\": {\"properties\": {\"languages\": {\"type\": \"integer\"}, \"salary\": {\"type\": \"integer\"}, "
                + "\"still_hired\": {\"type\": \"boolean\"}}}}"
        );
        client().performRequest(create);
        try {
            Request bulk = new Request("POST", "/agg_idx/_bulk");
            bulk.addParameter("refresh", "true");
            bulk.setJsonEntity(
                "{\"index\":{}}\n{\"languages\":1,\"salary\":100,\"still_hired\":true}\n"
                    + "{\"index\":{}}\n{\"languages\":2,\"salary\":200,\"still_hired\":false}\n"
                    + "{\"index\":{}}\n{\"languages\":2,\"salary\":300,\"still_hired\":true}\n"
            );
            client().performRequest(bulk);
            String stats =
                " | STATS c = COUNT(*), s = SUM(salary), mn = MIN(salary), mx = MAX(salary) BY languages | SORT languages | LIMIT 50";
            Map<String, Object> idx = query("FROM agg_idx" + stats, true);
            Map<String, Object> sat = query("FROM agg_sat_ds | KEEP languages, salary" + stats, true);
            Map<String, Object> both = query("FROM agg_idx, agg_sat_ds | KEEP languages, salary" + stats, true);
            assertThat("mixed result", numbers(rows(both)), equalTo(numbers(merge(rows(idx), rows(sat)))));
        } finally {
            client().performRequest(new Request("DELETE", "/agg_idx"));
            dropAggregateDatasets();
        }
    }

    /** The rows with every number as a long, since a JSON number parses as an integer or a long as it fits. */
    private static List<List<Object>> numbers(List<List<Object>> rows) {
        return rows.stream()
            .map(row -> row.stream().map(cell -> cell instanceof Number n ? (Object) n.longValue() : cell).toList())
            .toList();
    }

    /** The {@code values} of a response: rows of cells. */
    @SuppressWarnings("unchecked")
    private static List<List<Object>> rows(Map<String, Object> response) {
        return (List<List<Object>>) response.get("values");
    }

    /** Merges two grouped results [c, s, mn, mx, key] the way a final aggregation would. */
    private static List<List<Object>> merge(List<List<Object>> a, List<List<Object>> b) {
        Map<Object, List<Object>> byKey = new java.util.TreeMap<>(
            java.util.Comparator.comparing(k -> k == null ? Long.MAX_VALUE : ((Number) k).longValue())
        );
        for (List<Object> row : java.util.stream.Stream.concat(a.stream(), b.stream()).toList()) {
            Object key = row.get(4);
            List<Object> into = byKey.get(key);
            if (into == null) {
                byKey.put(key, new java.util.ArrayList<>(row));
            } else {
                into.set(0, ((Number) into.get(0)).longValue() + ((Number) row.get(0)).longValue());
                into.set(1, ((Number) into.get(1)).longValue() + ((Number) row.get(1)).longValue());
                into.set(2, Math.min(((Number) into.get(2)).longValue(), ((Number) row.get(2)).longValue()));
                into.set(3, Math.max(((Number) into.get(3)).longValue(), ((Number) row.get(3)).longValue()));
            }
        }
        return new java.util.ArrayList<>(byKey.values());
    }

    @SuppressWarnings("unchecked")
    private static boolean aggregatePushed(Map<String, Object> profiled) {
        Map<String, Object> profile = (Map<String, Object>) profiled.get("profile");
        for (Map<String, Object> driver : (List<Map<String, Object>>) profile.get("drivers")) {
            for (Map<String, Object> op : (List<Map<String, Object>>) driver.get("operators")) {
                String operator = String.valueOf(op.get("operator"));
                if (operator.startsWith("ExternalDataSourceOperator[fragment=") && operator.contains("\"op\":\"aggregate\"")) {
                    return true;
                }
            }
        }
        return false;
    }

    @SuppressWarnings("unchecked")
    private static boolean pushed(Map<String, Object> profiled) {
        Map<String, Object> profile = (Map<String, Object>) profiled.get("profile");
        for (Map<String, Object> driver : (List<Map<String, Object>>) profile.get("drivers")) {
            for (Map<String, Object> op : (List<Map<String, Object>>) driver.get("operators")) {
                if (String.valueOf(op.get("operator")).startsWith("ExternalDataSourceOperator[fragment=")) {
                    return true;
                }
            }
        }
        return false;
    }

    private Map<String, Object> profiled(String esql) throws IOException {
        Request req = new Request("POST", "/_query");
        req.setJsonEntity("{\"query\": \"" + esql.replace("\\", "\\\\").replace("\"", "\\\"") + "\", \"profile\": true}");
        req.setOptions(RequestOptions.DEFAULT.toBuilder().setWarningsHandler(WarningsHandler.PERMISSIVE));
        return entityAsMap(client().performRequest(req));
    }

    public void testCountStarReadsNoColumns() throws IOException {
        assumeTrue("set -Dtests.satellite.endpoint=host:port to run against a satellite", ENDPOINT != null);
        String esql = "EXTERNAL \"flight://" + ENDPOINT + "/employees\" | STATS c = COUNT(*)";
        Map<String, Object> off = query(esql, false);
        Map<String, Object> on = query(esql, true);
        assertThat(on.get("values"), equalTo(off.get("values")));
        assertThat(off.get("values"), equalTo(List.of(List.of(100))));
    }

    /**
     * The third configuration of the equivalence criterion: ES's own CSV reader over the very file the satellite serves, a satellite
     * with the pushdown, and the same satellite without it must agree. This ties the satellite to ES's reader, not only to ES's
     * evaluation of rows the satellite sent. A blank keyword cell is an empty string for ES's reader unless {@code null_value} is set
     * to the empty string, which is how the satellite reads it (null), so the native dataset sets it. Needs
     * {@code -Dtests.satellite.data=DIR}, the directory the satellite serves.
     */
    public void testNativeCsvAndSatelliteAgree() throws IOException {
        assumeTrue("set -Dtests.satellite.endpoint and -Dtests.satellite.data", ENDPOINT != null && DATA_DIR != null);
        Request source = new Request("PUT", "/_query/data_source/native_src");
        source.setJsonEntity("{\"type\": \"local\"}");
        client().performRequest(source);
        Request nativeDs = new Request("PUT", "/_query/dataset/employees_native");
        nativeDs.setJsonEntity(
            "{\"data_source\": \"native_src\", \"resource\": \"file://"
                + DATA_DIR
                + "/employees.csv\","
                + " \"settings\": {\"null_value\": \"\"}}"
        );
        client().performRequest(nativeDs);
        Request satSource = new Request("PUT", "/_query/data_source/native_cmp_sat");
        satSource.setJsonEntity("{\"type\": \"flight\"}");
        client().performRequest(satSource);
        Request satDs = new Request("PUT", "/_query/dataset/employees_sat");
        satDs.setJsonEntity("{\"data_source\": \"native_cmp_sat\", \"resource\": \"flight://" + ENDPOINT + "/employees\"}");
        client().performRequest(satDs);
        try {
            for (String q : ORDERED) {
                Map<String, Object> nativeRows = query("FROM employees_native | " + q, true);
                Map<String, Object> satellite = profiled("FROM employees_sat | " + q);
                Map<String, Object> satelliteOff = query("FROM employees_sat | " + q, false);
                assertThat("native vs satellite columns: " + q, satellite.get("columns"), equalTo(nativeRows.get("columns")));
                assertThat("native vs satellite rows: " + q, satellite.get("values"), equalTo(nativeRows.get("values")));
                assertThat("pushdown off vs native: " + q, satelliteOff.get("values"), equalTo(nativeRows.get("values")));
                assertTrue("not pushed: " + q, pushed(satellite));
            }
        } finally {
            for (String path : List.of(
                "/_query/dataset/employees_native",
                "/_query/dataset/employees_sat",
                "/_query/data_source/native_src",
                "/_query/data_source/native_cmp_sat"
            )) {
                try {
                    client().performRequest(new Request("DELETE", path));
                } catch (org.elasticsearch.client.ResponseException e) {
                    // already gone
                }
            }
        }
    }

    /** The same rows spread over four files, so four splits (partitions), which every distribution strategy places differently. */
    public void testFourPartitionsUnderEveryDistribution() throws IOException {
        assumeTrue("set -Dtests.satellite.endpoint=host:port to run against a satellite", ENDPOINT != null);
        Request source = new Request("PUT", "/_query/data_source/sat_multi");
        source.setJsonEntity("{\"type\": \"flight\"}");
        client().performRequest(source);
        Request dataset = new Request("PUT", "/_query/dataset/employees_multi_ds");
        dataset.setJsonEntity("{\"data_source\": \"sat_multi\", \"resource\": \"flight://" + ENDPOINT + "/employees_multi\"}");
        client().performRequest(dataset);
        try {
            for (String q : ORDERED) {
                Map<String, Object> expected = query("EXTERNAL \"flight://" + ENDPOINT + "/employees\" | " + q, false);
                for (String distribution : new String[] { null, "coordinator_only", "round_robin" }) {
                    for (boolean pushdown : new boolean[] { false, true }) {
                        Map<String, Object> actual = query("FROM employees_multi_ds | " + q, pushdown, distribution);
                        String label = q + " [" + distribution + ", pushdown=" + pushdown + "]";
                        assertThat(label, actual.get("columns"), equalTo(expected.get("columns")));
                        assertThat(label, actual.get("values"), equalTo(expected.get("values")));
                        if (pushdown && "coordinator_only".equals(distribution) == false) {
                            assertTrue("not pushed: " + label, pushed(actual));
                        }
                    }
                }
            }
        } finally {
            client().performRequest(new Request("DELETE", "/_query/dataset/employees_multi_ds"));
            client().performRequest(new Request("DELETE", "/_query/data_source/sat_multi"));
        }
    }

    private Map<String, Object> query(String esql, boolean pushdown) throws IOException {
        return query(esql, pushdown, null);
    }

    private Map<String, Object> query(String esql, boolean pushdown, String distribution) throws IOException {
        Request req = new Request("POST", "/_query");
        req.setJsonEntity(
            "{\"query\": \""
                + esql.replace("\\", "\\\\").replace("\"", "\\\"")
                + "\", \"profile\": true, \"pragma\": {\"satellite_pushdown\": "
                + pushdown
                + (distribution == null ? "" : ", \"external_distribution\": \"" + distribution + "\"")
                + "}}"
        );
        req.setOptions(RequestOptions.DEFAULT.toBuilder().setWarningsHandler(WarningsHandler.PERMISSIVE));
        return entityAsMap(client().performRequest(req));
    }
}
