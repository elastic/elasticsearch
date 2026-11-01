/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.esql.datasource.ndjson.NdJsonDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.equalTo;

/** NDJSON binding of {@link AbstractExternalAggregatePushdownMatrixIT}, plus the big-integer-&gt;DOUBLE case. */
public class NdjsonExternalAggregatePushdownMatrixIT extends AbstractExternalAggregatePushdownMatrixIT {

    @Override
    protected String format() {
        return "ndjson";
    }

    @Override
    protected Collection<Class<? extends Plugin>> formatPlugins() {
        return List.of(NdJsonDataSourcePlugin.class);
    }

    @Override
    protected String writeFixture(Path dir, int rows) throws Exception {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < rows; i++) {
            sb.append("{\"emp_no\":")
                .append(i)
                .append(",\"label\":\"")
                .append(label(i))
                .append("\",\"val\":")
                .append(i + 0.5)
                .append("}\n");
        }
        Path file = dir.resolve("employees.ndjson");
        Files.writeString(file, sb.toString());
        return StoragePath.fileUri(file);
    }

    /**
     * The case the type-consistency fix targets: a UInt64-range column (values &gt; {@code Long.MAX}) infers
     * DOUBLE on the NDJSON path (Jackson {@code BIG_INTEGER} -&gt; double), mirroring the 100M ClickBench
     * {@code UserID}. The warm MIN/MAX must short-circuit AND return a value IDENTICAL to the cold full scan —
     * i.e. the harvest read the column as the same double the aggregate uses, never a divergent long.
     */
    public void testMinMaxBigIntegerInferredDoubleColdThenWarmShortCircuits() throws Exception {
        Path dir = createTempDir();
        BigInteger base = new BigInteger("10000000000000000000"); // 1e19 > Long.MAX
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < ROWS; i++) {
            // Space the values by 100000 so they stay distinct as doubles even past 2^63 (ULP ~2048 there).
            sb.append("{\"id\":").append(i).append(",\"uid\":").append(base.add(BigInteger.valueOf((long) i * 100_000))).append("}\n");
        }
        Path file = dir.resolve("big.ndjson");
        Files.writeString(file, sb.toString());
        registerDataset("big_employees", StoragePath.fileUri(file));

        String query = "FROM big_employees | STATS lo = MIN(uid), hi = MAX(uid)";
        List<List<Object>> cold;
        try (var response = run(syncEsqlQueryRequest(query).profile(true), TIMEOUT)) {
            assertThat("cold pass scans every row", response.documentsFound(), equalTo((long) ROWS));
            cold = getValuesList(response);
            // Sanity: both are doubles, min <= max.
            assertThat(((Number) cold.get(0).get(0)).doubleValue() <= ((Number) cold.get(0).get(1)).doubleValue(), equalTo(true));
        }
        try (var response = run(syncEsqlQueryRequest(query).profile(true), TIMEOUT)) {
            assertThat("warm big-integer->double MIN/MAX must short-circuit (0 docs)", response.documentsFound(), equalTo(0L));
            List<List<Object>> warm = getValuesList(response);
            assertThat("warm MIN(uid) == cold MIN(uid)", warm.get(0).get(0), equalTo(cold.get(0).get(0)));
            assertThat("warm MAX(uid) == cold MAX(uid)", warm.get(0).get(1), equalTo(cold.get(0).get(1)));
        }
    }

    /**
     * COUNT on a multivalued (array) column counts VALUES, not rows: ES|QL's {@code Count} returns the number
     * of values. The warm short-circuit must serve the same number a full scan computes — here every row has a
     * 2-element {@code tags} array, so {@code COUNT(tags)} is {@code 2*ROWS}, NOT {@code ROWS}.
     */
    public void testCountMultivaluedColumnColdThenWarmShortCircuits() throws Exception {
        Path dir = createTempDir();
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < ROWS; i++) {
            sb.append("{\"id\":").append(i).append(",\"tags\":[\"x").append(i).append("\",\"y").append(i).append("\"]}\n");
        }
        Path file = dir.resolve("mv.ndjson");
        Files.writeString(file, sb.toString());
        registerDataset("mv_employees", StoragePath.fileUri(file));

        assertColdThenWarmShortCircuit(
            "mv_employees",
            "STATS c = COUNT(tags)",
            ROWS,
            rows -> assertThat("COUNT(tags) counts values, not rows", ((Number) rows.get(0).get(0)).longValue(), equalTo(2L * ROWS))
        );
    }

    /**
     * MIN/MAX over a multivalued column must fold across ALL values, not per-row: row {@code i} carries
     * {@code tags=[label(2i), label(2i+1)]}, so the whole-file MIN is {@code label(0)} and MAX is
     * {@code label(2*ROWS-1)}. A per-row (rather than per-value) fold would miss the global extremes.
     */
    public void testMinMaxMultivaluedColumnColdThenWarmShortCircuits() throws Exception {
        Path dir = createTempDir();
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < ROWS; i++) {
            sb.append("{\"id\":")
                .append(i)
                .append(",\"tags\":[\"")
                .append(label(2 * i))
                .append("\",\"")
                .append(label(2 * i + 1))
                .append("\"]}\n");
        }
        Path file = dir.resolve("mv_minmax.ndjson");
        Files.writeString(file, sb.toString());
        registerDataset("mv_minmax_employees", StoragePath.fileUri(file));

        assertColdThenWarmShortCircuit("mv_minmax_employees", "STATS lo = MIN(tags), hi = MAX(tags)", ROWS, rows -> {
            assertThat("MIN(tags) folds across all values", String.valueOf(rows.get(0).get(0)), equalTo(label(0)));
            assertThat("MAX(tags) folds across all values", String.valueOf(rows.get(0).get(1)), equalTo(label(2 * ROWS - 1)));
        });
    }
}
