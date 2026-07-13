/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

/**
 * Cross-cutting parity guarantee for Hive-partitioned datasets: a path-derived (partition) column must
 * behave IDENTICALLY to the same column stored in the file payload, across every ES|QL operation.
 *
 * <p>Runs one operation sweep over TWIN datasets on a forced-distributed leg ({@code round_robin}, ≥2 data
 * nodes — the node where the partition-identity bugs live): dataset A carries {@code region}/{@code tier}
 * as real file columns; dataset B carries the same values as Hive partition directories
 * ({@code region=…/tier=…/}). Every query must return identical rows on both twins. Any future consumer
 * that mishandles partition identity on either node shows up here as a parity failure.
 *
 * <p>The partition columns are strings on purpose: it keeps the twins' column types identical (a keyword is
 * a keyword whether it comes from the path or the payload), so a value divergence means a real bug rather
 * than a text-vs-path type-inference artefact. Subclasses supply the per-format file writers.
 *
 * <p>This suite would have caught both bugs fixed alongside it: the parquet {@code SORT} cell (the #153503
 * TopN crash) and the parquet {@code WHERE region LIKE …} cell (the silent filter-pushdown wrong answer).
 */
public abstract class AbstractHivePartitionParityIT extends AbstractExternalDataSourceIT {

    // Columns: regionIdx, tierIdx, id, w, x, y, z. region/tier are the (string) partition keys; id is the
    // sort key; w/x/y/z are four extra data columns so the parquet TopN late-materialisation rule fires
    // (>= DEFERRED_COLUMN_MIN deferred after pinning id + the partition columns eager).
    protected static final int[][] ROWS = {
        { 0, 0, 0, 10, 100, 1000, 10000 },
        { 0, 0, 1, 11, 101, 1001, 10001 },
        { 0, 1, 2, 12, 102, 1002, 10002 },
        { 1, 0, 3, 13, 103, 1003, 10003 },
        { 1, 1, 4, 14, 104, 1004, 10004 },
        { 1, 1, 5, 15, 105, 1005, 10005 }, };
    protected static final String[] REGION = { "east", "west" };
    protected static final String[] TIER = { "gold", "silver" };

    /** File extension for this format (also the format selector, e.g. {@code "parquet"}, {@code "csv"}). */
    protected abstract String extension();

    /** Writes all columns (id, region, tier, w, x, y, z) for {@code rows} to {@code file}. */
    protected abstract void writeFlat(Path file, List<int[]> rows) throws IOException;

    /** Writes only the data columns (id, w, x, y, z) for {@code rows} to {@code file}; region/tier come from the path. */
    protected abstract void writePartitioned(Path file, List<int[]> rows) throws IOException;

    private String registerFlatTwin() throws IOException {
        Path dir = createTempDir().resolve("parity_flat_" + extension());
        Files.createDirectories(dir);
        // Split across two files so the flat twin produces >= 2 splits and distributes across data nodes too
        // (the partition twin already has one file per partition dir) — the forced-distributed leg asserts
        // spread on both twins.
        List<int[]> all = List.of(ROWS);
        int mid = all.size() / 2;
        writeFlat(dir.resolve("data_0." + extension()), new ArrayList<>(all.subList(0, mid)));
        writeFlat(dir.resolve("data_1." + extension()), new ArrayList<>(all.subList(mid, all.size())));
        return registerDataset("parity_flat_" + extension(), StoragePath.fileUri(dir) + "/*." + extension(), Map.of());
    }

    private String registerPartitionedTwin() throws IOException {
        Path root = createTempDir().resolve("parity_part_" + extension());
        Map<String, List<int[]>> byPartition = new LinkedHashMap<>();
        for (int[] r : ROWS) {
            byPartition.computeIfAbsent("region=" + REGION[r[0]] + "/tier=" + TIER[r[1]], k -> new ArrayList<>()).add(r);
        }
        for (Map.Entry<String, List<int[]>> e : new TreeMap<>(byPartition).entrySet()) {
            String[] parts = e.getKey().split("/");
            Path dir = root.resolve(parts[0]).resolve(parts[1]);
            Files.createDirectories(dir);
            writePartitioned(dir.resolve("data." + extension()), e.getValue());
        }
        @SuppressWarnings("checkstyle:EmptyJavadoc") // the glob's '/**/' is misread as Javadoc
        String glob = StoragePath.fileUri(root) + "/**/*." + extension();
        return registerDataset("parity_part_" + extension(), glob, Map.of("hive_partitioning", true));
    }

    public void testParitySweep() throws Exception {
        internalCluster().ensureAtLeastNumDataNodes(2);
        String flat = registerFlatTwin();
        String part = registerPartitionedTwin();

        List<String> ops = List.of(
            "| SORT id",
            "| SORT region, id",
            "| SORT region DESC, id",
            "| WHERE region == \"east\" | SORT id",
            "| WHERE region != \"east\" | SORT id",
            "| WHERE region LIKE \"eas*\" | SORT id",
            "| WHERE region IN (\"east\", \"west\") | SORT id",
            "| WHERE STARTS_WITH(region, \"e\") | SORT id",
            "| WHERE region == \"east\" AND tier == \"gold\" | SORT id",
            "| WHERE region == \"east\" AND id >= 1 | SORT id",
            "| STATS c = COUNT(*) BY region | SORT region",
            "| STATS c = COUNT(region) BY tier | SORT tier",
            "| STATS c = COUNT(*) BY region, tier | SORT region, tier",
            "| EVAL r2 = CONCAT(region, \"_x\") | SORT id",
            "| EVAL up = TO_UPPER(region) | SORT id",
            "| KEEP region, id | SORT id",
            "| DROP region | SORT id",
            "| RENAME region AS r | WHERE r == \"east\" | SORT id"
        );

        List<String> divergences = new ArrayList<>();
        for (String op : ops) {
            String flatResult;
            String partResult;
            try {
                flatResult = normalized(runDistributed("FROM " + flat + " " + op));
            } catch (Exception e) {
                divergences.add("op [" + op + "] THREW on the FLAT twin (harness bug): " + rootMessage(e));
                continue;
            }
            try {
                partResult = normalized(runDistributed("FROM " + part + " " + op));
            } catch (Exception e) {
                divergences.add("op [" + op + "] THREW on the PARTITION twin but not the flat one: " + rootMessage(e));
                continue;
            }
            if (flatResult.equals(partResult) == false) {
                divergences.add("op [" + op + "] DIVERGED\n    flat: " + flatResult + "\n    part: " + partResult);
            }
        }

        if (divergences.isEmpty() == false) {
            fail(
                "["
                    + extension()
                    + "] PARITY DIVERGENCES ("
                    + divergences.size()
                    + "/"
                    + ops.size()
                    + "):\n"
                    + String.join("\n", divergences)
            );
        }
    }

    private List<List<Object>> runDistributed(String query) {
        QueryPragmas pragmas = new QueryPragmas(Settings.builder().put(QueryPragmas.EXTERNAL_DISTRIBUTION.getKey(), "round_robin").build());
        var request = syncEsqlQueryRequest(query);
        request.pragmas(pragmas);
        request.acceptedPragmaRisks(true); // pragmas are rejected on non-snapshot builds without this
        request.profile(true);
        try (var response = run(request)) {
            assertThat(
                "external scan must distribute across >= 2 data nodes for [" + query + "]",
                externalScanNodeNames(response).size(),
                greaterThanOrEqualTo(2)
            );
            // Tag each cell name:type=value so the diff catches a column-TYPE regression (a partition column
            // surfacing as a different type than the file-resident twin), not just a value regression.
            List<String> tags = response.columns().stream().map(c -> c.name() + ":" + c.outputType()).toList();
            List<List<Object>> raw = getValuesList(response);
            List<List<Object>> tagged = new ArrayList<>();
            for (List<Object> row : raw) {
                List<Object> withNames = new ArrayList<>();
                for (int i = 0; i < tags.size(); i++) {
                    withNames.add(tags.get(i) + "=" + row.get(i));
                }
                tagged.add(withNames);
            }
            return tagged;
        }
    }

    /**
     * Normal form of a tagged result: cells within a row are sorted (column-order-insensitive, since the twins
     * append partition columns in a different position), but ROW order is preserved. Every op in the sweep ends
     * in a total SORT, so both twins return rows in the same deterministic order — comparing rows in order means a
     * SORT-ORDER regression on the partition twin is caught rather than normalised away.
     */
    private static String normalized(List<List<Object>> taggedRows) {
        List<String> rows = new ArrayList<>();
        for (List<Object> row : taggedRows) {
            List<String> cells = new ArrayList<>();
            for (Object cell : row) {
                cells.add(String.valueOf(cell));
            }
            cells.sort(String::compareTo);
            rows.add(cells.toString());
        }
        return rows.toString();
    }

    private static String rootMessage(Throwable t) {
        Throwable c = t;
        while (c.getCause() != null && c.getCause() != c) {
            c = c.getCause();
        }
        return c.getClass().getSimpleName() + ": " + c.getMessage();
    }
}
