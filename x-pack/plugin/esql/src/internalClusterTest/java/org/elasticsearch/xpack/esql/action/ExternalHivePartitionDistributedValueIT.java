/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.esql.datasource.csv.CsvDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasource.parquet.ParquetDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

/**
 * Multi-node end-to-end guard that Hive partition-column <em>values</em> attach correctly when an external-source
 * read is DISTRIBUTED to data nodes — for both Parquet and CSV/text. This is the leg that had no coverage on main:
 * the partition-name union that makes this work landed in elastic/elasticsearch#150920 (for the {@code COUNT(p)}
 * safe-miss-to-scan path), but nothing exercised the distributed <em>value</em> attachment for any format.
 *
 * <p>Why the read must actually distribute: a partition column lives in the directory path, not the file payload.
 * On the coordinator the partition names ride in the {@code FileList}; on a data node that {@code FileList} is NOT
 * serialized ({@code ExternalSourceExec.writeTo} drops it), so the names must instead come from the serialized
 * {@code _partition.columns} stamp in {@code sourceMetadata}. If the read ran coordinator-local it would resolve the
 * values from the {@code FileList} and mask the distributed leg — a false green. Each test therefore forces
 * distribution ({@code external_distribution=round_robin} over a &gt;1-split dataset with
 * {@code ensureAtLeastNumDataNodes(2)}) and asserts via the profile that the {@code ExternalDataSource} scan ran on
 * at least two distinct data nodes.
 *
 * <p>Why {@code STATS COUNT(*) BY p}: grouping on the partition column collapses every row into a single {@code null}
 * group if the value failed to attach on the data node. Getting back two groups {@code {a=3, b=2}} proves the
 * per-file partition values reached the data-node read intact — a stronger signal than reading the raw
 * {@code a,a,a,b,b} column, which a single missing value would only dent rather than collapse.
 */
public class ExternalHivePartitionDistributedValueIT extends AbstractExternalDataSourceIT {

    @Override
    protected Collection<Class<? extends Plugin>> formatPlugins() {
        return List.of(ParquetDataSourcePlugin.class, CsvDataSourcePlugin.class);
    }

    /**
     * Parquet twin: two partitions ({@code p=a}: 3 rows, {@code p=b}: 2 rows), each its own file (so &gt;1 split).
     * The distributed read must attach the path-derived {@code p} value on every data node.
     */
    public void testParquetHivePartitionValuesAttachOnDistributedRead() throws Exception {
        Path root = createTempDir().resolve("hive_parquet_values");
        writeIdParquet(root.resolve("p=a"), 3);
        writeIdParquet(root.resolve("p=b"), 2);
        @SuppressWarnings("checkstyle:EmptyJavadoc") // the glob's '/**/' is misread as Javadoc
        String glob = StoragePath.fileUri(root) + "/**/*.parquet";
        String dataset = registerDataset("hive_parquet_values", glob, Map.of("hive_partitioning", true));
        assertPartitionValuesAttachOnDistributedRead(dataset);
    }

    /**
     * CSV/text twin of {@link #testParquetHivePartitionValuesAttachOnDistributedRead} — the previously unproven leg
     * (the CSV path once threw a {@code TopNOperator$RowFiller} AIOOBE). Same two-partition fixture, same distributed
     * value-attachment contract.
     */
    public void testCsvHivePartitionValuesAttachOnDistributedRead() throws Exception {
        Path root = createTempDir().resolve("hive_csv_values");
        writeIdCsv(root.resolve("p=a"), 3);
        writeIdCsv(root.resolve("p=b"), 2);
        @SuppressWarnings("checkstyle:EmptyJavadoc") // the glob's '/**/' is misread as Javadoc
        String glob = StoragePath.fileUri(root) + "/**/*.csv";
        String dataset = registerDataset("hive_csv_values", glob, Map.of("hive_partitioning", true));
        assertPartitionValuesAttachOnDistributedRead(dataset);
    }

    /**
     * Forces distribution across data nodes, runs {@code STATS COUNT(*) BY p}, and asserts (1) the external scan ran
     * on &gt;=2 distinct data nodes (so the distributed leg is genuinely exercised, not a coordinator-local
     * short-circuit) and (2) the partition column grouped into the two real path values {@code {a=3, b=2}}, never a
     * single {@code null} group.
     */
    private void assertPartitionValuesAttachOnDistributedRead(String dataset) {
        internalCluster().ensureAtLeastNumDataNodes(2);

        // round_robin distributes every split to a data node regardless of plan shape, so the read runs where the
        // coordinator FileList is UNRESOLVED and partition names must come from the serialized _partition.columns stamp.
        QueryPragmas pragmas = new QueryPragmas(Settings.builder().put(QueryPragmas.EXTERNAL_DISTRIBUTION.getKey(), "round_robin").build());

        String query = "FROM " + dataset + " | STATS c = COUNT(*) BY p | SORT p";
        var request = syncEsqlQueryRequest(query);
        request.pragmas(pragmas);
        request.acceptedPragmaRisks(true); // pragmas are rejected on non-snapshot builds without this
        request.profile(true);
        try (var response = run(request)) {
            // The ExternalDataSource scan must have run on >= 2 distinct data nodes. A coordinator-local run would
            // resolve partition values from the FileList and never test the distributed attachment leg (false green).
            Set<String> scanNodes = new HashSet<>();
            for (var driver : response.profile().drivers()) {
                for (var op : driver.operators()) {
                    if (op.operator().startsWith("ExternalDataSource")) {
                        scanNodes.add(driver.nodeName());
                    }
                }
            }
            assertThat("external scan must distribute across >= 2 data nodes", scanNodes.size(), greaterThanOrEqualTo(2));

            List<String> columns = response.columns().stream().map(c -> c.name()).collect(Collectors.toList());
            int cIdx = columns.indexOf("c");
            int pIdx = columns.indexOf("p");
            assertThat("missing count column", cIdx, greaterThanOrEqualTo(0));
            assertThat("missing partition column 'p'", pIdx, greaterThanOrEqualTo(0));

            Map<String, Long> countByPartition = new HashMap<>();
            for (List<Object> row : getValuesList(response)) {
                Object partition = row.get(pIdx);
                assertNotNull("partition value must attach on the distributed read, got null", partition);
                countByPartition.put(partition.toString(), ((Number) row.get(cIdx)).longValue());
            }
            assertThat(countByPartition, equalTo(Map.of("a", 3L, "b", 2L)));
        }
    }

    /** Writes a single-column ({@code id: int32}) Parquet file with {@code rowCount} rows (ids 0..rowCount-1). */
    private void writeIdParquet(Path dir, int rowCount) throws IOException {
        Files.createDirectories(dir);
        writeParquet(dir.resolve("data.parquet"), "message test { required int32 id; }", rowCount, 1024, (g, i) -> g.add("id", i));
    }

    /** Writes a single-column ({@code id}) CSV file with {@code rowCount} rows (ids 0..rowCount-1). */
    private static void writeIdCsv(Path dir, int rowCount) throws IOException {
        Files.createDirectories(dir);
        StringBuilder body = new StringBuilder("id\n");
        for (int i = 0; i < rowCount; i++) {
            body.append(i).append('\n');
        }
        Files.writeString(dir.resolve("data.csv"), body.toString(), StandardCharsets.UTF_8);
    }
}
