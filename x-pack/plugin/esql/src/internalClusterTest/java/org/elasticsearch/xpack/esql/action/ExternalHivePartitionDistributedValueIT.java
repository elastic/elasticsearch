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
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.instanceOf;

/**
 * Multi-node end-to-end guard that Hive partition-column <em>values</em> attach correctly when an external-source
 * read is DISTRIBUTED to data nodes, for a Parquet, a CSV/text, and an integer-typed partition.
 *
 * <p>Why the read must actually distribute: a partition column lives in the directory path, not the file payload.
 * On the coordinator the partition names ride in the {@code FileList}; on a data node that {@code FileList} is NOT
 * serialized ({@code ExternalSourceExec.writeTo} drops it), so the names come from the serialized
 * {@code _partition.columns} stamp in {@code sourceMetadata}. A coordinator-local run would resolve the values from
 * the {@code FileList} and mask the distributed leg — a false green — so each test forces distribution
 * ({@code external_distribution=round_robin} over a &gt;1-split dataset with {@code ensureAtLeastNumDataNodes(2)}) and
 * asserts via the profile that the {@code ExternalDataSource} scan ran on at least two distinct data nodes.
 *
 * <p>Why {@code STATS COUNT(*) BY <partition>}: grouping on the partition column collapses every row into a single
 * {@code null} group if the value fails to attach on the data node, and into a single group if the per-file values
 * cross-contaminate. Getting back the two distinct groups with their real counts proves the per-file partition values
 * reach the data-node read intact.
 *
 * <p>The Parquet and CSV tests use a STRING partition ({@code p=a}/{@code p=b}); the integer test uses an INTEGER
 * partition ({@code n=1}/{@code n=2}), which additionally exercises the {@code writeGenericMap}/{@code readGenericMap}
 * type round-trip on the data node (the value must arrive as an {@code Integer}, not degrade to String) and the
 * {@code case Integer} arm of {@code VirtualColumnIterator.createConstantBlock}.
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
        writeSingleColumnIdParquet(root.resolve("p=a"), 3);
        writeSingleColumnIdParquet(root.resolve("p=b"), 2);
        @SuppressWarnings("checkstyle:EmptyJavadoc") // the glob's '/**/' is misread as Javadoc
        String glob = StoragePath.fileUri(root) + "/**/*.parquet";
        String dataset = registerDataset("hive_parquet_values", glob, Map.of("hive_partitioning", true));
        assertStringPartitionGroups(dataset);
    }

    /**
     * CSV/text twin of {@link #testParquetHivePartitionValuesAttachOnDistributedRead}: the same two-partition fixture
     * and the same distributed value-attachment contract, exercised through the line-oriented CSV reader path.
     */
    public void testCsvHivePartitionValuesAttachOnDistributedRead() throws Exception {
        Path root = createTempDir().resolve("hive_csv_values");
        writeIdCsv(root.resolve("p=a"), 3);
        writeIdCsv(root.resolve("p=b"), 2);
        @SuppressWarnings("checkstyle:EmptyJavadoc") // the glob's '/**/' is misread as Javadoc
        String glob = StoragePath.fileUri(root) + "/**/*.csv";
        String dataset = registerDataset("hive_csv_values", glob, Map.of("hive_partitioning", true));
        assertStringPartitionGroups(dataset);
    }

    /**
     * INTEGER partition ({@code n=1}: 3 rows, {@code n=2}: 2 rows). {@code HivePartitionDetector} infers the numeric
     * segment as INTEGER (Spark-style), so the value rides the data node as a boxed {@code Integer} and must survive
     * the {@code writeGenericMap}/{@code readGenericMap} type round-trip and render through the {@code case Integer}
     * arm — a distributed-specific property the string cells cannot reach. A degrade to String would surface the
     * group key as a keyword and fail the {@code Integer} type assertion below.
     */
    public void testIntegerHivePartitionValuesAttachOnDistributedRead() throws Exception {
        Path root = createTempDir().resolve("hive_int_values");
        writeSingleColumnIdParquet(root.resolve("n=1"), 3);
        writeSingleColumnIdParquet(root.resolve("n=2"), 2);
        @SuppressWarnings("checkstyle:EmptyJavadoc") // the glob's '/**/' is misread as Javadoc
        String glob = StoragePath.fileUri(root) + "/**/*.parquet";
        String dataset = registerDataset("hive_int_values", glob, Map.of("hive_partitioning", true));

        Map<Object, Long> countByPartition = runDistributedStatsByPartition(dataset, "n");
        // Every group key must be a boxed Integer (not a String): a type degrade on the data node would surface the
        // partition as a keyword, which the Map equality below would also reject, but this gives the sharper message.
        countByPartition.keySet()
            .forEach(
                key -> assertThat("integer partition must survive the data-node round-trip as Integer", key, instanceOf(Integer.class))
            );
        assertThat(countByPartition, equalTo(Map.of(1, 3L, 2, 2L)));
    }

    /** Runs {@code STATS COUNT(*) BY p} distributed and asserts the two real string groups {@code {a=3, b=2}}. */
    private void assertStringPartitionGroups(String dataset) {
        assertThat(runDistributedStatsByPartition(dataset, "p"), equalTo(Map.of("a", 3L, "b", 2L)));
    }

    /**
     * Forces distribution across data nodes, runs {@code STATS c = COUNT(*) BY <partitionCol>}, and returns the
     * per-partition counts keyed by the partition value <em>as the engine typed it</em> (so a String key never equals
     * an expected {@code Integer} key). Asserts along the way that the external scan ran on &gt;=2 distinct data nodes
     * (so the distributed leg is genuinely exercised, not a coordinator-local short-circuit that would resolve values
     * from the {@code FileList}) and that exactly two groups came back with non-null keys (catching a null-attach, a
     * cross-contamination, or a duplicate row). The caller only asserts the expected {@code value -> count} map.
     */
    private Map<Object, Long> runDistributedStatsByPartition(String dataset, String partitionCol) {
        internalCluster().ensureAtLeastNumDataNodes(2);

        // round_robin distributes every split to a data node regardless of plan shape, so the read runs where the
        // coordinator FileList is UNRESOLVED and partition names must come from the serialized _partition.columns stamp.
        QueryPragmas pragmas = new QueryPragmas(Settings.builder().put(QueryPragmas.EXTERNAL_DISTRIBUTION.getKey(), "round_robin").build());

        String query = "FROM " + dataset + " | STATS c = COUNT(*) BY " + partitionCol;
        var request = syncEsqlQueryRequest(query);
        request.pragmas(pragmas);
        request.acceptedPragmaRisks(true); // pragmas are rejected on non-snapshot builds without this
        request.profile(true);
        try (var response = run(request)) {
            // A coordinator-local run would resolve partition values from the FileList and never test the distributed
            // attachment leg (false green), so require the external scan to have run on >= 2 distinct data nodes.
            assertThat(
                "external scan must distribute across >= 2 data nodes",
                externalScanNodeNames(response).size(),
                greaterThanOrEqualTo(2)
            );

            List<String> columns = response.columns().stream().map(c -> c.name()).collect(Collectors.toList());
            int cIdx = columns.indexOf("c");
            int partIdx = columns.indexOf(partitionCol);
            assertThat("missing count column", cIdx, greaterThanOrEqualTo(0));
            assertThat("missing partition column [" + partitionCol + "]", partIdx, greaterThanOrEqualTo(0));

            List<List<Object>> rows = getValuesList(response);
            assertThat("expected exactly two partition groups, no duplicate rows", rows.size(), equalTo(2));
            Map<Object, Long> countByPartition = new HashMap<>();
            for (List<Object> row : rows) {
                Object partition = row.get(partIdx);
                assertNotNull("partition value must attach on the distributed read, got null", partition);
                countByPartition.put(partition, ((Number) row.get(cIdx)).longValue());
            }
            return countByPartition;
        }
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
