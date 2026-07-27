/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.esql.datasource.parquet.ParquetDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

/**
 * End-to-end guard for #153503: {@code FROM <hive-partitioned parquet> | SORT …}
 * must return rows with the path-derived partition columns materialised, not crash.
 *
 * <p>Partition columns live in the directory path, not the file payload. The eager scan path
 * ({@code LIMIT} without {@code SORT}) strips them from the file schema and injects them as constant
 * blocks via {@code VirtualColumnIterator} — which is why {@code LIMIT} always worked. {@code SORT …}
 * instead enters TopN late materialisation: {@code InsertExternalFieldExtraction} narrows the forward
 * scan and defers the remaining columns to a positional parquet read. Partition columns are plain
 * {@code ReferenceAttribute}s (no {@code VirtualAttribute} marker), so before the fix they landed in
 * the deferred set and reached {@code ParquetColumnExtractor.extract}, which throws
 * {@code column [STATION] is missing or has an unsupported type} because the column is not in the file.
 *
 * <p>The sibling {@link ExternalParquetHivePartitionedIT} only exercises {@code COUNT(p)} and so never
 * hit this path — hence this dedicated SORT/TopN suite. Fixtures carry five data columns so that with
 * the partition columns correctly pinned eager the deferred set still clears
 * {@code InsertExternalFieldExtraction.DEFERRED_COLUMN_MIN} and the optimisation actually fires.
 */
public class ExternalParquetHivePartitionedTopNIT extends AbstractExternalDataSourceIT {

    @Override
    protected Collection<Class<? extends Plugin>> formatPlugins() {
        return List.of(ParquetDataSourcePlugin.class);
    }

    /**
     * Case 1 (the report): a fully pinned hive glob ({@code STATION=A/ELEMENT=TMAX/*.parquet}) with
     * zero partition settings — AUTO detection is on by default and recognises {@code KEY=VALUE}
     * directories. {@code SORT} used to crash with {@code column [STATION] is missing}; it must now
     * return every row with STATION/ELEMENT carrying the pinned path values.
     */
    public void testSortOverPinnedHivePartitionReturnsRowsAndPartitionValues() throws Exception {
        Path root = createTempDir().resolve("hive_parquet_topn_pinned");
        writeMultiColumnParquet(root.resolve("STATION=A").resolve("ELEMENT=TMAX"), 5);
        String glob = StoragePath.fileUri(root) + "/STATION=A/ELEMENT=TMAX/*.parquet";
        String dataset = registerDataset("hive_parquet_topn_pinned", glob, Map.of());

        String query = "FROM " + dataset + " | SORT id";
        try (var response = run(syncEsqlQueryRequest(query))) {
            List<List<Object>> rows = getValuesList(response);
            assertThat("SORT over a pinned hive-partitioned dataset must return all rows", rows.size(), equalTo(5));

            int station = columnIndex(response, "STATION");
            int element = columnIndex(response, "ELEMENT");
            for (List<Object> row : rows) {
                assertThat("partition column STATION materialised from the path", row.get(station), equalTo("A"));
                assertThat("partition column ELEMENT materialised from the path", row.get(element), equalTo("TMAX"));
            }
        }
    }

    /**
     * Case 2: an unpinned glob ({@code /**}{@code /*.parquet}) across two partitions with explicit
     * {@code hive_partitioning:true} and a {@code SORT}, forced to distribute across {@code >= 2} data
     * nodes via {@code external_distribution=round_robin}. Closes the SORT-over-partitioned coverage
     * gap AND exercises the genuinely distributed path: with the coordinator {@code FileList}
     * UNRESOLVED on the data node, the partition-column pin must read the serialized
     * {@code _partition.columns} stamp. Pins that the pinned-vs-unpinned glob was never the bug — an
     * unpinned glob fails identically without the fix (the partition columns are still deferred).
     */
    public void testSortOverUnpinnedHivePartitionSpansBothPartitions() throws Exception {
        internalCluster().ensureAtLeastNumDataNodes(2);

        Path root = createTempDir().resolve("hive_parquet_topn_unpinned");
        writeMultiColumnParquet(root.resolve("STATION=A").resolve("ELEMENT=TMAX"), 3);
        writeMultiColumnParquet(root.resolve("STATION=B").resolve("ELEMENT=TMIN"), 2);
        @SuppressWarnings("checkstyle:EmptyJavadoc") // the glob's '/**/' is misread as Javadoc
        String glob = StoragePath.fileUri(root) + "/**/*.parquet";
        String dataset = registerDataset("hive_parquet_topn_unpinned", glob, Map.of("hive_partitioning", true));

        // round_robin distributes every split to a data node regardless of plan shape, so the TopN
        // late-materialisation (and the partition-column pin) runs where the coordinator FileList is
        // UNRESOLVED and partition names come only from the serialized _partition.columns stamp.
        QueryPragmas pragmas = new QueryPragmas(Settings.builder().put(QueryPragmas.EXTERNAL_DISTRIBUTION.getKey(), "round_robin").build());
        var request = syncEsqlQueryRequest("FROM " + dataset + " | SORT id");
        request.pragmas(pragmas);
        request.acceptedPragmaRisks(true); // pragmas are rejected on non-snapshot builds without this
        request.profile(true);
        try (var response = run(request)) {
            assertThat(
                "external scan must distribute across >= 2 data nodes to exercise the UNRESOLVED-FileList path",
                externalScanNodeNames(response).size(),
                greaterThanOrEqualTo(2)
            );

            List<List<Object>> rows = getValuesList(response);
            assertThat("SORT over an unpinned hive-partitioned dataset must return rows from both partitions", rows.size(), equalTo(5));

            int station = columnIndex(response, "STATION");
            List<String> stations = rows.stream().map(row -> (String) row.get(station)).sorted().toList();
            assertThat("both partitions' path-derived STATION values materialise", stations, equalTo(List.of("A", "A", "A", "B", "B")));
        }
    }

    // ---------------------------------------------------------------------------------------------
    // helpers
    // ---------------------------------------------------------------------------------------------

    /**
     * Writes a {@code data.parquet} with one sort key ({@code id: int64}) and four projection-only data
     * columns under {@code dir}, {@code rowCount} rows. Five payload columns keep the deferred set above
     * {@code DEFERRED_COLUMN_MIN} once the two path-derived partition columns are (correctly) pinned eager.
     */
    private static Path writeMultiColumnParquet(Path dir, int rowCount) throws IOException {
        Files.createDirectories(dir);
        return writeParquet(
            dir.resolve("data.parquet"),
            "message test { required int64 id; required int32 v1; required int32 v2; required int32 v3; required int32 v4; }",
            rowCount,
            1024,
            (g, i) -> {
                g.add("id", (long) i);
                g.add("v1", i);
                g.add("v2", i * 2);
                g.add("v3", i * 3);
                g.add("v4", i * 4);
            }
        );
    }

    private static int columnIndex(EsqlQueryResponse response, String name) {
        var columns = response.columns();
        for (int i = 0; i < columns.size(); i++) {
            if (columns.get(i).name().equals(name)) {
                return i;
            }
        }
        throw new AssertionError("column [" + name + "] not found in response columns " + columns);
    }
}
