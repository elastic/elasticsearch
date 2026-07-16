/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasource.parquet.ParquetDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.equalTo;

/**
 * Multi-node end-to-end guard for H4 (elastic/elasticsearch#150920): {@code COUNT(partition_column)} over a
 * Hive-partitioned Parquet dataset must SAFE-MISS to a scan on the data node, never warm-fold to a constant 0.
 *
 * <p>A partition column lives in the directory path, not the file payload, so it is absent from every file's
 * footer column stats. Under the footer implicit-nulls contract an absent column reads as all-null, so a naive
 * warm fold serves {@code rowCount - rowCount = 0}. The fix safe-misses partition-column aggregates so the engine
 * scans instead.
 *
 * <p>This suite forces a &gt;=2 data-node cluster ON PURPOSE: the partition-column signal travels in the
 * coordinator's {@code FileList}, which is NOT serialized ({@code ExternalRelation} reconstructs it as
 * {@code UNRESOLVED} on a remote node). The fold runs on a data node from serialized split stats, so a
 * coordinator-only guard would still fold {@code COUNT(p)} to 0 here. A single-node local-transport short-circuit
 * hides that — hence the {@code ensureAtLeastNumDataNodes(2)} below.
 */
public class ExternalParquetHivePartitionedIT extends AbstractExternalDataSourceIT {

    @Override
    protected Collection<Class<? extends Plugin>> formatPlugins() {
        return List.of(ParquetDataSourcePlugin.class);
    }

    /**
     * A capitalized boolean partition folder ({@code flag=True/}, {@code flag=False/}, the casing common data
     * writers emit) must type the {@code flag} column as {@code BOOLEAN} and be queryable rather than failing
     * partition-value casting. Partition-value casting is format-agnostic ({@code HivePartitionDetector.castValue}),
     * so a single format end-to-end guard suffices; the detector-level coverage (any casing, hive/template
     * delegation, null-sentinel mix) lives in {@code HivePartitionDetectorTests} / {@code TemplatePartitionDetectorTests}.
     */
    public void testCapitalizedBooleanPartitionFolderQueryable() throws Exception {
        internalCluster().ensureAtLeastNumDataNodes(2);

        Path root = createTempDir().resolve("hive_parquet_bool");
        writeSingleColumnIdParquet(root.resolve("flag=True"), 3); // ids 0,1,2
        writeSingleColumnIdParquet(root.resolve("flag=False"), 2); // ids 0,1
        @SuppressWarnings("checkstyle:EmptyJavadoc") // the glob's '/**/' is misread as Javadoc
        String glob = StoragePath.fileUri(root) + "/**/*.parquet";
        String dataset = registerDataset("hive_parquet_bool", glob, Map.of("hive_partitioning", true));

        var request = syncEsqlQueryRequest("FROM " + dataset + " | SORT flag, id");
        request.profile(true);
        try (var response = run(request)) {
            assertFalse("external scan must run on a data node", externalScanNodeNames(response).isEmpty());

            int flagIdx = response.columns().stream().map(ColumnInfoImpl::name).toList().indexOf("flag");
            assertThat(
                "path-derived boolean partition types as BOOLEAN",
                response.columns().get(flagIdx).type(),
                equalTo(DataType.BOOLEAN)
            );

            List<List<Object>> rows = getValuesList(response);
            assertThat("all five rows return across both boolean partitions", rows.size(), equalTo(5));
            long trueRows = rows.stream().filter(r -> Boolean.TRUE.equals(r.get(flagIdx))).count();
            long falseRows = rows.stream().filter(r -> Boolean.FALSE.equals(r.get(flagIdx))).count();
            assertThat("flag=True folder contributes 3 rows typed boolean true", trueRows, equalTo(3L));
            assertThat("flag=False folder contributes 2 rows typed boolean false", falseRows, equalTo(2L));
        }

        // Typed boolean equality over the path-derived column keeps only the flag=False rows.
        try (var response = run(syncEsqlQueryRequest("FROM " + dataset + " | WHERE flag == false"))) {
            int flagIdx = response.columns().stream().map(ColumnInfoImpl::name).toList().indexOf("flag");
            List<List<Object>> rows = getValuesList(response);
            assertThat("WHERE flag == false keeps exactly the two flag=False rows", rows.size(), equalTo(2));
            assertTrue(
                "every kept row is flag=false, not just the right count",
                rows.stream().allMatch(r -> Boolean.FALSE.equals(r.get(flagIdx)))
            );
        }
    }

    /**
     * Two partitions ({@code p=a}: 3 rows, {@code p=b}: 2 rows). {@code COUNT(p)} must SAFE-MISS to a scan on the
     * data node, proving the partition-column signal reaches the data-node fold via the serialized
     * {@code _partition.columns} metadata (the coordinator-only {@code FileList} is {@code UNRESOLVED} there), not
     * just the coordinator gate. Without the serialized stamp the data-node fold warm-folds {@code COUNT(p)} to 0
     * (a {@code LocalSourceExec} with no scan operator).
     */
    public void testCountOfPartitionColumnSafeMissesToScanAcrossNodes() throws Exception {
        internalCluster().ensureAtLeastNumDataNodes(2);

        Path root = createTempDir().resolve("hive_parquet_count");
        writeSingleColumnIdParquet(root.resolve("p=a"), 3); // ids 0,1,2
        writeSingleColumnIdParquet(root.resolve("p=b"), 2); // ids 0,1
        @SuppressWarnings("checkstyle:EmptyJavadoc") // the glob's '/**/' is misread as Javadoc
        String glob = StoragePath.fileUri(root) + "/**/*.parquet";
        String dataset = registerDataset("hive_parquet_count", glob, Map.of("hive_partitioning", true));

        // COUNT(p) must SAFE-MISS to a scan on the data node (ExternalDataSourceOperator present), NOT warm-fold to
        // 0. Without the serialized partition-column stamp the data-node fold sees an empty partition set (fileList
        // is UNRESOLVED there) and folds COUNT(p) to a constant 0 -> a LocalSourceExec with no ExternalDataSource
        // operator. The scan then attaches the partition value to every row, so COUNT(p) counts all 5 rows.
        String query = "FROM " + dataset + " | STATS c = COUNT(p)";
        var request = syncEsqlQueryRequest(query);
        request.profile(true);
        try (var response = run(request)) {
            assertFalse(
                "COUNT(partition_column) must safe-miss to a scan on the data node, not warm-fold to 0",
                externalScanNodeNames(response).isEmpty()
            );

            List<List<Object>> rows = getValuesList(response);
            assertThat("COUNT(p) returns a single row", rows.size(), equalTo(1));
            assertThat(
                "COUNT(p) counts the partition value attached to all 5 rows (3 under p=a + 2 under p=b), not a folded 0",
                ((Number) rows.getFirst().getFirst()).longValue(),
                equalTo(5L)
            );
        }
    }
}
