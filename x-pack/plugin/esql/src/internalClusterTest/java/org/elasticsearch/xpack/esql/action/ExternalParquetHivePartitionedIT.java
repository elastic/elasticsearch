/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.apache.parquet.conf.PlainParquetConfiguration;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.esql.datasource.parquet.ParquetDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;

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
     * Two partitions ({@code p=a}: 3 rows, {@code p=b}: 2 rows). {@code COUNT(p)} must SAFE-MISS to a scan on the
     * data node — proving the partition-column signal reaches the data-node fold via the serialized
     * {@code _partition.columns} metadata (the coordinator-only {@code FileList} is {@code UNRESOLVED} there), not
     * just the coordinator gate. Without the serialized stamp the data-node fold warm-folds {@code COUNT(p)} to 0
     * (a {@code LocalSourceExec} with no scan operator).
     */
    public void testCountOfPartitionColumnSafeMissesToScanAcrossNodes() throws Exception {
        internalCluster().ensureAtLeastNumDataNodes(2);

        Path root = createTempDir().resolve("hive_parquet_count");
        writeIdParquet(root.resolve("p=a"), 3); // ids 0,1,2
        writeIdParquet(root.resolve("p=b"), 2); // ids 0,1
        @SuppressWarnings("checkstyle:EmptyJavadoc") // the glob's '/**/' is misread as Javadoc
        String glob = StoragePath.fileUri(root) + "/**/*.parquet";
        String dataset = registerDataset("hive_parquet_count", glob, Map.of("hive_partitioning", true));

        // COUNT(p) must SAFE-MISS to a scan on the data node (ExternalDataSourceOperator present), NOT warm-fold to
        // 0. Without the serialized partition-column stamp the data-node fold sees an empty partition set (fileList
        // is UNRESOLVED there) and folds COUNT(p) to a constant 0 -> a LocalSourceExec with no ExternalDataSource
        // operator. NOTE: the exact COUNT(p) VALUE is deliberately not asserted here — parquet hive-partition-column
        // value attachment (separate from this fold fix) is broken for multi-file reads and tracked separately.
        String query = "FROM " + dataset + " | STATS c = COUNT(p)";
        var request = syncEsqlQueryRequest(query);
        request.profile(true);
        try (var response = run(request)) {
            boolean scanned = false;
            for (var d : response.profile().drivers()) {
                for (var op : d.operators()) {
                    if (op.operator().startsWith("ExternalDataSource")) {
                        scanned = true;
                    }
                }
            }
            assertTrue("COUNT(partition_column) must safe-miss to a scan on the data node, not warm-fold to 0", scanned);
        }
    }

    /** Writes a single-column ({@code id: INT32}) Parquet file with {@code rowCount} rows (ids 0..rowCount-1). */
    private void writeIdParquet(Path dir, int rowCount) throws IOException {
        Files.createDirectories(dir);
        MessageType schema = Types.buildMessage().required(PrimitiveType.PrimitiveTypeName.INT32).named("id").named("test_schema");

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        SimpleGroupFactory factory = new SimpleGroupFactory(schema);
        try (
            ParquetWriter<Group> writer = ExampleParquetWriter.builder(createOutputFile(baos))
                .withConf(new PlainParquetConfiguration())
                .withType(schema)
                .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
                .build()
        ) {
            for (int i = 0; i < rowCount; i++) {
                Group g = factory.newGroup();
                g.add("id", i);
                writer.write(g);
            }
        }
        Files.write(dir.resolve("data.parquet"), baos.toByteArray());
    }
}
