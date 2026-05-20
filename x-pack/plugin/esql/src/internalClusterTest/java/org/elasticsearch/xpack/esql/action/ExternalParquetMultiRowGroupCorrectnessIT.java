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
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;
import org.elasticsearch.plugins.ExtensiblePlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.core.esql.action.ColumnInfo;
import org.elasticsearch.xpack.esql.datasource.http.HttpDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasource.parquet.ParquetDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlCapabilities.Cap.EXTERNAL_COMMAND;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.equalTo;

/**
 * End-to-end correctness check for the {@code EXTERNAL ... | STATS} pipeline against a
 * Parquet file with many row groups. Generates a controlled fixture, runs
 * {@code STATS COUNT(*) BY ...} (group-by prevents the {@code COUNT(*)} pushdown that
 * {@link ExternalParquetCountPushdownIT} exercises), and asserts that the sum of the group
 * counts and each per-bucket count match the exact number of rows written.
 *
 * <p>This is the regression net for the over-read / lost-wakeup class of bugs fixed in
 * {@link org.elasticsearch.xpack.esql.datasources.AsyncExternalSourceBuffer}: a single
 * shard or row group reading too many (or too few) pages, or a stalled driver, would
 * surface here as a mismatched total. The grouped {@code COUNT(*)} shape is what keeps the
 * test on the slow-scan path: both
 * {@link org.elasticsearch.xpack.esql.optimizer.rules.physical.local.PushStatsToExternalSource}
 * and
 * {@link org.elasticsearch.xpack.esql.optimizer.rules.physical.local.PushAggregatesToExternalSource}
 * bail out when {@code groupings()} is non-empty, so the rule for {@code COUNT(*)} pushdown
 * exercised by {@link ExternalParquetCountPushdownIT} does not fire here.
 *
 * <p>The Parquet writer is configured {@code UNCOMPRESSED}. Compressed codecs require
 * Parquet-MR's Hadoop-backed {@code CodecFactory} (and Woodstox/XML) on the writer side,
 * while the plugin's Hadoop-free codec factory is package-private. The over-read bug
 * class operates on decoded {@link org.elasticsearch.compute.data.Page}s — codec choice
 * does not change the buffer/operator path under test. Codec-coverage tests already live
 * in the parquet datasource module.
 */
public class ExternalParquetMultiRowGroupCorrectnessIT extends AbstractEsqlIntegTestCase {

    /**
     * Re-enables extension loading that {@link EsqlPluginWithEnterpriseOrTrialLicense} suppresses,
     * so the Parquet and HTTP data source plugins are discovered.
     */
    public static final class EsqlEnterpriseWithDatasourceExtensions extends EsqlPluginWithEnterpriseOrTrialLicense {
        @Override
        public void loadExtensions(ExtensiblePlugin.ExtensionLoader loader) {
            super.loadExtensions(loader);
        }
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.remove(EsqlPluginWithEnterpriseOrTrialLicense.class);
        plugins.add(EsqlEnterpriseWithDatasourceExtensions.class);
        plugins.add(HttpDataSourcePlugin.class);
        plugins.add(ParquetDataSourcePlugin.class);
        return plugins;
    }

    @Override
    protected QueryPragmas getPragmas() {
        return QueryPragmas.EMPTY;
    }

    /**
     * STATS COUNT(*) BY ... over a multi-row-group Parquet file. The GROUP BY defeats both
     * {@code COUNT(*)} pushdown rules so every row group is read through the async source
     * operator. Both the sum of per-bucket counts and each individual per-bucket count must
     * match the exact number of rows written, exercising the full ESQL plan (real planner,
     * real Driver, real buffer) end-to-end.
     */
    public void testGroupedCountMatchesRowCountWithMultipleRowGroups() throws Exception {
        assumeTrue("requires EXTERNAL command capability", EXTERNAL_COMMAND.isEnabled());

        int rowCount = 50_000;
        int bucketCount = 8;
        // 50_000 rows of 512-byte payload at rowGroupSize=64 KiB forces many row groups even
        // uncompressed — that's the property this test relies on. The fixture is regenerated
        // every run, so a writer regression collapsing everything into one row group would
        // still pass the count assertion but reduce the multi-row-group coverage; that is an
        // acceptable graceful degradation rather than a hard prerequisite for the test.
        Path parquetFile = writeMultiRowGroupParquetFile(rowCount, bucketCount);
        try {
            String query = "EXTERNAL \"" + StoragePath.fileUri(parquetFile) + "\" | STATS c = COUNT(*) BY bucket | KEEP c, bucket";
            var request = syncEsqlQueryRequest(query);

            try (var response = run(request)) {
                List<? extends ColumnInfo> columns = response.columns();
                assertThat("expected (c, bucket) columns, got " + columns, columns.size(), equalTo(2));
                assertThat(columns.get(0).name(), equalTo("c"));
                assertThat(columns.get(1).name(), equalTo("bucket"));

                List<List<Object>> rows = getValuesList(response);
                assertThat("expected one row per bucket key", (long) rows.size(), equalTo((long) bucketCount));

                long total = 0;
                long expectedPerBucket = (long) rowCount / bucketCount;
                for (List<Object> row : rows) {
                    long count = ((Number) row.get(0)).longValue();
                    assertThat("per-bucket count must equal rowCount/bucketCount (row=" + row + ")", count, equalTo(expectedPerBucket));
                    total += count;
                }
                assertThat("sum of grouped counts must equal the row count written", total, equalTo((long) rowCount));
            }
        } finally {
            Files.deleteIfExists(parquetFile);
        }
    }

    /**
     * Writes an UNCOMPRESSED Parquet file with {@code rowCount} rows split evenly across
     * {@code bucketCount} groups and a small row-group size to force multiple row groups. The
     * 512-byte payload column is the row-size lever that makes 64 KiB row groups roll over
     * many times for 50_000 rows.
     */
    private Path writeMultiRowGroupParquetFile(int rowCount, int bucketCount) throws IOException {
        MessageType schema = Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.INT64)
            .named("id")
            .required(PrimitiveType.PrimitiveTypeName.INT32)
            .named("bucket")
            .required(PrimitiveType.PrimitiveTypeName.BINARY)
            .as(LogicalTypeAnnotation.stringType())
            .named("payload")
            .named("external_multi_rg_schema");

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        OutputFile outputFile = createOutputFile(baos);
        SimpleGroupFactory factory = new SimpleGroupFactory(schema);

        String payload = "a".repeat(512);

        try (
            ParquetWriter<Group> writer = ExampleParquetWriter.builder(outputFile)
                .withConf(new PlainParquetConfiguration())
                .withType(schema)
                .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
                .withRowGroupSize(64L * 1024L)
                .withPageSize(8 * 1024)
                .build()
        ) {
            for (int i = 0; i < rowCount; i++) {
                Group g = factory.newGroup();
                g.add("id", (long) i);
                g.add("bucket", i % bucketCount);
                g.add("payload", payload);
                writer.write(g);
            }
        }

        Path tempFile = createTempDir().resolve("multi_rg_correctness.parquet");
        Files.write(tempFile, baos.toByteArray());
        return tempFile;
    }

    private static OutputFile createOutputFile(ByteArrayOutputStream baos) {
        return new OutputFile() {
            @Override
            public PositionOutputStream create(long blockSizeHint) {
                return new PositionOutputStream() {
                    private long position = 0;

                    @Override
                    public long getPos() {
                        return position;
                    }

                    @Override
                    public void write(int b) throws IOException {
                        baos.write(b);
                        position++;
                    }

                    @Override
                    public void write(byte[] b, int off, int len) throws IOException {
                        baos.write(b, off, len);
                        position += len;
                    }
                };
            }

            @Override
            public PositionOutputStream createOrOverwrite(long blockSizeHint) {
                return create(blockSizeHint);
            }

            @Override
            public boolean supportsBlockSize() {
                return false;
            }

            @Override
            public long defaultBlockSize() {
                return 0;
            }
        };
    }
}
