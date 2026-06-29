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
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.core.esql.action.ColumnInfo;
import org.elasticsearch.xpack.esql.datasource.parquet.ParquetDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.elasticsearch.xpack.esql.plugin.ComputeService.DATA_DESCRIPTION;
import static org.hamcrest.Matchers.emptyString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.not;

/**
 * End-to-end correctness check for the {@code FROM <dataset> ... | STATS} pipeline against a
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
public class ExternalParquetMultiRowGroupCorrectnessIT extends AbstractExternalDataSourceIT {

    @Override
    protected Collection<Class<? extends Plugin>> formatPlugins() {
        return List.of(ParquetDataSourcePlugin.class);
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
        int rowCount = 50_000;
        int bucketCount = 8;
        // Per-bucket assertion relies on exact integer division; otherwise truncation would
        // silently change the expected count and surface as a confusing assertion failure.
        assertEquals("rowCount must be evenly divisible by bucketCount for this test", 0, rowCount % bucketCount);
        // 50_000 rows of 512-byte payload at rowGroupSize=64 KiB forces many row groups even
        // uncompressed — that's the property this test relies on. The fixture is regenerated
        // every run, so a writer regression collapsing everything into one row group would
        // still pass the count assertion but reduce the multi-row-group coverage; that is an
        // acceptable graceful degradation rather than a hard prerequisite for the test.
        Path parquetFile = writeMultiRowGroupParquetFile(rowCount, bucketCount);
        try {
            String dataset = registerDataset("multi_rowgroup", StoragePath.fileUri(parquetFile), Map.of());
            String query = "FROM " + dataset + " | STATS c = COUNT(*) BY bucket | KEEP c, bucket";
            var request = syncEsqlQueryRequest(query);
            request.profile(true);

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

                var profile = response.profile();
                assertNotNull("profile must be present (request had profile=true)", profile);
                List<String> driverDescriptions = profile.drivers().stream().map(d -> d.description()).toList();
                // Empirically the fixture produces one driver per external split plus a single
                // coordinator driver, with no node-level reduction kicking in for the
                // external path on this query shape. Assert the kinds exactly (and that no
                // unexpected driver kind sneaks in) rather than the count: the split count is
                // a property of the Parquet writer / external splitter, not the bug under test.
                long dataCount = driverDescriptions.stream().filter(DATA_DESCRIPTION::equals).count();
                long finalCount = driverDescriptions.stream().filter("final"::equals).count();
                assertThat("coordinator must contribute exactly one 'final' driver, got " + driverDescriptions, finalCount, equalTo(1L));
                assertThat(
                    "at least one data-node '"
                        + DATA_DESCRIPTION
                        + "' driver must be present (this is what was lost pre-fix), got "
                        + driverDescriptions,
                    dataCount,
                    greaterThanOrEqualTo(1L)
                );
                assertThat(
                    "unexpected driver kinds in profile " + driverDescriptions + " (expected only '" + DATA_DESCRIPTION + "' and 'final')",
                    (long) driverDescriptions.size(),
                    equalTo(dataCount + finalCount)
                );

                List<String> planDescriptions = profile.plans().stream().map(p -> p.description()).toList();
                long dataPlanCount = planDescriptions.stream().filter(DATA_DESCRIPTION::equals).count();
                long finalPlanCount = planDescriptions.stream().filter(d -> d.endsWith("final")).count();
                assertThat(
                    "coordinator must contribute exactly one 'final' plan profile, got " + planDescriptions,
                    finalPlanCount,
                    equalTo(1L)
                );
                assertThat(
                    "at least one data-node '"
                        + DATA_DESCRIPTION
                        + "' plan profile must be present (this is what was lost pre-fix), got "
                        + planDescriptions,
                    dataPlanCount,
                    greaterThanOrEqualTo(1L)
                );
                assertThat(
                    "unexpected plan profile kinds " + planDescriptions + " (expected only '" + DATA_DESCRIPTION + "' and '*final')",
                    (long) planDescriptions.size(),
                    equalTo(dataPlanCount + finalPlanCount)
                );
                String dataPlanTree = profile.plans()
                    .stream()
                    .filter(p -> DATA_DESCRIPTION.equals(p.description()))
                    .map(p -> p.planTree())
                    .findFirst()
                    .orElse(null);
                assertThat("data-node plan profile must include the local physical plan tree", dataPlanTree, not(emptyString()));
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
}
