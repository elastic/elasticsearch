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
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.esql.datasource.parquet.ParquetDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.equalTo;

/**
 * Verifies that ungrouped MIN/MAX over temporal columns in Parquet files return
 * epoch-millis values that match the scan-path decode, not raw physical units
 * (days, microseconds). Covers date32, timestamp[us], and timestamp[ms].
 */
public class ExternalParquetTemporalAggregatePushdownIT extends AbstractExternalDataSourceIT {

    @Override
    protected Collection<Class<? extends Plugin>> formatPlugins() {
        return List.of(ParquetDataSourcePlugin.class);
    }

    @Override
    protected QueryPragmas getPragmas() {
        return new QueryPragmas(Settings.builder().put("parsing_parallelism", 1).build());
    }

    public void testMinMaxTemporalColumns() throws Exception {
        long millis2000 = Instant.parse("2000-01-01T00:00:00Z").toEpochMilli();
        long millis2020 = Instant.parse("2020-01-01T00:00:00Z").toEpochMilli();
        int days2000 = (int) (millis2000 / 86_400_000L);
        int days2020 = (int) (millis2020 / 86_400_000L);
        long micros2000 = millis2000 * 1_000;
        long micros2020 = millis2020 * 1_000;

        Path parquetFile = writeTemporalParquetFile(days2000, days2020, micros2000, micros2020, millis2000, millis2020);
        try {
            String fileUri = StoragePath.fileUri(parquetFile);
            String dataset = registerDataset("temporal_pushdown", fileUri, Map.of());

            // STATS MIN/MAX is served from the Parquet footer stats via PushStatsToExternalSource.
            // This is the path that returned raw days/micros before the decode fix; profile=true +
            // the no-Async assertion below ensure the values come from pushdown, not a scan fallback
            // (the scan path was always correct, so without this a regression could hide).
            String statsQuery = "FROM "
                + dataset
                + " | STATS lo_d32=MIN(d32), hi_d32=MAX(d32), lo_tus=MIN(tus), hi_tus=MAX(tus), lo_tms=MIN(tms), hi_tms=MAX(tms)";

            try (var response = run(syncEsqlQueryRequest(statsQuery).profile(true))) {
                List<List<Object>> rows = getValuesList(response);
                assertThat(rows.size(), equalTo(1));
                List<Object> row = rows.get(0);

                assertThat("date32 MIN", toEpochMillis(row.get(0)), equalTo(millis2000));
                assertThat("date32 MAX", toEpochMillis(row.get(1)), equalTo(millis2020));
                assertThat("timestamp[us] MIN", toEpochMillis(row.get(2)), equalTo(millis2000));
                assertThat("timestamp[us] MAX", toEpochMillis(row.get(3)), equalTo(millis2020));
                assertThat("timestamp[ms] MIN", toEpochMillis(row.get(4)), equalTo(millis2000));
                assertThat("timestamp[ms] MAX", toEpochMillis(row.get(5)), equalTo(millis2020));

                assertPushdownFired(response);
            }
        } finally {
            Files.deleteIfExists(parquetFile);
        }
    }

    /**
     * When MIN/MAX pushdown fires the plan is a LocalSourceExec served from footer stats — there is
     * no AsyncExternalSourceOperatorFactory scanning the file. Mirrors {@code ExternalParquetCountPushdownIT}.
     */
    private static void assertPushdownFired(EsqlQueryResponse response) {
        var profile = response.profile();
        assertNotNull("profile must be present (request had profile=true)", profile);
        for (var driver : profile.drivers()) {
            for (var op : driver.operators()) {
                assertFalse(
                    "expected MIN/MAX pushdown (no Async* operators) but found: " + op.operator(),
                    op.operator().startsWith("Async")
                );
            }
        }
    }

    private Path writeTemporalParquetFile(int days2000, int days2020, long micros2000, long micros2020, long millis2000, long millis2020)
        throws IOException {
        MessageType schema = Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.INT32)
            .as(LogicalTypeAnnotation.dateType())
            .named("d32")
            .required(PrimitiveType.PrimitiveTypeName.INT64)
            .as(LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.MICROS))
            .named("tus")
            .required(PrimitiveType.PrimitiveTypeName.INT64)
            .as(LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.MILLIS))
            .named("tms")
            .named("test_schema");

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        OutputFile outputFile = createOutputFile(baos);
        SimpleGroupFactory factory = new SimpleGroupFactory(schema);

        try (
            ParquetWriter<Group> writer = ExampleParquetWriter.builder(outputFile)
                .withConf(new PlainParquetConfiguration())
                .withType(schema)
                .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
                .build()
        ) {
            Group g1 = factory.newGroup();
            g1.add("d32", days2000);
            g1.add("tus", micros2000);
            g1.add("tms", millis2000);
            writer.write(g1);

            Group g2 = factory.newGroup();
            g2.add("d32", days2020);
            g2.add("tus", micros2020);
            g2.add("tms", millis2020);
            writer.write(g2);
        }

        Path tempFile = createTempDir().resolve("temporal_pushdown_test.parquet");
        Files.write(tempFile, baos.toByteArray());
        return tempFile;
    }

    private static long toEpochMillis(Object value) {
        if (value instanceof String s) {
            return Instant.parse(s).toEpochMilli();
        }
        return ((Number) value).longValue();
    }
}
