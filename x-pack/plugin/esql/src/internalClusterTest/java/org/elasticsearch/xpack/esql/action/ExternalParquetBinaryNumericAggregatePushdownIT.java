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
import org.apache.parquet.io.api.Binary;
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
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;

/**
 * Verifies that ungrouped MIN/MAX over Binary-backed FLOAT16 and DECIMAL Parquet columns (logical
 * types over BINARY/FIXED_LEN_BYTE_ARRAY) return numeric values via footer-stats pushdown instead
 * of throwing a {@code ClassCastException}. Before the fix, {@code normalizeStatValue} stringified
 * the Binary footer stat, and the DOUBLE-typed aggregate tried to cast that String to a Double.
 */
public class ExternalParquetBinaryNumericAggregatePushdownIT extends AbstractExternalDataSourceIT {

    @Override
    protected Collection<Class<? extends Plugin>> formatPlugins() {
        return List.of(ParquetDataSourcePlugin.class);
    }

    @Override
    protected QueryPragmas getPragmas() {
        return new QueryPragmas(Settings.builder().put("parsing_parallelism", 1).build());
    }

    public void testMinMaxBinaryBackedFloat16AndDecimalColumns() throws Exception {
        float f16Lo = -1.0f;
        float f16Hi = 3.14f;
        double f16LoExpected = Float.float16ToFloat(Float.floatToFloat16(f16Lo));
        double f16HiExpected = Float.float16ToFloat(Float.floatToFloat16(f16Hi));

        int decimalScale = 2;
        long decimalLoUnscaled = -100; // -1.00
        long decimalHiUnscaled = 1234567; // 12345.67
        double decimalLoExpected = new BigDecimal(BigInteger.valueOf(decimalLoUnscaled), decimalScale).doubleValue();
        double decimalHiExpected = new BigDecimal(BigInteger.valueOf(decimalHiUnscaled), decimalScale).doubleValue();

        Path parquetFile = writeBinaryNumericParquetFile(f16Lo, f16Hi, decimalScale, decimalLoUnscaled, decimalHiUnscaled);
        try {
            String fileUri = StoragePath.fileUri(parquetFile);
            String dataset = registerDataset("binary_numeric_pushdown", fileUri, Map.of());

            String statsQuery = "FROM " + dataset + " | STATS lo_f16=MIN(f16), hi_f16=MAX(f16), lo_dec=MIN(dec), hi_dec=MAX(dec)";

            try (var response = run(syncEsqlQueryRequest(statsQuery).profile(true))) {
                List<List<Object>> rows = getValuesList(response);
                assertThat(rows.size(), equalTo(1));
                List<Object> row = rows.get(0);

                assertThat("float16 MIN", ((Number) row.get(0)).doubleValue(), closeTo(f16LoExpected, 0.001));
                assertThat("float16 MAX", ((Number) row.get(1)).doubleValue(), closeTo(f16HiExpected, 0.001));
                assertThat("decimal MIN", ((Number) row.get(2)).doubleValue(), closeTo(decimalLoExpected, 0.001));
                assertThat("decimal MAX", ((Number) row.get(3)).doubleValue(), closeTo(decimalHiExpected, 0.001));

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

    private Path writeBinaryNumericParquetFile(float f16Lo, float f16Hi, int decimalScale, long decimalLoUnscaled, long decimalHiUnscaled)
        throws IOException {
        MessageType schema = Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY)
            .length(2)
            .as(LogicalTypeAnnotation.float16Type())
            .named("f16")
            .required(PrimitiveType.PrimitiveTypeName.BINARY)
            .as(LogicalTypeAnnotation.decimalType(decimalScale, 10))
            .named("dec")
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
            g1.add("f16", Binary.fromConstantByteArray(toFloat16Bytes(f16Lo)));
            g1.add("dec", Binary.fromConstantByteArray(BigInteger.valueOf(decimalLoUnscaled).toByteArray()));
            writer.write(g1);

            Group g2 = factory.newGroup();
            g2.add("f16", Binary.fromConstantByteArray(toFloat16Bytes(f16Hi)));
            g2.add("dec", Binary.fromConstantByteArray(BigInteger.valueOf(decimalHiUnscaled).toByteArray()));
            writer.write(g2);
        }

        Path tempFile = createTempDir().resolve("binary_numeric_pushdown_test.parquet");
        Files.write(tempFile, baos.toByteArray());
        return tempFile;
    }

    private static byte[] toFloat16Bytes(float value) {
        short float16 = Float.floatToFloat16(value);
        byte[] bytes = new byte[2];
        bytes[0] = (byte) (float16 & 0xFF);
        bytes[1] = (byte) ((float16 >> 8) & 0xFF);
        return bytes;
    }
}
