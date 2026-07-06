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
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.ExtensiblePlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.esql.datasource.http.HttpDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasource.parquet.ParquetDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlCapabilities.Cap.EXTERNAL_COMMAND;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;

/**
 * Verifies two related {@code normalizeStatValue} representation bugs in the Parquet footer-stats
 * MIN/MAX pushdown:
 * <ul>
 *   <li>Binary-backed FLOAT16/DECIMAL columns (logical types over BINARY/FIXED_LEN_BYTE_ARRAY) used
 *   to 500 with a {@code ClassCastException}: the footer stat was stringified, and the DOUBLE-typed
 *   aggregate tried to cast that String to a Double.</li>
 *   <li>{@code unsigned_long} columns used to silently return a wrong (even inverted, min &gt; max)
 *   MIN/MAX: the raw INT64 footer stat was left un-encoded while the aggregate expects the
 *   sign-flip-encoded representation the scan path produces.</li>
 * </ul>
 * <p>
 * The {@code EsqlEnterpriseWithDatasourceExtensions}/{@code nodePlugins()}/{@code createOutputFile}
 * boilerplate mirrors {@link ExternalParquetTemporalAggregatePushdownIT}; there is no shared base for
 * these {@code EXTERNAL "file://..."} tests, so the pattern is duplicated for consistency rather than
 * extracted here.
 */
public class ExternalParquetBinaryNumericAggregatePushdownIT extends AbstractEsqlIntegTestCase {

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
        return new QueryPragmas(Settings.builder().put("parsing_parallelism", 1).build());
    }

    public void testMinMaxBinaryBackedFloat16AndDecimalColumns() throws Exception {
        assumeTrue("requires EXTERNAL command capability", EXTERNAL_COMMAND.isEnabled());

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

            String statsQuery = "EXTERNAL \"" + fileUri + "\" | STATS lo_f16=MIN(f16), hi_f16=MAX(f16), lo_dec=MIN(dec), hi_dec=MAX(dec)";

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

    public void testMinMaxUnsignedLongColumn() throws Exception {
        assumeTrue("requires EXTERNAL command capability", EXTERNAL_COMMAND.isEnabled());

        // Reproduces the issue's exact values: u64 = {0, 5, 2^63}. Stats pushdown used to return
        // MIN/MAX in raw (un-encoded) signed space while the aggregate expects the sign-flip-encoded
        // representation, so the values came back silently swapped (MIN > MAX, no error).
        Path parquetFile = writeUnsignedLongParquetFile();
        try {
            String fileUri = StoragePath.fileUri(parquetFile);
            String statsQuery = "EXTERNAL \"" + fileUri + "\" | STATS lo = MIN(u64), hi = MAX(u64)";

            try (var response = run(syncEsqlQueryRequest(statsQuery).profile(true))) {
                List<List<Object>> rows = getValuesList(response);
                assertThat(rows.size(), equalTo(1));
                List<Object> row = rows.get(0);

                assertThat("unsigned_long MIN", toBigInteger(row.get(0)), equalTo(BigInteger.ZERO));
                assertThat("unsigned_long MAX", toBigInteger(row.get(1)), equalTo(BigInteger.TWO.pow(63)));

                assertPushdownFired(response);
            }
        } finally {
            Files.deleteIfExists(parquetFile);
        }
    }

    /**
     * {@code unsigned_long} response values may come back as a {@link Long} (fits in a signed long)
     * or a {@link Double}/{@link String} for values above {@code Long.MAX_VALUE}, depending on the
     * response encoding. Normalizing through {@link BigDecimal} handles all of those uniformly.
     */
    private static BigInteger toBigInteger(Object value) {
        return new BigDecimal(value.toString()).toBigInteger();
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

    private Path writeUnsignedLongParquetFile() throws IOException {
        MessageType schema = Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.INT64)
            .as(LogicalTypeAnnotation.intType(64, false)) // unsigned, bit-width 64
            .named("u64")
            .named("test_schema");

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        OutputFile outputFile = createOutputFile(baos);
        SimpleGroupFactory factory = new SimpleGroupFactory(schema);

        // Raw signed INT64 bits: unsigned 0 -> 0L, unsigned 5 -> 5L, unsigned 2^63 -> Long.MIN_VALUE.
        try (
            ParquetWriter<Group> writer = ExampleParquetWriter.builder(outputFile)
                .withConf(new PlainParquetConfiguration())
                .withType(schema)
                .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
                .build()
        ) {
            writer.write(factory.newGroup().append("u64", 0L));
            writer.write(factory.newGroup().append("u64", 5L));
            writer.write(factory.newGroup().append("u64", Long.MIN_VALUE));
        }

        Path tempFile = createTempDir().resolve("unsigned_long_pushdown_test.parquet");
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
