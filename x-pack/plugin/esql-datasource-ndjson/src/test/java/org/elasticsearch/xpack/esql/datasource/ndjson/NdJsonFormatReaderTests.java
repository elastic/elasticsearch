/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.ndjson;

import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.LimitedBreaker;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.CloseableIterator;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.DrainSimulatingStorageObject;
import org.elasticsearch.xpack.esql.datasources.ExternalSourceSettings;
import org.elasticsearch.xpack.esql.datasources.spi.AbstractTestStorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReadContext;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.SourceMetadata;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.datasources.spi.WidenedColumn;
import org.hamcrest.Matchers;
import org.junit.Before;

import java.io.ByteArrayInputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;

/**
 * Unit tests for {@link NdJsonFormatReader#openForSchemaInference(StorageObject, boolean)}.
 *
 * <p>Covers the contract that a non-first split starts the schema-inference stream at the first
 * byte of the first complete record: LF, CRLF, and lone-CR terminators, on both markable and
 * non-markable underlying streams, plus the stream-ends-before-newline edge case.
 */
public class NdJsonFormatReaderTests extends ESTestCase {

    private BlockFactory blockFactory;

    @Before
    public void setUpBlockFactory() {
        blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(NoopCircuitBreaker.INSTANCE).build();
    }

    /**
     * esql-planning#2143: planning-time inference charges the reader's breaker, and a refusal leaves
     * {@code metadata()} as a {@link CircuitBreakingException} (HTTP 429), not as an {@code ExternalClientException}
     * about the user's data, and without having been retried on later records.
     */
    public void testMetadataSurfacesBreakerTripAndReleasesReservation() {
        StringBuilder record = new StringBuilder("{");
        for (int i = 0; i < 5_000; i++) {
            record.append(i == 0 ? "" : ",").append("\"column_").append(i).append("\":1");
        }
        byte[] bytes = (record + "}\n").repeat(3).getBytes(StandardCharsets.UTF_8);
        LimitedBreaker breaker = new LimitedBreaker("test", ByteSizeValue.ofKb(100));
        BlockFactory limited = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(breaker).build();

        expectThrows(CircuitBreakingException.class, () -> new NdJsonFormatReader(null, limited).metadata(new BytesObject(bytes)));
        assertEquals(0L, breaker.getUsed());
    }

    /** A schema that fits is returned and leaves nothing reserved, since the caller accounts for what it keeps. */
    public void testMetadataReleasesReservationOnSuccess() throws IOException {
        LimitedBreaker breaker = new LimitedBreaker("test", ByteSizeValue.ofMb(16));
        BlockFactory limited = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(breaker).build();
        byte[] bytes = "{\"a\":1,\"b\":{\"c\":\"x\"}}\n".getBytes(StandardCharsets.UTF_8);

        assertEquals(2, new NdJsonFormatReader(null, limited).metadata(new BytesObject(bytes)).schema().size());
        assertEquals(0L, breaker.getUsed());
    }

    /** The default cap follows {@code index.mapping.total_fields.limit}: 1000 fields infer, the next one is refused. */
    public void testMetadataAppliesTheDefaultFieldCap() throws IOException {
        int limit = ExternalSourceSettings.DEFAULT_SCHEMA_MAX_FIELDS;
        NdJsonFormatReader reader = new NdJsonFormatReader(null, blockFactory);
        assertEquals(limit, reader.metadata(new BytesObject(flatRecord(limit))).schema().size());
        expectThrows(IllegalArgumentException.class, () -> reader.metadata(new BytesObject(flatRecord(limit + 1))));
    }

    /** A dataset raises or lowers the cap with {@code schema_max_fields}, and registration refuses one outside 1 to the ceiling. */
    public void testSchemaMaxFieldsConfiguresTheCap() throws IOException {
        int limit = ExternalSourceSettings.DEFAULT_SCHEMA_MAX_FIELDS;
        FormatReader raised = new NdJsonFormatReader(null, blockFactory).withConfigTrackingConsumedKeys(
            Map.of(NdJsonFormatReader.CONFIG_SCHEMA_MAX_FIELDS, limit + 1)
        ).value();
        assertEquals(limit + 1, raised.metadata(new BytesObject(flatRecord(limit + 1))).schema().size());

        FormatReader lowered = new NdJsonFormatReader(null, blockFactory).withConfigTrackingConsumedKeys(
            Map.of(NdJsonFormatReader.CONFIG_SCHEMA_MAX_FIELDS, 2)
        ).value();
        expectThrows(IllegalArgumentException.class, () -> lowered.metadata(new BytesObject(flatRecord(3))));

        expectThrows(
            IllegalArgumentException.class,
            () -> NdJsonFormatReader.validateConfig(Map.of(NdJsonFormatReader.CONFIG_SCHEMA_MAX_FIELDS, 0))
        );
        NdJsonFormatReader.validateConfig(
            Map.of(NdJsonFormatReader.CONFIG_SCHEMA_MAX_FIELDS, ExternalSourceSettings.MAX_SCHEMA_MAX_FIELDS)
        );
        expectThrows(
            IllegalArgumentException.class,
            () -> NdJsonFormatReader.validateConfig(
                Map.of(NdJsonFormatReader.CONFIG_SCHEMA_MAX_FIELDS, ExternalSourceSettings.MAX_SCHEMA_MAX_FIELDS + 1)
            )
        );
    }

    /** The node setting replaces the default, and a dataset's {@code schema_max_fields} still overrides it. */
    public void testNodeSettingSetsTheDefaultFieldCap() throws IOException {
        Settings settings = Settings.builder().put(ExternalSourceSettings.SCHEMA_MAX_FIELDS.getKey(), 2).build();
        NdJsonFormatReader reader = new NdJsonFormatReader(settings, blockFactory);
        assertEquals(2, reader.metadata(new BytesObject(flatRecord(2))).schema().size());
        expectThrows(IllegalArgumentException.class, () -> reader.metadata(new BytesObject(flatRecord(3))));

        FormatReader overridden = reader.withConfigTrackingConsumedKeys(Map.of(NdJsonFormatReader.CONFIG_SCHEMA_MAX_FIELDS, 3)).value();
        assertEquals(3, overridden.metadata(new BytesObject(flatRecord(3))).schema().size());
    }

    /** A value that is not a number at all is refused with a message naming the key, not the JDK's bare one. */
    public void testSchemaMaxFieldsRejectsNonIntegerNamingTheKey() {
        for (Object value : new Object[] { "abc", "", 500.0 }) {
            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> NdJsonFormatReader.validateConfig(Map.of(NdJsonFormatReader.CONFIG_SCHEMA_MAX_FIELDS, value))
            );
            assertEquals("[schema_max_fields] must be an integer between 1 and 100000, got [" + value + "]", e.getMessage());
        }
        assertEquals(500, ExternalSourceSettings.parseDatasetSchemaMaxFields("500", NdJsonFormatReader.CONFIG_SCHEMA_MAX_FIELDS, 1));
    }

    /** At the ceiling, the refusal does not tell the user to raise a cap that cannot go higher. */
    public void testFieldCapAtCeilingDoesNotSuggestRaisingIt() throws IOException {
        int ceiling = ExternalSourceSettings.MAX_SCHEMA_MAX_FIELDS;
        FormatReader atCeiling = new NdJsonFormatReader(null, blockFactory).withConfigTrackingConsumedKeys(
            Map.of(NdJsonFormatReader.CONFIG_SCHEMA_MAX_FIELDS, ceiling)
        ).value();
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> atCeiling.metadata(new BytesObject(flatRecord(ceiling + 1)))
        );
        assertThat(e.getMessage(), containsString("the most [schema_max_fields] allows"));
        assertThat(e.getMessage(), not(containsString("raise")));

        FormatReader below = new NdJsonFormatReader(null, blockFactory).withConfigTrackingConsumedKeys(
            Map.of(NdJsonFormatReader.CONFIG_SCHEMA_MAX_FIELDS, 2)
        ).value();
        e = expectThrows(IllegalArgumentException.class, () -> below.metadata(new BytesObject(flatRecord(3))));
        assertThat(e.getMessage(), containsString("raise [schema_max_fields]"));
    }

    /** The node setting is bounded like the dataset key, so neither can lift the cap past the ceiling. */
    public void testNodeSettingRejectsValuesAboveTheCeiling() {
        int ceiling = ExternalSourceSettings.MAX_SCHEMA_MAX_FIELDS;
        Settings atCeiling = Settings.builder().put(ExternalSourceSettings.SCHEMA_MAX_FIELDS.getKey(), ceiling).build();
        assertEquals(ceiling, (int) ExternalSourceSettings.SCHEMA_MAX_FIELDS.get(atCeiling));

        Settings aboveCeiling = Settings.builder().put(ExternalSourceSettings.SCHEMA_MAX_FIELDS.getKey(), ceiling + 1).build();
        expectThrows(IllegalArgumentException.class, () -> new NdJsonFormatReader(aboveCeiling, blockFactory));
    }

    private static byte[] flatRecord(int columns) {
        StringBuilder record = new StringBuilder("{");
        for (int i = 0; i < columns; i++) {
            record.append(i == 0 ? "" : ",").append("\"c").append(i).append("\":1");
        }
        return (record + "}\n").getBytes(StandardCharsets.UTF_8);
    }

    public void testSkipFirstLineFalseReturnsStreamUnchanged() throws IOException {
        byte[] bytes = "whatever".getBytes(StandardCharsets.UTF_8);
        try (InputStream s = NdJsonFormatReader.openForSchemaInference(new BytesObject(bytes), false)) {
            assertArrayEquals(bytes, s.readAllBytes());
        }
    }

    public void testSkipFirstLineLf() throws IOException {
        byte[] bytes = "partial-record\n{\"id\":1}\n{\"id\":2}\n".getBytes(StandardCharsets.UTF_8);
        try (InputStream s = NdJsonFormatReader.openForSchemaInference(new BytesObject(bytes), true)) {
            assertEquals("{\"id\":1}\n{\"id\":2}\n", new String(s.readAllBytes(), StandardCharsets.UTF_8));
        }
    }

    public void testSkipFirstLineCrLf() throws IOException {
        byte[] bytes = "partial\r\n{\"id\":1}\r\n".getBytes(StandardCharsets.UTF_8);
        try (InputStream s = NdJsonFormatReader.openForSchemaInference(new BytesObject(bytes), true)) {
            assertEquals("{\"id\":1}\r\n", new String(s.readAllBytes(), StandardCharsets.UTF_8));
        }
    }

    /** Lone CR on a markable underlying stream: the scanner's peeked byte is unread back onto the pushback wrapper. */
    public void testSkipFirstLineLoneCrMarkable() throws IOException {
        byte[] bytes = "partial\r{\"id\":1}\n".getBytes(StandardCharsets.UTF_8);
        try (InputStream s = NdJsonFormatReader.openForSchemaInference(new BytesObject(bytes), true)) {
            assertEquals("{\"id\":1}\n", new String(s.readAllBytes(), StandardCharsets.UTF_8));
        }
    }

    /** Lone CR on a non-markable underlying stream: the pushback wrapper still accepts the unread and returns it on the next read. */
    public void testSkipFirstLineLoneCrNonMarkable() throws IOException {
        byte[] bytes = "partial\r{\"id\":1}\n".getBytes(StandardCharsets.UTF_8);
        try (InputStream s = NdJsonFormatReader.openForSchemaInference(new NonMarkableBytesObject(bytes), true)) {
            assertEquals("{\"id\":1}\n", new String(s.readAllBytes(), StandardCharsets.UTF_8));
        }
    }

    /** Stream ends before any newline is seen: skipping yields an empty stream, not an error. */
    public void testSkipFirstLineNoTerminatorYieldsEof() throws IOException {
        byte[] bytes = "no-terminator-at-all".getBytes(StandardCharsets.UTF_8);
        try (InputStream s = NdJsonFormatReader.openForSchemaInference(new BytesObject(bytes), true)) {
            assertEquals(-1, s.read());
        }
    }

    /** Stream ends right after a lone CR (no byte to peek): safe, no dropped bytes from next line. */
    public void testSkipFirstLineCrAtEof() throws IOException {
        byte[] bytes = "partial\r".getBytes(StandardCharsets.UTF_8);
        try (InputStream s = NdJsonFormatReader.openForSchemaInference(new BytesObject(bytes), true)) {
            assertEquals(-1, s.read());
        }
    }

    // --- Stream drain prevention ---

    /**
     * Regression guard: {@code metadata()} must not drain the full stream body after reading the
     * schema sample. On S3, {@code close()} drains all remaining bytes to reuse the HTTP
     * connection; for a multi-GB file this would block the search thread for minutes. The fix
     * calls {@code object.abortStream(stream)} instead of closing directly.
     */
    public void testMetadataDoesNotDrainStream() throws IOException {
        // 200 000 records; the schema sample reads at most DEFAULT_SCHEMA_SAMPLE_SIZE (20 000).
        // The remaining ~180 000 records must not be consumed on close.
        StringBuilder ndjson = new StringBuilder();
        for (int i = 0; i < 200_000; i++) {
            ndjson.append("{\"id\":").append(i).append(",\"name\":\"n_").append(i).append("\",\"v\":").append(i * 1.5).append("}\n");
        }
        byte[] bytes = ndjson.toString().getBytes(StandardCharsets.UTF_8);
        assertThat("test file must be significantly larger than the schema sample", bytes.length, Matchers.greaterThan(2_000_000));

        DrainSimulatingStorageObject.Tracking tracking = new DrainSimulatingStorageObject.Tracking();
        StorageObject object = DrainSimulatingStorageObject.create(bytes, tracking);

        new NdJsonFormatReader(null, blockFactory).metadata(object);

        assertThat(
            "metadata() must not drain beyond the schema sample; consumed "
                + tracking.bytesConsumed.get()
                + " of "
                + bytes.length
                + " bytes",
            tracking.bytesConsumed.get(),
            Matchers.lessThan((long) bytes.length / 2)
        );
    }

    /**
     * Regression guard: {@code openForSchemaInference} returns a stream whose {@code close()}
     * calls {@code abortStream} rather than a draining {@code close()}. The callers read only a
     * schema sample and then close via try-with-resources; without the abort path S3 would drain
     * the remaining bytes before releasing the HTTP connection.
     */
    public void testOpenForSchemaInferenceDoesNotDrainStream() throws IOException {
        StringBuilder ndjson = new StringBuilder();
        for (int i = 0; i < 200_000; i++) {
            ndjson.append("{\"id\":").append(i).append(",\"name\":\"n_").append(i).append("\",\"v\":").append(i * 1.5).append("}\n");
        }
        byte[] bytes = ndjson.toString().getBytes(StandardCharsets.UTF_8);
        assertThat("test file must be significantly larger than the schema sample", bytes.length, Matchers.greaterThan(2_000_000));

        DrainSimulatingStorageObject.Tracking tracking = new DrainSimulatingStorageObject.Tracking();
        StorageObject object = DrainSimulatingStorageObject.create(bytes, tracking);

        // Simulate the production caller: open, read a small prefix (schema sample), close.
        try (InputStream stream = NdJsonFormatReader.openForSchemaInference(object, false)) {
            byte[] sample = new byte[4096];
            // noinspection ResultOfMethodCallIgnored
            stream.read(sample);
        }

        assertThat(
            "openForSchemaInference close() must not drain the stream; consumed "
                + tracking.bytesConsumed.get()
                + " of "
                + bytes.length
                + " bytes",
            tracking.bytesConsumed.get(),
            Matchers.lessThan((long) bytes.length / 2)
        );
    }

    /**
     * Regression guard for the {@code skipFirstLine=true} happy path: after the scanner
     * consumes the partial first record, the returned stream's {@code close()} must abort
     * the raw stream rather than draining it. Without the abort path, closing after a
     * schema-sample read on S3 would drain the remaining body of a multi-GB file.
     */
    public void testOpenForSchemaInferenceWithSkipFirstLineDoesNotDrainStream() throws IOException {
        StringBuilder ndjson = new StringBuilder("incomplete-first-record\n");
        for (int i = 0; i < 200_000; i++) {
            ndjson.append("{\"id\":").append(i).append(",\"name\":\"n_").append(i).append("\",\"v\":").append(i * 1.5).append("}\n");
        }
        byte[] bytes = ndjson.toString().getBytes(StandardCharsets.UTF_8);
        assertThat("test file must be significantly larger than the schema sample", bytes.length, Matchers.greaterThan(2_000_000));

        DrainSimulatingStorageObject.Tracking tracking = new DrainSimulatingStorageObject.Tracking();
        StorageObject object = DrainSimulatingStorageObject.create(bytes, tracking);

        try (InputStream stream = NdJsonFormatReader.openForSchemaInference(object, true)) {
            byte[] sample = new byte[4096];
            // noinspection ResultOfMethodCallIgnored
            stream.read(sample);
        }

        assertThat(
            "openForSchemaInference(skipFirstLine=true) close() must not drain the stream; consumed "
                + tracking.bytesConsumed.get()
                + " of "
                + bytes.length
                + " bytes",
            tracking.bytesConsumed.get(),
            Matchers.lessThan((long) bytes.length / 2)
        );
    }

    /**
     * Regression guard: the {@link InputStream#close()} contract requires {@code close()} to be
     * idempotent (a no-op when the stream is already closed). The wrapper returned by
     * {@code openForSchemaInference} must honour that and call {@code abortStream} at most once,
     * even when {@code close()} is invoked twice (a real pattern in defensive cleanup chains).
     * {@code Abortable.abort()} is not contractually guaranteed to be idempotent, so a double
     * call could break on future SDK versions or alternate {@code Abortable} implementations.
     */
    public void testOpenForSchemaInferenceCloseIsIdempotent() throws IOException {
        AtomicLong abortCount = new AtomicLong();
        StorageObject object = new BytesObject("{\"id\":1}\n".getBytes(StandardCharsets.UTF_8)) {
            @Override
            public void abortStream(InputStream stream) throws IOException {
                abortCount.incrementAndGet();
                stream.close();
            }
        };

        InputStream stream = NdJsonFormatReader.openForSchemaInference(object, false);
        stream.close();
        stream.close();
        stream.close();

        assertEquals("close() must call abortStream at most once across repeated invocations", 1, abortCount.get());
    }

    /**
     * Regression guard for the {@code skipFirstLine=true} failure path: if
     * {@code scanForTerminator} throws while looking for the first newline, the raw stream
     * must be aborted (not drained) before the exception propagates. Without the catch-block
     * abort, the throwing stream would be left dangling for a finalizer/GC and on real S3
     * the connection would stay leased to the client.
     */
    public void testOpenForSchemaInferenceAbortsRawOnSkipFirstLineScanFailure() {
        AtomicBoolean aborted = new AtomicBoolean(false);
        IOException scanFailure = new IOException("simulated read failure during skip-first-line scan");
        StorageObject object = new BytesObject(new byte[0]) {
            @Override
            public InputStream newStream() {
                return new InputStream() {
                    @Override
                    public int read() throws IOException {
                        throw scanFailure;
                    }

                    @Override
                    public int read(byte[] buf, int off, int len) throws IOException {
                        throw scanFailure;
                    }
                };
            }

            @Override
            public void abortStream(InputStream stream) {
                aborted.set(true);
            }
        };

        IOException thrown = expectThrows(IOException.class, () -> NdJsonFormatReader.openForSchemaInference(object, true));
        assertSame("the original scan failure must propagate unchanged", scanFailure, thrown);
        assertTrue("raw stream must be aborted when scanForTerminator fails", aborted.get());
    }

    /**
     * 4-digit all-digit values must not be inferred as {@link DataType#DATETIME}
     * when using the default {@code strict_date_optional_time} formatter.
     */
    public void testFourDigitNumbersNotInferredAsDatetime() throws IOException {
        // JSON numeric values: 5327 and 4536 must be inferred as INTEGER, not DATETIME.
        byte[] numericBytes = "{\"code\":5327,\"id\":4536}\n".getBytes(StandardCharsets.UTF_8);
        List<Attribute> numericSchema = new NdJsonFormatReader(null, blockFactory).metadata(new BytesObject(numericBytes)).schema();
        assertEquals(2, numericSchema.size());
        assertEquals("code", numericSchema.get(0).name());
        assertEquals(DataType.INTEGER, numericSchema.get(0).dataType());
        assertEquals("id", numericSchema.get(1).name());
        assertEquals(DataType.INTEGER, numericSchema.get(1).dataType());

        // JSON string values containing only digits must not be inferred as DATETIME by the
        // default strict_date_optional_time formatter — they must resolve to KEYWORD.
        byte[] stringBytes = "{\"code\":\"5327\",\"id\":\"4536\"}\n".getBytes(StandardCharsets.UTF_8);
        List<Attribute> stringSchema = new NdJsonFormatReader(null, blockFactory).metadata(new BytesObject(stringBytes)).schema();
        assertEquals(2, stringSchema.size());
        assertEquals("code", stringSchema.get(0).name());
        assertEquals(DataType.KEYWORD, stringSchema.get(0).dataType());
        assertEquals("id", stringSchema.get(1).name());
        assertEquals(DataType.KEYWORD, stringSchema.get(1).dataType());
    }

    /**
     * A field whose sampled records disagree (here {@code integer} then {@code string}) must fold to
     * {@code keyword} and report the widen — naming the field and the forced type — via both
     * {@link org.elasticsearch.xpack.esql.datasources.spi.SourceMetadata#warnings()} and
     * {@link org.elasticsearch.xpack.esql.datasources.spi.SourceMetadata#widenedColumns()}, matching
     * the CSV/TSV behaviour.
     */
    public void testFieldTypeDisagreementEmitsWarningAndWidenedColumn() throws IOException {
        byte[] bytes = "{\"a\":1}\n{\"a\":2}\n{\"a\":\"oops\"}\n".getBytes(StandardCharsets.UTF_8);
        SourceMetadata metadata = new NdJsonFormatReader(null, blockFactory).metadata(new BytesObject(bytes));

        assertEquals(DataType.KEYWORD, metadata.schema().get(0).dataType());
        assertFalse("a field type disagreement must be reported as a warning", metadata.warnings().isEmpty());
        assertTrue(
            "the warning must name the field and the forced type",
            metadata.warnings().stream().anyMatch(w -> w.contains("column [a]") && w.contains("[keyword]"))
        );
        assertEquals(1, metadata.widenedColumns().size());
        WidenedColumn widened = metadata.widenedColumns().get(0);
        assertEquals("a", widened.columnName());
        assertEquals(DataType.INTEGER, widened.fromType());
        assertEquals(DataType.KEYWORD, widened.toType());
        assertEquals("oops", widened.value());
    }

    /**
     * The fold to KEYWORD must be blamed on the row that actually completed it, not on whichever
     * distinct type happened to be seen last. {@code boolean} (row 1) then {@code integer} (row 2)
     * already fold to KEYWORD at row 2 — {@code TypeWidening.join} has no edge between them below
     * KEYWORD — so a later, unrelated datetime string at row 3 must not be reported as the cause. The
     * inferrer tracks the fold incrementally (like {@code CsvSchemaInferrer.narrowCandidate})
     * specifically to get this right: reporting "first distinct type seen" / "last distinct type seen"
     * instead would name row 3's datetime as the culprit, which is wrong.
     */
    public void testKeywordFoldIsBlamedOnTheRowThatCompletedItNotTheLastDistinctType() throws IOException {
        byte[] bytes = "{\"a\":true}\n{\"a\":1}\n{\"a\":\"2023-10-23T12:15:03Z\"}\n".getBytes(StandardCharsets.UTF_8);
        SourceMetadata metadata = new NdJsonFormatReader(null, blockFactory).metadata(new BytesObject(bytes));

        assertEquals(DataType.KEYWORD, metadata.schema().get(0).dataType());
        assertEquals(1, metadata.widenedColumns().size());
        WidenedColumn widened = metadata.widenedColumns().get(0);
        assertEquals(DataType.BOOLEAN, widened.fromType());
        assertEquals(DataType.KEYWORD, widened.toType());
        assertEquals("the boolean/integer disagreement at row 2 is what forced the fold, not row 3's datetime", "1", widened.value());
        assertEquals(2, widened.sampleRow());
    }

    /** A lossless promotion ({@code integer -> long}) must stay silent, matching the CSV/TSV gating. */
    public void testFieldLosslessPromotionEmitsNoWidening() throws IOException {
        byte[] bytes = "{\"a\":1}\n{\"a\":9999999999}\n".getBytes(StandardCharsets.UTF_8);
        SourceMetadata metadata = new NdJsonFormatReader(null, blockFactory).metadata(new BytesObject(bytes));

        assertEquals(DataType.LONG, metadata.schema().get(0).dataType());
        assertEquals(List.of(), metadata.widenedColumns());
        assertEquals(List.of(), metadata.warnings());
    }

    /**
     * The reverse order of a long/double merge: a field resolved to {@code double} by a genuine decimal
     * first, then a later value that happens to be exactly long-representable. The running fold doesn't
     * visibly move ({@code join(DOUBLE, LONG) == DOUBLE}), so the merge has to be detected by set
     * membership ({@code LONG} and {@code DOUBLE} both contributing), not by whether the fold changed —
     * this is exactly the case {@code emitPrecisionLossWarnings} exists to report cross-file: a column
     * unified to {@code double} that silently loses precision above 2^53. {@code fromType} reports
     * {@code LONG} (this value's own shape), not {@code DOUBLE} (the field's unchanged resolved type),
     * since the latter would say {@code fromType == toType}.
     */
    public void testLongDoubleMergeReversedOrderIsReported() throws IOException {
        // 9007199254740993 is 2^53 + 1, the smallest long a double cannot represent exactly.
        byte[] bytes = "{\"a\":1.5}\n{\"a\":9007199254740993}\n".getBytes(StandardCharsets.UTF_8);
        SourceMetadata metadata = new NdJsonFormatReader(null, blockFactory).metadata(new BytesObject(bytes));

        assertEquals(DataType.DOUBLE, metadata.schema().get(0).dataType());
        assertEquals(1, metadata.widenedColumns().size());
        WidenedColumn widened = metadata.widenedColumns().get(0);
        assertEquals(DataType.LONG, widened.fromType());
        assertEquals(DataType.DOUBLE, widened.toType());
        assertEquals("9007199254740993", widened.value());
        assertEquals(2, widened.sampleRow());
    }

    /**
     * A field confirmed {@code LONG} by a modest value (past int32, nowhere near 2^53) must still
     * report the merge once a <em>later</em> {@code LONG} value crosses the precision threshold —
     * even though {@code LONG} is already a member of {@code types} by then, so the usual
     * "added the second of LONG/DOUBLE" check never runs for that call. Catches a regression where
     * the precision-loss latch flipping on an already-seen type was silently dropped by the early
     * return in {@code FieldInfo#addType}.
     */
    public void testLongDoubleMergeReportedWhenLaterLongCrossesThreshold() throws IOException {
        // 3000000000 is past int32 but nowhere near 2^53: no precision lost by itself.
        // 1152921504606846976 is 2^60, well past 2^53: the value that actually crosses the threshold.
        byte[] bytes = "{\"a\":1.5}\n{\"a\":3000000000}\n{\"a\":1152921504606846976}\n".getBytes(StandardCharsets.UTF_8);
        SourceMetadata metadata = new NdJsonFormatReader(null, blockFactory).metadata(new BytesObject(bytes));

        assertEquals(DataType.DOUBLE, metadata.schema().get(0).dataType());
        assertEquals(1, metadata.widenedColumns().size());
        WidenedColumn widened = metadata.widenedColumns().get(0);
        assertEquals(DataType.LONG, widened.fromType());
        assertEquals(DataType.DOUBLE, widened.toType());
        assertEquals("1152921504606846976", widened.value());
        assertEquals(3, widened.sampleRow());
    }

    /**
     * A field mixing whole numbers and decimals entirely within the range a double represents
     * exactly (e.g. {@code 1}, {@code 2}, {@code 1.5}) must not be flagged — nothing is lost there.
     */
    public void testOrdinaryWholeNumberAndDecimalMixReportsNoWidening() throws IOException {
        byte[] bytes = "{\"a\":1.5}\n{\"a\":2}\n".getBytes(StandardCharsets.UTF_8);
        SourceMetadata metadata = new NdJsonFormatReader(null, blockFactory).metadata(new BytesObject(bytes));

        assertEquals(DataType.DOUBLE, metadata.schema().get(0).dataType());
        assertEquals(List.of(), metadata.widenedColumns());
    }

    /**
     * {@code read()} re-infers its own schema inline (via {@code inferSchemaIfNeeded}) whenever the
     * caller hands it no pre-resolved {@code readSchema} — a separate path from {@code metadata()},
     * taken when planning has not already attached a schema to the request (e.g. a cold first-split
     * read). A within-file widen discovered there must still reach the client as a warning, the same
     * as one discovered through {@code metadata()}; this path has no {@code SourceMetadata} to attach a
     * {@code widenedColumns()} to, so only the warning is expected here.
     */
    public void testReadWithoutPreResolvedSchemaStillWarnsOnWidening() throws IOException {
        byte[] bytes = "{\"a\":1}\n{\"a\":2}\n{\"a\":\"oops\"}\n".getBytes(StandardCharsets.UTF_8);
        StorageObject object = new BytesObject(bytes);
        NdJsonFormatReader reader = new NdJsonFormatReader(null, blockFactory);

        List<String> warnings = new ArrayList<>();
        FormatReadContext context = FormatReadContext.builder().batchSize(100).informationalWarningSink(warnings::add).build();
        try (CloseableIterator<Page> pages = reader.read(object, context)) {
            while (pages.hasNext()) {
                pages.next().releaseBlocks();
            }
        }

        assertTrue(
            "a within-file widen discovered on the no-pre-resolved-schema read path must still warn, got: " + warnings,
            warnings.stream().anyMatch(w -> w.contains("column [a]") && w.contains("[keyword]"))
        );
    }

    // -- helpers --

    private static class BytesObject extends AbstractTestStorageObject {
        protected final byte[] bytes;

        BytesObject(byte[] bytes) {
            this.bytes = bytes;
        }

        @Override
        public InputStream newStream() {
            return new ByteArrayInputStream(bytes);
        }

        @Override
        public InputStream newStream(long position, long length) {
            return new ByteArrayInputStream(bytes, (int) position, (int) length);
        }

        @Override
        public long length() {
            return bytes.length;
        }

        @Override
        public Instant lastModified() {
            return Instant.EPOCH;
        }

        @Override
        public boolean exists() {
            return true;
        }

        @Override
        public StoragePath path() {
            return StoragePath.of("mem://test");
        }
    }

    /** Wraps the underlying stream in a FilterInputStream that reports markSupported == false. */
    private static final class NonMarkableBytesObject extends BytesObject {
        NonMarkableBytesObject(byte[] bytes) {
            super(bytes);
        }

        @Override
        public InputStream newStream() {
            return new FilterInputStream(new ByteArrayInputStream(bytes)) {
                @Override
                public boolean markSupported() {
                    return false;
                }

                @Override
                public void mark(int readlimit) {}

                @Override
                public void reset() throws IOException {
                    throw new IOException("mark/reset not supported");
                }
            };
        }
    }
}
