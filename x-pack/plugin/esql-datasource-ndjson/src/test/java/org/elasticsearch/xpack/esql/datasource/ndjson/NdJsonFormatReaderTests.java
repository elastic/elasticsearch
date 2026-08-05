/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.ndjson;

import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.DrainSimulatingStorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.hamcrest.Matchers;
import org.junit.Before;

import java.io.ByteArrayInputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

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
        blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(new NoopCircuitBreaker("none")).build();
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

    // -- helpers --

    private static class BytesObject implements StorageObject {
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
