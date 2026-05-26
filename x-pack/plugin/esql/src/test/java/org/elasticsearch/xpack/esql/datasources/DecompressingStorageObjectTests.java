/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import com.github.luben.zstd.ZstdOutputStream;

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorOutputStream;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.QlIllegalArgumentException;
import org.elasticsearch.xpack.esql.datasource.bzip2.Bzip2DecompressionCodec;
import org.elasticsearch.xpack.esql.datasources.spi.DecompressionCodec;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObjectMetrics;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.hamcrest.Matchers;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.zip.GZIPOutputStream;

/**
 * Unit tests for {@link DecompressingStorageObject}.
 */
public class DecompressingStorageObjectTests extends ESTestCase {

    public void testDecompressStream() throws IOException {
        byte[] original = "hello,world\n1,2".getBytes(StandardCharsets.UTF_8);
        byte[] compressed = gzip(original);

        StorageObject rawObject = new BytesStorageObject(compressed, StoragePath.of("file:///data.csv.gz"));
        DecompressionCodec codec = new org.elasticsearch.xpack.esql.datasource.gzip.GzipDecompressionCodec();

        DecompressingStorageObject decompressing = new DecompressingStorageObject(rawObject, codec);
        try (InputStream stream = decompressing.newStream()) {
            byte[] decompressed = stream.readAllBytes();
            assertArrayEquals(original, decompressed);
        }
    }

    public void testDecompressStreamBzip2() throws IOException {
        byte[] original = "hello,world\n1,2".getBytes(StandardCharsets.UTF_8);
        byte[] compressed = bzip2(original);

        StorageObject rawObject = new BytesStorageObject(compressed, StoragePath.of("file:///data.csv.bz2"));
        DecompressionCodec codec = new Bzip2DecompressionCodec(EsExecutors.DIRECT_EXECUTOR_SERVICE);

        DecompressingStorageObject decompressing = new DecompressingStorageObject(rawObject, codec);
        try (InputStream stream = decompressing.newStream()) {
            byte[] decompressed = stream.readAllBytes();
            assertArrayEquals(original, decompressed);
        }
    }

    public void testDecompressStreamZstd() throws IOException {
        byte[] original = "hello,world\n1,2".getBytes(StandardCharsets.UTF_8);
        byte[] compressed = zstd(original);

        StorageObject rawObject = new BytesStorageObject(compressed, StoragePath.of("file:///data.csv.zst"));
        DecompressionCodec codec = new org.elasticsearch.xpack.esql.datasource.zstd.ZstdDecompressionCodec();

        DecompressingStorageObject decompressing = new DecompressingStorageObject(rawObject, codec);
        try (InputStream stream = decompressing.newStream()) {
            byte[] decompressed = stream.readAllBytes();
            assertArrayEquals(original, decompressed);
        }
    }

    public void testSplittableCodecNewStreamPositionLengthBzip2() throws IOException {
        byte[] original = "hello,world\n1,2".getBytes(StandardCharsets.UTF_8);
        byte[] compressed = bzip2(original);

        StorageObject rawObject = new BytesStorageObject(compressed, StoragePath.of("file:///data.csv.bz2"));
        DecompressionCodec codec = new Bzip2DecompressionCodec(EsExecutors.DIRECT_EXECUTOR_SERVICE);
        DecompressingStorageObject decompressing = new DecompressingStorageObject(rawObject, codec);

        try (InputStream stream = decompressing.newStream(0, compressed.length)) {
            byte[] decompressed = stream.readAllBytes();
            assertArrayEquals(original, decompressed);
        }
    }

    public void testNewStreamPositionLengthThrows() throws IOException {
        StorageObject rawObject = new BytesStorageObject(new byte[0], StoragePath.of("file:///data.csv.gz"));
        DecompressionCodec codec = new org.elasticsearch.xpack.esql.datasource.gzip.GzipDecompressionCodec();
        DecompressingStorageObject decompressing = new DecompressingStorageObject(rawObject, codec);

        UnsupportedOperationException e = expectThrows(UnsupportedOperationException.class, () -> decompressing.newStream(0, 100));
        assertTrue(e.getMessage().contains("Stream-only compression"));
        assertTrue(e.getMessage().contains("gzip"));
    }

    public void testLengthThrows() throws IOException {
        StorageObject rawObject = new BytesStorageObject(new byte[0], StoragePath.of("file:///data.csv.gz"));
        DecompressionCodec codec = new org.elasticsearch.xpack.esql.datasource.gzip.GzipDecompressionCodec();
        DecompressingStorageObject decompressing = new DecompressingStorageObject(rawObject, codec);

        UnsupportedOperationException e = expectThrows(UnsupportedOperationException.class, decompressing::length);
        assertTrue(e.getMessage().contains("Decompressed length is unknown"));
        assertTrue(e.getMessage().contains("gzip"));
    }

    public void testDelegatesLastModifiedAndExistsAndPath() throws IOException {
        Instant now = Instant.now();
        StoragePath path = StoragePath.of("file:///data.csv.gz");
        StorageObject rawObject = new BytesStorageObject(new byte[0], path, now, true);
        DecompressionCodec codec = new org.elasticsearch.xpack.esql.datasource.gzip.GzipDecompressionCodec();
        DecompressingStorageObject decompressing = new DecompressingStorageObject(rawObject, codec);

        assertEquals(now, decompressing.lastModified());
        assertTrue(decompressing.exists());
        assertEquals(path, decompressing.path());
    }

    public void testMetricsDelegatesToWrapped() {
        StorageObjectMetrics snapshot = new StorageObjectMetrics(3, 555, 1024, 0);
        StorageObject rawObject = new BytesStorageObject(new byte[0], StoragePath.of("file:///x.gz")) {
            @Override
            public StorageObjectMetrics metrics() {
                return snapshot;
            }
        };
        DecompressionCodec codec = new org.elasticsearch.xpack.esql.datasource.gzip.GzipDecompressionCodec();
        DecompressingStorageObject decompressing = new DecompressingStorageObject(rawObject, codec);
        assertSame(snapshot, decompressing.metrics());
    }

    public void testNullDelegateThrows() {
        DecompressionCodec codec = new org.elasticsearch.xpack.esql.datasource.gzip.GzipDecompressionCodec();
        StorageObject rawObject = new BytesStorageObject(new byte[0], StoragePath.of("file:///x"));
        expectThrows(QlIllegalArgumentException.class, () -> new DecompressingStorageObject(null, codec));
    }

    public void testNullCodecThrows() {
        StorageObject rawObject = new BytesStorageObject(new byte[0], StoragePath.of("file:///x"));
        expectThrows(QlIllegalArgumentException.class, () -> new DecompressingStorageObject(rawObject, null));
    }

    // --- Stream drain prevention through the decompressing wrapper ---

    /**
     * Regression guard: {@code abortStream} on a {@link DecompressingStorageObject} must route
     * the abort through to the underlying delegate, not fall back to a draining {@code close()}.
     * <p>
     * In production, the delegate is an S3 {@code StorageObject} whose {@code abortStream}
     * calls {@code ResponseInputStream.abort()} to discard the HTTP connection without draining.
     * If the decompressing wrapper merely closes the {@code GZIPInputStream} it returned, the
     * close cascades through {@code GZIPInputStream.close()} to {@code S3ResponseInputStream.
     * close()} — which drains every remaining compressed byte to reuse the connection pool.
     * For multi-GB compressed objects (typical for CSV/TSV/NDJSON on S3) that blocks the
     * search thread for the full object transfer.
     * <p>
     * This test wraps a {@link StorageObject} whose raw stream simulates the Apache HttpClient
     * drain-on-close behaviour, then layers a real gzip decompressor on top via
     * {@link DecompressingStorageObject}. It opens the decompressed stream, reads a tiny
     * prefix (mirroring schema inference), and calls {@code decompressing.abortStream(stream)}.
     * The assertion is that the raw, compressed stream beneath the gzip wrapper is not drained.
     */
    public void testAbortStreamDoesNotDrainUnderlyingStream() throws IOException {
        StringBuilder csv = new StringBuilder();
        for (int i = 0; i < 200_000; i++) {
            csv.append("id_").append(i).append(",name_").append(i).append(",value_").append(i * 1.5).append("\n");
        }
        byte[] original = csv.toString().getBytes(StandardCharsets.UTF_8);
        byte[] compressed = gzip(original);
        assertThat(
            "compressed payload must be significantly larger than the prefix we read",
            compressed.length,
            Matchers.greaterThan(200_000)
        );

        DrainSimulatingStorageObject.Tracking tracking = new DrainSimulatingStorageObject.Tracking();
        StorageObject rawObject = DrainSimulatingStorageObject.create(compressed, tracking);
        DecompressionCodec codec = new org.elasticsearch.xpack.esql.datasource.gzip.GzipDecompressionCodec();
        DecompressingStorageObject decompressing = new DecompressingStorageObject(rawObject, codec);

        InputStream stream = decompressing.newStream();
        try {
            byte[] prefix = new byte[4096];
            int n = stream.read(prefix);
            assertThat("expected to read some decompressed bytes", n, Matchers.greaterThan(0));
        } finally {
            decompressing.abortStream(stream);
        }

        assertThat(
            "abortStream must not drain the underlying raw stream; consumed "
                + tracking.bytesConsumed.get()
                + " of "
                + compressed.length
                + " raw bytes",
            tracking.bytesConsumed.get(),
            Matchers.lessThan((long) compressed.length / 2)
        );
    }

    /**
     * Regression guard: a normal {@code close()} on the wrapper stream returned by
     * {@link DecompressingStorageObject#newStream()} must close the underlying raw stream so
     * its connection is released. The decompressor itself sees an {@code UncloseableInputStream}
     * over raw (to keep {@code abortStream} from triggering the connection-pool drain on
     * partial reads), so without a close-time override on the wrapper the raw stream would
     * leak.
     * <p>
     * The simulated raw stream tracks whether {@code close()} was called; after fully reading
     * the decompressed payload, the assertion is that the raw close fired exactly once.
     */
    public void testCloseAfterFullReadReleasesUnderlyingStream() throws IOException {
        byte[] original = "id,name\n1,a\n2,b\n3,c\n".getBytes(StandardCharsets.UTF_8);
        byte[] compressed = gzip(original);

        DrainSimulatingStorageObject.Tracking tracking = new DrainSimulatingStorageObject.Tracking();
        StorageObject rawObject = DrainSimulatingStorageObject.create(compressed, tracking);
        DecompressionCodec codec = new org.elasticsearch.xpack.esql.datasource.gzip.GzipDecompressionCodec();
        DecompressingStorageObject decompressing = new DecompressingStorageObject(rawObject, codec);

        try (InputStream stream = decompressing.newStream()) {
            byte[] decompressed = stream.readAllBytes();
            assertArrayEquals(original, decompressed);
        }

        assertTrue("raw stream must be closed after the wrapper is closed (otherwise connection leaks)", tracking.closed.get());
    }

    private static byte[] gzip(byte[] input) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (GZIPOutputStream gzipOut = new GZIPOutputStream(baos)) {
            gzipOut.write(input);
        }
        return baos.toByteArray();
    }

    private static byte[] bzip2(byte[] input) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (BZip2CompressorOutputStream bzip2Out = new BZip2CompressorOutputStream(baos)) {
            bzip2Out.write(input);
        }
        return baos.toByteArray();
    }

    private static byte[] zstd(byte[] input) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (ZstdOutputStream zstdOut = new ZstdOutputStream(baos)) {
            zstdOut.write(input);
        }
        return baos.toByteArray();
    }

    private static class BytesStorageObject implements StorageObject {
        private final byte[] data;
        private final StoragePath path;
        private final Instant lastModified;
        private final boolean exists;

        BytesStorageObject(byte[] data, StoragePath path) {
            this(data, path, Instant.EPOCH, true);
        }

        BytesStorageObject(byte[] data, StoragePath path, Instant lastModified, boolean exists) {
            this.data = data;
            this.path = path;
            this.lastModified = lastModified;
            this.exists = exists;
        }

        @Override
        public InputStream newStream() {
            return new ByteArrayInputStream(data);
        }

        @Override
        public InputStream newStream(long position, long length) throws IOException {
            return new ByteArrayInputStream(data, (int) position, (int) length);
        }

        @Override
        public long length() {
            return data.length;
        }

        @Override
        public Instant lastModified() {
            return lastModified;
        }

        @Override
        public boolean exists() {
            return exists;
        }

        @Override
        public StoragePath path() {
            return path;
        }
    }
}
