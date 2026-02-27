/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorOutputStream;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.QlIllegalArgumentException;
import org.elasticsearch.xpack.esql.datasources.spi.DecompressionCodec;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

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
        DecompressionCodec codec = new org.elasticsearch.xpack.esql.datasource.bzip2.Bzip2DecompressionCodec();

        DecompressingStorageObject decompressing = new DecompressingStorageObject(rawObject, codec);
        try (InputStream stream = decompressing.newStream()) {
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

    public void testNullDelegateThrows() {
        DecompressionCodec codec = new org.elasticsearch.xpack.esql.datasource.gzip.GzipDecompressionCodec();
        StorageObject rawObject = new BytesStorageObject(new byte[0], StoragePath.of("file:///x"));
        expectThrows(QlIllegalArgumentException.class, () -> new DecompressingStorageObject(null, codec));
    }

    public void testNullCodecThrows() {
        StorageObject rawObject = new BytesStorageObject(new byte[0], StoragePath.of("file:///x"));
        expectThrows(QlIllegalArgumentException.class, () -> new DecompressingStorageObject(rawObject, null));
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
