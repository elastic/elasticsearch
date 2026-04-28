/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.ndjson;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.io.ByteArrayInputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;

/**
 * Unit tests for {@link NdJsonFormatReader#openForSchemaInference(StorageObject, boolean)}.
 *
 * <p>Covers the contract that a non-first split starts the schema-inference stream at the first
 * byte of the first complete record: LF, CRLF, and lone-CR terminators, on both markable and
 * non-markable underlying streams, plus the stream-ends-before-newline edge case.
 */
public class NdJsonFormatReaderTests extends ESTestCase {

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
