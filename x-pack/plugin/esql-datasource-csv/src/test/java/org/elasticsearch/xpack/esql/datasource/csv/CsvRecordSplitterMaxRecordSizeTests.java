/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.csv;

import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.ParallelParsingCoordinator;
import org.elasticsearch.xpack.esql.datasources.spi.RecordSplitter;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;

public class CsvRecordSplitterMaxRecordSizeTests extends ESTestCase {

    public void testConstructorRejectsNonPositiveMaxRecordBytes() {
        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> new CsvRecordSplitter(CsvFormatOptions.DEFAULT, 0)
        );
        assertEquals("maxRecordBytes must be positive, got: 0", ex.getMessage());
    }

    public void testQuotedFieldsOnlyReturnsRecordTooLargeForUnclosedQuote() throws IOException {
        int maxRecordBytes = 32;
        RecordSplitter splitter = new CsvRecordSplitter(CsvFormatOptions.TSV, maxRecordBytes);
        byte[] bytes = bytes("\"" + "x".repeat(maxRecordBytes + 1));

        assertEquals(RecordSplitter.RECORD_TOO_LARGE, splitter.findNextRecordBoundary(new ByteArrayInputStream(bytes)));
        assertEquals(RecordSplitter.RECORD_TOO_LARGE, splitter.findLastRecordBoundary(bytes, bytes.length));
    }

    public void testBracketMvcReturnsRecordTooLargeForUnclosedBracket() throws IOException {
        int maxRecordBytes = 32;
        RecordSplitter splitter = new CsvRecordSplitter(CsvFormatOptions.DEFAULT, maxRecordBytes);
        byte[] bytes = bytes("a,[" + "x".repeat(maxRecordBytes + 1));

        assertEquals(RecordSplitter.RECORD_TOO_LARGE, splitter.findNextRecordBoundary(new ByteArrayInputStream(bytes)));
        assertEquals(RecordSplitter.RECORD_TOO_LARGE, splitter.findLastRecordBoundary(bytes, bytes.length));
    }

    public void testCsvFormatReaderRecordSplitterUsesInjectedMaxRecordSize() throws IOException {
        int maxRecordBytes = 16;
        CsvFormatReader reader = new CsvFormatReader(blockFactory());
        RecordSplitter splitter = reader.recordSplitter(maxRecordBytes);

        assertEquals(
            RecordSplitter.RECORD_TOO_LARGE,
            splitter.findNextRecordBoundary(new ByteArrayInputStream(bytes("a,\"" + "x".repeat(maxRecordBytes + 1))))
        );
        assertEquals(maxRecordBytes, splitter.maxRecordBytes());
    }

    public void testComputeSegmentsFallsBackWhenBoundaryProbeExceedsMaxRecordSize() throws IOException {
        int maxRecordBytes = 32;
        String csv = "a,b\n1,\"" + "x".repeat(1024) + "\n";
        byte[] bytes = bytes(csv);
        CsvFormatReader reader = new CsvFormatReader(blockFactory());
        StorageObject object = new ByteArrayStorageObject(bytes);

        List<long[]> segments = ParallelParsingCoordinator.computeSegments(reader, object, bytes.length, 4, 1, maxRecordBytes);

        assertEquals(1, segments.size());
        assertArrayEquals(new long[] { 0, bytes.length }, segments.get(0));
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static BlockFactory blockFactory() {
        return BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(new NoopCircuitBreaker("test")).build();
    }

    private static class ByteArrayStorageObject implements StorageObject {
        private final byte[] bytes;

        ByteArrayStorageObject(byte[] bytes) {
            this.bytes = bytes;
        }

        @Override
        public InputStream newStream() {
            return new ByteArrayInputStream(bytes);
        }

        @Override
        public InputStream newStream(long position, long length) {
            return new ByteArrayInputStream(bytes, Math.toIntExact(position), Math.toIntExact(length));
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
            return StoragePath.of("mem://csv-record-splitter-max-record-size.csv");
        }
    }
}
