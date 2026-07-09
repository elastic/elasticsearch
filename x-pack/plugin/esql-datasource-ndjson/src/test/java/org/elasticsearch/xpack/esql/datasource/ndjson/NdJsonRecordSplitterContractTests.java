/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.ndjson;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.spi.RecordSplitter;
import org.elasticsearch.xpack.esql.datasources.spi.SegmentableFormatReader;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public class NdJsonRecordSplitterContractTests extends ESTestCase {

    public void testEmptyInputReturnsEof() throws IOException {
        RecordSplitter splitter = newSplitter(128);
        assertEquals(-1L, splitter.findNextRecordBoundary(new ByteArrayInputStream(new byte[0])));
        assertEquals(-1, splitter.findLastRecordBoundary(new byte[0], 0, 0));
    }

    public void testEofBeforeAnyTerminatorReturnsEof() throws IOException {
        assertEquals(-1L, newSplitter(128).findNextRecordBoundary(new ByteArrayInputStream(bytes("unterminated"))));
    }

    public void testLfConsumesThroughTerminator() throws IOException {
        assertEquals(4L, newSplitter(128).findNextRecordBoundary(new ByteArrayInputStream(bytes("row\nnext"))));
    }

    public void testCrLfConsumesThroughBothTerminatorBytes() throws IOException {
        assertEquals(5L, newSplitter(128).findNextRecordBoundary(new ByteArrayInputStream(bytes("row\r\nnext"))));
    }

    public void testLoneCrConsumesOnlyTheCrAndExposesPeekedByte() throws IOException {
        NdJsonRecordSplitter splitter = newSplitter(128);
        NdJsonRecordSplitter.LineScan scan = splitter.scanForTerminator(new ByteArrayInputStream(bytes("row\rnext")));

        assertEquals(4L, scan.consumed());
        assertEquals('n', scan.peekedByte());
    }

    public void testFindNextRecordBoundaryReportsRecordTooLarge() throws IOException {
        RecordSplitter splitter = newSplitter(8);

        assertEquals(RecordSplitter.RECORD_TOO_LARGE, splitter.findNextRecordBoundary(new ByteArrayInputStream(repeatedByte('x', 9))));
    }

    public void testFindLastRecordBoundaryReportsRecordTooLarge() throws IOException {
        RecordSplitter splitter = newSplitter(8);

        assertEquals(RecordSplitter.RECORD_TOO_LARGE, splitter.findLastRecordBoundary(repeatedByte('x', 9), 0, 9));
    }

    public void testTerminatedOversizedRecordIsNotDispatchable() throws IOException {
        RecordSplitter splitter = newSplitter(8);
        byte[] oversized = bytes("x".repeat(8) + "\n");

        assertEquals(RecordSplitter.RECORD_TOO_LARGE, splitter.findNextRecordBoundary(new ByteArrayInputStream(oversized)));
        assertEquals(RecordSplitter.RECORD_TOO_LARGE, splitter.findLastRecordBoundary(oversized, 0, oversized.length));
    }

    public void testSafeRecordBeforeOversizedTailCanBeReturnedOnce() throws IOException {
        RecordSplitter splitter = newSplitter(8);
        byte[] safe = bytes("ok\n");
        byte[] oversizedTail = bytes("x".repeat(9) + "\n");
        byte[] combined = concat(safe, oversizedTail);

        assertEquals(safe.length - 1, splitter.findLastRecordBoundary(combined, 0, combined.length));
        assertEquals(RecordSplitter.RECORD_TOO_LARGE, splitter.findLastRecordBoundary(combined, safe.length, oversizedTail.length));
    }

    public void testFindLastRecordBoundaryUsesOffsetAwareReverseScan() throws IOException {
        byte[] prefix = bytes("ignored\n");
        byte[] payload = bytes("{\"a\":1}\n{\"b\":2}");
        byte[] input = concat(prefix, payload);

        RecordSplitter splitter = newSplitter(128);

        assertEquals(prefix.length + "{\"a\":1}\n".length() - 1, splitter.findLastRecordBoundary(input, prefix.length, payload.length));
    }

    public void testFormatReaderUsesConfiguredMaxRecordBytes() throws IOException {
        NdJsonFormatReader reader = new NdJsonFormatReader(null, null);
        RecordSplitter splitter = reader.recordSplitter(8);

        assertEquals(8, splitter.maxRecordBytes());
        assertEquals(RecordSplitter.RECORD_TOO_LARGE, splitter.findNextRecordBoundary(new ByteArrayInputStream(repeatedByte('x', 9))));
    }

    public void testFormatReaderDefaultSplitter() throws IOException {
        NdJsonFormatReader reader = new NdJsonFormatReader(null, null);
        byte[] payload = bytes("first\nsecond");

        assertEquals(6L, reader.recordSplitter().findNextRecordBoundary(new ByteArrayInputStream(payload)));
        assertEquals(5, reader.recordSplitter().findLastRecordBoundary(payload, payload.length));
    }

    public void testDefaultSplitterUsesDefaultMaxRecordBytes() {
        NdJsonFormatReader reader = new NdJsonFormatReader(null, null);

        assertEquals(SegmentableFormatReader.DEFAULT_MAX_RECORD_BYTES, reader.recordSplitter().maxRecordBytes());
    }

    private static NdJsonRecordSplitter newSplitter(int maxRecordBytes) {
        return new NdJsonRecordSplitter(maxRecordBytes);
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static byte[] repeatedByte(char value, int count) {
        byte[] bytes = new byte[count];
        Arrays.fill(bytes, (byte) value);
        return bytes;
    }

    private static byte[] concat(byte[]... chunks) {
        int length = 0;
        for (byte[] chunk : chunks) {
            length += chunk.length;
        }
        byte[] result = new byte[length];
        int offset = 0;
        for (byte[] chunk : chunks) {
            System.arraycopy(chunk, 0, result, offset, chunk.length);
            offset += chunk.length;
        }
        return result;
    }
}
