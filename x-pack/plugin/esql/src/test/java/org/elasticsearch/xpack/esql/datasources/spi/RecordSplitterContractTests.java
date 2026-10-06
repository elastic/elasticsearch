/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import org.elasticsearch.test.ESTestCase;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public abstract class RecordSplitterContractTests extends ESTestCase {

    protected abstract RecordSplitter newSplitter();

    protected abstract Fixtures fixtures();

    public void testEmptyInputReturnsEof() throws IOException {
        RecordSplitter splitter = newSplitter();
        assertEquals(-1L, splitter.findNextRecordBoundary(new ByteArrayInputStream(new byte[0])));
        assertEquals(-1, splitter.findLastRecordBoundary(new byte[0], 0, 0));
    }

    public void testEofBeforeAnyTerminatorReturnsEof() throws IOException {
        assertEquals(-1L, newSplitter().findNextRecordBoundary(new ByteArrayInputStream(bytes("unterminated"))));
    }

    public void testSingleLfTerminatedRecordConsumesThroughTerminator() throws IOException {
        byte[] input = terminatedRecord("row");
        assertEquals(3L + fixtures().recordTerminatorBytes(), newSplitter().findNextRecordBoundary(new ByteArrayInputStream(input)));
    }

    public void testCrLfTerminatorWhenSupported() throws IOException {
        assumeTrue("splitter does not support CRLF", fixtures().handlesCRLF());

        assertEquals(5L, newSplitter().findNextRecordBoundary(new ByteArrayInputStream(bytes("row\r\nnext"))));
    }

    public void testLoneCrTerminatorWhenSupported() throws IOException {
        assumeTrue("splitter does not support lone CR", fixtures().handlesLoneCR());

        assertEquals(4L, newSplitter().findNextRecordBoundary(new ByteArrayInputStream(bytes("row\rnext"))));
    }

    public void testEmbeddedTerminatorWhenSupported() throws IOException {
        assumeTrue("splitter does not support embedded terminators", fixtures().canEmbedTerminatorInRecord());

        byte[] input = recordWithEmbeddedTerminator();
        assertEquals(recordWithEmbeddedTerminatorBytes(), newSplitter().findNextRecordBoundary(new ByteArrayInputStream(input)));
    }

    public void testRecordTooLargeSentinelWhenSupported() throws IOException {
        RecordSplitter splitter = newSplitter();
        if (fixtures().canReturnRecordTooLarge() == false) {
            long consumed = splitter.findNextRecordBoundary(new ByteArrayInputStream(bytes("unterminated")));
            assertFalse("bridge splitters must not produce RECORD_TOO_LARGE", consumed == RecordSplitter.RECORD_TOO_LARGE);
            return;
        }

        assumeTrue("contract test avoids allocating very large records", splitter.maxRecordBytes() < 1024 * 1024);
        assertEquals(
            RecordSplitter.RECORD_TOO_LARGE,
            splitter.findNextRecordBoundary(new ByteArrayInputStream(repeatedByte('x', splitter.maxRecordBytes() + 1)))
        );
    }

    /**
     * A splitter that is not proven-capable must reject the proven-probe surface rather than silently fall back to a
     * quote-blind scan: both {@link RecordSplitter#findProvenRecordBoundary} and
     * {@link RecordSplitter#findRecordStartAtOrAfter} throw {@link UnsupportedOperationException}. Proven-capable
     * splitters implement them and are exercised by their own differential tests, so this only covers the default.
     */
    public void testProvenProbingRejectedWhenUnsupported() {
        RecordSplitter splitter = newSplitter();
        assumeFalse("proven-capable splitters have dedicated coverage", splitter.supportsProvenProbing());
        expectThrows(UnsupportedOperationException.class, () -> splitter.findProvenRecordBoundary(new ByteArrayInputStream(bytes("x"))));
        expectThrows(
            UnsupportedOperationException.class,
            () -> splitter.findRecordStartAtOrAfter(new ByteArrayInputStream(bytes("x")), 1L, () -> false)
        );
    }

    public void testFindLastRecordBoundaryMatchesForwardScan() throws IOException {
        byte[] payload = concat(terminatedRecord("first"), terminatedRecord("second"), bytes("tail"));
        int expectedBoundary = driveForwardToLastBoundary(newSplitter(), payload);

        RecordSplitter splitter = newSplitter();
        assertEquals(expectedBoundary, splitter.findLastRecordBoundary(payload, 0, payload.length));
        assertEquals(expectedBoundary, splitter.findLastRecordBoundary(payload, payload.length));

        byte[] prefix = bytes("xx");
        byte[] prefixedPayload = concat(prefix, payload);
        assertEquals(prefix.length + expectedBoundary, splitter.findLastRecordBoundary(prefixedPayload, prefix.length, payload.length));
    }

    protected byte[] recordWithEmbeddedTerminator() {
        throw new UnsupportedOperationException("subclasses with embedded terminators must provide a fixture");
    }

    protected int recordWithEmbeddedTerminatorBytes() {
        return recordWithEmbeddedTerminator().length;
    }

    protected byte[] terminatedRecord(String record) {
        return concat(bytes(record), fixtures().recordTerminator());
    }

    protected static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    protected static byte[] concat(byte[]... chunks) {
        int length = Arrays.stream(chunks).mapToInt(chunk -> chunk.length).sum();
        byte[] result = new byte[length];
        int offset = 0;
        for (byte[] chunk : chunks) {
            System.arraycopy(chunk, 0, result, offset, chunk.length);
            offset += chunk.length;
        }
        return result;
    }

    protected static byte[] repeatedByte(char value, int count) {
        byte[] bytes = new byte[count];
        Arrays.fill(bytes, (byte) value);
        return bytes;
    }

    private static int driveForwardToLastBoundary(RecordSplitter splitter, byte[] input) throws IOException {
        int cumulative = 0;
        int lastBoundary = -1;
        while (cumulative < input.length) {
            long consumed = splitter.findNextRecordBoundary(new ByteArrayInputStream(input, cumulative, input.length - cumulative));
            if (consumed < 0) {
                return lastBoundary;
            }
            assertTrue("splitter must make progress", consumed > 0);
            assertTrue("splitter consumed past the buffer", consumed <= input.length - cumulative);
            cumulative += Math.toIntExact(consumed);
            lastBoundary = cumulative - 1;
        }
        return lastBoundary;
    }

    protected record Fixtures(
        byte[] recordTerminator,
        boolean handlesCRLF,
        boolean handlesLoneCR,
        boolean canEmbedTerminatorInRecord,
        boolean canReturnRecordTooLarge
    ) {
        protected int recordTerminatorBytes() {
            return recordTerminator.length;
        }
    }
}
