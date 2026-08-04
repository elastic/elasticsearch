/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasource.csv.CsvFormatReader;
import org.elasticsearch.xpack.esql.datasource.ndjson.NdJsonFormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.RecordSplitter;
import org.elasticsearch.xpack.esql.datasources.spi.SegmentableFormatReader;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

/**
 * The safety argument for {@link ChunkedStorageInputStream}.
 * <p>
 * Reading in chunks must be invisible to record splitting: for every offset, probing through the chunked stream
 * has to return exactly the boundary that probing through a plain whole-remainder stream returns. This is not a
 * nice-to-have equivalence — a boundary that lands one byte off does not fail, it silently shifts where one
 * split ends and the next begins, and the query returns a wrong row count. So the tests here run adversarially
 * small chunks, which puts chunk edges on every interesting byte: inside a record, on a terminator, between the
 * {@code CR} and {@code LF} of a {@code CRLF} pair, and on the last byte before the range ends.
 */
public class ChunkedProbeEquivalenceTests extends ESTestCase {

    private static final int MAX_RECORD_BYTES = SegmentableFormatReader.DEFAULT_MAX_RECORD_BYTES;

    private BlockFactory blockFactory() {
        return BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(new NoopCircuitBreaker("test")).build();
    }

    private SegmentableFormatReader plainCsvReader() {
        return (SegmentableFormatReader) new CsvFormatReader(blockFactory()).withConfig(Map.of("mode", "plain"));
    }

    private SegmentableFormatReader quotedCsvReader() {
        return (SegmentableFormatReader) new CsvFormatReader(blockFactory()).withConfig(Map.of("mode", "quoted"));
    }

    private SegmentableFormatReader ndjsonReader() {
        return new NdJsonFormatReader(Settings.EMPTY, blockFactory(), List.of());
    }

    /**
     * Probes every offset of a payload both ways and asserts the answers agree. Chunk sizes are deliberately
     * tiny so the chunk grid sweeps across every byte of every record.
     */
    private void assertBoundaryEquivalence(SegmentableFormatReader reader, byte[] payload, int maxRecordBytes) throws IOException {
        RecordSplitter splitter = reader.recordSplitter(maxRecordBytes);
        assertTrue("this corpus is only meaningful for a strided splitter", splitter.supportsStridedProbing());
        RecordingStorageObject object = new RecordingStorageObject(payload);

        for (int pos = 0; pos < payload.length; pos++) {
            long viaPlainStream;
            try (InputStream plain = object.newStream(pos, payload.length - pos)) {
                viaPlainStream = splitter.findNextRecordBoundary(plain);
            }

            int firstChunk = randomIntBetween(1, 17);
            long viaChunkedStream;
            try (InputStream chunked = new ChunkedStorageInputStream(object, pos, payload.length, firstChunk, firstChunk * 2)) {
                viaChunkedStream = splitter.findNextRecordBoundary(chunked);
            }

            assertEquals("boundary disagreed at offset " + pos + " with first chunk " + firstChunk, viaPlainStream, viaChunkedStream);
        }
        assertEquals("the chunked probe path must never abort", 0, object.abortCalls.get());
    }

    public void testNdjsonBoundaryEquivalenceAcrossEveryOffset() throws IOException {
        StringBuilder ndjson = new StringBuilder();
        for (int i = 0; i < 60; i++) {
            ndjson.append("{\"id\":").append(i).append(",\"name\":\"value_").append(i).append("\"}\n");
        }
        assertBoundaryEquivalence(ndjsonReader(), ndjson.toString().getBytes(StandardCharsets.UTF_8), MAX_RECORD_BYTES);
    }

    public void testNdjsonWithCrlfBoundaryEquivalence() throws IOException {
        StringBuilder ndjson = new StringBuilder();
        for (int i = 0; i < 60; i++) {
            ndjson.append("{\"id\":").append(i).append("}\r\n");
        }
        assertBoundaryEquivalence(ndjsonReader(), ndjson.toString().getBytes(StandardCharsets.UTF_8), MAX_RECORD_BYTES);
    }

    public void testNdjsonWithLoneCarriageReturnsBoundaryEquivalence() throws IOException {
        StringBuilder ndjson = new StringBuilder();
        for (int i = 0; i < 60; i++) {
            ndjson.append("{\"id\":").append(i).append("}\r");
        }
        assertBoundaryEquivalence(ndjsonReader(), ndjson.toString().getBytes(StandardCharsets.UTF_8), MAX_RECORD_BYTES);
    }

    public void testNdjsonWithMixedTerminatorsAndRaggedRecordsBoundaryEquivalence() throws IOException {
        StringBuilder ndjson = new StringBuilder();
        for (int i = 0; i < 80; i++) {
            ndjson.append("{\"id\":").append(i).append(",\"pad\":\"").append("x".repeat(randomIntBetween(0, 40))).append("\"}");
            switch (randomIntBetween(0, 2)) {
                case 0 -> ndjson.append('\n');
                case 1 -> ndjson.append("\r\n");
                default -> ndjson.append('\r');
            }
        }
        assertBoundaryEquivalence(ndjsonReader(), ndjson.toString().getBytes(StandardCharsets.UTF_8), MAX_RECORD_BYTES);
    }

    public void testPayloadWithNoTrailingTerminatorBoundaryEquivalence() throws IOException {
        byte[] payload = "{\"id\":1}\n{\"id\":2}\n{\"id\":3-unterminated".getBytes(StandardCharsets.UTF_8);
        assertBoundaryEquivalence(ndjsonReader(), payload, MAX_RECORD_BYTES);
    }

    public void testPlainCsvBoundaryEquivalenceAcrossEveryOffset() throws IOException {
        StringBuilder csv = new StringBuilder("id,name\n");
        for (int i = 0; i < 60; i++) {
            csv.append(i).append(",value_").append(i).append('\n');
        }
        assertBoundaryEquivalence(plainCsvReader(), csv.toString().getBytes(StandardCharsets.UTF_8), MAX_RECORD_BYTES);
    }

    /**
     * A record longer than the byte budget must reach the same verdict either way. The budget is counted by the
     * splitter as it consumes bytes, so it has to survive those bytes arriving in many chunks rather than one.
     */
    public void testRecordExceedingMaxRecordBytesReachesSameVerdict() throws IOException {
        byte[] payload = ("{\"id\":1}\n" + "y".repeat(4096) + "\n{\"id\":2}\n").getBytes(StandardCharsets.UTF_8);
        int budget = 512;
        RecordSplitter splitter = ndjsonReader().recordSplitter(budget);
        RecordingStorageObject object = new RecordingStorageObject(payload);

        int pos = 9; // first byte of the oversized record
        long viaPlainStream;
        try (InputStream plain = object.newStream(pos, payload.length - pos)) {
            viaPlainStream = splitter.findNextRecordBoundary(plain);
        }
        long viaChunkedStream;
        try (InputStream chunked = new ChunkedStorageInputStream(object, pos, payload.length, 7, 32)) {
            viaChunkedStream = splitter.findNextRecordBoundary(chunked);
        }

        assertEquals(RecordSplitter.RECORD_TOO_LARGE, viaPlainStream);
        assertEquals("the budget must be counted identically across chunk boundaries", viaPlainStream, viaChunkedStream);
    }

    /** The proven-probe and exact-walk shapes read the same stream, so they need the same guarantee. */
    public void testQuotedCsvProvenProbeAndExactWalkEquivalence() throws IOException {
        StringBuilder csv = new StringBuilder("id,name\n");
        for (int i = 0; i < 60; i++) {
            csv.append(i).append(",\"value ").append(i).append("\"\n");
        }
        byte[] payload = csv.toString().getBytes(StandardCharsets.UTF_8);

        RecordSplitter splitter = quotedCsvReader().recordSplitter(MAX_RECORD_BYTES);
        assumeTrue("quoted CSV must support proven probing for this test", splitter.supportsProvenProbing());
        RecordingStorageObject object = new RecordingStorageObject(payload);

        for (int pos = 1; pos < payload.length; pos++) {
            long provenPlain;
            try (InputStream plain = object.newStream(pos, payload.length - pos)) {
                provenPlain = splitter.findProvenRecordBoundary(plain);
            }
            long provenChunked;
            try (InputStream chunked = new ChunkedStorageInputStream(object, pos, payload.length, randomIntBetween(1, 13), 32)) {
                provenChunked = splitter.findProvenRecordBoundary(chunked);
            }
            assertEquals("proven probe disagreed at offset " + pos, provenPlain, provenChunked);

            long walkPlain;
            try (InputStream plain = object.newStream(0, payload.length)) {
                walkPlain = splitter.findRecordStartAtOrAfter(plain, pos, () -> false);
            }
            long walkChunked;
            try (InputStream chunked = new ChunkedStorageInputStream(object, 0, payload.length, randomIntBetween(1, 13), 32)) {
                walkChunked = splitter.findRecordStartAtOrAfter(chunked, pos, () -> false);
            }
            assertEquals("exact walk disagreed at min skip " + pos, walkPlain, walkChunked);
        }
        assertEquals("the chunked probe path must never abort", 0, object.abortCalls.get());
    }

    /** The chunked probe must not read materially more than the plain probe consumed to find the same answer. */
    public void testProbeTransfersStayBoundedRelativeToTheRecordLength() throws IOException {
        StringBuilder ndjson = new StringBuilder();
        for (int i = 0; i < 5_000; i++) {
            ndjson.append("{\"id\":").append(i).append(",\"name\":\"value_").append(i).append("\"}\n");
        }
        byte[] payload = ndjson.toString().getBytes(StandardCharsets.UTF_8);
        RecordingStorageObject object = new RecordingStorageObject(payload);
        RecordSplitter splitter = ndjsonReader().recordSplitter(MAX_RECORD_BYTES);

        try (InputStream chunked = new ChunkedStorageInputStream(object, payload.length / 2, payload.length)) {
            assertThat(splitter.findNextRecordBoundary(chunked), Matchers.greaterThanOrEqualTo(0L));
        }

        assertEquals("a short record must be answered by one request", 1, object.readBytesCalls.size());
        assertThat(
            "one request must not exceed the first chunk size",
            object.readBytesCalls.get(0)[1],
            Matchers.lessThanOrEqualTo((long) ChunkedStorageInputStream.FIRST_CHUNK_SIZE)
        );
    }
}
