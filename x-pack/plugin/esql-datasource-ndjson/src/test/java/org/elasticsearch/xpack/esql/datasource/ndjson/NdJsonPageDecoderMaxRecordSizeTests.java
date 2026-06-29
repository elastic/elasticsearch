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
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.spi.ErrorPolicy;
import org.hamcrest.Matchers;
import org.junit.After;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * Decoder-level contract for {@code max_record_size} enforcement after the issue 965 change, which moved
 * enforcement out of a dedicated buffer sweep ({@code NdJsonRecordCappingInputStream} / the lenient pre-filter)
 * and into {@link NdJsonPageDecoder}'s existing per-record decode loop. The load-bearing properties are:
 * <ul>
 *   <li><b>Hot-path gate (#965):</b> on the byte-array path a record can never exceed the buffer that fully
 *       contains it, so when the whole segment is within the cap the decoder does no per-record cap work at
 *       all ({@link NdJsonPageDecoder#capEnforced()} is false). This is the streaming-parallel chunk case
 *       whose redundant traversal the issue removed.</li>
 *   <li><b>Strict:</b> an oversized record fails with a {@code max_record_size [N]} message.</li>
 *   <li><b>Lenient byte-array:</b> the oversized record is dropped and decoding continues, without compacting
 *       the buffer (so later rows keep their file offsets — see {@code NdJsonPageIteratorTests}).</li>
 *   <li><b>Lenient streaming:</b> there is no cheap resumption point, so the read truncates at the oversized
 *       record (matching the segmentator) and flags {@link NdJsonPageDecoder#truncated()}.</li>
 * </ul>
 */
public class NdJsonPageDecoderMaxRecordSizeTests extends ESTestCase {

    private static final List<Attribute> SCHEMA = List.of(
        NdJsonSchemaInferrer.attribute("id", DataType.INTEGER, false),
        NdJsonSchemaInferrer.attribute("text", DataType.KEYWORD, false)
    );
    private static final List<String> PROJECT_ID = List.of("id");

    private BlockFactory blockFactory;
    private NdJsonReaderCounters counters;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(new NoopCircuitBreaker("none")).build();
        counters = new NdJsonReaderCounters();
    }

    /**
     * The lenient streaming truncation path emits a response-header warning via
     * {@code HeaderWarning.addWarning(...)}; drop it so the parent {@code ensureNoWarnings} post-check passes.
     */
    @After
    public void clearWarningHeaders() {
        if (threadContext != null) {
            threadContext.stashContext();
        }
    }

    /**
     * #965 hot-path gate: when the whole byte-array segment is within the cap, no record can overflow it, so
     * the decoder skips per-record enforcement entirely. {@code maxRecordBytes == segment length} is still
     * "within the cap" (the threshold is strictly greater-than).
     */
    public void testByteArrayWithinCapSkipsEnforcement() throws IOException {
        byte[] data = ndjson("{\"id\":1}", "{\"id\":2}", "{\"id\":3}");
        try (NdJsonPageDecoder decoder = byteArrayDecoder(data, ErrorPolicy.STRICT)) {
            decoder.setMaxRecordBytes(data.length);
            assertFalse("a segment within the cap can hold no oversized record", decoder.capEnforced());
            consumeAll(decoder);
        }
    }

    /** The default (unset) cap also never enforces. */
    public void testByteArrayDefaultCapSkipsEnforcement() throws IOException {
        byte[] data = ndjson("{\"id\":1}", "{\"id\":2}");
        try (NdJsonPageDecoder decoder = byteArrayDecoder(data, ErrorPolicy.STRICT)) {
            decoder.setMaxRecordBytes(Integer.MAX_VALUE);
            assertFalse(decoder.capEnforced());
        }
    }

    /** Only the rare {@code max_record_size < segment_size} config can place an oversized record in a segment. */
    public void testByteArrayBelowSegmentEnablesEnforcement() throws IOException {
        byte[] data = ndjson("{\"id\":1}", "{\"id\":2}", "{\"id\":3}");
        try (NdJsonPageDecoder decoder = byteArrayDecoder(data, ErrorPolicy.STRICT)) {
            decoder.setMaxRecordBytes(data.length - 1);
            assertTrue(decoder.capEnforced());
        }
    }

    /** The streaming path has no buffer bound, so a finite cap always enforces per-record (closes the #1 gap). */
    public void testStreamingAlwaysEnforcesWithFiniteCap() throws IOException {
        byte[] data = ndjson("{\"id\":1}");
        try (NdJsonPageDecoder decoder = streamingDecoder(data, ErrorPolicy.STRICT)) {
            decoder.setMaxRecordBytes(16);
            assertTrue("streaming reads cannot bound record size up front", decoder.capEnforced());
        }
    }

    public void testStrictThrowsOnOversizedRecord() throws IOException {
        int cap = 16;
        byte[] data = ndjson("{\"id\":1}", "{\"id\":2,\"text\":\"" + "x".repeat(cap) + "\"}");
        try (NdJsonPageDecoder decoder = byteArrayDecoder(data, ErrorPolicy.STRICT)) {
            decoder.setMaxRecordBytes(cap);
            IOException ex = expectThrows(IOException.class, decoder::decodePage);
            assertThat(ex.getMessage(), Matchers.containsString("max_record_size [" + cap + "]"));
        }
    }

    public void testLenientByteArrayDropsOversizedAndContinues() throws IOException {
        int cap = 16;
        byte[] data = ndjson("{\"id\":1}", "{\"id\":2,\"text\":\"" + "x".repeat(cap) + "\"}", "{\"id\":3}");
        try (NdJsonPageDecoder decoder = byteArrayDecoder(data, ErrorPolicy.LENIENT)) {
            decoder.setMaxRecordBytes(cap);
            assertTrue(decoder.capEnforced());
            Page page = decoder.decodePage();
            assertNotNull(page);
            try {
                assertEquals("oversized record dropped, surrounding rows kept", 2, page.getPositionCount());
            } finally {
                page.releaseBlocks();
            }
            assertFalse("byte-array drop is resumable, not a truncation", decoder.truncated());
            assertNull("stream fully drained", decoder.decodePage());
        }
        assertEquals("dropped row must not count toward rowsEmitted (totalRowCount-- on drop)", 2L, counters.snapshot().rowsEmitted());
    }

    public void testLenientStreamingTruncatesAtOversizedRecord() throws IOException {
        int cap = 16;
        // r1 "{\"id\":1}\n" = 9 bytes; r2's opening brace is at byte 9. The record anchor is the offset just
        // past that brace (10), matching the byte-array offset convention pinned in NdJsonPageIteratorTests.
        long expectedTruncationAnchor = "{\"id\":1}\n".length() + 1;
        byte[] data = ndjson("{\"id\":1}", "{\"id\":2,\"text\":\"" + "x".repeat(cap) + "\"}", "{\"id\":3}");
        try (NdJsonPageDecoder decoder = streamingDecoder(data, ErrorPolicy.LENIENT)) {
            decoder.setMaxRecordBytes(cap);
            Page page = decoder.decodePage();
            assertNotNull(page);
            try {
                assertEquals("only rows before the oversized record are emitted", 1, page.getPositionCount());
            } finally {
                page.releaseBlocks();
            }
            assertTrue("streaming read truncates at the oversized record", decoder.truncated());
            assertEquals("truncation anchor is the oversized record's file offset", expectedTruncationAnchor, decoder.truncatedAtByte());
            assertNull("no further pages after truncation", decoder.decodePage());
        }
        assertEquals("only the pre-truncation row counts toward rowsEmitted", 1L, counters.snapshot().rowsEmitted());
    }

    public void testLenientStreamingTruncatesWhenFirstRecordOversized() throws IOException {
        int cap = 16;
        byte[] data = ndjson("{\"id\":1,\"text\":\"" + "x".repeat(cap) + "\"}", "{\"id\":2}");
        try (NdJsonPageDecoder decoder = streamingDecoder(data, ErrorPolicy.LENIENT)) {
            decoder.setMaxRecordBytes(cap);
            assertNull("first record oversized: no rows emitted, immediate truncation", decoder.decodePage());
            assertTrue(decoder.truncated());
        }
    }

    private NdJsonPageDecoder byteArrayDecoder(byte[] data, ErrorPolicy policy) throws IOException {
        return new NdJsonPageDecoder(
            data,
            0,
            data.length,
            null /* DateFormatter */,
            SCHEMA,
            PROJECT_ID,
            100,
            blockFactory,
            policy,
            "test",
            counters
        );
    }

    private NdJsonPageDecoder streamingDecoder(byte[] data, ErrorPolicy policy) throws IOException {
        return new NdJsonPageDecoder(
            new ByteArrayInputStream(data),
            null /* DateFormatter */,
            SCHEMA,
            PROJECT_ID,
            100,
            blockFactory,
            policy,
            "test",
            counters
        );
    }

    private static void consumeAll(NdJsonPageDecoder decoder) throws IOException {
        Page page;
        while ((page = decoder.decodePage()) != null) {
            page.releaseBlocks();
        }
    }

    private static byte[] ndjson(String... records) {
        StringBuilder sb = new StringBuilder();
        for (String r : records) {
            sb.append(r).append('\n');
        }
        return sb.toString().getBytes(StandardCharsets.UTF_8);
    }
}
