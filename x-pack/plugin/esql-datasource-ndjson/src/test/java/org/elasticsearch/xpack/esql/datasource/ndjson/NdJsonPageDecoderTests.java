/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.ndjson;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.spi.ErrorPolicy;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Targeted unit tests for {@link NdJsonPageDecoder}'s keyword-decode path. Sibling
 * {@link NdJsonPageIteratorTests} covers end-to-end correctness across types; these tests focus on
 * the reusable {@code keywordScratch} buffer introduced to remove per-field allocation churn.
 */
public class NdJsonPageDecoderTests extends ESTestCase {

    private BlockFactory blockFactory;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(new NoopCircuitBreaker("none")).build();
    }

    /**
     * The decoded bytes must match the previous {@code new BytesRef(str)} encoding for every
     * Unicode shape that NDJSON exposes: ASCII, multi-byte UTF-8 (Latin/CJK), surrogate pairs
     * (emoji), embedded controls, and the empty string.
     */
    public void testKeywordEncodingMatchesNewBytesRef() throws IOException {
        List<String> values = List.of(
            "",
            "ascii",
            "café",                 // 2-byte UTF-8
            "汉字",                  // 3-byte UTF-8
            "🚀rocket",              // surrogate pair (4-byte UTF-8) at start
            "tail🚀",                // surrogate pair at end
            "mix-ascii-汉字-🚀-end",   // mixed
            "with\ttab and\nnewline" // control chars
        );

        List<BytesRef> decoded = decodeKeywords(values);
        assertEquals(values.size(), decoded.size());
        for (int i = 0; i < values.size(); i++) {
            BytesRef expected = new BytesRef(values.get(i));
            assertEquals("row " + i + " bytes mismatch", expected, decoded.get(i));
            // Cross-check via UTF-8 String round-trip in case BytesRef.equals had a bug; cheap belt-and-braces.
            assertEquals("row " + i + " string round-trip", values.get(i), decoded.get(i).utf8ToString());
        }
    }

    /**
     * Multi-value (JSON array) keywords share one scratch within a position entry. Each element
     * must be copied into the {@link org.elasticsearch.common.util.BytesRefArray} before the
     * scratch is overwritten by the next element, so the resulting MV block must contain all
     * values intact.
     */
    public void testKeywordMvArrayRoundTrips() throws IOException {
        String ndjson = "{\"k\":[\"a\",\"b\",\"c\"]}\n"
            + "{\"k\":[\"long-string-one\",\"long-string-two\"]}\n"
            + "{\"k\":[\"汉字\",\"🚀\",\"x\"]}\n";

        try (Page page = decodePage(ndjson, List.of(attribute("k", DataType.KEYWORD)))) {
            assertNotNull("page expected", page);
            assertEquals(3, page.getPositionCount());
            BytesRefBlock block = page.getBlock(0);
            BytesRef scratch = new BytesRef();

            assertMvAt(block, 0, scratch, List.of("a", "b", "c"));
            assertMvAt(block, 1, scratch, List.of("long-string-one", "long-string-two"));
            assertMvAt(block, 2, scratch, List.of("汉字", "🚀", "x"));
        }
    }

    /**
     * The buffer is grown only when a value exceeds the largest UTF-8 length seen so far. After
     * decoding a long row, subsequent shorter rows must reuse the same backing array, and the
     * long row itself must still decode to the exact bytes of the source value (catching any bug
     * in offset/length bookkeeping when the buffer is grown in the same call that fills it).
     */
    public void testScratchBufferGrowsOnDemandAndStaysGrown() throws Exception {
        String longValue = "x".repeat(2048);
        String shortValue = "y";
        String ndjson = "{\"k\":\"" + longValue + "\"}\n" + "{\"k\":\"" + shortValue + "\"}\n" + "{\"k\":\"" + shortValue + "\"}\n";

        try (
            NdJsonPageDecoder decoder = new NdJsonPageDecoder(
                new ByteArrayInputStream(ndjson.getBytes(StandardCharsets.UTF_8)),
                null, // DateFormatter
                List.of(attribute("k", DataType.KEYWORD)),
                null,
                10,
                blockFactory,
                ErrorPolicy.STRICT,
                "test://growth",
                new NdJsonReaderCounters()
            )
        ) {
            try (Page page = decoder.decodePage()) {
                assertNotNull(page);
                assertEquals(3, page.getPositionCount());
                BytesRefBlock block = page.getBlock(0);
                BytesRef scratch = new BytesRef();
                assertEquals(new BytesRef(longValue), BytesRef.deepCopyOf(block.getBytesRef(0, scratch)));
                assertEquals(new BytesRef(shortValue), BytesRef.deepCopyOf(block.getBytesRef(1, scratch)));
                assertEquals(new BytesRef(shortValue), BytesRef.deepCopyOf(block.getBytesRef(2, scratch)));
            }
            // Capacity is rounded up via UnicodeUtil.maxUTF8Length(charLen) = 3 * charLen; bind the
            // assertion to that formula so a future change that sizes to exact UTF-8 length (which
            // would silently force a re-grow on any non-ASCII follow-up) trips the test.
            int capacityAfter = scratchCapacity(decoder);
            assertTrue(
                "scratch must have grown to fit the long value (3 * charLen), got capacity " + capacityAfter,
                capacityAfter >= 3 * longValue.length()
            );
        }
    }

    /**
     * Two keyword columns on the same row share one scratch. A missing copy in
     * {@link org.elasticsearch.common.util.BytesRefArray#append(BytesRef)} would surface here as
     * the first column adopting the second column's value (or vice versa). Complements the
     * MV-within-one-column scenario.
     */
    public void testScratchReuseAcrossMultipleKeywordColumns() throws IOException {
        String ndjson = "{\"k1\":\"alpha\",\"k2\":\"beta\"}\n"
            + "{\"k1\":\"gamma\",\"k2\":\"delta-longer-than-gamma\"}\n"
            + "{\"k1\":\"epsilon-longer-than-zeta\",\"k2\":\"zeta\"}\n";

        try (Page page = decodePage(ndjson, List.of(attribute("k1", DataType.KEYWORD), attribute("k2", DataType.KEYWORD)))) {
            assertNotNull(page);
            assertEquals(3, page.getPositionCount());
            BytesRefBlock k1 = page.getBlock(0);
            BytesRefBlock k2 = page.getBlock(1);
            BytesRef scratch = new BytesRef();
            assertEquals(new BytesRef("alpha"), BytesRef.deepCopyOf(k1.getBytesRef(0, scratch)));
            assertEquals(new BytesRef("beta"), BytesRef.deepCopyOf(k2.getBytesRef(0, scratch)));
            assertEquals(new BytesRef("gamma"), BytesRef.deepCopyOf(k1.getBytesRef(1, scratch)));
            assertEquals(new BytesRef("delta-longer-than-gamma"), BytesRef.deepCopyOf(k2.getBytesRef(1, scratch)));
            assertEquals(new BytesRef("epsilon-longer-than-zeta"), BytesRef.deepCopyOf(k1.getBytesRef(2, scratch)));
            assertEquals(new BytesRef("zeta"), BytesRef.deepCopyOf(k2.getBytesRef(2, scratch)));
        }
    }

    /**
     * Decoding only non-keyword fields must not allocate the scratch backing array — it should
     * stay at {@link BytesRef#EMPTY_BYTES} (length 0). Confirms the scratch is allocated lazily
     * via the keyword path, not in the decoder constructor.
     */
    public void testScratchNotGrownWhenNoKeywordFields() throws Exception {
        String ndjson = "{\"i\":1}\n{\"i\":2}\n{\"i\":3}\n";

        try (
            NdJsonPageDecoder decoder = new NdJsonPageDecoder(
                new ByteArrayInputStream(ndjson.getBytes(StandardCharsets.UTF_8)),
                null, // DateFormatter
                List.of(attribute("i", DataType.INTEGER)),
                null,
                10,
                blockFactory,
                ErrorPolicy.STRICT,
                "test://no-keyword",
                new NdJsonReaderCounters()
            )
        ) {
            try (Page page = decoder.decodePage()) {
                assertNotNull(page);
                assertEquals(3, page.getPositionCount());
            }
            assertEquals("scratch must remain empty when no keyword fields are decoded", 0, scratchCapacity(decoder));
        }
    }

    // -----------------------------------------------------------------------------------------

    private List<BytesRef> decodeKeywords(List<String> values) throws IOException {
        StringBuilder sb = new StringBuilder();
        for (String v : values) {
            // Manual JSON-encode of the value: escape backslash and quote and a few controls.
            sb.append("{\"k\":\"");
            for (int i = 0; i < v.length(); i++) {
                char c = v.charAt(i);
                switch (c) {
                    case '\\' -> sb.append("\\\\");
                    case '"' -> sb.append("\\\"");
                    case '\n' -> sb.append("\\n");
                    case '\r' -> sb.append("\\r");
                    case '\t' -> sb.append("\\t");
                    default -> sb.append(c);
                }
            }
            sb.append("\"}\n");
        }

        try (Page page = decodePage(sb.toString(), List.of(attribute("k", DataType.KEYWORD)))) {
            assertNotNull(page);
            assertEquals(values.size(), page.getPositionCount());
            BytesRefBlock block = page.getBlock(0);
            BytesRef scratch = new BytesRef();
            List<BytesRef> out = new ArrayList<>(values.size());
            for (int i = 0; i < block.getPositionCount(); i++) {
                BytesRef ref = block.getBytesRef(block.getFirstValueIndex(i), scratch);
                out.add(BytesRef.deepCopyOf(ref));
            }
            return out;
        }
    }

    private Page decodePage(String ndjson, List<Attribute> attributes) throws IOException {
        try (
            NdJsonPageDecoder decoder = new NdJsonPageDecoder(
                new ByteArrayInputStream(ndjson.getBytes(StandardCharsets.UTF_8)),
                null, // DateFormatter
                attributes,
                null,
                1024,
                blockFactory,
                ErrorPolicy.STRICT,
                "test://decode",
                new NdJsonReaderCounters()
            )
        ) {
            return decoder.decodePage();
        }
    }

    private static Attribute attribute(String name, DataType type) {
        return NdJsonSchemaInferrer.attribute(name, type, true);
    }

    private static void assertMvAt(BytesRefBlock block, int position, BytesRef scratch, List<String> expected) {
        int count = block.getValueCount(position);
        assertEquals("value count at position " + position, expected.size(), count);
        int first = block.getFirstValueIndex(position);
        for (int v = 0; v < count; v++) {
            BytesRef ref = block.getBytesRef(first + v, scratch);
            // Pin both byte-level identity (to catch offset/length bookkeeping bugs) and string
            // round-trip (to catch any UTF-8 encoding bug that still happens to produce a
            // BytesRef::equals match against a corrupted reference).
            assertEquals("position " + position + " value " + v + " bytes", new BytesRef(expected.get(v)), ref);
            assertEquals("position " + position + " value " + v + " string", expected.get(v), ref.utf8ToString());
        }
    }

    /**
     * Read the {@code keywordScratch.bytes.length} via reflection. Used to assert allocation
     * behavior (growth, lack of growth) without exposing the field outside the package.
     */
    @SuppressForbidden(reason = "test-only reflection over the private keywordScratch to assert allocation behavior")
    private static int scratchCapacity(NdJsonPageDecoder decoder) throws Exception {
        Field f = NdJsonPageDecoder.class.getDeclaredField("keywordScratch");
        f.setAccessible(true);
        BytesRef ref = (BytesRef) f.get(decoder);
        return ref.bytes.length;
    }
}
