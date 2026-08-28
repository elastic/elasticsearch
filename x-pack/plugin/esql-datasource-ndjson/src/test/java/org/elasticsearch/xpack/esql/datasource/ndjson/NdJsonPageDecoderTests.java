/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.ndjson;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.logging.HeaderWarning;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.NumericUtils;
import org.elasticsearch.xpack.esql.datasources.CountingBreaker;
import org.elasticsearch.xpack.esql.datasources.DeclaredSchemaValidator;
import org.elasticsearch.xpack.esql.datasources.spi.DeclaredTypeCoercions;
import org.elasticsearch.xpack.esql.datasources.spi.ErrorPolicy;
import org.elasticsearch.xpack.esql.parser.ParsingException;
import org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Targeted unit tests for {@link NdJsonPageDecoder}: keyword-scratch reuse, schema-shape conflicts,
 * declared formats, and block-builder allocation sizing. Sibling {@link NdJsonPageIteratorTests}
 * covers end-to-end correctness across types.
 */
public class NdJsonPageDecoderTests extends ESTestCase {

    private BlockFactory blockFactory;

    @Before
    public void initBlockFactory() {
        blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(new NoopCircuitBreaker("none")).build();
    }

    /**
     * Non-strict {@link ErrorPolicy} shape-conflict tests below emit response-header warnings via
     * {@code HeaderWarning.addWarning(...)}; drop them so the parent {@code ensureNoWarnings} post-check passes.
     */
    @After
    public void clearWarningHeaders() {
        if (threadContext != null) {
            threadContext.stashContext();
        }
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

    /**
     * A dotted-prefix column such as {@code address.city} builds a structural (intermediate) decoder node with no
     * scalar block builder of its own. When a row provides a JSON {@code null} where an object was expected, the
     * leaf columns must be filled with null for that row instead of throwing a {@link NullPointerException}.
     * Regression test for https://github.com/elastic/elasticsearch/issues/152574. A JSON {@code null} is a common,
     * legitimate shape (e.g. an intermittently-null nested object) and stays silent under every {@link ErrorPolicy}
     * Unlike an actual scalar value where an object was expected, that is skipped and null-filled under every
     * {@link ErrorPolicy}, including STRICT.
     * <p>
     * This drives the decoder with an explicit dotted schema, i.e. the planner-resolved (bound) read-schema path
     * where {@code address} exists only as a nested-object prefix. It deliberately does not go through per-file
     * schema inference: when a mixed object/scalar field is <em>sampled</em>, inference now resolves a single shape
     * (see {@link NdJsonSchemaInferrerTests}); that inference interaction is exercised end-to-end in the iterator
     * tests.
     */
    public void testNullWhereNestedObjectExpected() throws IOException {
        String ndjson = "{\"address\": {\"city\": \"NYC\", \"zip\": \"10001\"}}\n"
            + "{\"address\": null}\n"
            + "{\"address\": {\"city\": \"London\", \"zip\": \"SW1A\"}}\n";

        try (
            Page page = decodePage(
                ndjson,
                List.of(attribute("address.city", DataType.KEYWORD), attribute("address.zip", DataType.KEYWORD)),
                ErrorPolicy.STRICT
            )
        ) {
            assertNotNull(page);
            assertEquals(3, page.getPositionCount());
            BytesRefBlock city = page.getBlock(0);
            BytesRefBlock zip = page.getBlock(1);
            BytesRef scratch = new BytesRef();
            assertEquals(new BytesRef("NYC"), BytesRef.deepCopyOf(city.getBytesRef(0, scratch)));
            assertEquals(new BytesRef("10001"), BytesRef.deepCopyOf(zip.getBytesRef(0, scratch)));
            assertTrue("null object row -> city null", city.isNull(1));
            assertTrue("null object row -> zip null", zip.isNull(1));
            assertEquals(new BytesRef("London"), BytesRef.deepCopyOf(city.getBytesRef(2, scratch)));
            assertEquals(new BytesRef("SW1A"), BytesRef.deepCopyOf(zip.getBytesRef(2, scratch)));
        }
    }

    /**
     * A column whose schema name is dotted ({@code a.b}) must be reachable when a record spells it as a single flat
     * JSON key ({@code {"a.b":1}}), not only as the nested object ({@code {"a":{"b":1}}}). Indexing the same bytes
     * dot-expands the flat spelling into the nested field, so a schema-on-read of the file must agree: the value is
     * decoded into the same output column, never silently null.
     */
    public void testFlatDottedFieldSpellingDecodesIntoDottedColumn() throws IOException {
        String ndjson = "{\"a.b\":1}\n{\"a.b\":2}\n";

        try (Page page = decodePage(ndjson, List.of(attribute("a.b", DataType.LONG)))) {
            assertNotNull(page);
            assertEquals(2, page.getPositionCount());
            LongBlock ab = page.getBlock(0);
            assertFalse("flat a.b present at row 0", ab.isNull(0));
            assertEquals(1L, ab.getLong(ab.getFirstValueIndex(0)));
            assertFalse("flat a.b present at row 1", ab.isNull(1));
            assertEquals(2L, ab.getLong(ab.getFirstValueIndex(1)));
        }
    }

    /**
     * A file mixing both spellings across records ({@code {"a":{"b":1}}} then {@code {"a.b":2}}) must decode every
     * record into the one output column, regardless of which spelling each record used.
     */
    public void testMixedDottedAndNestedSpellingsAcrossRecords() throws IOException {
        String ndjson = "{\"a\":{\"b\":1}}\n{\"a.b\":2}\n";

        try (Page page = decodePage(ndjson, List.of(attribute("a.b", DataType.LONG)))) {
            assertNotNull(page);
            assertEquals(2, page.getPositionCount());
            LongBlock ab = page.getBlock(0);
            assertFalse("nested a.b present at row 0", ab.isNull(0));
            assertEquals(1L, ab.getLong(ab.getFirstValueIndex(0)));
            assertFalse("flat a.b present at row 1", ab.isNull(1));
            assertEquals(2L, ab.getLong(ab.getFirstValueIndex(1)));
        }
    }

    /**
     * A deeper dotted column ({@code a.b.c}) must resolve from a flat spelling of any depth: fully flat
     * ({@code {"a.b.c":1}}), fully nested ({@code {"a":{"b":{"c":2}}}}), and a mixed flat-prefix-plus-nested-
     * remainder ({@code {"a.b":{"c":3}}}). All reach the one output column.
     */
    public void testDeepDottedColumnResolvesFromEverySpelling() throws IOException {
        String ndjson = "{\"a.b.c\":1}\n{\"a\":{\"b\":{\"c\":2}}}\n{\"a.b\":{\"c\":3}}\n{\"a\":{\"b.c\":4}}\n";

        try (Page page = decodePage(ndjson, List.of(attribute("a.b.c", DataType.LONG)))) {
            assertNotNull(page);
            assertEquals(4, page.getPositionCount());
            LongBlock abc = page.getBlock(0);
            for (int p = 0; p < 4; p++) {
                assertFalse("a.b.c present at row " + p, abc.isNull(p));
                assertEquals(p + 1L, abc.getLong(abc.getFirstValueIndex(p)));
            }
        }
    }

    /**
     * A scalar {@code languages} beside {@code languages.long} shares one node that is both a leaf and a prefix.
     * The flat key {@code "languages.long"} reaches {@code languages → long} via {@code resolveDottedPath}. Both
     * columns decode from a single record.
     */
    public void testScalarSiblingPrefixConflictStillDecodes() throws IOException {
        String ndjson = "{\"languages\":5,\"languages.long\":42}\n";

        try (Page page = decodePage(ndjson, List.of(attribute("languages", DataType.LONG), attribute("languages.long", DataType.LONG)))) {
            assertNotNull(page);
            assertEquals(1, page.getPositionCount());
            LongBlock languages = page.getBlock(0);
            LongBlock languagesLong = page.getBlock(1);
            assertEquals(5L, languages.getLong(languages.getFirstValueIndex(0)));
            assertEquals(42L, languagesLong.getLong(languagesLong.getFirstValueIndex(0)));
        }
    }

    /**
     * A flat dotted key nested deeper than the schema's leaf ({@code a.b.c} against a scalar schema leaf
     * {@code a.b}) is unreachable: the path walk cannot continue past the leaf (a leaf has no children), so the
     * key is treated as unprojected and the cell is null, exactly as an unknown field null-fills.
     */
    public void testFlatKeyDeeperThanSchemaLeafIsNull() throws IOException {
        String ndjson = "{\"a.b.c\":1}\n";

        try (Page page = decodePage(ndjson, List.of(attribute("a.b", DataType.LONG)))) {
            assertNotNull(page);
            assertEquals(1, page.getPositionCount());
            LongBlock ab = page.getBlock(0);
            assertTrue("a.b unreachable from deeper flat key -> null", ab.isNull(0));
        }
    }

    /**
     * An array of objects at an ancestor of a leaf-and-prefix node ({@code x.a} beside {@code x.a.b}) opens a
     * multivalue entry on both columns. An element that fills only one of them leaves the other entry empty; that
     * empty entry is committed as null so the row stays one position per column. A sibling {@code id} pins alignment.
     */
    public void testSparseObjectArrayOnLeafAndPrefixNullFillsTheUnfilledColumn() throws IOException {
        String ndjson = "{\"x\":[{\"a\":1}],\"id\":10}\n" + "{\"x\":[{\"a\":{\"b\":2}}],\"id\":20}\n";

        try (
            Page page = decodePage(
                ndjson,
                List.of(attribute("x.a", DataType.LONG), attribute("x.a.b", DataType.LONG), attribute("id", DataType.LONG))
            )
        ) {
            assertNotNull(page);
            assertEquals(2, page.getPositionCount());
            LongBlock xa = page.getBlock(0);
            LongBlock xab = page.getBlock(1);
            LongBlock id = page.getBlock(2);
            assertEquals(1L, xa.getLong(xa.getFirstValueIndex(0)));
            assertTrue("object-array element with only x.a leaves x.a.b null", xab.isNull(0));
            assertTrue("object-array element with only x.a.b leaves x.a null", xa.isNull(1));
            assertEquals(2L, xab.getLong(xab.getFirstValueIndex(1)));
            assertEquals(10L, id.getLong(id.getFirstValueIndex(0)));
            assertEquals(20L, id.getLong(id.getFirstValueIndex(1)));
        }
    }

    /**
     * A later object at a leaf-and-prefix node whose scalar is already claimed still populates dotted children.
     * {@code {"a.b":1,"a":{"b":{"c":2}}}} with columns {@code a.b} and {@code a.b.c} fills both; a sibling
     * {@code id} pins alignment.
     */
    public void testLaterObjectAtClaimedDualNodeStillDecodesDescendants() throws IOException {
        String ndjson = "{\"a.b\":1,\"a\":{\"b\":{\"c\":2}},\"id\":10}\n" + "{\"a\":1,\"a\":{\"b\":2},\"id\":20}\n";

        try (
            Page page = decodePage(
                ndjson,
                List.of(
                    attribute("a.b", DataType.LONG),
                    attribute("a.b.c", DataType.LONG),
                    attribute("a", DataType.LONG),
                    attribute("id", DataType.LONG)
                )
            )
        ) {
            assertNotNull(page);
            assertEquals(2, page.getPositionCount());
            LongBlock ab = page.getBlock(0);
            LongBlock abc = page.getBlock(1);
            LongBlock a = page.getBlock(2);
            LongBlock id = page.getBlock(3);
            assertEquals(1L, ab.getLong(ab.getFirstValueIndex(0)));
            assertEquals(2L, abc.getLong(abc.getFirstValueIndex(0)));
            assertTrue(a.isNull(0));
            assertTrue(abc.isNull(1));
            assertEquals(1L, a.getLong(a.getFirstValueIndex(1)));
            assertEquals(2L, ab.getLong(ab.getFirstValueIndex(1)));
            assertEquals(10L, id.getLong(id.getFirstValueIndex(0)));
            assertEquals(20L, id.getLong(id.getFirstValueIndex(1)));
        }
    }

    /**
     * A later empty array on a prefix must not append a second position on a descendant a flat spelling already
     * filled. {@code {"a.b":1,"a":[]}} keeps {@code a.b=1} aligned with {@code id}.
     */
    public void testLaterEmptyArrayOnPrefixDoesNotAddPosition() throws IOException {
        String ndjson = "{\"a.b\":1,\"a\":[],\"id\":10}\n";

        try (Page page = decodePage(ndjson, List.of(attribute("a.b", DataType.LONG), attribute("id", DataType.LONG)))) {
            assertNotNull(page);
            assertEquals(1, page.getPositionCount());
            LongBlock ab = page.getBlock(0);
            LongBlock id = page.getBlock(1);
            assertEquals(1L, ab.getLong(ab.getFirstValueIndex(0)));
            assertEquals(1, ab.getValueCount(0));
            assertEquals(10L, id.getLong(id.getFirstValueIndex(0)));
        }
    }

    /**
     * A later array of objects on a prefix merges into a descendant a flat spelling already filled:
     * {@code {"a.b":1,"a":[{"b":2}]}} yields {@code [1, 2]} on {@code a.b}, one position, aligned with {@code id}.
     */
    public void testLaterObjectArrayOnPrefixMergesIntoClaimedDescendant() throws IOException {
        String ndjson = "{\"a.b\":1,\"a\":[{\"b\":2}],\"id\":10}\n";

        try (Page page = decodePage(ndjson, List.of(attribute("a.b", DataType.LONG), attribute("id", DataType.LONG)))) {
            assertNotNull(page);
            assertEquals(1, page.getPositionCount());
            LongBlock ab = page.getBlock(0);
            LongBlock id = page.getBlock(1);
            assertEquals(2, ab.getValueCount(0));
            int first = ab.getFirstValueIndex(0);
            assertEquals(1L, ab.getLong(first));
            assertEquals(2L, ab.getLong(first + 1));
            assertEquals(10L, id.getLong(id.getFirstValueIndex(0)));
        }
    }

    /**
     * An empty array contributes nothing and does not claim the leaf's cell, so a later spelling in the same
     * record still fills it: ingest with {@code subobjects: false} indexes {@code {"a.b":[],"a":[{"b":1}]}} as
     * {@code a.b=[1]}, not as a null (see {@code NdJsonIngestParityTests}). Claiming the cell eagerly would both
     * pin the column to null against that value and, since a null cannot be reopened to gain values, leave the
     * following array with no open entry to append into, which starts a SECOND position for the column and
     * leaves it one longer than its siblings.
     * <p>
     * Both spellings of the empty array reach the same leaf ({@code "a.b":[]} flat, {@code "a":[]} on the
     * prefix), and the deeper {@code a.b.c} case pins the behavior below the array's own node. A sibling
     * {@code id} column pins the alignment.
     */
    public void testEmptyArrayDoesNotClaimTheCellAgainstALaterValue() throws IOException {
        String ndjson = "{\"a.b\":[],\"a\":[{\"b\":1}],\"id\":10}\n" + "{\"a\":[],\"a.b\":2,\"id\":20}\n" + "{\"a.b\":[],\"id\":30}\n";

        try (Page page = decodePage(ndjson, List.of(attribute("a.b", DataType.LONG), attribute("id", DataType.LONG)))) {
            assertNotNull(page);
            assertEquals(3, page.getPositionCount());
            LongBlock ab = page.getBlock(0);
            LongBlock id = page.getBlock(1);
            assertEquals("[] must not add a position of its own", 3, ab.getPositionCount());
            assertEquals(1L, ab.getLong(ab.getFirstValueIndex(0)));
            assertEquals(1, ab.getValueCount(0));
            assertEquals(2L, ab.getLong(ab.getFirstValueIndex(1)));
            assertTrue("[] alone leaves the cell to the end-of-record fill", ab.isNull(2));
            assertEquals(10L, id.getLong(id.getFirstValueIndex(0)));
            assertEquals(20L, id.getLong(id.getFirstValueIndex(1)));
            assertEquals(30L, id.getLong(id.getFirstValueIndex(2)));
        }

        String deeper = "{\"a.b.c\":[],\"a\":[{\"b\":{\"c\":1}}],\"id\":10}\n";
        try (Page page = decodePage(deeper, List.of(attribute("a.b.c", DataType.LONG), attribute("id", DataType.LONG)))) {
            assertNotNull(page);
            assertEquals(1, page.getPositionCount());
            LongBlock abc = page.getBlock(0);
            assertEquals(1L, abc.getLong(abc.getFirstValueIndex(0)));
            assertEquals(10L, ((LongBlock) page.getBlock(1)).getLong(0));
        }
    }

    /**
     * A cell nulled by an error policy cannot be widened, so an array of objects on an ancestor finds no open
     * entry on that leaf. Its values must be dropped rather than appended: appending with no entry open starts a
     * SECOND position for the column, and {@code Page} only asserts equal position counts, so the record would
     * otherwise ship crooked in production. The leaf's sibling {@code a.c} in the same array is unaffected.
     */
    public void testArrayOnPrefixCannotWidenAPolicyNulledCell() throws IOException {
        String ndjson = "{\"a.b\":\"notanumber\",\"a\":[{\"b\":1,\"c\":7},{\"b\":2,\"c\":8}],\"id\":10}\n";

        try (
            Page page = decodePage(
                ndjson,
                List.of(attribute("a.b", DataType.LONG), attribute("a.c", DataType.LONG), attribute("id", DataType.LONG)),
                // Ratio 0.0 disables the ratio check: this record carries more than one bad value and the test is
                // about the rollback, not about the error budget.
                new ErrorPolicy(ErrorPolicy.Mode.NULL_FIELD, 100, 0.0, false)
            )
        ) {
            assertNotNull(page);
            assertEquals(1, page.getPositionCount());
            LongBlock ab = page.getBlock(0);
            LongBlock ac = page.getBlock(1);
            LongBlock id = page.getBlock(2);
            assertEquals("the nulled leaf must not gain a position of its own", 1, ab.getPositionCount());
            assertTrue("null_field nulled this cell; the later array cannot widen it", ab.isNull(0));
            assertEquals(2, ac.getValueCount(0));
            int first = ac.getFirstValueIndex(0);
            assertEquals(7L, ac.getLong(first));
            assertEquals(8L, ac.getLong(first + 1));
            assertEquals(10L, id.getLong(id.getFirstValueIndex(0)));
        }
    }

    /**
     * A coercion failure inside an array poisons the position, and {@code cancelAndNullPositionEntry} rolls every
     * column under that array back to a null. A column whose reopen was refused (see
     * {@link #testArrayOnPrefixCannotWidenAPolicyNulledCell}) has no entry to roll back and already holds that
     * null, so it must be left alone: {@code cancelPositionEntry} asserts when no entry is open. Under
     * {@code null_field} the record survives with both {@code a.*} cells null and {@code id} intact.
     */
    public void testPoisonedArrayWithRefusedReopenRollsBackCleanly() throws IOException {
        String ndjson = "{\"a.b\":\"notanumber\",\"a\":[{\"b\":1,\"c\":\"alsobad\"}],\"id\":10}\n";

        try (
            Page page = decodePage(
                ndjson,
                List.of(attribute("a.b", DataType.LONG), attribute("a.c", DataType.LONG), attribute("id", DataType.LONG)),
                // Ratio 0.0 disables the ratio check: this record carries more than one bad value and the test is
                // about the rollback, not about the error budget.
                new ErrorPolicy(ErrorPolicy.Mode.NULL_FIELD, 100, 0.0, false)
            )
        ) {
            assertNotNull(page);
            assertEquals(1, page.getPositionCount());
            assertTrue(((LongBlock) page.getBlock(0)).isNull(0));
            assertTrue("the poisoned array nulls the whole position for this column", ((LongBlock) page.getBlock(1)).isNull(0));
            assertEquals(10L, ((LongBlock) page.getBlock(2)).getLong(0));
        }
    }

    /**
     * A declared column that appears only as JSON null is present in the file. The cell is still null, but
     * {@code close()} must not emit an absent-declared-column warning.
     */
    public void testJsonNullMarksDeclaredColumnPresent() throws IOException {
        String ndjson = "{\"a.b\":null}\n{\"a\":{\"b\":null}}\n";
        List<String> warnings = new ArrayList<>();
        try (
            NdJsonPageDecoder decoder = new NdJsonPageDecoder(
                new ByteArrayInputStream(ndjson.getBytes(StandardCharsets.UTF_8)),
                null,
                List.of(attribute("a.b", DataType.LONG)),
                null,
                1024,
                blockFactory,
                ErrorPolicy.STRICT,
                "test://decode",
                new NdJsonReaderCounters(),
                warnings::add
            )
        ) {
            try (Page page = decoder.decodePage()) {
                assertNotNull(page);
                assertEquals(2, page.getPositionCount());
                LongBlock ab = page.getBlock(0);
                assertTrue(ab.isNull(0));
                assertTrue(ab.isNull(1));
            }
        }
        assertTrue("JSON null is a present key, not an absent column: " + warnings, warnings.isEmpty());
    }

    /**
     * A record spelling one dotted column more than once ({@code {"a":{"b":1},"a.b":2}}, either order, a repeated
     * nested key, or a repeated flat key) contributes every occurrence to one cell as a multivalue, which is the
     * value list indexing that same document produces. The column still occupies exactly one position, so it stays
     * aligned with its siblings; a sibling {@code id} column pins that.
     */
    public void testSameRecordDuplicateSpellingsMergeIntoMultivalue() throws IOException {
        String ndjson = "{\"a\":{\"b\":1},\"a.b\":2,\"id\":10}\n"
            + "{\"a.b\":3,\"a\":{\"b\":4},\"id\":20}\n"
            + "{\"a\":{\"b\":5},\"a\":{\"b\":6},\"id\":30}\n"
            + "{\"a.b\":7,\"a.b\":8,\"id\":40}\n";

        try (Page page = decodePage(ndjson, List.of(attribute("a.b", DataType.LONG), attribute("id", DataType.LONG)))) {
            assertNotNull(page);
            assertEquals(4, page.getPositionCount());
            LongBlock ab = page.getBlock(0);
            LongBlock id = page.getBlock(1);
            long[][] expected = { { 1L, 2L }, { 3L, 4L }, { 5L, 6L }, { 7L, 8L } };
            for (int p = 0; p < 4; p++) {
                assertEquals("value count at row " + p, 2, ab.getValueCount(p));
                int first = ab.getFirstValueIndex(p);
                assertEquals("first value at row " + p, expected[p][0], ab.getLong(first));
                assertEquals("second value at row " + p, expected[p][1], ab.getLong(first + 1));
            }
            assertEquals(10L, id.getLong(id.getFirstValueIndex(0)));
            assertEquals(20L, id.getLong(id.getFirstValueIndex(1)));
            assertEquals(30L, id.getLong(id.getFirstValueIndex(2)));
            assertEquals(40L, id.getLong(id.getFirstValueIndex(3)));
        }
    }

    /**
     * An array occurrence contributes each of its elements to the merged cell rather than one nested value, so a
     * scalar spelling beside an array spelling flattens exactly as two arrays would.
     */
    public void testSameRecordDuplicateSpellingsFlattenArrayOccurrence() throws IOException {
        String ndjson = "{\"a\":{\"b\":1},\"a.b\":[2,3],\"id\":10}\n" + "{\"a.b\":[4,5],\"a\":{\"b\":6},\"id\":20}\n";

        try (Page page = decodePage(ndjson, List.of(attribute("a.b", DataType.LONG), attribute("id", DataType.LONG)))) {
            assertNotNull(page);
            assertEquals(2, page.getPositionCount());
            LongBlock ab = page.getBlock(0);
            LongBlock id = page.getBlock(1);
            assertEquals(3, ab.getValueCount(0));
            int first = ab.getFirstValueIndex(0);
            assertEquals(1L, ab.getLong(first));
            assertEquals(2L, ab.getLong(first + 1));
            assertEquals(3L, ab.getLong(first + 2));
            assertEquals(3, ab.getValueCount(1));
            int second = ab.getFirstValueIndex(1);
            assertEquals(4L, ab.getLong(second));
            assertEquals(5L, ab.getLong(second + 1));
            assertEquals(6L, ab.getLong(second + 2));
            assertEquals(10L, id.getLong(id.getFirstValueIndex(0)));
            assertEquals(20L, id.getLong(id.getFirstValueIndex(1)));
        }
    }

    /**
     * A JSON null for one spelling must not block a real value from the other spelling of the same dotted column
     * in one record ({@code {"a":{"b":null},"a.b":2}} and the reverse both yield the value). A record providing
     * only a null still nulls the cell, and the value/null cases stay aligned with a sibling column.
     */
    public void testSameRecordDuplicateNullDoesNotBlockValue() throws IOException {
        String ndjson = "{\"a\":{\"b\":null},\"a.b\":2,\"id\":10}\n"
            + "{\"a.b\":3,\"a\":{\"b\":null},\"id\":20}\n"
            + "{\"a\":{\"b\":null},\"id\":30}\n";

        try (Page page = decodePage(ndjson, List.of(attribute("a.b", DataType.LONG), attribute("id", DataType.LONG)))) {
            assertNotNull(page);
            assertEquals(3, page.getPositionCount());
            LongBlock ab = page.getBlock(0);
            LongBlock id = page.getBlock(1);
            assertEquals("null-then-value -> value", 2L, ab.getLong(ab.getFirstValueIndex(0)));
            assertEquals("value-then-null -> value", 3L, ab.getLong(ab.getFirstValueIndex(1)));
            assertTrue("only-null -> null", ab.isNull(2));
            assertEquals(10L, id.getLong(id.getFirstValueIndex(0)));
            assertEquals(20L, id.getLong(id.getFirstValueIndex(1)));
            assertEquals(30L, id.getLong(id.getFirstValueIndex(2)));
        }
    }

    /**
     * The same-record duplicate resolves silently under {@link ErrorPolicy#STRICT}: two spellings of one dotted
     * column are legitimate names for the same field (the shape indexes cleanly), so the query must not fail. A
     * sibling {@code id} column pins that the merge did not skew the row under fail-fast.
     */
    public void testSameRecordDuplicateSpellingsDoesNotFailStrict() throws IOException {
        String ndjson = "{\"a\":{\"b\":1},\"a.b\":2,\"id\":10}\n";

        try (Page page = decodePage(ndjson, List.of(attribute("a.b", DataType.LONG), attribute("id", DataType.LONG)), ErrorPolicy.STRICT)) {
            assertNotNull(page);
            assertEquals(1, page.getPositionCount());
            LongBlock ab = page.getBlock(0);
            LongBlock id = page.getBlock(1);
            assertEquals(2, ab.getValueCount(0));
            assertEquals(1L, ab.getLong(ab.getFirstValueIndex(0)));
            assertEquals(2L, ab.getLong(ab.getFirstValueIndex(0) + 1));
            assertEquals(10L, id.getLong(id.getFirstValueIndex(0)));
        }
    }

    /**
     * An empty array {@code []} contributes no value and does not claim the row, exactly like a plain JSON null:
     * the other spelling of the column in the same record supplies the value, in either order. This matches
     * ingest with {@code subobjects: false}, which indexes {@code {"a.b":[],"a":{"b":2}}} as {@code a.b=[2]}
     * (see {@code NdJsonIngestParityTests}). A sibling {@code id} column pins alignment.
     */
    public void testSameRecordDuplicateEmptyArrayDoesNotClaimRow() throws IOException {
        String ndjson = "{\"a.b\":[],\"a\":{\"b\":2},\"id\":10}\n" + "{\"a\":{\"b\":3},\"a.b\":[],\"id\":20}\n";

        try (Page page = decodePage(ndjson, List.of(attribute("a.b", DataType.LONG), attribute("id", DataType.LONG)))) {
            assertNotNull(page);
            assertEquals(2, page.getPositionCount());
            LongBlock ab = page.getBlock(0);
            LongBlock id = page.getBlock(1);
            assertEquals("[] first, value second", 2L, ab.getLong(ab.getFirstValueIndex(0)));
            assertEquals(1, ab.getValueCount(0));
            assertEquals("value first, [] second", 3L, ab.getLong(ab.getFirstValueIndex(1)));
            assertEquals(1, ab.getValueCount(1));
            assertEquals(10L, id.getLong(id.getFirstValueIndex(0)));
            assertEquals(20L, id.getLong(id.getFirstValueIndex(1)));
        }
    }

    /**
     * A bad value in any occurrence nulls the whole merged cell under a lenient policy, in either order: a cell the
     * policy already nulled cannot be widened by a later good spelling, and a later bad spelling poisons the
     * position the good one had opened. The all-or-nothing outcome matches the array contract, where one
     * unrepresentable element nulls the entry rather than committing it in part. A sibling {@code id} column pins
     * that neither direction skewed the row.
     */
    public void testSameRecordDuplicateCoercionFailureNullsWholeCell() throws IOException {
        String ndjson = "{\"a.b\":\"bad\",\"a\":{\"b\":2},\"id\":10}\n" + "{\"a\":{\"b\":3},\"a.b\":\"bad\",\"id\":20}\n";

        try (
            Page page = decodePage(ndjson, List.of(attribute("a.b", DataType.LONG), attribute("id", DataType.LONG)), ErrorPolicy.PERMISSIVE)
        ) {
            assertNotNull(page);
            assertEquals(2, page.getPositionCount());
            LongBlock ab = page.getBlock(0);
            LongBlock id = page.getBlock(1);
            assertTrue("bad value first -> null", ab.isNull(0));
            assertTrue("bad value second -> null", ab.isNull(1));
            assertEquals(10L, id.getLong(id.getFirstValueIndex(0)));
            assertEquals(20L, id.getLong(id.getFirstValueIndex(1)));
        }
    }

    /**
     * The same-record duplicate must keep the dotted column aligned with its siblings on the lenient (per-record
     * scratch) decode path too, not just fail-fast. The lenient path builds each row in scratch builders and
     * copies one position per record, so a per-column position skew from a duplicate would corrupt the copy;
     * asserting a sibling {@code id} column at every row detects that skew. A lenient policy must not error on the
     * duplicate either.
     */
    public void testSameRecordDuplicateSpellingsAlignsOnLenientPath() throws IOException {
        String ndjson = "{\"a\":{\"b\":1},\"a.b\":2,\"id\":10}\n" + "{\"id\":20}\n" + "{\"a.b\":3,\"a\":{\"b\":4},\"id\":30}\n";

        try (
            Page page = decodePage(ndjson, List.of(attribute("a.b", DataType.LONG), attribute("id", DataType.LONG)), ErrorPolicy.PERMISSIVE)
        ) {
            assertNotNull(page);
            assertEquals(3, page.getPositionCount());
            LongBlock ab = page.getBlock(0);
            LongBlock id = page.getBlock(1);
            assertEquals(1L, ab.getLong(ab.getFirstValueIndex(0)));
            assertTrue("no a.b in row 1 -> null", ab.isNull(1));
            assertEquals(3L, ab.getLong(ab.getFirstValueIndex(2)));
            assertEquals(10L, id.getLong(id.getFirstValueIndex(0)));
            assertEquals(20L, id.getLong(id.getFirstValueIndex(1)));
            assertEquals(30L, id.getLong(id.getFirstValueIndex(2)));
        }
    }

    /**
     * An exact duplicate JSON key on a plain (non-dotted) column ({@code {"b":1,"b":2}}) merges the same way: NDJSON
     * parsing does not enable strict duplicate detection, so both keys are emitted, and both values land in one
     * aligned position.
     */
    public void testExactDuplicateKeyMergesIntoMultivalueAndAligns() throws IOException {
        String ndjson = "{\"b\":1,\"b\":2,\"id\":10}\n";

        try (Page page = decodePage(ndjson, List.of(attribute("b", DataType.LONG), attribute("id", DataType.LONG)))) {
            assertNotNull(page);
            assertEquals(1, page.getPositionCount());
            LongBlock b = page.getBlock(0);
            LongBlock id = page.getBlock(1);
            assertEquals(2, b.getValueCount(0));
            assertEquals(1L, b.getLong(b.getFirstValueIndex(0)));
            assertEquals(2L, b.getLong(b.getFirstValueIndex(0) + 1));
            assertEquals(10L, id.getLong(id.getFirstValueIndex(0)));
        }
    }

    /**
     * A scalar where the schema only knows dotted leaf columns for this field (e.g. {@code address.city}/
     * {@code address.zip}) is not a conflict: the schema knows no column named {@code address}, so the scalar names
     * nothing projected and null-fills the row's dotted columns exactly as an unknown field does. Not even STRICT fails.
     */
    public void testScalarWhereNestedObjectExpectedIsNotAConflict() throws IOException {
        String ndjson = "{\"address\": {\"city\": \"NYC\", \"zip\": \"10001\"}, \"id\": 1}\n"
            + "{\"address\": \"unstructured\", \"id\": 2}\n";

        try (
            Page page = decodePage(
                ndjson,
                List.of(
                    attribute("address.city", DataType.KEYWORD),
                    attribute("address.zip", DataType.KEYWORD),
                    attribute("id", DataType.INTEGER)
                ),
                ErrorPolicy.STRICT
            )
        ) {
            assertNotNull(page);
            assertEquals(2, page.getPositionCount());
            BytesRefBlock city = page.getBlock(0);
            BytesRefBlock zip = page.getBlock(1);
            IntBlock id = page.getBlock(2);
            BytesRef scratch = new BytesRef();
            assertEquals(new BytesRef("NYC"), BytesRef.deepCopyOf(city.getBytesRef(0, scratch)));
            assertTrue(city.isNull(1));
            assertTrue(zip.isNull(1));
            assertEquals(1, id.getInt(id.getFirstValueIndex(0)));
            assertEquals(2, id.getInt(id.getFirstValueIndex(1)));
        }
        assertTrue("no shape conflict, so no warning", drainWarnings().isEmpty());
    }

    /**
     * A {@code null} element inside a JSON array of objects (e.g. {@code "events": [{"type":"a"}, null]}) reaches a
     * structural decoder node with {@code inArray == true}. The null element must be ignored (nulls in arrays are not
     * supported) without throwing on the null {@code blockBuilder}, leaving the surrounding multi-value entry intact.
     * Companion to {@link #testNullWhereNestedObjectExpected} for the in-array path (#152574).
     */
    public void testNullElementInArrayOfObjects() throws IOException {
        String ndjson = "{\"events\": [{\"type\": \"click\"}, {\"type\": \"view\"}]}\n" + "{\"events\": [{\"type\": \"scroll\"}, null]}\n";

        try (Page page = decodePage(ndjson, List.of(attribute("events.type", DataType.KEYWORD)))) {
            assertNotNull(page);
            assertEquals(2, page.getPositionCount());
            BytesRefBlock type = page.getBlock(0);
            BytesRef scratch = new BytesRef();
            assertMvAt(type, 0, scratch, List.of("click", "view"));
            assertMvAt(type, 1, scratch, List.of("scroll"));
        }
    }

    /**
     * An array of objects with a leading (or all-)null element must still align with sibling columns. The MV shape is
     * decided from the first non-null element; a leading null previously left the child columns without an open
     * multi-value entry while later objects appended values, misaligning rows across columns (#152574). Covers
     * leading-null, mid-null, and all-null arrays against a scalar {@code id} column that pins the expected row count.
     */
    public void testArrayOfObjectsWithNullElements() throws IOException {
        String ndjson = "{\"events\": [{\"type\": \"a\"}, {\"type\": \"b\"}], \"id\": 1}\n"
            + "{\"events\": [null, {\"type\": \"c\"}, {\"type\": \"d\"}], \"id\": 2}\n"
            + "{\"events\": [{\"type\": \"e\"}, null, {\"type\": \"f\"}], \"id\": 3}\n"
            + "{\"events\": [null, null], \"id\": 4}\n";

        try (Page page = decodePage(ndjson, List.of(attribute("events.type", DataType.KEYWORD), attribute("id", DataType.INTEGER)))) {
            assertNotNull(page);
            assertEquals(4, page.getPositionCount());
            BytesRefBlock type = page.getBlock(0);
            IntBlock id = page.getBlock(1);
            assertEquals(type.getPositionCount(), id.getPositionCount());
            BytesRef scratch = new BytesRef();
            assertMvAt(type, 0, scratch, List.of("a", "b"));
            assertMvAt(type, 1, scratch, List.of("c", "d"));
            assertMvAt(type, 2, scratch, List.of("e", "f"));
            assertTrue("all-null array -> type null", type.isNull(3));
            for (int p = 0; p < 4; p++) {
                assertFalse("id must be present for row " + p, id.isNull(p));
                assertEquals(p + 1, id.getInt(id.getFirstValueIndex(p)));
            }
        }
    }

    /**
     * An array of objects on a structural node whose leading element(s) are stray scalars (e.g.
     * {@code ["x", {"type":"a"}, {"type":"b"}]}) must still align with sibling columns. A structural prefix carries
     * no scalar values of its own, so leading scalars are skipped when deciding the multi-value shape; otherwise
     * {@code includeChildren} stayed false and the later objects appended into never-opened child builders,
     * reproducing the same cross-column misalignment as the leading-null case (#152574). Covers leading-scalar,
     * mid-scalar, and all-scalar arrays against a scalar {@code id} column that pins the expected row count.
     */
    public void testArrayOfObjectsWithScalarElements() throws IOException {
        String ndjson = "{\"events\": [\"x\", {\"type\": \"a\"}, {\"type\": \"b\"}], \"id\": 1}\n"
            + "{\"events\": [{\"type\": \"c\"}, \"y\", {\"type\": \"d\"}], \"id\": 2}\n"
            + "{\"events\": [null, \"z\", {\"type\": \"e\"}], \"id\": 3}\n"
            + "{\"events\": [\"only-scalars\", \"more\"], \"id\": 4}\n";

        try (Page page = decodePage(ndjson, List.of(attribute("events.type", DataType.KEYWORD), attribute("id", DataType.INTEGER)))) {
            assertNotNull(page);
            assertEquals(4, page.getPositionCount());
            BytesRefBlock type = page.getBlock(0);
            IntBlock id = page.getBlock(1);
            assertEquals(type.getPositionCount(), id.getPositionCount());
            BytesRef scratch = new BytesRef();
            assertMvAt(type, 0, scratch, List.of("a", "b"));
            assertMvAt(type, 1, scratch, List.of("c", "d"));
            assertMvAt(type, 2, scratch, List.of("e"));
            assertTrue("all-scalar array -> type null", type.isNull(3));
            for (int p = 0; p < 4; p++) {
                assertFalse("id must be present for row " + p, id.isNull(p));
                assertEquals(p + 1, id.getInt(id.getFirstValueIndex(p)));
            }
        }
    }

    /**
     * Mirror of {@link #testArrayOfObjectsWithScalarElements}: an array of scalars on a leaf column whose
     * elements are occasionally objects (e.g. {@code ["a", {"x":1}, "b"]}). A stray object among array
     * scalars is a distinct, supported shape — not the record-level scalar/object conflict
     * the record-level shape-conflict path targets — so it must be silently omitted from the multi-value entry under
     * every {@link ErrorPolicy}, including {@code STRICT}; only a genuine top-level (non-array) conflict
     * a genuine top-level non-array schema conflict fails the query. Covers leading-object,
     * mid-object, and all-object arrays against a scalar {@code id} column that pins the expected row count.
     */
    public void testArrayOfScalarsWithObjectElements() throws IOException {
        String ndjson = "{\"tags\": [\"a\", {\"x\": 1}, \"b\"], \"id\": 1}\n"
            + "{\"tags\": [{\"x\": 1}, \"c\", \"d\"], \"id\": 2}\n"
            + "{\"tags\": [null, {\"x\": 1}, \"e\"], \"id\": 3}\n"
            + "{\"tags\": [{\"x\": 1}, {\"y\": 2}], \"id\": 4}\n";

        try (Page page = decodePage(ndjson, List.of(attribute("tags", DataType.KEYWORD), attribute("id", DataType.INTEGER)))) {
            assertNotNull(page);
            assertEquals(4, page.getPositionCount());
            BytesRefBlock tags = page.getBlock(0);
            IntBlock id = page.getBlock(1);
            assertEquals(tags.getPositionCount(), id.getPositionCount());
            BytesRef scratch = new BytesRef();
            assertMvAt(tags, 0, scratch, List.of("a", "b"));
            assertMvAt(tags, 1, scratch, List.of("c", "d"));
            assertMvAt(tags, 2, scratch, List.of("e"));
            assertTrue("all-object array -> tags null", tags.isNull(3));
            for (int p = 0; p < 4; p++) {
                assertFalse("id must be present for row " + p, id.isNull(p));
                assertEquals(p + 1, id.getInt(id.getFirstValueIndex(p)));
            }
        }
    }

    private Page decodePage(String ndjson, List<Attribute> attributes) throws IOException {
        return decodePage(ndjson, attributes, ErrorPolicy.STRICT);
    }

    private Page decodePage(String ndjson, List<Attribute> attributes, ErrorPolicy errorPolicy) throws IOException {
        try (
            NdJsonPageDecoder decoder = new NdJsonPageDecoder(
                new ByteArrayInputStream(ndjson.getBytes(StandardCharsets.UTF_8)),
                null, // DateFormatter
                attributes,
                null,
                1024,
                blockFactory,
                errorPolicy,
                "test://decode",
                new NdJsonReaderCounters()
            )
        ) {
            return decoder.decodePage();
        }
    }

    public void testDeclaredDateFormatZoneAware() throws Exception {
        // A per-column declared format parses this column with its own ES DateFormatter (zone-aware): the -0700 offset
        // is honored, landing 10/Oct/2000:13:55:36 -0700 at 2000-10-10T20:55:36Z (971211336000), not 13:55:36Z.
        String ndjson = "{\"ts\":\"10/Oct/2000:13:55:36 -0700\"}\n";
        try (
            NdJsonPageDecoder decoder = new NdJsonPageDecoder(
                new ByteArrayInputStream(ndjson.getBytes(StandardCharsets.UTF_8)),
                null, // file-level formatter unused; the column carries its own declared format
                List.of(attribute("ts", DataType.DATETIME)),
                null,
                10,
                blockFactory,
                ErrorPolicy.STRICT,
                "test://declared-date",
                new NdJsonReaderCounters(),
                Map.of("ts", "dd/MMM/yyyy:HH:mm:ss Z")
            )
        ) {
            try (Page page = decoder.decodePage()) {
                assertNotNull(page);
                assertEquals(1, page.getPositionCount());
                assertEquals(971211336000L, ((LongBlock) page.getBlock(0)).getLong(0));
            }
        }
    }

    public void testDeclaredEpochSecondFormatParsesNumericTokens() throws Exception {
        // A declared epoch_second format parses a JSON INT token as whole seconds and a JSON FLOAT token as fractional
        // seconds, overriding the numeric-epoch-millis shortcut — the parse-dialect / epoch-unit semantic.
        String ndjson = "{\"ts\":1704067200}\n{\"ts\":1704067200.5}\n";
        try (
            NdJsonPageDecoder decoder = new NdJsonPageDecoder(
                new ByteArrayInputStream(ndjson.getBytes(StandardCharsets.UTF_8)),
                null,
                List.of(attribute("ts", DataType.DATETIME)),
                null,
                10,
                blockFactory,
                ErrorPolicy.STRICT,
                "test://declared-epoch-second",
                new NdJsonReaderCounters(),
                Map.of("ts", "epoch_second")
            )
        ) {
            try (Page page = decoder.decodePage()) {
                assertNotNull(page);
                assertEquals(2, page.getPositionCount());
                LongBlock ts = (LongBlock) page.getBlock(0);
                assertEquals("epoch_second on an int token reads whole seconds", 1704067200000L, ts.getLong(0));
                assertEquals("epoch_second on a float token reads fractional seconds", 1704067200500L, ts.getLong(1));
            }
        }
    }

    public void testNoFormatFloatDatetimeRoundsToEpochMillis() throws Exception {
        // With no declared format a fractional JSON number in a datetime column is epoch millis and rounds to the
        // nearest milli — the ::datetime / safeDoubleToLong semantic, matching the columnar double->datetime coercion.
        try (Page page = decodePage("{\"ts\":1704067200000.6}\n", List.of(attribute("ts", DataType.DATETIME)))) {
            assertEquals(1704067200001L, ((LongBlock) page.getBlock(0)).getLong(0));
        }
    }

    // --- declared date_nanos reads ---

    /**
     * An ISO string in a declared date_nanos column parses through the file-level formatter rail
     * (strict_date_optional_time by default) into dateNanosToLong — and sub-millisecond digits SURVIVE:
     * strict_date_optional_time parses fractions to nanosecond resolution, so the default rail does not
     * truncate the very precision the type exists for.
     */
    public void testDeclaredDateNanosIsoStringKeepsNanoPrecision() throws IOException {
        String ndjson = "{\"v\":\"2024-01-15T12:34:56.123456789Z\"}\n";
        try (Page page = decodeOneColumn(ndjson, DataType.DATE_NANOS, ErrorPolicy.STRICT)) {
            LongBlock block = page.getBlock(0);
            assertEquals(EsqlDataTypeConverter.dateNanosToLong("2024-01-15T12:34:56.123456789Z"), block.getLong(0));
        }
    }

    /**
     * A numeric token in a declared date_nanos column with NO declared format is epoch NANOSECONDS — the
     * declared type names the numeric unit (datetime = millis, date_nanos = nanos) — matching the CSV numeric
     * rail and the columnar whole-number identity coercion. NOT the mapper-ingest millis reading.
     */
    public void testDeclaredDateNanosNumericTokenIsEpochNanos() throws IOException {
        long nanos = 1_700_000_000_123_456_789L;
        try (Page page = decodeOneColumn("{\"v\":" + nanos + "}\n", DataType.DATE_NANOS, ErrorPolicy.STRICT)) {
            LongBlock block = page.getBlock(0);
            assertEquals("identity epoch-nanos reinterpret, no scaling", nanos, block.getLong(0));
        }
    }

    /**
     * A declared `format` is authoritative and OVERRIDES the numeric-epoch shortcut, exactly as the datetime
     * arm does: a column declared {date_nanos, format:"yyyyMMdd"} reads the token 20260101 as 2026-01-01, NOT
     * as an epoch-nanos number. This is the unit rule — the format names the unit, else the type does.
     */
    public void testDeclaredDateNanosFormatOverridesNumericShortcut() throws IOException {
        String ndjson = "{\"ts\":20260101}\n";
        try (
            NdJsonPageDecoder decoder = new NdJsonPageDecoder(
                new ByteArrayInputStream(ndjson.getBytes(StandardCharsets.UTF_8)),
                null, // file-level formatter unused; the column carries its own declared format
                List.of(attribute("ts", DataType.DATE_NANOS)),
                null,
                10,
                blockFactory,
                ErrorPolicy.STRICT,
                "test://declared-date-nanos",
                new NdJsonReaderCounters(),
                Map.of("ts", "yyyyMMdd")
            )
        ) {
            try (Page page = decoder.decodePage()) {
                assertNotNull(page);
                assertEquals(EsqlDataTypeConverter.dateNanosToLong("2026-01-01T00:00:00Z"), ((LongBlock) page.getBlock(0)).getLong(0));
            }
        }
    }

    /**
     * A negative epoch has no date_nanos representation: never a negative nanos long — the cell fails through
     * the error policy (null_field nulls + warns; fail_fast fails the read).
     */
    public void testDeclaredDateNanosNegativeEpochIsPerCellFailure() throws IOException {
        try (Page page = decodeOneColumn("{\"v\":-1}\n{\"v\":5}\n", DataType.DATE_NANOS, ErrorPolicy.PERMISSIVE)) {
            LongBlock block = page.getBlock(0);
            assertTrue("negative epoch nulls the cell", block.isNull(0));
            assertEquals("the good cell still decodes", 5L, block.getLong(block.getFirstValueIndex(1)));
        }
        drainWarnings();
        expectThrows(ParsingException.class, () -> decodeOneColumn("{\"v\":-1}\n", DataType.DATE_NANOS, ErrorPolicy.STRICT));
    }

    /**
     * With NO declared format, a boolean or a fractional number in a date_nanos column is an unsupported cross-kind
     * drift. The fractional case differs from the datetime arm on purpose: a fraction of a nanosecond has no meaning
     * (nanos is this type's finest unit), whereas a fractional epoch-milli rounds. With a declared format a fractional
     * token IS meaningful and parses — pinned by {@link #testDeclaredDateNanosFractionalTokenParsesThroughFormat}.
     */
    public void testDeclaredDateNanosCrossKindDrift() throws IOException {
        try (Page page = decodeOneColumn("{\"v\":true}\n{\"v\":1.5}\n{\"v\":7}\n", DataType.DATE_NANOS, ErrorPolicy.PERMISSIVE)) {
            LongBlock block = page.getBlock(0);
            assertTrue("boolean in a date_nanos column nulls the cell", block.isNull(0));
            assertTrue("fractional number with no format nulls the cell", block.isNull(1));
            assertEquals(7L, block.getLong(block.getFirstValueIndex(2)));
        }
        drainWarnings();
    }

    /**
     * A fractional token under a declared format parses through it: {@code epoch_second} on {@code 1704067200.5} is
     * sub-second precision that date_nanos can represent exactly. The unit rule again — the format names the unit, so
     * the token is a fractional SECOND, not a fractional nanosecond.
     */
    public void testDeclaredDateNanosFractionalTokenParsesThroughFormat() throws IOException {
        try (
            NdJsonPageDecoder decoder = new NdJsonPageDecoder(
                new ByteArrayInputStream("{\"ts\":1704067200.5}\n".getBytes(StandardCharsets.UTF_8)),
                null,
                List.of(attribute("ts", DataType.DATE_NANOS)),
                null,
                10,
                blockFactory,
                ErrorPolicy.STRICT,
                "test://declared-date-nanos-fraction",
                new NdJsonReaderCounters(),
                Map.of("ts", "epoch_second")
            )
        ) {
            try (Page page = decoder.decodePage()) {
                assertNotNull(page);
                assertEquals(EsqlDataTypeConverter.dateNanosToLong("2024-01-01T00:00:00.5Z"), ((LongBlock) page.getBlock(0)).getLong(0));
            }
        }
    }

    /**
     * Reads the response-header warnings emitted on the test thread and clears them so the parent
     * {@code ensureNoWarnings} post-check passes. Returns the unwrapped warning messages.
     */
    private List<String> drainWarnings() {
        List<String> raw = threadContext.getResponseHeaders().getOrDefault("Warning", List.of());
        List<String> messages = raw.stream().map(s -> HeaderWarning.extractWarningValueFromWarningHeader(s, false)).toList();
        threadContext.stashContext();
        return messages;
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

    // --- declared unsigned_long reads ---

    private static long encoded(String magnitude) {
        return NumericUtils.asLongUnsigned(new BigInteger(magnitude));
    }

    private Page decodeOneColumn(String ndjson, DataType type, ErrorPolicy policy) throws IOException {
        try (
            NdJsonPageDecoder decoder = new NdJsonPageDecoder(
                new ByteArrayInputStream(ndjson.getBytes(StandardCharsets.UTF_8)),
                null,
                List.of(attribute("v", type)),
                null,
                10,
                blockFactory,
                policy,
                "test://ul",
                new NdJsonReaderCounters()
            )
        ) {
            return decoder.decodePage();
        }
    }

    /**
     * Before this change setupBuilders threw for a declared unsigned_long at block-builder construction — up
     * front, per page — so the read failed regardless of error_mode. The full [0, 2^64-1] domain must now decode,
     * from JSON integer tokens that overflow a signed long and from string tokens alike.
     */
    public void testDeclaredUnsignedLongDecodesFullDomain() throws IOException {
        String ndjson =
            "{\"v\":0}\n{\"v\":12345}\n{\"v\":9223372036854775808}\n{\"v\":18446744073709551615}\n{\"v\":\"18446744073709551614\"}\n";
        try (Page page = decodeOneColumn(ndjson, DataType.UNSIGNED_LONG, ErrorPolicy.STRICT)) {
            assertNotNull(page);
            LongBlock block = page.getBlock(0);
            assertEquals(5, block.getPositionCount());
            assertEquals(encoded("0"), block.getLong(0));
            assertEquals(encoded("12345"), block.getLong(1));
            assertEquals(encoded("9223372036854775808"), block.getLong(2));   // 2^63 — getLongValue would overflow
            assertEquals(encoded("18446744073709551615"), block.getLong(3));  // 2^64-1
            assertEquals(encoded("18446744073709551614"), block.getLong(4));  // string token
        }
    }

    /** Fractional and scientific tokens truncate toward zero, matching ::unsigned_long and the CSV reader. */
    public void testDeclaredUnsignedLongTruncatesTowardZero() throws IOException {
        try (Page page = decodeOneColumn("{\"v\":42.9}\n{\"v\":\"1e3\"}\n", DataType.UNSIGNED_LONG, ErrorPolicy.STRICT)) {
            LongBlock block = page.getBlock(0);
            assertEquals(encoded("42"), block.getLong(0));
            assertEquals(encoded("1000"), block.getLong(1));
        }
    }

    /** A missing field nulls the cell, exactly as for a declared long. */
    public void testDeclaredUnsignedLongNullsMissingField() throws IOException {
        try (Page page = decodeOneColumn("{\"other\":1}\n{\"v\":7}\n", DataType.UNSIGNED_LONG, ErrorPolicy.STRICT)) {
            LongBlock block = page.getBlock(0);
            assertTrue("absent field must null the cell", block.isNull(0));
            assertEquals(encoded("7"), block.getLong(1));
        }
    }

    /**
     * A bad VALUE is a per-cell data failure the error policy governs — never the blanket
     * unsupportedTypeForNdjson throw that used to fire before any cell was even looked at.
     */
    public void testDeclaredUnsignedLongBadValueIsPerCellUnderNullField() throws IOException {
        String ndjson = "{\"v\":-1}\n{\"v\":18446744073709551616}\n{\"v\":\"abc\"}\n{\"v\":5}\n";
        try (Page page = decodeOneColumn(ndjson, DataType.UNSIGNED_LONG, ErrorPolicy.PERMISSIVE)) {
            LongBlock block = page.getBlock(0);
            assertEquals(4, block.getPositionCount());
            assertTrue("negative nulls the cell", block.isNull(0));
            assertTrue("2^64 nulls the cell", block.isNull(1));
            assertTrue("garbage nulls the cell", block.isNull(2));
            assertEquals("the good cell still decodes", encoded("5"), block.getLong(3));
        }
    }

    /**
     * "1e999999999" makes BigDecimal.toBigInteger() throw ArithmeticException -- not an IllegalArgumentException, so
     * an unhandled one escapes the per-cell catch and hard-fails the read on every error_mode. It must be an
     * ordinary out-of-range cell instead.
     */
    public void testDeclaredUnsignedLongExoticExponentIsAPerCellFailure() throws IOException {
        String ndjson = "{\"v\":\"1e999999999\"}\n{\"v\":1e999999999}\n{\"v\":5}\n";
        try (Page page = decodeOneColumn(ndjson, DataType.UNSIGNED_LONG, ErrorPolicy.PERMISSIVE)) {
            LongBlock block = page.getBlock(0);
            assertTrue("string exotic exponent nulls the cell", block.isNull(0));
            assertTrue("numeric exotic exponent nulls the cell", block.isNull(1));
            assertEquals("the good cell still decodes", encoded("5"), block.getLong(2));
        }
    }

    /** Multivalue unsigned_long arrays decode element-by-element through the same coercer. */
    public void testDeclaredUnsignedLongMultivalue() throws IOException {
        try (Page page = decodeOneColumn("{\"v\":[1,18446744073709551615]}\n", DataType.UNSIGNED_LONG, ErrorPolicy.STRICT)) {
            LongBlock block = page.getBlock(0);
            assertEquals(2, block.getValueCount(0));
            int first = block.getFirstValueIndex(0);
            assertEquals(encoded("1"), block.getLong(first));
            assertEquals(encoded("18446744073709551615"), block.getLong(first + 1));
        }
    }

    /**
     * A coercion failure on any element of a declared-type array must null the whole position, not
     * silently drop the bad element and keep the good ones as a partial multivalue. Matches the
     * columnar reader contract (see {@code DeclaredTypeCoercionsTests.testMultiValuePositionNullsWholePositionOnFailure}).
     * <p>
     * Input: three rows — a clean multivalue, a poisoned multivalue (one bad element), and a second
     * clean multivalue. Under {@code null_field} the poisoned position is null; both clean positions
     * carry all their elements; one warning is emitted.
     */
    public void testArrayCoercionFailureNullsWholePositionUnderNullField() throws IOException {
        String ndjson = "{\"v\":[10,20]}\n{\"v\":[10,\"notanumber\",30]}\n{\"v\":[40,50]}\n";
        List<String> warnings = new ArrayList<>();
        try (
            NdJsonPageDecoder decoder = new NdJsonPageDecoder(
                new ByteArrayInputStream(ndjson.getBytes(StandardCharsets.UTF_8)),
                null,
                List.of(attribute("v", DataType.LONG)),
                null,
                10,
                blockFactory,
                ErrorPolicy.PERMISSIVE,
                "test://array-poison",
                new NdJsonReaderCounters(),
                warnings::add
            );
            Page page = decoder.decodePage()
        ) {
            assertNotNull(page);
            LongBlock block = page.getBlock(0);
            assertEquals(3, block.getPositionCount());

            // first position: [10, 20]
            assertFalse("first row is not null", block.isNull(0));
            assertEquals(2, block.getValueCount(0));
            int i0 = block.getFirstValueIndex(0);
            assertEquals(10L, block.getLong(i0));
            assertEquals(20L, block.getLong(i0 + 1));

            // second position: poisoned by "notanumber" → whole position is null
            assertTrue("poisoned array position is null", block.isNull(1));

            // third position: [40, 50]
            assertFalse("third row is not null", block.isNull(2));
            assertEquals(2, block.getValueCount(2));
            int i2 = block.getFirstValueIndex(2);
            assertEquals(40L, block.getLong(i2));
            assertEquals(50L, block.getLong(i2 + 1));
        }
        // SkipWarnings.add() emits a one-time summary header on the first call, then the detail — 2 messages total.
        assertEquals("one summary + one detail warning for the poisoned element", 2, warnings.size());
        assertThat(warnings.get(1), Matchers.containsString("notanumber"));
    }

    /**
     * Under {@code skip_row}, a coercion failure inside an array drops the entire record — matching
     * the scalar coercion skip_row contract, and NOT just the poisoned position.
     */
    public void testArrayCoercionFailureSkipsRowUnderSkipRow() throws IOException {
        String ndjson = "{\"v\":[10,20]}\n{\"v\":[10,\"notanumber\",30]}\n{\"v\":[40,50]}\n";
        List<String> warnings = new ArrayList<>();
        try (
            NdJsonPageDecoder decoder = new NdJsonPageDecoder(
                new ByteArrayInputStream(ndjson.getBytes(StandardCharsets.UTF_8)),
                null,
                List.of(attribute("v", DataType.LONG)),
                null,
                10,
                blockFactory,
                ErrorPolicy.LENIENT,
                "test://array-skip",
                new NdJsonReaderCounters(),
                warnings::add
            );
            Page page = decoder.decodePage()
        ) {
            assertNotNull(page);
            LongBlock block = page.getBlock(0);
            assertEquals("poisoned row is dropped, two remain", 2, block.getPositionCount());

            // first surviving row: [10, 20]
            assertFalse(block.isNull(0));
            assertEquals(2, block.getValueCount(0));
            int i0 = block.getFirstValueIndex(0);
            assertEquals(10L, block.getLong(i0));
            assertEquals(20L, block.getLong(i0 + 1));

            // second surviving row: [40, 50]
            assertFalse(block.isNull(1));
            assertEquals(2, block.getValueCount(1));
            int i1 = block.getFirstValueIndex(1);
            assertEquals(40L, block.getLong(i1));
            assertEquals(50L, block.getLong(i1 + 1));
        }
        // SkipWarnings.add() emits a one-time summary header on the first call, then the detail — 2 messages total.
        assertEquals("one summary + one detail warning for the dropped row", 2, warnings.size());
        assertThat(warnings.get(1), Matchers.containsString("notanumber"));
    }

    /**
     * Under {@code skip_row}, a scalar coercion failure (a non-numeric string where a LONG is expected) drops the
     * entire record — the same contract the array coercion skip_row test asserts for multi-value positions.
     * Input: two records; the second has an uncoercible value in column {@code n}. Only the first record survives.
     */
    public void testScalarCoercionFailureDropsRowUnderSkipRow() throws IOException {
        String ndjson = "{\"n\": 42, \"k\": \"good\"}\n{\"n\": \"not_a_number\", \"k\": \"bad\"}\n";
        List<String> warnings = new ArrayList<>();
        try (
            NdJsonPageDecoder decoder = new NdJsonPageDecoder(
                new ByteArrayInputStream(ndjson.getBytes(StandardCharsets.UTF_8)),
                null,
                List.of(attribute("n", DataType.LONG), attribute("k", DataType.KEYWORD)),
                null,
                10,
                blockFactory,
                ErrorPolicy.LENIENT,
                "test://scalar-skip",
                new NdJsonReaderCounters(),
                warnings::add
            );
            Page page = decoder.decodePage()
        ) {
            assertNotNull(page);
            LongBlock nBlock = page.getBlock(0);
            BytesRefBlock kBlock = page.getBlock(1);
            assertEquals("poisoned row is dropped, one remains", 1, page.getPositionCount());
            assertFalse(nBlock.isNull(0));
            assertEquals(42L, nBlock.getLong(nBlock.getFirstValueIndex(0)));
            BytesRef scratch = new BytesRef();
            assertEquals(new BytesRef("good"), BytesRef.deepCopyOf(kBlock.getBytesRef(0, scratch)));
        }
        // SkipWarnings.add() emits a one-time summary header on the first call, then the detail — 2 messages total.
        assertEquals("one summary + one detail warning for the dropped row", 2, warnings.size());
        assertThat(warnings.get(1), Matchers.containsString("not_a_number"));
    }

    /**
     * Under {@code null_field}, a scalar coercion failure nulls only the cell where coercion failed; the rest of the
     * record survives. Companion to {@link #testScalarCoercionFailureDropsRowUnderSkipRow} for the permissive path.
     * Input: two records; the second has an uncoercible value in column {@code n}. Both rows survive; the bad cell is null.
     */
    public void testScalarCoercionFailureNullsFieldUnderNullField() throws IOException {
        String ndjson = "{\"n\": 42, \"k\": \"good\"}\n{\"n\": \"not_a_number\", \"k\": \"bad\"}\n";
        List<String> warnings = new ArrayList<>();
        try (
            NdJsonPageDecoder decoder = new NdJsonPageDecoder(
                new ByteArrayInputStream(ndjson.getBytes(StandardCharsets.UTF_8)),
                null,
                List.of(attribute("n", DataType.LONG), attribute("k", DataType.KEYWORD)),
                null,
                10,
                blockFactory,
                ErrorPolicy.PERMISSIVE,
                "test://scalar-null-field",
                new NdJsonReaderCounters(),
                warnings::add
            );
            Page page = decoder.decodePage()
        ) {
            assertNotNull(page);
            LongBlock nBlock = page.getBlock(0);
            BytesRefBlock kBlock = page.getBlock(1);
            assertEquals("both rows survive under null_field", 2, page.getPositionCount());
            assertFalse("row 0 n is not null", nBlock.isNull(0));
            assertEquals(42L, nBlock.getLong(nBlock.getFirstValueIndex(0)));
            assertTrue("row 1 n is null (coercion failed)", nBlock.isNull(1));
            BytesRef scratch = new BytesRef();
            assertEquals(new BytesRef("good"), BytesRef.deepCopyOf(kBlock.getBytesRef(0, scratch)));
            assertEquals(new BytesRef("bad"), BytesRef.deepCopyOf(kBlock.getBytesRef(1, scratch)));
        }
        // SkipWarnings.add() emits a one-time summary header on the first call, then the detail — 2 messages total.
        assertEquals("one summary + one detail warning for the nulled cell", 2, warnings.size());
        assertThat(warnings.get(1), Matchers.containsString("not_a_number"));
    }

    // ---------------------------------------------------------------------------------------------
    // StreamReadConstraints violations. Jackson enforces its read limits in the TOKEN SCANNER, so
    // these never reach a decode arm or the per-cell coercionFailure sink — they belong to the
    // whole-line class routed through onNdjsonLineParseError. See that method's javadoc for why
    // null_field drops the line here instead of nulling a cell.
    // ---------------------------------------------------------------------------------------------

    /** A JSON number token longer than {@code StreamReadConstraints.getMaxNumberLength()} (1000). */
    private static String oversizedNumberRecord() {
        return "{\"v\":" + "1".repeat(1200) + "}";
    }

    /** A field name longer than {@code getMaxNameLength()} (50000), on a field that is NOT projected. */
    private static String oversizedFieldNameRecord() {
        return "{\"" + "n".repeat(60_000) + "\":1,\"v\":5}";
    }

    /** Array nesting deeper than {@code getMaxNestingDepth()} (1000). */
    private static String excessiveNestingRecord() {
        int depth = 1100;
        return "{\"v\":" + "[".repeat(depth) + "1" + "]".repeat(depth) + "}";
    }

    /**
     * fail_fast: an oversized number token aborts the read through the whole-line contract every other
     * whole-line failure uses, carrying Jackson's own limit text. Without the constraint arm the raw
     * {@code StreamConstraintsException} escapes {@code decodePage} and is typed by
     * {@code ExternalFailures.surface}, leaving {@code error_mode} no say on any mode. The
     * {@code Over-limit} label distinguishes a record that is well-formed but past a parser limit from
     * one that is genuinely malformed.
     */
    public void testOversizedNumberTokenFailsFastUnderStrict() {
        String ndjson = "{\"v\":1}\n" + oversizedNumberRecord() + "\n{\"v\":3}\n";
        ParsingException e = expectThrows(ParsingException.class, () -> decodeOneColumn(ndjson, DataType.LONG, ErrorPolicy.STRICT));
        assertThat(e.getMessage(), Matchers.containsString("Over-limit NDJSON"));
        assertThat(e.getMessage(), Matchers.containsString("Number value length"));
    }

    /** null_field: the offending line is dropped (not null-filled) and both good lines survive. */
    public void testOversizedNumberTokenDropsLineUnderNullField() throws IOException {
        assertConstraintViolationDropsLine(oversizedNumberRecord(), ErrorPolicy.PERMISSIVE, "Number value length");
    }

    /** skip_row: identical outcome to null_field — the whole-line class cannot null a cell. */
    public void testOversizedNumberTokenDropsLineUnderSkipRow() throws IOException {
        assertConstraintViolationDropsLine(oversizedNumberRecord(), ErrorPolicy.LENIENT, "Number value length");
    }

    /**
     * The name-length limit trips on a field the query never projected, so there is no cell to null even
     * in principle — the case that rules a per-cell treatment out for this class.
     */
    public void testOversizedFieldNameDropsLineUnderNullField() throws IOException {
        assertConstraintViolationDropsLine(oversizedFieldNameRecord(), ErrorPolicy.PERMISSIVE, "Name length");
    }

    /** The depth limit trips on structure rather than on any value; same whole-line outcome. */
    public void testExcessiveNestingDropsLineUnderSkipRow() throws IOException {
        assertConstraintViolationDropsLine(excessiveNestingRecord(), ErrorPolicy.LENIENT, "Document nesting depth");
    }

    /** fail_fast on a non-number limit, pinning that the routing is class-level rather than number-specific. */
    public void testExcessiveNestingFailsFastUnderStrict() {
        String ndjson = "{\"v\":1}\n" + excessiveNestingRecord() + "\n";
        ParsingException e = expectThrows(ParsingException.class, () -> decodeOneColumn(ndjson, DataType.LONG, ErrorPolicy.STRICT));
        assertThat(e.getMessage(), Matchers.containsString("Over-limit NDJSON"));
        assertThat(e.getMessage(), Matchers.containsString("Document nesting depth"));
    }

    /**
     * The scanner reaches the token before projection is consulted, so an over-long number in a column the
     * query never asked for still takes the line. Worth pinning because the opposite is the intuitive guess:
     * a user who does not project {@code other} would expect its contents not to matter at all.
     */
    public void testOversizedNumberInAnUnprojectedFieldDropsLine() throws IOException {
        assertConstraintViolationDropsLine("{\"v\":5,\"other\":" + "1".repeat(1200) + "}", ErrorPolicy.LENIENT, "Number value length");
    }

    /** Same, for a token nested inside an array rather than sitting directly under a field. */
    public void testOversizedNumberInsideAnArrayDropsLine() throws IOException {
        assertConstraintViolationDropsLine("{\"v\":[9," + "1".repeat(1200) + "]}", ErrorPolicy.LENIENT, "Number value length");
    }

    /**
     * A record can fail twice — a per-cell coercion failure, then a constraint violation raised while the rest
     * of the record is drained — and must still cost the budget once. {@code max_errors} and
     * {@code max_error_ratio} are documented in records ("maximum malformed rows"), and
     * {@code coercionFailure} already enforces charge-once among per-cell failures; the whole-line sink has to
     * honour the same invariant or a single bad line can exhaust a budget of two.
     * <p>
     * Both warnings are still emitted. Under {@code null_field} the coercion warning says the cell was nulled
     * and the record kept, which the constraint violation then overrides by dropping the record whole — so
     * suppressing the second would leave the client with a warning that no longer describes the outcome.
     */
    public void testARecordFailingBothPerCellAndWholeLineIsChargedOnce() throws IOException {
        for (ErrorPolicy policy : List.of(ErrorPolicy.LENIENT, ErrorPolicy.PERMISSIVE)) {
            String ndjson = "{\"v\":1}\n{\"v\":\"notanumber\",\"other\":" + "1".repeat(1200) + "}\n{\"v\":3}\n";
            List<String> warnings = new ArrayList<>();
            NdJsonReaderCounters counters = new NdJsonReaderCounters();
            try (
                NdJsonPageDecoder decoder = new NdJsonPageDecoder(
                    new ByteArrayInputStream(ndjson.getBytes(StandardCharsets.UTF_8)),
                    null,
                    List.of(attribute("v", DataType.LONG)),
                    null,
                    10,
                    blockFactory,
                    policy,
                    "test://double-charge",
                    counters,
                    warnings::add
                );
                Page page = decoder.decodePage()
            ) {
                LongBlock block = page.getBlock(0);
                assertEquals(policy.modeName() + ": the doubly-bad line is dropped", 2, block.getPositionCount());
                assertEquals(1L, block.getLong(0));
                assertEquals(3L, block.getLong(1));
            }
            assertEquals(policy.modeName() + ": one line, one charge", 1L, counters.snapshot().parseErrors());
        }
    }

    /**
     * The charge-once flag is per record, not sticky across records. The second line here is a BARE token, so
     * its violation is raised by the record-opening {@code nextToken} — which runs before the rest of the
     * per-record state is cleared. Resetting the flag with that other state instead of ahead of the token read
     * would let the first line's charge suppress the second's, and a file of consecutive bad lines would cost
     * the budget once in total. Asserts only the charge count: what the parser does with the line after a bare
     * record is a separate, pre-existing question ({@link #testConstraintViolationOnRecordOpeningTokenMatchesBareScalarRecovery}).
     */
    public void testConsecutiveFailingLinesAreEachCharged() throws IOException {
        String ndjson = "{\"v\":\"notanumber\"}\n" + "1".repeat(1200) + "\n";
        List<String> warnings = new ArrayList<>();
        NdJsonReaderCounters counters = new NdJsonReaderCounters();
        try (
            NdJsonPageDecoder decoder = new NdJsonPageDecoder(
                new ByteArrayInputStream(ndjson.getBytes(StandardCharsets.UTF_8)),
                null,
                List.of(attribute("v", DataType.LONG)),
                null,
                10,
                blockFactory,
                ErrorPolicy.LENIENT,
                "test://consecutive",
                counters,
                warnings::add
            );
            Page page = decoder.decodePage()
        ) {
            assertTrue("both lines are bad, so nothing is committed", page == null || page.getPositionCount() == 0);
        }
        assertEquals("two distinct bad lines, two charges", 2L, counters.snapshot().parseErrors());
    }

    /**
     * Demonstrates the type-independence the rest of this block argues structurally: the violation is raised
     * while the token is scanned, before any {@code DataType} dispatch, so a keyword column loses the same
     * line a numeric column does. If the routing ever regressed to a per-arm fix in the numeric decoders,
     * this is the cell that would catch it.
     */
    public void testOversizedNumberDropsLineForAKeywordColumn() throws IOException {
        String ndjson = "{\"v\":\"a\"}\n" + oversizedNumberRecord() + "\n{\"v\":\"b\"}\n";
        try (Page page = decodeOneColumn(ndjson, DataType.KEYWORD, ErrorPolicy.LENIENT)) {
            BytesRefBlock block = page.getBlock(0);
            assertEquals("the offending line is dropped, not null-filled", 2, block.getPositionCount());
            BytesRef scratch = new BytesRef();
            assertEquals(new BytesRef("a"), BytesRef.deepCopyOf(block.getBytesRef(0, scratch)));
            assertEquals(new BytesRef("b"), BytesRef.deepCopyOf(block.getBytesRef(1, scratch)));
        }
    }

    /**
     * Recovery on the byte-array path re-anchors by scanning for the next line terminator from the failed
     * parser's byte offset. That offset lands inside the bad line for a constraint violation, so the record
     * that follows it must still decode — a resync that skipped it would lose good data silently.
     */
    public void testConstraintViolationRecoversOnByteArrayPath() throws IOException {
        String ndjson = "{\"v\":1}\n" + oversizedNumberRecord() + "\n{\"v\":3}\n";
        byte[] bytes = ndjson.getBytes(StandardCharsets.UTF_8);
        List<String> warnings = new ArrayList<>();
        try (
            NdJsonPageDecoder decoder = new NdJsonPageDecoder(
                bytes,
                0,
                bytes.length,
                null,
                List.of(attribute("v", DataType.LONG)),
                null,
                10,
                blockFactory,
                ErrorPolicy.PERMISSIVE,
                "test://constraint-bytes",
                new NdJsonReaderCounters(),
                warnings::add
            );
            Page page = decoder.decodePage()
        ) {
            assertNotNull(page);
            LongBlock block = page.getBlock(0);
            assertEquals("the bad line is dropped, both good lines survive", 2, block.getPositionCount());
            assertEquals(1L, block.getLong(0));
            assertEquals(3L, block.getLong(1));
        }
        assertThat(warnings.get(1), Matchers.containsString("Number value length"));
    }

    /**
     * The streaming path recovers through {@code NdJsonUtils.moveToNextLine} rather than by re-anchoring in a
     * byte array. Two good lines follow the bad one so the assertion fails if recovery lands anywhere but the
     * very next record.
     */
    public void testConstraintViolationRecoversOnStreamingPath() throws IOException {
        String ndjson = "{\"v\":1}\n" + oversizedNumberRecord() + "\n{\"v\":3}\n{\"v\":4}\n";
        try (Page page = decodeOneColumn(ndjson, DataType.LONG, ErrorPolicy.PERMISSIVE)) {
            assertNotNull(page);
            LongBlock block = page.getBlock(0);
            assertEquals("the bad line is dropped, all three good lines survive", 3, block.getPositionCount());
            assertEquals(1L, block.getLong(0));
            assertEquals(3L, block.getLong(1));
            assertEquals(4L, block.getLong(2));
        }
    }

    /**
     * A constraint violation is an ordinary member of the non-strict error budget, not a free pass: with
     * {@code max_errors: 0} the first one trips the budget and fails the read.
     */
    public void testConstraintViolationCountsAgainstErrorBudget() {
        String ndjson = "{\"v\":1}\n" + oversizedNumberRecord() + "\n{\"v\":3}\n";
        ErrorPolicy noBudget = new ErrorPolicy(ErrorPolicy.Mode.SKIP_ROW, 0, 0.0, false);
        ParsingException e = expectThrows(ParsingException.class, () -> decodeOneColumn(ndjson, DataType.LONG, noBudget));
        assertThat(e.getMessage(), Matchers.containsString("NDJSON error budget exceeded"));
    }

    /**
     * Bad data is the client's, not ours, so every strict NDJSON read failure answers 400. This asserts the
     * status directly rather than the exception type, because the status is the contract users actually see and
     * the type is only how we carry it. Carrying any of these in the {@code QlServerException} family instead —
     * which has no {@code status()} override and so answers 500 — would page someone over a malformed input file.
     * The two genuine invariant failures in this class (missing lenient scratch builders) deliberately stay
     * server-class and are not listed here.
     */
    public void testEveryStrictReadFailureIsAClientError() {
        String oversized = "{\"v\":" + "1".repeat(1200) + "}\n";
        String badValue = "{\"v\":\"notanumber\"}\n";
        String shapeConflict = "{\"v\":1}\n{\"v\":{\"nested\":2}}\n";

        assertEquals(
            "whole-line parse failure",
            RestStatus.BAD_REQUEST,
            expectThrows(ParsingException.class, () -> decodeOneColumn(oversized, DataType.LONG, ErrorPolicy.STRICT)).status()
        );
        assertEquals(
            "per-cell coercion failure",
            RestStatus.BAD_REQUEST,
            expectThrows(ParsingException.class, () -> decodeOneColumn(badValue, DataType.LONG, ErrorPolicy.STRICT)).status()
        );
        assertEquals(
            "error budget exhausted",
            RestStatus.BAD_REQUEST,
            expectThrows(
                ParsingException.class,
                () -> decodeOneColumn(oversized, DataType.LONG, new ErrorPolicy(ErrorPolicy.Mode.SKIP_ROW, 0, 0.0, false))
            ).status()
        );
    }

    /**
     * The fail-fast loop guards two call sites: the {@code nextToken} that opens a record, and {@code
     * decodeObject}. Every other strict test here lands on the second. A bare oversized token on its own line —
     * no enclosing object — is scanned by the record-opening {@code nextToken}, so this is the only test that
     * exercises the first. The {@code [nextToken]} phase label in the message is what proves which site ran; a
     * violation routed through {@code decodeObject} would read {@code [decodeObject]} instead.
     */
    public void testConstraintViolationOnRecordOpeningTokenFailsFastUnderStrict() {
        String ndjson = "{\"v\":1}\n" + "1".repeat(1200) + "\n";
        ParsingException e = expectThrows(ParsingException.class, () -> decodeOneColumn(ndjson, DataType.LONG, ErrorPolicy.STRICT));
        assertThat(e.getMessage(), Matchers.containsString("Over-limit NDJSON at logical row [2] (nextToken)"));
        assertThat(e.getMessage(), Matchers.containsString("Number value length"));
        assertEquals(RestStatus.BAD_REQUEST, e.status());
    }

    /**
     * The limit is a cliff, and this pins which side of it does what. A number that overflows {@code long} but
     * stays under {@code getMaxNumberLength()} is scanned successfully and fails per CELL — the row survives with
     * that one column null. One digit past the limit the scanner never yields the token at all, so the whole line
     * goes. Same column, same {@code error_mode}, two different outcomes; a reader hitting the second case should
     * not be surprised into thinking the first case was in play.
     */
    public void testNumberLengthLimitIsTheBoundaryBetweenPerCellAndPerLine() throws IOException {
        String underLimit = "9".repeat(999);
        String overLimit = "9".repeat(1001);

        try (Page page = decodeOneColumn("{\"v\":" + underLimit + "}\n{\"v\":7}\n", DataType.LONG, ErrorPolicy.PERMISSIVE)) {
            LongBlock block = page.getBlock(0);
            assertEquals("under the limit the row survives with a null cell", 2, block.getPositionCount());
            assertTrue("the over-long-but-scannable number nulls its cell", block.isNull(0));
            assertEquals(7L, block.getLong(1));
        }

        try (Page page = decodeOneColumn("{\"v\":" + overLimit + "}\n{\"v\":7}\n", DataType.LONG, ErrorPolicy.PERMISSIVE)) {
            LongBlock block = page.getBlock(0);
            assertEquals("past the limit the whole line goes", 1, block.getPositionCount());
            assertEquals(7L, block.getLong(0));
        }
    }

    /**
     * The decode loops catch the constraint violation at two sites: around {@code decodeObject}, and around the
     * {@code nextToken} that opens a record. A bare oversized scalar on its own line — no enclosing object — trips
     * the second one, which every other test here leaves unexercised.
     * <p>
     * Asserted differentially against a bare scalar that is perfectly VALID, because recovery from a bare
     * (non-object) top-level record overshoots and swallows the line after it. That overshoot is pre-existing and
     * has nothing to do with constraints — the valid scalar loses its successor identically — so this pins the two
     * as equivalent rather than blessing the overshoot as correct (tracked as elastic/esql-planning#1731). A fix
     * to bare-record recovery keeps this test passing so long as it corrects both arms; it fails only if one arm
     * is fixed and the other is left behind, which is what the pairing is for.
     */
    public void testConstraintViolationOnRecordOpeningTokenMatchesBareScalarRecovery() throws IOException {
        String oversized = "{\"v\":1}\n" + "1".repeat(1200) + "\n{\"v\":3}\n";
        String validBareScalar = "{\"v\":1}\n12345\n{\"v\":3}\n";

        try (
            Page constraintPage = decodeOneColumn(oversized, DataType.LONG, ErrorPolicy.PERMISSIVE);
            Page baselinePage = decodeOneColumn(validBareScalar, DataType.LONG, ErrorPolicy.PERMISSIVE)
        ) {
            LongBlock constraintBlock = constraintPage.getBlock(0);
            LongBlock baselineBlock = baselinePage.getBlock(0);
            assertEquals(
                "an oversized bare token must recover exactly as a valid bare scalar does",
                baselineBlock.getPositionCount(),
                constraintBlock.getPositionCount()
            );
            assertEquals(1L, constraintBlock.getLong(0));
        }
    }

    /**
     * Shared body for the non-strict cases: one good line, the offending line, one good line. The offending
     * line is dropped, both good lines decode, and the client sees SkipWarnings' summary plus a detail
     * carrying Jackson's own limit text (the same passthrough {@code CsvFormatReader} does for its own
     * constraint violation).
     */
    /**
     * {@code expectedDetail} names the limit but deliberately omits the numbers Jackson interpolates into its
     * message. Jackson formats them with the default locale, so under a locale with non-Western digits (the
     * randomized runner picks one often enough — {@code -Dtests.locale=fa-IR} reproduces it) "1200" arrives as
     * "\u06F1\u06F2\u06F0\u06F0" and a digit-bearing assertion fails for no real reason. The limit name alone still proves the
     * passthrough this is checking.
     */
    private void assertConstraintViolationDropsLine(String badRecord, ErrorPolicy policy, String expectedDetail) throws IOException {
        String ndjson = "{\"v\":1}\n" + badRecord + "\n{\"v\":3}\n";
        List<String> warnings = new ArrayList<>();
        NdJsonReaderCounters counters = new NdJsonReaderCounters();
        try (
            NdJsonPageDecoder decoder = new NdJsonPageDecoder(
                new ByteArrayInputStream(ndjson.getBytes(StandardCharsets.UTF_8)),
                null,
                List.of(attribute("v", DataType.LONG)),
                null,
                10,
                blockFactory,
                policy,
                "test://constraint",
                counters,
                warnings::add
            );
            Page page = decoder.decodePage()
        ) {
            assertNotNull(page);
            LongBlock block = page.getBlock(0);
            assertEquals("the offending line is dropped, not null-filled", 2, block.getPositionCount());
            assertFalse("a dropped line must not leave a null position behind", block.isNull(0));
            assertFalse("a dropped line must not leave a null position behind", block.isNull(1));
            assertEquals(1L, block.getLong(0));
            assertEquals(3L, block.getLong(1));
        }
        // SkipWarnings.add() emits a one-time summary header on the first call, then the detail.
        assertEquals("one summary + one detail warning for the dropped line", 2, warnings.size());
        assertThat(warnings.get(1), Matchers.containsString("Over-limit NDJSON"));
        assertThat(warnings.get(1), Matchers.containsString(expectedDetail));
        assertEquals("the dropped line is charged exactly once", 1L, counters.snapshot().parseErrors());
    }

    /**
     * A coercion failure inside a nested array (array of arrays flattened) must not let the poison
     * escape past the inner END_ARRAY — otherwise the outer array's drain loop stops too early and
     * sibling fields on the same record read the wrong tokens and come back null.
     *
     * <p>Input: {@code {"v":[[10,"notanumber"],30],"w":1}} under {@code null_field}.
     * Expected: {@code v} is null (whole position cancelled), {@code w} is 1.
     */
    public void testNestedArrayPoisonDrainsToInnerEndArray() throws IOException {
        String ndjson = "{\"v\":[[10,\"notanumber\"],30],\"w\":1}\n";
        List<String> warnings = new ArrayList<>();
        try (
            NdJsonPageDecoder decoder = new NdJsonPageDecoder(
                new ByteArrayInputStream(ndjson.getBytes(StandardCharsets.UTF_8)),
                null,
                List.of(attribute("v", DataType.LONG), attribute("w", DataType.LONG)),
                null,
                10,
                blockFactory,
                ErrorPolicy.PERMISSIVE,
                "test://nested-array-poison",
                new NdJsonReaderCounters(),
                warnings::add
            );
            Page page = decoder.decodePage()
        ) {
            assertNotNull(page);
            LongBlock v = page.getBlock(0);
            LongBlock w = page.getBlock(1);
            assertEquals(1, v.getPositionCount());
            assertEquals(1, w.getPositionCount());
            assertTrue("v is null because its nested array was poisoned", v.isNull(0));
            assertFalse("w must not be null — sibling field after the poisoned array", w.isNull(0));
            assertEquals(1L, w.getLong(w.getFirstValueIndex(0)));
        }
    }

    /**
     * Under {@code skip_row}, two poisoned arrays in the same record must not assert.
     * The first array sets {@code rowDroppedBySkipRow = true}; the second array then re-enters
     * {@code coercionFailure} with {@code rowDroppedBySkipRow} already set and {@code inArray = true}.
     * Without the fix the early return exits without throwing {@code PoisonedPositionException}, so
     * the array loop calls {@code endPositionEntry} with no values appended → {@code AssertionError}.
     */
    public void testSkipRowTwoPoisonedArraysNoAssert() throws IOException {
        String ndjson = "{\"a\":[\"x\"],\"b\":[\"y\"]}\n{\"a\":[1],\"b\":[2]}\n";
        List<String> warnings = new ArrayList<>();
        try (
            NdJsonPageDecoder decoder = new NdJsonPageDecoder(
                new ByteArrayInputStream(ndjson.getBytes(StandardCharsets.UTF_8)),
                null,
                List.of(attribute("a", DataType.LONG), attribute("b", DataType.LONG)),
                null,
                10,
                blockFactory,
                ErrorPolicy.LENIENT,
                "test://skip-row-two-arrays",
                new NdJsonReaderCounters(),
                warnings::add
            );
            Page page = decoder.decodePage()
        ) {
            assertNotNull(page);
            LongBlock a = page.getBlock(0);
            LongBlock b = page.getBlock(1);
            assertEquals("only the clean second row survives", 1, a.getPositionCount());
            assertEquals(1L, a.getLong(a.getFirstValueIndex(0)));
            assertEquals(2L, b.getLong(b.getFirstValueIndex(0)));
        }
    }

    /**
     * One decodable JSON token per declarable type, so a test can sweep {@link DeclaredSchemaValidator#declarableTypes()}
     * and fail loudly when a newly declarable type has no fixture rather than silently skipping it.
     */
    private static final Map<DataType, String> DECLARABLE_TOKEN = Map.of(
        DataType.KEYWORD,
        "\"abc\"",
        DataType.TEXT,
        "\"abc\"",
        DataType.LONG,
        "123",
        DataType.INTEGER,
        "123",
        DataType.DOUBLE,
        "1.5",
        DataType.BOOLEAN,
        "true",
        DataType.DATETIME,
        "\"2020-01-01T00:00:00.000Z\"",
        DataType.DATE_NANOS,
        "\"2020-01-01T00:00:00.000Z\"",
        DataType.UNSIGNED_LONG,
        "18446744073709551615",
        DataType.IP,
        "\"192.168.0.1\""
    );

    /**
     * Drift pin. setupBuilders no longer enumerates the type -> shape mapping; it derives it from the shared
     * authority. Every declarable type must therefore build, with the shape that authority prescribes, and no
     * type may reach unsupportedTypeForNdjson. This is what stops the next declarable type repeating the
     * unsigned_long bug.
     */
    public void testEveryDeclarableTypeBuildsTheAuthorityShape() throws IOException {
        for (DataType type : DeclaredSchemaValidator.declarableTypes()) {
            String cell = DECLARABLE_TOKEN.get(type);
            assertNotNull("no fixture token for declarable type [" + type + "] — add one", cell);
            try (Page page = decodeOneColumn("{\"v\":" + cell + "}\n", type, ErrorPolicy.STRICT)) {
                assertNotNull("no page for declared [" + type + "]", page);
                Block block = page.getBlock(0);
                assertEquals("shape drift for declared [" + type + "]", DeclaredTypeCoercions.elementTypeFor(type), block.elementType());
                assertFalse("declared [" + type + "] produced a null cell — missing decode arm?", block.isNull(0));
            }
        }
    }

    // --- lenient scratch-builder sizing ---

    /**
     * Batch size for the scratch-sizing tests. Large enough that one page-sized builder reservation
     * ({@code batchSize * Long.BYTES}) is unmistakable next to the tens of bytes a record-sized scratch
     * builder reserves.
     */
    private static final int LENIENT_BATCH_SIZE = 32 * 1024;

    /** What a LONG builder charges for a page-sized backing array. */
    private static final long PAGE_SIZED_BYTES = (long) LENIENT_BATCH_SIZE * Long.BYTES;

    /**
     * The lenient decode path builds a fresh set of scratch builders for every record so a mid-record parse
     * error can be discarded without corrupting the page. Those builders hold exactly one record, so they must
     * be sized for one record: sizing them at {@code batchSize} zero-fills a page-sized array and reserves it on
     * the breaker once per record, which is the dominant cost of the lenient path on a large file.
     * <p>
     * Asserted as a count of page-sized reservations: only the once-per-page builders set up in
     * {@code decodePage} may make one, so the total is the number of projected columns whose builders draw on
     * {@code breaker} — regardless of how many records were decoded. A per-record page-sized scratch turns that
     * into {@code columns * (1 + records)}.
     */
    private static void assertScratchIsRecordSized(CountingBreaker breaker, int pageSizedColumns) {
        assertEquals(
            "only the once-per-page builders may reserve page-sized memory; more means the lenient per-record "
                + "scratch builders are page-sized again",
            pageSizedColumns,
            breaker.reservationsOfAtLeast(PAGE_SIZED_BYTES)
        );
        assertEquals("every reservation released once decoder and page are closed", 0L, breaker.used());
    }

    /**
     * {@code error_mode: null_field} ({@link ErrorPolicy#PERMISSIVE}) decodes through the per-record scratch
     * builders. Covers a multivalue array (exercising the scratch's grow-on-demand path now that it no longer
     * starts page-sized), a single value, a missing field, a keyword column, and a dotted column reached through
     * {@code setupBuilders}' recursion — so the nested arm is held to the same sizing as the flat one. Pins that
     * the decoded values are unchanged alongside the allocation invariant.
     * <p>
     * The KEYWORD column is not counted: {@code BytesRefBlockBuilder} backs onto {@code BytesRefArray} over
     * {@link BigArrays#NON_RECYCLING_INSTANCE}, which draws on no breaker. Its sizing is covered by
     * {@link #testLenientCostsNoMoreThanStrictForEveryDeclarableType}.
     */
    public void testLenientScratchBuildersAreRecordSizedNotPageSized() throws IOException {
        String ndjson = """
            {"v":[1,2,3],"w":10,"k":"a","a":{"b":100}}
            {"v":42,"w":20,"k":"b","a":{"b":200}}
            {"w":30,"k":"c","a":{"b":300}}
            {"v":[7,8],"w":40,"k":"d","a":{"b":400}}
            """;
        CountingBreaker breaker = new CountingBreaker();
        BlockFactory trackingFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(breaker).build();
        try (
            NdJsonPageDecoder decoder = new NdJsonPageDecoder(
                new ByteArrayInputStream(ndjson.getBytes(StandardCharsets.UTF_8)),
                null,
                List.of(
                    attribute("v", DataType.LONG),
                    attribute("w", DataType.LONG),
                    attribute("k", DataType.KEYWORD),
                    attribute("a.b", DataType.LONG)
                ),
                null,
                LENIENT_BATCH_SIZE,
                trackingFactory,
                ErrorPolicy.PERMISSIVE,
                "test://lenient-scratch",
                new NdJsonReaderCounters()
            );
            Page page = decoder.decodePage()
        ) {
            assertNotNull(page);
            assertEquals(4, page.getPositionCount());
            LongBlock v = page.getBlock(0);
            LongBlock w = page.getBlock(1);
            BytesRefBlock k = page.getBlock(2);
            LongBlock nested = page.getBlock(3);
            BytesRef scratch = new BytesRef();

            assertEquals(3, v.getValueCount(0));
            int i0 = v.getFirstValueIndex(0);
            assertEquals(1L, v.getLong(i0));
            assertEquals(2L, v.getLong(i0 + 1));
            assertEquals(3L, v.getLong(i0 + 2));

            assertEquals(42L, v.getLong(v.getFirstValueIndex(1)));
            assertTrue("missing field nulls the cell", v.isNull(2));

            assertEquals(2, v.getValueCount(3));
            int i3 = v.getFirstValueIndex(3);
            assertEquals(7L, v.getLong(i3));
            assertEquals(8L, v.getLong(i3 + 1));

            for (int p = 0; p < 4; p++) {
                assertFalse("w present at " + p, w.isNull(p));
                assertEquals((p + 1) * 10L, w.getLong(w.getFirstValueIndex(p)));
                assertFalse("a.b present at " + p, nested.isNull(p));
                assertEquals((p + 1) * 100L, nested.getLong(nested.getFirstValueIndex(p)));
            }
            assertMvAt(k, 0, scratch, List.of("a"));
            assertMvAt(k, 1, scratch, List.of("b"));
            assertMvAt(k, 2, scratch, List.of("c"));
            assertMvAt(k, 3, scratch, List.of("d"));
        }
        assertScratchIsRecordSized(breaker, 3);
    }

    /**
     * The same invariant across every declarable type, stated against the strict path.
     * <p>
     * The two assertions above count reservations made through the block factory's breaker, which covers the
     * fixed-width types but not {@code keyword}/{@code text}/{@code ip}: those back onto a {@code BytesRefArray}
     * charged to {@link BigArrays} instead. Wiring one counter into both places and comparing lenient against
     * strict covers every type uniformly — strict decodes the same records into the same page builders with no
     * scratch at all, so it is the natural baseline for what the data legitimately costs, and no knowledge of how
     * {@code BigArrays} splits a large allocation into reservations is needed to state the claim.
     * <p>
     * Sweeping {@link DeclaredSchemaValidator#declarableTypes()} rather than naming types means a type that
     * becomes declarable later is held to this invariant automatically, and one whose builder is special-cased
     * out of the shared {@code setupBuilders} path fails here rather than silently regressing.
     */
    public void testLenientCostsNoMoreThanStrictForEveryDeclarableType() throws IOException {
        for (DataType type : DeclaredSchemaValidator.declarableTypes()) {
            String cell = DECLARABLE_TOKEN.get(type);
            assertNotNull("no fixture token for declarable type [" + type + "] — add one", cell);
            String ndjson = ("{\"v\":" + cell + "}\n").repeat(5);
            assertEquals(
                "lenient decoding of ["
                    + type
                    + "] must not reserve page-scale memory that strict does not: any excess is per-record "
                    + "scratch sized for a whole page",
                pageScaleReservations(type, ndjson, ErrorPolicy.STRICT),
                pageScaleReservations(type, ndjson, ErrorPolicy.PERMISSIVE)
            );
        }
    }

    /**
     * Decodes one page of a single {@code type} column under {@code policy} and returns how many reservations
     * were page-scale. One counting breaker serves both the block factory and {@link BigArrays} so that
     * fixed-width backing arrays and {@code BytesRefArray} byte storage land in the same tally. The floor is one
     * {@link PageCacheRecycler#PAGE_SIZE_IN_BYTES BigArrays page}: a builder holding a whole batch always reaches
     * it (the narrowest declarable type, boolean, charges a byte per position, and the string types' offset
     * arrays are charged in page units), while a builder holding one record never comes close. Do not raise this
     * to the batch size in bytes — the string types' page-scale reservations land below that, and the sweep would
     * silently compare zero against zero for keyword, text and ip.
     */
    private int pageScaleReservations(DataType type, String ndjson, ErrorPolicy policy) throws IOException {
        CountingBreaker breaker = new CountingBreaker();
        BigArrays bigArrays = new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, breaker.service());
        BlockFactory trackingFactory = BlockFactory.builder(bigArrays).breaker(breaker).build();
        breaker.reset();
        try (
            NdJsonPageDecoder decoder = new NdJsonPageDecoder(
                new ByteArrayInputStream(ndjson.getBytes(StandardCharsets.UTF_8)),
                null,
                List.of(attribute("v", type)),
                null,
                LENIENT_BATCH_SIZE,
                trackingFactory,
                policy,
                "test://lenient-scratch-" + type.typeName(),
                new NdJsonReaderCounters()
            );
            Page page = decoder.decodePage()
        ) {
            assertNotNull("no page for declared [" + type + "]", page);
            assertEquals(5, page.getPositionCount());
            Block block = page.getBlock(0);
            assertFalse("declared [" + type + "] produced a null cell", block.isNull(0));
        }
        assertEquals("every reservation for [" + type + "] released once decoder and page are closed", 0L, breaker.used());
        return breaker.reservationsOfAtLeast(PageCacheRecycler.PAGE_SIZE_IN_BYTES);
    }

    /**
     * {@code error_mode: skip_row} ({@link ErrorPolicy#LENIENT}) takes the same scratch path, including when a
     * poisoned record is dropped whole and its scratch builders are released without reaching the page.
     */
    public void testLenientScratchBuildersAreRecordSizedUnderSkipRow() throws IOException {
        String ndjson = """
            {"v":[1,"bad"],"w":10}
            {"v":5,"w":20}
            {"v":[6,7],"w":30}
            """;
        CountingBreaker breaker = new CountingBreaker();
        BlockFactory trackingFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(breaker).build();
        List<String> warnings = new ArrayList<>();
        try (
            NdJsonPageDecoder decoder = new NdJsonPageDecoder(
                new ByteArrayInputStream(ndjson.getBytes(StandardCharsets.UTF_8)),
                null,
                List.of(attribute("v", DataType.LONG), attribute("w", DataType.LONG)),
                null,
                LENIENT_BATCH_SIZE,
                trackingFactory,
                ErrorPolicy.LENIENT,
                "test://lenient-scratch-skip-row",
                new NdJsonReaderCounters(),
                warnings::add
            );
            Page page = decoder.decodePage()
        ) {
            assertNotNull(page);
            assertEquals("the poisoned record is dropped whole", 2, page.getPositionCount());
            LongBlock v = page.getBlock(0);
            LongBlock w = page.getBlock(1);
            assertEquals(5L, v.getLong(v.getFirstValueIndex(0)));
            assertEquals(20L, w.getLong(w.getFirstValueIndex(0)));
            assertEquals(2, v.getValueCount(1));
            int i1 = v.getFirstValueIndex(1);
            assertEquals(6L, v.getLong(i1));
            assertEquals(7L, v.getLong(i1 + 1));
            assertEquals(30L, w.getLong(w.getFirstValueIndex(1)));
        }
        assertFalse("expected skip_row warnings for the poisoned record", warnings.isEmpty());
        assertScratchIsRecordSized(breaker, 2);
    }

}
