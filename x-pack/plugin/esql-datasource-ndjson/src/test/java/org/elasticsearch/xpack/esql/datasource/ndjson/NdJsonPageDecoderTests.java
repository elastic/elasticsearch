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
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.NumericUtils;
import org.elasticsearch.xpack.esql.datasources.DeclaredSchemaValidator;
import org.elasticsearch.xpack.esql.datasources.spi.DeclaredTypeCoercions;
import org.elasticsearch.xpack.esql.datasources.spi.ErrorPolicy;
import org.elasticsearch.xpack.esql.datasources.spi.SkipWarnings;
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
 * Targeted unit tests for {@link NdJsonPageDecoder}'s keyword-decode path. Sibling
 * {@link NdJsonPageIteratorTests} covers end-to-end correctness across types; these tests focus on
 * the reusable {@code keywordScratch} buffer introduced to remove per-field allocation churn.
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
     * — unlike an actual scalar value where an object was expected, which is a genuine schema conflict (see
     * {@link #testScalarWhereNestedObjectExpectedStrictFails}).
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
     * A scalar value where a nested object was expected (the schema only knows dotted leaf columns for this field,
     * e.g. {@code address.city}/{@code address.zip}, but a row's {@code address} is a plain string) is a genuine
     * scalar/object schema conflict: core ES dynamic mapping treats the same ambiguity as a hard document-parsing
     * conflict, so under {@link ErrorPolicy#STRICT} it must fail the query with an actionable message naming the
     * field and both shapes rather than silently null-filling. Before that fix, this
     * mismatch was silently null-filled even under {@code STRICT} (see the pre-#1028 revision of
     * {@code testNullOrScalarWhereNestedObjectExpected}).
     */
    public void testScalarWhereNestedObjectExpectedStrictFails() {
        String ndjson = "{\"address\": {\"city\": \"NYC\", \"zip\": \"10001\"}}\n" + "{\"address\": \"unstructured\"}\n";

        EsqlIllegalArgumentException ex = expectThrows(
            EsqlIllegalArgumentException.class,
            () -> decodePage(
                ndjson,
                List.of(attribute("address.city", DataType.KEYWORD), attribute("address.zip", DataType.KEYWORD)),
                ErrorPolicy.STRICT
            )
        );
        assertThat(ex.getMessage(), Matchers.containsString("address"));
        assertThat(ex.getMessage(), Matchers.containsString("a string"));
        assertThat(ex.getMessage(), Matchers.containsString("an object"));
    }

    /**
     * Same conflict as {@link #testScalarWhereNestedObjectExpectedStrictFails}, but under skip_row: the conflicting
     * record is dropped whole and a client warning is surfaced, while the other records still decode. error_mode
     * governs the outcome the same for a bound/declared or an inferred schema; null_field keeps the record and nulls
     * the conflicting field instead (see the NdJsonPageIteratorTests null_field pin).
     */
    public void testScalarWhereNestedObjectExpectedSkipRowDropsRecordAndWarns() throws IOException {
        String ndjson = "{\"address\": {\"city\": \"NYC\", \"zip\": \"10001\"}, \"id\": 1}\n"
            + "{\"address\": \"unstructured\", \"id\": 2}\n"
            + "{\"address\": {\"city\": \"London\", \"zip\": \"SW1A\"}, \"id\": 3}\n";

        try (
            Page page = decodePage(
                ndjson,
                List.of(
                    attribute("address.city", DataType.KEYWORD),
                    attribute("address.zip", DataType.KEYWORD),
                    attribute("id", DataType.INTEGER)
                ),
                ErrorPolicy.LENIENT
            )
        ) {
            assertNotNull(page);
            // The scalar-where-object record is dropped whole under skip_row; the two structured records survive.
            assertEquals(2, page.getPositionCount());
            BytesRefBlock city = page.getBlock(0);
            BytesRefBlock zip = page.getBlock(1);
            IntBlock id = page.getBlock(2);
            BytesRef scratch = new BytesRef();
            assertEquals(new BytesRef("NYC"), BytesRef.deepCopyOf(city.getBytesRef(0, scratch)));
            assertEquals(1, id.getInt(id.getFirstValueIndex(0)));
            assertEquals(new BytesRef("London"), BytesRef.deepCopyOf(city.getBytesRef(1, scratch)));
            assertEquals(new BytesRef("SW1A"), BytesRef.deepCopyOf(zip.getBytesRef(1, scratch)));
            assertEquals(3, id.getInt(id.getFirstValueIndex(1)));
        }

        List<String> warnings = drainWarnings();
        assertFalse("expected a client warning for the shape conflict", warnings.isEmpty());
        assertTrue("warning should name the conflicting field, got: " + warnings, warnings.stream().anyMatch(w -> w.contains("address")));
    }

    /**
     * Same shape conflict as {@link #testScalarWhereNestedObjectExpectedSkipRowDropsRecordAndWarns}, but with a
     * {@code warningSink} supplied: the decoder must route every emitted message through the sink instead of
     * {@link HeaderWarning}, since {@link NdJsonPageDecoder}'s decode loop can run on a background reader thread
     * whose thread-local response headers never reach the client (see {@link SkipWarnings}).
     */
    public void testScalarWhereNestedObjectExpectedLenientRoutesThroughWarningSink() throws IOException {
        String ndjson = "{\"address\": {\"city\": \"NYC\"}, \"id\": 1}\n" + "{\"address\": \"unstructured\", \"id\": 2}\n";
        List<String> sunk = new ArrayList<>();
        try (
            NdJsonPageDecoder decoder = new NdJsonPageDecoder(
                new ByteArrayInputStream(ndjson.getBytes(StandardCharsets.UTF_8)),
                List.of(attribute("address.city", DataType.KEYWORD), attribute("id", DataType.INTEGER)),
                null,
                1024,
                blockFactory,
                ErrorPolicy.LENIENT,
                "test://decode",
                NdJsonUtils.JSON_FACTORY,
                sunk::add
            )
        ) {
            try (Page page = decoder.decodePage()) {
                assertNotNull(page);
            }
        }

        assertFalse("expected a client warning routed through the sink", sunk.isEmpty());
        assertTrue("warning should name the conflicting field, got: " + sunk, sunk.stream().anyMatch(w -> w.contains("address")));
        assertTrue("no message should reach the thread-local response headers when a sink is supplied", drainWarnings().isEmpty());
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
     * (see {@link #testScalarWhereNestedObjectExpectedStrictFails}) fails the query. Covers leading-object,
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
        expectThrows(EsqlIllegalArgumentException.class, () -> decodeOneColumn("{\"v\":-1}\n", DataType.DATE_NANOS, ErrorPolicy.STRICT));
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
     * Drift pin. setupBuilders no longer enumerates the type -> shape mapping; it derives it from the shared
     * authority. Every declarable type must therefore build, with the shape that authority prescribes, and no
     * type may reach unsupportedTypeForNdjson. This is what stops the next declarable type repeating the
     * unsigned_long bug.
     */
    public void testEveryDeclarableTypeBuildsTheAuthorityShape() throws IOException {
        Map<DataType, String> token = Map.of(
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
        for (DataType type : DeclaredSchemaValidator.declarableTypes()) {
            String cell = token.get(type);
            assertNotNull("no fixture token for declarable type [" + type + "] — add one", cell);
            try (Page page = decodeOneColumn("{\"v\":" + cell + "}\n", type, ErrorPolicy.STRICT)) {
                assertNotNull("no page for declared [" + type + "]", page);
                Block block = page.getBlock(0);
                assertEquals("shape drift for declared [" + type + "]", DeclaredTypeCoercions.elementTypeFor(type), block.elementType());
                assertFalse("declared [" + type + "] produced a null cell — missing decode arm?", block.isNull(0));
            }
        }
    }
}
