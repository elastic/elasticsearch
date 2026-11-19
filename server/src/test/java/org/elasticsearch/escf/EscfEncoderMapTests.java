/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.escf;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.MockPageCacheRecycler;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.sourcebatch.LeafSink;
import org.elasticsearch.sourcebatch.SourceRowToXContent;
import org.elasticsearch.sourcebatch.SourceValueType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.BytesRefRecycler;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentString;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Tests for the Map-based {@link EscfEncoder#parseToScratch(Map)} and
 * {@link EscfEncoder#parseToScratch(Map, LeafSink)} paths.
 *
 * <p>Differential tests parse a JSON string into a {@link Map} (via {@link XContentHelper#convertToMap}),
 * encode both via the Map path and the Jackson path, and assert that the decoded row maps are identical.
 * Supplementary tests verify {@link LeafSink} callback dispatch and value types that do not appear in
 * JSON-derived maps, such as {@link Float} and {@link BigDecimal}.
 */
public class EscfEncoderMapTests extends ESTestCase {

    // -----------------------------------------------------------------------
    // Differential: Map path ≡ Jackson for standard JSON shapes
    // -----------------------------------------------------------------------

    public void testFlatScalars() throws IOException {
        assertSameAsJackson("""
            {"i":42,"l":10000000000,"d":1.5,"s":"hello","b":true,"f":false,"n":null}""");
    }

    public void testNestedObjects() throws IOException {
        assertSameAsJackson("""
            {"user":{"name":"alice","age":30},"status":"active"}""");
    }

    public void testEmptyObject() throws IOException {
        assertSameAsJackson("""
            {"empty":{},"x":1}""");
    }

    public void testEmptyObjectDistinctFromAbsent() throws IOException {
        assertSameAsJackson("""
            {"obj":{}}""", """
            {"other":1}""");
    }

    public void testEmptyObjectAndNestedObjectAcrossDocs() throws IOException {
        assertSameAsJackson("""
            {"obj":{}}""", """
            {"obj":{"k":1}}""");
    }

    public void testFixedLongArray() throws IOException {
        assertSameAsJackson("""
            {"vals":[1,2,3,4]}""");
    }

    public void testFixedDoubleArray() throws IOException {
        assertSameAsJackson("""
            {"vals":[1.5,2.5,-3.25]}""");
    }

    public void testFixedStringArray() throws IOException {
        assertSameAsJackson("""
            {"tags":["a","bb","ccc"]}""");
    }

    public void testArrayOfObjectsGoesToUnion() throws IOException {
        assertSameAsJackson("""
            {"items":[{"x":1},{"y":"two"}]}""");
    }

    public void testHeterogeneousArrayGoesToUnion() throws IOException {
        assertSameAsJackson("""
            {"mixed":[1,"two",3.5,true]}""");
    }

    public void testExplicitNull() throws IOException {
        assertSameAsJackson("""
            {"a":null,"b":5}""");
    }

    public void testEmptyArray() throws IOException {
        assertSameAsJackson("""
            {"empty":[],"x":1}""");
    }

    public void testHeterogeneousColumnAcrossDocs() throws IOException {
        assertSameAsJackson("""
            {"a":1,"keep":true}""", """
            {"a":"text","keep":false}""", """
            {"keep":true}""");
    }

    public void testDeepNesting() throws IOException {
        assertSameAsJackson("""
            {"a":{"b":{"c":{"d":1}}}}""");
    }

    public void testEmptyDocument() throws IOException {
        assertSameAsJackson("{}");
    }

    // -----------------------------------------------------------------------
    // No-sink convenience overload
    // -----------------------------------------------------------------------

    public void testNoSinkOverload() throws IOException {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("k", "v");
        map.put("n", 42);
        try (EscfEncoder encoder = new EscfEncoder(newRecycler(), false)) {
            encoder.parseToScratch(map);
            encoder.commitScratchTo(0);
            try (EscfBatch batch = encoder.buildPartition(0)) {
                assertEquals(1, batch.docCount());
                Map<String, Object> result = reconstruct(batch, 0);
                assertEquals("v", result.get("k"));
                assertEquals(42, result.get("n"));
            }
        }
    }

    // -----------------------------------------------------------------------
    // Typed LeafSink: callback dispatch per primitive type
    // -----------------------------------------------------------------------

    /**
     * Verifies that {@link EscfEncoder#parseToScratch(Map, LeafSink)} fires the correct typed
     * callback for each primitive value type. Integer in the map yields INT, Long yields LONG,
     * an exact-float Double yields FLOAT, a non-exact-float Double yields DOUBLE.
     */
    public void testTypedSinkReceivesCallbacksForAllPrimitives() throws IOException {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("i", 42);                  // Integer -> INT
        map.put("l", 10_000_000_000L);     // Long -> LONG
        map.put("fval", 1.5);              // Double, exact float -> FLOAT
        map.put("dval", 1.23456789012345); // Double, not exact float -> DOUBLE
        map.put("b", true);
        map.put("s", "hello");

        Map<String, Byte> observedTypes = new LinkedHashMap<>();
        Map<String, Long> observedLongs = new LinkedHashMap<>();
        Map<String, Double> observedDoubles = new LinkedHashMap<>();

        LeafSink sink = new LeafSink() {
            @Override
            public boolean passRawText() {
                return false;
            }

            @Override
            public void onLongPrimitive(int col, String path, byte type, long val) {
                observedTypes.put(path, type);
                observedLongs.put(path, val);
            }

            @Override
            public void onDoublePrimitive(int col, String path, byte type, double val) {
                observedTypes.put(path, type);
                observedDoubles.put(path, val);
            }

            @Override
            public void onBooleanPrimitive(int col, String path, boolean val) {
                observedTypes.put(path, val ? SourceValueType.TRUE : SourceValueType.FALSE);
            }

            @Override
            public void onTextPrimitive(int col, String path, byte type, XContentString.UTF8Bytes bytes) {
                observedTypes.put(path, type);
            }
        };

        try (EscfEncoder encoder = new EscfEncoder(newRecycler(), false)) {
            encoder.parseToScratch(map, sink);
            encoder.commitScratchTo(0);
            try (EscfBatch batch = encoder.buildPartition(0)) {
                assertEquals(1, batch.docCount());
            }
        }

        assertEquals(SourceValueType.INT, (byte) observedTypes.get("i"));
        assertEquals(42L, (long) observedLongs.get("i"));
        assertEquals(SourceValueType.LONG, (byte) observedTypes.get("l"));
        assertEquals(10_000_000_000L, (long) observedLongs.get("l"));
        assertEquals(SourceValueType.FLOAT, (byte) observedTypes.get("fval"));
        assertEquals(1.5, observedDoubles.get("fval"), 0.0);
        assertEquals(SourceValueType.DOUBLE, (byte) observedTypes.get("dval"));
        assertEquals(1.23456789012345, observedDoubles.get("dval"), 0.0);
        assertEquals(SourceValueType.TRUE, (byte) observedTypes.get("b"));
        assertEquals(SourceValueType.STRING, (byte) observedTypes.get("s"));
    }

    // -----------------------------------------------------------------------
    // rawTextMode: values arrive as Java toString() representations
    // -----------------------------------------------------------------------

    /**
     * In rawTextMode, {@code MapXContentParser} cannot supply the original source bytes, so
     * numeric and boolean leaves arrive as {@link Object#toString()} representations. This test
     * verifies the expected strings for common primitive types.
     */
    public void testRawTextSinkReceivesStringRepresentations() throws IOException {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("n", 42);      // Integer -> "42"
        map.put("d", 1.5);     // Double -> "1.5"
        map.put("b", true);    // Boolean -> "true"
        map.put("s", "hello"); // String -> "hello"

        List<String> capturedPaths = new ArrayList<>();
        List<String> capturedTexts = new ArrayList<>();

        LeafSink rawSink = new LeafSink() {
            @Override
            public boolean passRawText() {
                return true;
            }

            @Override
            public void onTextPrimitive(int col, String path, byte type, XContentString.UTF8Bytes bytes) {
                capturedPaths.add(path);
                capturedTexts.add(new String(bytes.bytes(), bytes.offset(), bytes.length(), StandardCharsets.UTF_8));
            }
        };

        try (EscfEncoder encoder = new EscfEncoder(newRecycler(), false)) {
            encoder.parseToScratch(map, rawSink);
            encoder.commitScratchTo(0);
            try (EscfBatch batch = encoder.buildPartition(0)) {
                assertEquals(1, batch.docCount());
            }
        }

        assertEquals(4, capturedPaths.size());
        assertEquals("42", capturedTexts.get(capturedPaths.indexOf("n")));
        assertEquals("1.5", capturedTexts.get(capturedPaths.indexOf("d")));
        assertEquals("true", capturedTexts.get(capturedPaths.indexOf("b")));
        assertEquals("hello", capturedTexts.get(capturedPaths.indexOf("s")));
    }

    // -----------------------------------------------------------------------
    // Value types not produced by JSON parsing
    // -----------------------------------------------------------------------

    /**
     * A {@link Float} value in the map goes through {@code MapXContentParser.numberType() == FLOAT},
     * is promoted to double for storage, and round-trips faithfully for exact-float values.
     */
    public void testFloatValueInMap() throws IOException {
        Map<String, Object> map = Map.of("f", 1.5f);
        try (EscfEncoder encoder = new EscfEncoder(newRecycler(), false)) {
            encoder.parseToScratch(map);
            encoder.commitScratchTo(0);
            try (EscfBatch batch = encoder.buildPartition(0)) {
                assertEquals(1.5, reconstruct(batch, 0).get("f"));
            }
        }
    }

    /**
     * A {@link BigDecimal} value falls through to the string branch in {@code flattenObject}
     * and is stored as its decimal string representation.
     */
    public void testBigDecimalStoredAsString() throws IOException {
        Map<String, Object> map = Map.of("bd", new BigDecimal("3.141592653589793238"));
        try (EscfEncoder encoder = new EscfEncoder(newRecycler(), false)) {
            encoder.parseToScratch(map);
            encoder.commitScratchTo(0);
            try (EscfBatch batch = encoder.buildPartition(0)) {
                assertEquals("3.141592653589793238", reconstruct(batch, 0).get("bd"));
            }
        }
    }

    /**
     * A {@link BigInteger} value beyond long range falls through to the string branch and is
     * stored as its decimal string representation.
     */
    public void testBigIntegerStoredAsString() throws IOException {
        Map<String, Object> map = Map.of("bi", new BigInteger("99999999999999999999999999999"));
        try (EscfEncoder encoder = new EscfEncoder(newRecycler(), false)) {
            encoder.parseToScratch(map);
            encoder.commitScratchTo(0);
            try (EscfBatch batch = encoder.buildPartition(0)) {
                assertEquals("99999999999999999999999999999", reconstruct(batch, 0).get("bi"));
            }
        }
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    /**
     * Parses each JSON string into a {@link Map}, encodes all documents via the Map path and the
     * Jackson path, and asserts that every row's decoded source map is identical.
     */
    private static void assertSameAsJackson(String... jsonDocs) throws IOException {
        Recycler<BytesRef> recycler = newRecycler();
        try (EscfEncoder mapEncoder = new EscfEncoder(recycler, false); EscfEncoder jacksonEncoder = new EscfEncoder(recycler, false)) {
            for (String json : jsonDocs) {
                mapEncoder.parseToScratch(asMap(json));
                mapEncoder.commitScratchTo(0);
                jacksonEncoder.addDocument(new BytesArray(json), XContentType.JSON, 0);
            }
            try (EscfBatch mapBatch = mapEncoder.buildPartition(0); EscfBatch jacksonBatch = jacksonEncoder.buildPartition(0)) {
                assertEquals(jacksonBatch.docCount(), mapBatch.docCount());
                for (int i = 0; i < jacksonBatch.docCount(); i++) {
                    assertEquals("row " + i, reconstruct(jacksonBatch, i), reconstruct(mapBatch, i));
                }
            }
        }
    }

    private static Map<String, Object> reconstruct(EscfBatch batch, int row) throws IOException {
        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            SourceRowToXContent.writeRow(batch.row(row), batch.schema(), builder);
            return XContentHelper.convertToMap(BytesReference.bytes(builder), false, XContentType.JSON).v2();
        }
    }

    private static Map<String, Object> asMap(String json) {
        return XContentHelper.convertToMap(new BytesArray(json), false, XContentType.JSON).v2();
    }

    private static Recycler<BytesRef> newRecycler() {
        return new BytesRefRecycler(new MockPageCacheRecycler(Settings.EMPTY));
    }
}
