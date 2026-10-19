/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.painless;

import org.apache.lucene.document.InetAddressPoint;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.index.fielddata.MultiGeoPointValues;
import org.elasticsearch.index.fielddata.SortableBinaryDocValues;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;
import org.elasticsearch.index.fielddata.SortedNumericLongValues;
import org.elasticsearch.painless.spi.PainlessTestScript;
import org.elasticsearch.painless.spi.WhitelistLoader;
import org.elasticsearch.script.DynamicMap;
import org.elasticsearch.script.field.BinaryDocValuesField;
import org.elasticsearch.script.field.DoubleDocValuesField;
import org.elasticsearch.script.field.GeoPointDocValuesField;
import org.elasticsearch.script.field.HalfFloatDocValuesField;
import org.elasticsearch.script.field.IpDocValuesField;
import org.elasticsearch.script.field.KeywordDocValuesField;
import org.elasticsearch.script.field.LongDocValuesField;
import org.elasticsearch.search.lookup.Source;
import org.elasticsearch.search.lookup.SourceFilter;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.greaterThan;

/**
 * Tests for the Elasticsearch field-access allocation annotations in {@code org.elasticsearch.txt}: the constant
 * {@code GeoPoint} constructors (exercised end to end) and the estimators for {@code BytesRef.utf8ToString} and
 * {@code GeoPoints.getLats/getLons} (whose receivers come from doc values and so are exercised directly — the whitelist
 * loading in the end-to-end tests confirms their annotations resolve).
 *
 * <p>Also covers the doc-value reads: keyword, BytesRef and binary reads charged after the call from the real result, a miss
 * that charges nothing, and the list builders sized from the value count.
 */
public class AllocationEsFieldTests extends AllocationTestCase {

    /** Reading the five-char term {@code hello}, its byte copy sized at three bytes per char. */
    private static final long KEYWORD_HELLO_BYTES = 32L + 32L + (32L + 2L * 5L);

    public void testGeoPointConstructorCharged() {
        assertEquals(32L, allocatedBytes("new GeoPoint(1.0, 2.0); return \"x\";"));
    }

    public void testGeoPointConstructorTripsLimit() {
        assertTripsLimit("new GeoPoint(1.0, 2.0); return \"x\";");
    }

    public void testUtf8ToStringEstimator() {
        // A BytesRef of N UTF-8 bytes yields a String of at most N chars: overhead (32) + 2 bytes per char.
        assertEquals(32L + 2L * 5, AllocationEstimators.utf8ToStringBytes(new BytesRef("hello")));
        assertEquals(32L, AllocationEstimators.utf8ToStringBytes(new BytesRef("")));
        assertEquals(32L, AllocationEstimators.utf8ToStringBytes(null));
    }

    /**
     * A keyword read: the byte copy, the {@link BytesRef}, and the new String. Only the String is in hand, so the copy is sized
     * at three bytes per char, the most UTF-8 takes. Five chars is (16 + 15, padded to 32) + 32 + (32 + 2 * 5) = 106.
     */
    public void testKeywordReadChargedFromTermLength() throws IOException {
        assertEquals(KEYWORD_HELLO_BYTES, AllocationEstimators.termStringBytes(5));

        Map<String, Object> params = Map.of("field", keywordField("hello").toScriptDocValues());
        assertEquals(KEYWORD_HELLO_BYTES, allocatedBytes("params.field.value", params));
        assertEquals(KEYWORD_HELLO_BYTES, allocatedBytes("params.field.get(0)", params));
        assertEquals(KEYWORD_HELLO_BYTES, allocatedBytes("params.field[0]", params));
        // The numeric dot shortcut is the same list read.
        assertEquals(KEYWORD_HELLO_BYTES, allocatedBytes("params.field.0", params));
        assertEquals(KEYWORD_HELLO_BYTES, allocatedBytes("def f = params.field; return f.0;", params));
        // A two byte char counts the same as any other char.
        Map<String, Object> accented = Map.of("field", keywordField("h\u00e9llo").toScriptDocValues());
        assertEquals(KEYWORD_HELLO_BYTES, allocatedBytes("params.field.value", accented));
        // With the static type known the wrapper is called directly rather than through def.
        assertEquals(
            KEYWORD_HELLO_BYTES,
            allocatedBytes("ScriptDocValues.Strings f = (ScriptDocValues.Strings) params.field; f.value", params)
        );
    }

    /** The same read through the fields API. */
    public void testKeywordReadChargedInScript() throws IOException {
        Map<String, Object> params = Map.of("field", keywordField("hello"));

        assertEquals(KEYWORD_HELLO_BYTES, allocatedBytes("params.field.get(0, '')", params));
        assertEquals(KEYWORD_HELLO_BYTES, allocatedBytes("params.field.get('')", params));
    }

    /** A read inside a lambda still reaches the script, typed or through def. The read alone is past a 60 byte limit. */
    public void testKeywordReadChargedInsideLambda() throws IOException {
        Map<String, Object> params = Map.of("field", keywordField("hello").toScriptDocValues());
        assertTripsLimit("Optional.empty().orElseGet(() -> params.field.value)", "60b", params);
        assertTripsLimit("Optional.empty().orElseGet(() -> params.field[0])", "60b", params);
        assertTripsLimit("Optional.empty().orElseGet(() -> params.field.0)", "60b", params);
        assertTripsLimit(
            "ScriptDocValues.Strings f = (ScriptDocValues.Strings) params.field; Optional.empty().orElseGet(() -> f.value)",
            "60b",
            params
        );
    }

    /** A miss returns the default and allocates nothing. */
    public void testMissingKeywordChargesNothing() throws IOException {
        assertEquals(0L, allocatedBytes("params.field.get(1, '')", Map.of("field", keywordField("hello"))));
    }

    /** An ip field formats its string on read. The charge follows the string it made. */
    public void testFormattedStringReadChargedFromResult() throws IOException {
        IpDocValuesField ipField = new IpDocValuesField(ipDocValues("192.168.0.1"), "test");
        ipField.setNextDocId(0);
        Map<String, Object> params = Map.of("field", ipField.toScriptDocValues());

        // An ip read decodes the address before it formats the text, so it costs more than a keyword of the same length, and
        // every way of reading it pays the same.
        long read = AllocationEstimators.ipStringReadBytes("192.168.0.1".length());
        assertTrue(read > AllocationEstimators.termStringBytes("192.168.0.1".length()));
        assertEquals(read, allocatedBytes("params.field.value", params));
        assertEquals(read, allocatedBytes("params.field.get(0)", params));
        assertEquals(read, allocatedBytes("params.field[0]", params));
        assertEquals(read, allocatedBytes("params.field.0", params));

        // A short text does not make the decode cheaper.
        IpDocValuesField shortField = new IpDocValuesField(ipDocValues("::"), "test");
        shortField.setNextDocId(0);
        Map<String, Object> shortParams = Map.of("field", shortField.toScriptDocValues());
        assertEquals(AllocationEstimators.ipStringReadBytes(2), allocatedBytes("params.field.value", shortParams));
        assertTrue(AllocationEstimators.ipStringReadBytes(2) > 92);
    }

    /** A BytesRef read is the byte copy and the BytesRef. A binary read adds the buffer that wraps the copy. */
    public void testBytesReadsCharged() throws IOException {
        BinaryDocValuesField field = new BinaryDocValuesField(binaryDocValues("hello"), "test");
        field.setNextDocId(0);
        Map<String, Object> params = Map.of("field", field, "refs", field.toScriptDocValues());

        assertEquals(AllocationEstimators.termCopyBytes(5), allocatedBytes("params.refs.value", params));
        assertEquals(AllocationEstimators.termCopyBytes(5), allocatedBytes("params.refs.get(0)", params));
        assertEquals(AllocationEstimators.termCopyBytes(5), allocatedBytes("params.refs[0]", params));
        assertEquals(AllocationEstimators.termCopyBytes(5), allocatedBytes("params.refs.0", params));
        assertEquals(
            AllocationEstimators.byteBufferBytes(5),
            allocatedBytes("BinaryDocValuesField f = (BinaryDocValuesField) params.field; f.get(null)", params)
        );
        assertEquals(0L, allocatedBytes("BinaryDocValuesField f = (BinaryDocValuesField) params.field; f.get(1, null)", params));
    }

    public void testKeywordReadTripsLimit() throws IOException {
        assertTripsLimit("params.field.get(0, '')", "1b", Map.of("field", keywordField("hello")));
    }

    /** {@code asDoubles()}: a list of boxed doubles, 40 + 40 + 3 * 24 = 152. */
    public void testAsDoublesChargedFromValueCount() throws IOException {
        HalfFloatDocValuesField field = new HalfFloatDocValuesField(doubleDocValues(1.5, 2.5, 3.5), "test");
        field.setNextDocId(0);

        assertEquals(152L, AllocationEstimators.halfFloatDoublesBytes(field));
    }

    /** {@code asStrings()}: a list of 40 + 32, plus 160 to decode and 32 + 2 * 45 for the string, per value. */
    public void testAsStringsChargedFromValueCount() throws IOException {
        IpDocValuesField field = new IpDocValuesField(ipDocValues("192.168.0.1", "10.0.0.1"), "test");
        field.setNextDocId(0);

        assertEquals(40L + 32L + 2L * (160L + 32L + 2L * 45L), AllocationEstimators.ipStringsBytes(field));

        // A null receiver is charged an empty list. The real call never sees one.
        assertEquals(40L + 16L, AllocationEstimators.ipStringsBytes(null));
    }

    public void testBoxedNumericListReadsCharged() throws IOException {
        // get(index) returns the value boxed; getValue() returns the primitive and costs nothing.
        LongDocValuesField longs = new LongDocValuesField(longDocValues(1000L), "test");
        longs.setNextDocId(0);
        Map<String, Object> longParams = Map.of("field", longs.toScriptDocValues());
        assertEquals(AllocSizes.boxSize(long.class), allocatedBytes("params.field.get(0)", longParams));
        assertEquals(
            AllocSizes.boxSize(long.class),
            allocatedBytes("ScriptDocValues.Longs f = (ScriptDocValues.Longs) params.field; f.get(0)", longParams)
        );
        assertEquals(0L, allocatedBytes("long v = ((ScriptDocValues.Longs) params.field).getValue(); return null;", longParams));
        // The def bracket form reaches the same get(int). (The typed form does not compile on these classes: get returns Long
        // while the inherited set takes def, and the list shortcut requires them to match.)
        assertEquals(AllocSizes.boxSize(long.class), allocatedBytes("params.field[0]", longParams));
        assertEquals(AllocSizes.boxSize(long.class), allocatedBytes("params.field.0", longParams));

        DoubleDocValuesField doubles = new DoubleDocValuesField(doubleDocValues(2.5), "test");
        doubles.setNextDocId(0);
        Map<String, Object> doubleParams = Map.of("field", doubles.toScriptDocValues());
        assertEquals(AllocSizes.boxSize(double.class), allocatedBytes("params.field.get(0)", doubleParams));
    }

    public void testGeoPointReadsCharged() throws IOException {
        // Every read copies the point into a new GeoPoint: get, the getters behind .value and .lat, and the distance methods.
        Map<String, Object> params = Map.of("field", geoPointField(12.5, 45.25).toScriptDocValues());
        long point = AllocationEstimators.geoPointBytes();
        assertEquals(point, allocatedBytes("params.field.get(0)", params));
        assertEquals(point, allocatedBytes("params.field.value", params));
        assertEquals(point, allocatedBytes("double d = params.field.lat; return null;", params));
        assertEquals(
            point,
            allocatedBytes("ScriptDocValues.GeoPoints f = (ScriptDocValues.GeoPoints) params.field; double d = f.lon; return null;", params)
        );
        assertEquals(point, allocatedBytes("double d = params.field.arcDistance(0, 0); return null;", params));
        assertEquals(point, allocatedBytes("double d = params.field.geohashDistance('u0'); return null;", params));
    }

    private static final String SOURCE_JSON =
        "{\"name\":\"kimchy\",\"age\":42,\"score\":1.5,\"ok\":true,\"tags\":[\"a\",\"b\"],\"nested\":{\"x\":null}}";

    /** The lookup function the server's script contexts put on {@code _source}: parse the document behind the supplier. */
    @SuppressWarnings("unchecked")
    private static final Map<String, Function<Object, Object>> SOURCE_FUNCTION = Map.of(
        "_source",
        value -> ((Supplier<Source>) value).get().source()
    );

    /** A parsed document as the server hands it to {@code params._source}: ordered maps, lists, Strings and boxed numbers. */
    private static Map<String, Object> parsedSource() {
        return XContentHelper.convertToMap(new BytesArray(SOURCE_JSON), true).v2();
    }

    /** A byte-backed document that remembers whether anyone parsed it. */
    private static final class RecordingSource implements Source {
        final BytesReference bytes = new BytesArray(SOURCE_JSON);
        boolean parsed;
        Map<String, Object> map;

        @Override
        public XContentType sourceContentType() {
            return XContentType.JSON;
        }

        @Override
        public Map<String, Object> source() {
            parsed = true;
            if (map == null) {
                map = XContentHelper.convertToMap(bytes, true).v2();
            }
            return map;
        }

        @Override
        public BytesReference internalSourceRef() {
            return bytes;
        }

        @Override
        public Source filter(SourceFilter sourceFilter) {
            throw new UnsupportedOperationException();
        }
    }

    /** A params map built the way the server builds it: the document behind a supplier, parsed by the lookup function. */
    private static Map<String, Object> serverParams(Supplier<Source> document) {
        Map<String, Object> backing = new HashMap<>();
        backing.put("_source", document);
        return new DynamicMap(backing, SOURCE_FUNCTION);
    }

    public void testSourceReadChargesLoadedBytesAndParseFloorBeforeTheParse() {
        long length = new RecordingSource().bytes.length();
        long floor = AllocationEstimators.parseFloorBytes(length);
        long parsed = AllocationEstimators.parsedSourceBytes(parsedSource());
        assertThat(parsed, greaterThan(floor));
        // The whole read: the loaded bytes, the parse floor, then what the real tree costs beyond the floor.
        RecordingSource document = new RecordingSource();
        assertEquals(length + parsed, allocatedBytes("return params._source.name;", serverParams(() -> document)));
        assertTrue(document.parsed);
        // A document too big for the budget is never parsed.
        RecordingSource big = new RecordingSource();
        assertTripsLimit("return params._source.name;", (length + floor - 1) + "b", serverParams(() -> big));
        assertFalse(big.parsed);
        // A second read of the same document pays nothing more, at either step.
        RecordingSource twice = new RecordingSource();
        assertEquals(
            length + parsed,
            allocatedBytes("def n = params._source.name; return params['_source'].age;", serverParams(() -> twice))
        );
    }

    public void testChargedParamsChargesEachNewDocumentOnce() {
        // The wrapper tells documents apart by the loaded and parsed objects, which the server keeps per doc id.
        PainlessScript script = (PainlessScript) compile("return null;", "1mb");
        long length = new RecordingSource().bytes.length();
        long one = length + AllocationEstimators.parsedSourceBytes(parsedSource());
        Source[] current = { new RecordingSource() };
        ChargedParams params = new ChargedParams(script, serverParams(() -> current[0]));
        params.get("_source");
        params.get("_source");
        assertEquals(one, script.getAllocBytes());
        current[0] = new RecordingSource();
        params.get("_source");
        assertEquals(2L * one, script.getAllocBytes());
    }

    public void testParsedSourceEstimatorSizesEveryNode() {
        // An empty map is the shell plus the default 16-slot table. Each entry adds the linked entry, its key String and its
        // value: a small int is a 16-byte box, a double a 24-byte box, null and booleans are shared and cost nothing.
        long emptyMap = 88L + AllocSizes.arrayBytes(16, AllocSizes.REFERENCE_SIZE);
        long key = 32L + 2L;
        assertEquals(emptyMap, AllocationEstimators.parsedSourceBytes(Map.of()));
        assertEquals(emptyMap + 56L + key + 16L, AllocationEstimators.parsedSourceBytes(Map.of("a", 1)));
        assertEquals(emptyMap + 56L + key + 24L, AllocationEstimators.parsedSourceBytes(Map.of("a", 1.5)));
        assertEquals(emptyMap + 56L + key, AllocationEstimators.parsedSourceBytes(Map.of("a", true)));
        Map<String, Object> withNull = new HashMap<>();
        withNull.put("a", null);
        assertEquals(emptyMap + 56L + key, AllocationEstimators.parsedSourceBytes(withNull));
        // A list is its shell plus an array sized to its elements, then the elements. A nested map is a whole map again.
        long list = 40L + AllocSizes.arrayBytes(2, AllocSizes.REFERENCE_SIZE) + 2L * key;
        assertEquals(emptyMap + 56L + key + list, AllocationEstimators.parsedSourceBytes(Map.of("a", List.of("x", "y"))));
        assertEquals(2L * (emptyMap + 56L + key), AllocationEstimators.parsedSourceBytes(Map.of("a", Map.of("b", true))));
        // Thirteen entries push the table to 32 slots.
        Map<String, Object> thirteen = new HashMap<>();
        for (int i = 0; i < 13; i++) {
            thirteen.put(String.valueOf((char) ('a' + i)), true);
        }
        long table32 = 88L + AllocSizes.arrayBytes(32, AllocSizes.REFERENCE_SIZE);
        assertEquals(table32 + 13L * (56L + key), AllocationEstimators.parsedSourceBytes(thirteen));
    }

    public void testSourceReadChargesTheParsedDocumentOncePerExecution() {
        Map<String, Object> source = parsedSource();
        long parsed = AllocationEstimators.parsedSourceBytes(source);
        Map<String, Object> params = Map.of("_source", source, "other", "x");
        // Every spelling of the read, including through def, charges the whole parsed document.
        assertEquals(parsed, allocatedBytes("return params._source.name;", params));
        assertEquals(parsed, allocatedBytes("return params['_source'].name;", params));
        assertEquals(parsed, allocatedBytes("return params.get('_source').name;", params));
        assertEquals(parsed, allocatedBytes("def p = params; return p._source.name;", params));
        // Reading it again in the same execution gets the same parsed map back and is not charged again.
        assertEquals(
            parsed,
            allocatedBytes("def n = params._source.name; def a = params._source.age; return params['_source'].tags;", params)
        );
        // Other keys and a missing _source charge nothing, and nothing is charged with tracking off.
        assertEquals(0L, allocatedBytes("return params.other;", params));
        assertEquals(0L, allocatedBytes("return params._source;", Map.of("other", "x")));
        PainlessTestScript off = compile("return params._source.name;", MAX_ALLOCATION_BYTES_DISABLED, params);
        off.execute();
        assertEquals(0L, ((PainlessScript) off).getAllocBytes());
    }

    public void testSourceReadTripsLimit() {
        Map<String, Object> source = parsedSource();
        Map<String, Object> params = Map.of("_source", source);
        assertTripsLimit("return params._source.name;", (AllocationEstimators.parsedSourceBytes(source) - 1) + "b", params);
    }

    public void testChargedParamsChargesEachNewParsedDocumentOnce() {
        // The wrapper tells documents apart by the parsed map object, which the server keeps per document.
        PainlessScript script = (PainlessScript) compile("return null;", "1mb");
        Map<String, Object> first = parsedSource();
        Map<String, Object> second = parsedSource();
        long parsed = AllocationEstimators.parsedSourceBytes(first);
        Map<String, Object> backing = new HashMap<>(Map.of("_source", first));
        ChargedParams params = new ChargedParams(script, backing);
        assertSame(first, params.get("_source"));
        assertSame(first, params.get("_source"));
        assertEquals(parsed, script.getAllocBytes());
        backing.put("_source", second);
        assertSame(second, params.get("_source"));
        assertEquals(2L * parsed, script.getAllocBytes());
        backing.put("_source", null);
        assertNull(params.get("_source"));
        assertEquals(2L * parsed, script.getAllocBytes());
        assertEquals(backing.toString(), params.toString());
    }

    public void testContextWhitelistsWithConstantAnnotationsParse() {
        // StatsSummary (score) and sha1/256/512 (ingest/reindex/update/update_by_query) live in context whitelists that the
        // base test context does not load, so parse them directly to validate their new @allocates annotations.
        WhitelistLoader.loadFromResourceFiles(
            PainlessPlugin.class,
            "org.elasticsearch.script.score.txt",
            "org.elasticsearch.script.ingest.txt",
            "org.elasticsearch.script.reindex.txt",
            "org.elasticsearch.script.update.txt",
            "org.elasticsearch.script.update_by_query.txt"
        );
    }

    /** A keyword field with {@code terms} in one document, already on that document. */
    static KeywordDocValuesField keywordField(String... terms) throws IOException {
        KeywordDocValuesField field = new KeywordDocValuesField(binaryDocValues(terms), "test");
        field.setNextDocId(0);
        return field;
    }

    /** Binary doc values over one document holding {@code terms}. */
    private static SortableBinaryDocValues binaryDocValues(String... terms) {
        BytesRef[] refs = new BytesRef[terms.length];
        for (int i = 0; i < terms.length; ++i) {
            refs[i] = new BytesRef(terms[i]);
        }
        return binaryDocValues(refs);
    }

    /** Binary doc values over one document holding the encoded {@code addresses}. */
    private static SortableBinaryDocValues ipDocValues(String... addresses) {
        BytesRef[] refs = new BytesRef[addresses.length];
        for (int i = 0; i < addresses.length; ++i) {
            refs[i] = new BytesRef(InetAddressPoint.encode(InetAddresses.forString(addresses[i])));
        }
        return binaryDocValues(refs);
    }

    private static SortableBinaryDocValues binaryDocValues(BytesRef[] values) {
        return new SortableBinaryDocValues(null) {
            private int next;

            @Override
            public boolean advanceExact(int doc) {
                next = 0;
                return doc == 0;
            }

            @Override
            public int docValueCount() {
                return values.length;
            }

            @Override
            public BytesRef nextValue() {
                return values[next++];
            }
        };
    }

    /** Numeric doc values over one document holding {@code values}. */
    private static SortedNumericDoubleValues doubleDocValues(double... values) {
        return new SortedNumericDoubleValues(null) {
            private int next;

            @Override
            public boolean advanceExact(int doc) {
                next = 0;
                return doc == 0;
            }

            @Override
            public int docValueCount() {
                return values.length;
            }

            @Override
            public double nextValue() {
                return values[next++];
            }
        };
    }

    /** Numeric doc values over one document holding {@code values}. */
    private static SortedNumericLongValues longDocValues(long... values) {
        return new SortedNumericLongValues(null) {
            private int next;

            @Override
            public boolean advanceExact(int doc) {
                next = 0;
                return doc == 0;
            }

            @Override
            public int docValueCount() {
                return values.length;
            }

            @Override
            public long nextValue() {
                return values[next++];
            }
        };
    }

    /** A geo point field with one point in one document, already on that document. */
    static GeoPointDocValuesField geoPointField(double lat, double lon) throws IOException {
        long encoded = new GeoPoint(lat, lon).getEncoded();
        GeoPointDocValuesField field = new GeoPointDocValuesField(new MultiGeoPointValues(longDocValues(encoded)), "test");
        field.setNextDocId(0);
        return field;
    }
}
