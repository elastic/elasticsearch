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
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.fielddata.MultiGeoPointValues;
import org.elasticsearch.index.fielddata.SortableBinaryDocValues;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;
import org.elasticsearch.index.fielddata.SortedNumericLongValues;
import org.elasticsearch.index.mapper.vectors.DenseVectorScriptDocValues;
import org.elasticsearch.painless.spi.WhitelistLoader;
import org.elasticsearch.script.field.BinaryDocValuesField;
import org.elasticsearch.script.field.DoubleDocValuesField;
import org.elasticsearch.script.field.GeoPointDocValuesField;
import org.elasticsearch.script.field.HalfFloatDocValuesField;
import org.elasticsearch.script.field.IpDocValuesField;
import org.elasticsearch.script.field.KeywordDocValuesField;
import org.elasticsearch.script.field.LongDocValuesField;
import org.elasticsearch.script.field.vectors.BinaryDenseVector;
import org.elasticsearch.script.field.vectors.DenseVector;
import org.elasticsearch.script.field.vectors.KnnDenseVector;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

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

    /**
     * Iterating keyword doc values decodes a new String on every {@code next()}. The loop never sees the script, so the whole
     * document is charged up front with the iterator, sized from the stored bytes: five and six bytes here.
     */
    public void testKeywordIterationChargedUpFront() throws IOException {
        KeywordDocValuesField field = keywordField("hello", "world!");
        Map<String, Object> params = Map.of("docValues", field.toScriptDocValues(), "field", field);
        long decode = AllocationEstimators.termDecodeBytes(5) + AllocationEstimators.termDecodeBytes(6);
        long loop = AllocSizes.ITERATOR_BYTES + decode;

        assertEquals(loop, AllocationEstimators.iteratorBytes(field.toScriptDocValues()));
        assertEquals(loop, AllocationEstimators.iteratorBytes(field));
        assertEquals(loop, AllocationEstimators.streamBytes(field.toScriptDocValues()));
        // A plain list is charged its iterator alone.
        assertEquals(AllocSizes.ITERATOR_BYTES, AllocationEstimators.iteratorBytes(List.of("hello")));
        assertEquals(AllocSizes.ITERATOR_BYTES, AllocationEstimators.streamBytes(List.of("hello")));

        // Through def and typed loops, over the doc values and the field.
        assertEquals(loop, allocatedBytes("int n = 0; for (def s : params.docValues) { n++; } return 'x';", params));
        assertEquals(loop, allocatedBytes("int n = 0; for (def s : params.field) { n++; } return 'x';", params));
        assertEquals(
            loop,
            allocatedBytes(
                "ScriptDocValues.Strings f = (ScriptDocValues.Strings) params.docValues;"
                    + " int n = 0; for (String s : f) { n++; } return 'x';",
                params
            )
        );
        assertEquals(loop, allocatedBytes("Field f = (Field) params.field; int n = 0; for (def s : f) { n++; } return 'x';", params));
        // An explicit iterator() or stream() call costs the same.
        assertEquals(loop, allocatedBytes("params.docValues.iterator(); return 'x';", params));
        assertEquals(loop, allocatedBytes("params.docValues.stream(); return 'x';", params));
        assertEquals(loop, allocatedBytes("List l = (List) params.docValues; l.stream(); return 'x';", params));
        // A loop that stops early still pays for the whole document.
        assertEquals(loop, allocatedBytes("int n = 0; for (def s : params.docValues) { n++; if (n > 0) { break; } } return 'x';", params));
        // The loop alone is past a 60 byte limit.
        assertTripsLimit("int n = 0; for (def s : params.docValues) { n++; } return 'x';", "60b", params);
    }

    /** Ip doc values format text per value and the ip field hands out addresses, so each loop is charged its own decode. */
    public void testIpIterationChargedUpFront() throws IOException {
        IpDocValuesField field = new IpDocValuesField(ipDocValues("192.168.0.1", "10.0.0.1"), "test");
        field.setNextDocId(0);
        Map<String, Object> params = Map.of("docValues", field.toScriptDocValues(), "field", field);
        long text = AllocSizes.ITERATOR_BYTES + 2L * AllocationEstimators.ipStringReadBytes(45);
        long addresses = AllocSizes.ITERATOR_BYTES + 2L * AllocationEstimators.ipAddressReadBytes(field, null);

        assertEquals(text, allocatedBytes("int n = 0; for (def s : params.docValues) { n++; } return 'x';", params));
        assertEquals(text, allocatedBytes("params.docValues.stream(); return 'x';", params));
        assertEquals(addresses, allocatedBytes("int n = 0; for (def a : params.field) { n++; } return 'x';", params));
    }

    /** BytesRef doc values copy the bytes per value and the binary field wraps the copy in a buffer. */
    public void testBytesIterationChargedUpFront() throws IOException {
        BinaryDocValuesField field = new BinaryDocValuesField(binaryDocValues("hello", "world!"), "test");
        field.setNextDocId(0);
        Map<String, Object> params = Map.of("refs", field.toScriptDocValues(), "field", field);
        long refs = AllocSizes.ITERATOR_BYTES + AllocationEstimators.termCopyBytes(5) + AllocationEstimators.termCopyBytes(6);
        long buffers = AllocSizes.ITERATOR_BYTES + AllocationEstimators.byteBufferBytes(5) + AllocationEstimators.byteBufferBytes(6);

        assertEquals(refs, allocatedBytes("int n = 0; for (def b : params.refs) { n++; } return 'x';", params));
        assertEquals(refs, allocatedBytes("params.refs.stream(); return 'x';", params));
        assertEquals(buffers, allocatedBytes("int n = 0; for (def b : params.field) { n++; } return 'x';", params));
    }

    /**
     * The helpers that walk their receiver decode every element over doc values, so each charges the document up front. The
     * same call over a plain list of the same strings differs by exactly that decode, whatever else the helper builds.
     */
    public void testReceiverWalkingHelpersChargeTheDecode() throws IOException {
        KeywordDocValuesField field = keywordField("hello", "world!");
        Map<String, Object> docValues = Map.of("r", field.toScriptDocValues());
        Map<String, Object> list = Map.of("r", new ArrayList<>(List.of("hello", "world!")));
        long decode = AllocationEstimators.decodeBytes(field.toScriptDocValues());
        assertTrue(decode > 0);

        String[] calls = new String[] {
            "r.forEach(x -> x.length())",
            "r.any(x -> x == 'z')",
            "r.count(x -> true)",
            "r.each(x -> x.length())",
            "r.eachWithIndex((x, i) -> x.length())",
            "r.every(x -> x != null)",
            "r.findResults(x -> x)",
            "r.groupBy(x -> x)",
            "r.join(',')",
            "try { r.sum() } catch (Exception e) {}",
            "r.sum(x -> x.length())",
            "r.removeIf(x -> false)",
            "r.collect(x -> x)",
            "r.collect(new ArrayList(), x -> x)",
            "r.find(x -> x == 'z')",
            "r.findAll(x -> true)",
            "r.findResult(x -> null)",
            "r.findResult('d', x -> null)",
            "r.split(x -> true)",
            "try { r.replaceAll(x -> x) } catch (Exception e) {}" };
        for (String call : calls) {
            String script = "def r = params.r; " + call + (call.endsWith("}") ? " " : "; ") + "return 'x';";
            assertEquals(call, decode, allocatedBytes(script, docValues) - allocatedBytes(script, list));
        }

        // asCollection() and asList() hand a list back as it is, so over doc values they decode nothing.
        assertEquals(0L, allocatedBytes("def r = params.r; r.asCollection(); return 'x';", docValues));
        assertEquals(0L, allocatedBytes("def r = params.r; r.asList(); return 'x';", docValues));
        // The field is not a list, so there they copy, and the copy decodes every element.
        Iterable<String> plain = () -> List.of("hello", "world!").iterator();
        assertEquals(decode, AllocationEstimators.asListBytes(field) - AllocationEstimators.asListBytes(plain));
        assertEquals(decode, AllocationEstimators.asCollectionBytes(field) - AllocationEstimators.asCollectionBytes(plain));
    }

    /** {@code getVector()} copies the vector out of the index. The stored float form hands out its decoded array instead. */
    public void testDenseVectorCopyCharged() {
        float[] vector = new float[] { 1f, 2f, 3f };
        long copy = AllocSizes.arrayBytes(3, Float.BYTES);
        Map<String, Object> params = Map.of("vec", new KnnDenseVector(vector));

        assertEquals(copy, AllocationEstimators.denseVectorCopyBytes(new KnnDenseVector(vector)));
        assertEquals(
            0L,
            AllocationEstimators.denseVectorCopyBytes(new BinaryDenseVector(vector, new BytesRef(), 3, IndexVersion.current()))
        );
        assertEquals(0L, AllocationEstimators.denseVectorCopyBytes(DenseVector.EMPTY));
        assertEquals(copy, allocatedBytes("params.vec.getVector(); return 'x';", params));
        assertEquals(copy, allocatedBytes("DenseVector v = (DenseVector) params.vec; v.getVector(); return 'x';", params));

        // The doc values read makes the same copy of whatever vector the supplier holds, so the stored float form is free there too.
        DenseVectorScriptDocValues docValues = denseVectorDocValues(new KnnDenseVector(vector));
        assertEquals(copy, AllocationEstimators.denseVectorCopyBytes(docValues));
        assertEquals(copy, allocatedBytes("params.dv.getVectorValue(); return 'x';", Map.of("dv", docValues)));
        DenseVectorScriptDocValues stored = denseVectorDocValues(new BinaryDenseVector(vector, new BytesRef(), 3, IndexVersion.current()));
        assertEquals(0L, AllocationEstimators.denseVectorCopyBytes(stored));
        assertEquals(0L, allocatedBytes("params.dv.getVectorValue(); return 'x';", Map.of("dv", stored)));
    }

    private static DenseVectorScriptDocValues denseVectorDocValues(DenseVector vector) {
        return new DenseVectorScriptDocValues(new DenseVectorScriptDocValues.DenseVectorSupplier() {
            @Override
            public void setNextDocId(int docId) {}

            @Override
            public DenseVector getInternal() {
                return vector;
            }

            @Override
            public int size() {
                return 1;
            }
        }, vector.getDims());
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
