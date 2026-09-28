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
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.index.fielddata.SortableBinaryDocValues;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;
import org.elasticsearch.painless.spi.WhitelistLoader;
import org.elasticsearch.script.field.BinaryDocValuesField;
import org.elasticsearch.script.field.HalfFloatDocValuesField;
import org.elasticsearch.script.field.IpDocValuesField;
import org.elasticsearch.script.field.KeywordDocValuesField;

import java.io.IOException;
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

    /** Reading the five-byte term {@code hello}. */
    private static final long KEYWORD_HELLO_BYTES = 24L + 32L + (32L + 2L * 5L);

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

    /** A keyword read: the byte copy, the {@link BytesRef}, and the new String. Five bytes is 24 + 32 + (32 + 2 * 5) = 98. */
    public void testKeywordReadChargedFromTermLength() throws IOException {
        assertEquals(KEYWORD_HELLO_BYTES, AllocationEstimators.termStringBytes(5));

        Map<String, Object> params = Map.of("field", keywordField("hello").toScriptDocValues());
        assertEquals(KEYWORD_HELLO_BYTES, allocatedBytes("params.field.value", params));
        assertEquals(KEYWORD_HELLO_BYTES, allocatedBytes("params.field.get(0)", params));
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

        assertEquals(AllocationEstimators.termStringBytes("192.168.0.1".length()), allocatedBytes("params.field.value", params));
    }

    /** A BytesRef read is the byte copy and the BytesRef. A binary read adds the buffer that wraps the copy. */
    public void testBytesReadsCharged() throws IOException {
        BinaryDocValuesField field = new BinaryDocValuesField(binaryDocValues("hello"), "test");
        field.setNextDocId(0);
        Map<String, Object> params = Map.of("field", field, "refs", field.toScriptDocValues());

        assertEquals(AllocationEstimators.termCopyBytes(5), allocatedBytes("params.refs.value", params));
        assertEquals(AllocationEstimators.termCopyBytes(5), allocatedBytes("params.refs.get(0)", params));
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
    private static KeywordDocValuesField keywordField(String... terms) throws IOException {
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
}
