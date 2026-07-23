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
import org.elasticsearch.eirf.EirfRowToXContent;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.BytesRefRecycler;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Round-trip tests for {@link EscfEncoder}: encode JSON documents into an {@link EscfBatch}, then
 * reconstruct each row's source via {@link EirfRowToXContent} and assert it matches the original
 * (compared as parsed maps, so key order and numeric width are irrelevant). Covers scalars, nested
 * objects, fixed arrays, arrays of objects (union/inline), explicit null, absent fields, booleans, and
 * heterogeneous columns that promote to a union.
 */
public class EscfEncoderTests extends ESTestCase {

    public void testScalars() throws IOException {
        assertRoundTrip("""
            {"i":42,"l":10000000000,"d":1.5,"s":"hello","b":true,"f":false}""");
    }

    public void testNestedObjects() throws IOException {
        assertRoundTrip("""
            {"user":{"name":"alice","age":30},"status":"active"}""");
    }

    public void testEmptyObject() throws IOException {
        assertRoundTrip("""
            {"empty":{},"x":1}""");
    }

    public void testEmptyObjectDistinctFromAbsent() throws IOException {
        // doc 0 has "obj" as an empty object; doc 1 doesn't have "obj" at all. These must not collapse
        // into the same (absent) representation.
        assertRoundTrip("""
            {"obj":{}}""", """
            {"other":1}""");
    }

    public void testEmptyObjectAndNestedObjectAcrossDocs() throws IOException {
        // "obj" is an empty object in doc 0 and a real nested object (with its own subfield leaf) in doc 1.
        assertRoundTrip("""
            {"obj":{}}""", """
            {"obj":{"k":1}}""");
    }

    public void testFixedLongArray() throws IOException {
        assertRoundTrip("""
            {"vals":[1,2,3,4]}""");
    }

    public void testFixedDoubleArray() throws IOException {
        assertRoundTrip("""
            {"vals":[1.5,2.5,-3.25]}""");
    }

    public void testFixedStringArray() throws IOException {
        assertRoundTrip("""
            {"tags":["a","bb","ccc"]}""");
    }

    public void testArrayOfObjectsGoesToUnion() throws IOException {
        assertRoundTrip("""
            {"items":[{"x":1},{"y":"two"}]}""");
    }

    public void testHeterogeneousArrayGoesToUnion() throws IOException {
        assertRoundTrip("""
            {"mixed":[1,"two",3.5,true]}""");
    }

    public void testExplicitNull() throws IOException {
        assertRoundTrip("""
            {"a":null,"b":5}""");
    }

    public void testEmptyArray() throws IOException {
        assertRoundTrip("""
            {"empty":[],"x":1}""");
    }

    public void testHeterogeneousColumnAcrossDocs() throws IOException {
        // "a" is a long in doc 0, a string in doc 1, absent in doc 2 -> promotes to a union column.
        assertRoundTrip("""
            {"a":1,"keep":true}""", """
            {"a":"text","keep":false}""", """
            {"keep":true}""");
    }

    public void testAbsentAndMixedArrayKinds() throws IOException {
        // "vals" is a long array in doc 0, a string array in doc 1 (different child kind -> union),
        // and absent in doc 2.
        assertRoundTrip("""
            {"vals":[1,2,3]}""", """
            {"vals":["x","y"]}""", """
            {"other":9}""");
    }

    /**
     * The column payloads are held as their native, possibly-paged {@link BytesReference}; when a column's
     * data exceeds one recycler page (16 KiB) it is a multi-page {@code CompositeBytesReference}. These
     * cases force that layout so the column views' page-straddling reads ({@code getLongLE} over a slot
     * that crosses a page boundary, and a single value's {@code slice(..).toBytesRef()}) are exercised.
     */
    public void testMultiPageNumericColumn() throws IOException {
        // 8 bytes/doc; 3000 docs -> 24 KiB long column -> multiple pages, with 8-byte slots straddling a boundary.
        List<String> docs = new ArrayList<>();
        for (int i = 0; i < 3000; i++) {
            docs.add("{\"n\":" + (1_000_000_000L + i) + "}");
        }
        assertRoundTrip(docs.toArray(new String[0]));
    }

    public void testMultiPageStringValueStraddlesPage() throws IOException {
        // A single string value larger than one page, so its bytes span multiple pages and the value slice straddles.
        String big = "x".repeat(40_000);
        assertRoundTrip("{\"s\":\"" + big + "\"}");
    }

    public void testMultiPageFixedArrayColumn() throws IOException {
        // A fixed long array whose child data (8 bytes/element) exceeds one page, straddling the array child column.
        StringBuilder sb = new StringBuilder("{\"vals\":[");
        for (int i = 0; i < 5000; i++) {
            if (i > 0) {
                sb.append(',');
            }
            sb.append(i);
        }
        sb.append("]}");
        assertRoundTrip(sb.toString());
    }

    public void testManyRandomDocs() throws IOException {
        List<String> docs = new ArrayList<>();
        for (int i = 0; i < 50; i++) {
            docs.add(switch (i % 5) {
                case 0 -> "{\"n\":" + i + ",\"t\":\"d" + i + "\"}";
                case 1 -> "{\"n\":" + i + ",\"arr\":[" + i + "," + (i + 1) + "]}";
                case 2 -> "{\"t\":\"only\",\"nested\":{\"k\":" + i + "}}";
                case 3 -> "{\"flag\":" + (i % 2 == 0) + "}";
                default -> "{\"n\":" + i + ".5}";
            });
        }
        assertRoundTrip(docs.toArray(new String[0]));
    }

    public void testAbsentBitsetNarrowerThanDocCount() throws IOException {
        // Regression: a column absent only in an early doc but present in every trailing doc has an absent
        // bitset sized to that early doc (one word), which is narrower than docCount. Reading a high-index
        // present doc must not index past the bitset. Use > 64 docs so the single-word bitset is exceeded.
        List<String> docs = new ArrayList<>();
        docs.add("{\"other\":0}"); // doc 0: "v" absent -> "v" absent bitset stays 64 bits wide
        for (int i = 1; i < 70; i++) {
            docs.add("{\"v\":" + i + "}"); // docs 1..69: "v" present, well past bit 64
        }
        assertRoundTrip(docs.toArray(new String[0]));
    }

    private static void assertRoundTrip(String... jsonDocs) throws IOException {
        List<BytesReference> sources = new ArrayList<>(jsonDocs.length);
        for (String doc : jsonDocs) {
            sources.add(new BytesArray(doc));
        }
        try (EscfBatch batch = encode(sources)) {
            assertEquals(jsonDocs.length, batch.docCount());
            for (int i = 0; i < jsonDocs.length; i++) {
                Map<String, Object> expected = asMap(jsonDocs[i]);
                Map<String, Object> actual = reconstruct(batch, i);
                assertEquals("row " + i, expected, actual);
            }
        }
    }

    /**
     * Encodes {@code sources} into a single-partition batch via a {@link MockPageCacheRecycler}-backed
     * encoder so the batch's column buffers are leak-tracked. Mirrors {@link EscfEncoder#encode} but with
     * a recycling encoder; the encoder is closed here (its builders were already consumed by
     * {@link EscfEncoder#buildPartition}), and the returned batch owns and releases the pages when closed.
     */
    private static EscfBatch encode(List<BytesReference> sources) throws IOException {
        Recycler<BytesRef> recycler = new BytesRefRecycler(new MockPageCacheRecycler(Settings.EMPTY));
        try (EscfEncoder encoder = new EscfEncoder(recycler)) {
            for (BytesReference source : sources) {
                encoder.addDocument(source, XContentType.JSON, 0);
            }
            return encoder.buildPartition(0);
        }
    }

    private static Map<String, Object> reconstruct(EscfBatch batch, int row) throws IOException {
        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            EirfRowToXContent.writeRow(batch.row(row), batch.schema(), builder);
            return XContentHelper.convertToMap(BytesReference.bytes(builder), false, XContentType.JSON).v2();
        }
    }

    private static Map<String, Object> asMap(String json) {
        return XContentHelper.convertToMap(new BytesArray(json), false, XContentType.JSON).v2();
    }
}
