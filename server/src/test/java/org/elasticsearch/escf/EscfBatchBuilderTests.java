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
import org.elasticsearch.sourcebatch.SourceBatchEncodeHelper;
import org.elasticsearch.sourcebatch.SourceRowToXContent;
import org.elasticsearch.sourcebatch.SourceValueType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.BytesRefRecycler;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentString;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class EscfBatchBuilderTests extends ESTestCase {

    public void testScalars() throws IOException {
        try (EscfBatchBuilder builder = newBuilder()) {
            EscfRowBuffer row = builder.beginRow();
            row.longField("i", 42L);
            row.longField("l", 10_000_000_000L);
            row.doubleField("d", 1.5);
            row.stringField("s", utf8("hello"));
            row.booleanField("b", true);
            row.booleanField("f", false);
            row.finishRow();
            builder.commit(0);
            try (EscfBatch batch = builder.buildPartition(0)) {
                assertEquals(1, batch.docCount());
                assertEquals(asMap("{\"i\":42,\"l\":10000000000,\"d\":1.5,\"s\":\"hello\",\"b\":true,\"f\":false}"), reconstruct(batch, 0));
            }
        }
    }

    public void testNestedObjects() throws IOException {
        try (EscfBatchBuilder builder = newBuilder()) {
            EscfRowBuffer row = builder.beginRow();
            row.startObject("user");
            row.stringField("name", utf8("alice"));
            row.longField("age", 30L);
            row.endObject();
            row.stringField("status", utf8("active"));
            row.finishRow();
            builder.commit(0);
            try (EscfBatch batch = builder.buildPartition(0)) {
                assertEquals(asMap("{\"user\":{\"name\":\"alice\",\"age\":30},\"status\":\"active\"}"), reconstruct(batch, 0));
            }
        }
    }

    public void testEmptyObject() throws IOException {
        try (EscfBatchBuilder builder = newBuilder()) {
            EscfRowBuffer row = builder.beginRow();
            row.emptyObject("empty");
            row.longField("x", 1L);
            row.finishRow();
            builder.commit(0);
            try (EscfBatch batch = builder.buildPartition(0)) {
                assertEquals(asMap("{\"empty\":{},\"x\":1}"), reconstruct(batch, 0));
            }
        }
    }

    public void testFixedLongArray() throws IOException {
        long[] longs = new long[] { 1L, 2L, 3L, 4L };
        byte[] packed = SourceBatchEncodeHelper.packFixedArray(SourceValueType.LONG, longs, new Object[4], 4);
        try (EscfBatchBuilder builder = newBuilder()) {
            EscfRowBuffer row = builder.beginRow();
            row.arrayField("vals", SourceValueType.FIXED_ARRAY, packed);
            row.finishRow();
            builder.commit(0);
            try (EscfBatch batch = builder.buildPartition(0)) {
                assertEquals(asMap("{\"vals\":[1,2,3,4]}"), reconstruct(batch, 0));
            }
        }
    }

    public void testFixedDoubleArray() throws IOException {
        long[] numerics = new long[] {
            Double.doubleToRawLongBits(1.5),
            Double.doubleToRawLongBits(2.5),
            Double.doubleToRawLongBits(-3.25) };
        byte[] packed = SourceBatchEncodeHelper.packFixedArray(SourceValueType.DOUBLE, numerics, new Object[3], 3);
        try (EscfBatchBuilder builder = newBuilder()) {
            EscfRowBuffer row = builder.beginRow();
            row.arrayField("vals", SourceValueType.FIXED_ARRAY, packed);
            row.finishRow();
            builder.commit(0);
            try (EscfBatch batch = builder.buildPartition(0)) {
                assertEquals(asMap("{\"vals\":[1.5,2.5,-3.25]}"), reconstruct(batch, 0));
            }
        }
    }

    public void testAbsentBackfill() throws IOException {
        try (EscfBatchBuilder builder = newBuilder()) {
            // Row 0: both "a" and "b" present
            EscfRowBuffer row = builder.beginRow();
            row.longField("a", 1L);
            row.stringField("b", utf8("hello"));
            row.finishRow();
            builder.commit(0);

            // Row 1: only "a" present; "b" must be absent (not null) in the batch
            row = builder.beginRow();
            row.longField("a", 2L);
            row.finishRow();
            builder.commit(0);

            try (EscfBatch batch = builder.buildPartition(0)) {
                assertEquals(2, batch.docCount());
                assertEquals(asMap("{\"a\":1,\"b\":\"hello\"}"), reconstruct(batch, 0));
                assertEquals(asMap("{\"a\":2}"), reconstruct(batch, 1));
            }
        }
    }

    public void testMultiplePartitions() throws IOException {
        try (EscfBatchBuilder builder = newBuilder()) {
            EscfRowBuffer row = builder.beginRow();
            row.longField("x", 10L);
            row.finishRow();
            builder.commit(0);

            row = builder.beginRow();
            row.longField("x", 20L);
            row.finishRow();
            builder.commit(1);

            row = builder.beginRow();
            row.longField("x", 30L);
            row.finishRow();
            builder.commit(0);

            assertEquals(2, builder.docCount(0));
            assertEquals(1, builder.docCount(1));

            try (EscfBatch batch0 = builder.buildPartition(0)) {
                assertEquals(2, batch0.docCount());
                assertEquals(asMap("{\"x\":10}"), reconstruct(batch0, 0));
                assertEquals(asMap("{\"x\":30}"), reconstruct(batch0, 1));
            }
            try (EscfBatch batch1 = builder.buildPartition(1)) {
                assertEquals(1, batch1.docCount());
                assertEquals(asMap("{\"x\":20}"), reconstruct(batch1, 0));
            }
        }
    }

    public void testNullField() throws IOException {
        try (EscfBatchBuilder builder = newBuilder()) {
            EscfRowBuffer row = builder.beginRow();
            row.nullField("a");
            row.longField("b", 5L);
            row.finishRow();
            builder.commit(0);
            try (EscfBatch batch = builder.buildPartition(0)) {
                assertEquals(asMap("{\"a\":null,\"b\":5}"), reconstruct(batch, 0));
            }
        }
    }

    /**
     * Mirrors the Jackson fallback after a failed SIMD parse: {@code beginRow()} without
     * {@code finishRow()} leaves the row unstaged; a subsequent {@code beginRow()} resets scratch
     * so the completed row does not inherit partial field values.
     */
    public void testBeginRowResetsUnfinishedRow() throws IOException {
        try (EscfBatchBuilder builder = newBuilder()) {
            EscfRowBuffer row = builder.beginRow();
            row.longField("a", 999L);
            row.longField("b", 888L);
            assertFalse(row.isStarted());
            expectThrows(IllegalStateException.class, () -> builder.commit(0));

            row = builder.beginRow();
            row.longField("a", 1L);
            row.longField("b", 2L);
            row.finishRow();
            builder.commit(0);

            try (EscfBatch batch = builder.buildPartition(0)) {
                assertEquals(1, batch.docCount());
                assertEquals(asMap("{\"a\":1,\"b\":2}"), reconstruct(batch, 0));
            }
        }
    }

    /** Rows of one shape, so every field after the first row is predicted correctly. */
    public void testRepeatedShapeRoundTrips() throws IOException {
        assertRoundTrip("""
            {"a":1, "obj":{"x":2, "y":3.5}, "b":"s1", "d":0.1}""", """
            {"a":4, "obj":{"x":5, "y":6.5}, "b":"s2", "d":0.2}""", """
            {"a":7, "obj":{"x":8, "y":9.5}, "b":"s3", "d":0.3}""", """
            {"a":10, "obj":{"x":11, "y":12.5}, "b":"s4", "d":0.4}""");
    }

    /** Fields written in a different order than the schema lists them, so the predicted field is wrong every time. */
    public void testReorderedFieldsRoundTrip() throws IOException {
        assertRoundTrip("""
            {"a":1, "b":2, "c":3}""", """
            {"b":4, "a":5, "c":6}""", """
            {"c":7, "b":8, "a":9}""", """
            {"a":10, "b":11, "c":12}""");
    }

    /**
     * A leaf name shared by the root and a nested object. After the root {@code name} the next prediction is
     * {@code obj.name}, which has the right name but the wrong parent, and must not be taken.
     */
    public void testSameLeafNameUnderDifferentParents() throws IOException {
        assertRoundTrip("""
            {"name":"top1", "obj":{"name":"inner1"}}""", """
            {"obj":{"name":"inner2"}, "name":"top2"}""", """
            {"name":"top3"}""", """
            {"obj":{"name":"inner4"}}""", """
            {"name":"top5", "obj":{"name":"inner5"}}""");
    }

    /**
     * The schema lists {@code a, obj.x, b}. A row without {@code obj} predicts {@code obj.x} for {@code b}, which has
     * the wrong name and parent.
     */
    public void testOmittedNestedObjectBetweenFields() throws IOException {
        assertRoundTrip("""
            {"a":1, "obj":{"x":2}, "b":3}""", """
            {"a":4, "b":5}""", """
            {"b":6, "a":7}""", """
            {"a":8, "obj":{"x":9}, "b":10}""");
    }

    /**
     * Rows that start at different fields than the schema's first one, after rows that ended deep in the schema.
     * Predictions start over at every row, so no row may be affected by where the previous one stopped.
     */
    public void testRowsStartAtDifferentFields() throws IOException {
        assertRoundTrip("""
            {"a":1, "b":2, "c":3, "obj":{"x":4}}""", """
            {"obj":{"x":5}}""", """
            {"c":6}""", """
            {"c":7, "a":8}""", """
            {"b":9, "obj":{"x":10}, "a":11}""");
    }

    /** Nested objects reordered, repeated under two parents, and empty, so the predicted non-leaf is wrong or has another parent. */
    public void testNestedObjectPredictions() throws IOException {
        assertRoundTrip("""
            {"o1":{"a":1}, "o2":{"a":2}}""", """
            {"o2":{"a":3}, "o1":{"a":4}}""", """
            {"p":{"inner":{"v":5}}, "q":{"inner":{"v":6}}}""", """
            {"q":{"inner":{"v":7}}, "p":{"inner":{"v":8}}}""", """
            {"q":{"inner":{"v":9}}}""", """
            {"e":{}, "x":10}""", """
            {"x":11, "e":{}}""");
    }

    /**
     * New fields appear after the schema has been established: in the middle of a known shape, where the predicted
     * index holds another field, and at the end of the schema, where the predicted index is past it.
     */
    public void testSchemaGrowsMidBatch() throws IOException {
        assertRoundTrip("""
            {"a":1, "b":2, "c":3}""", """
            {"a":4, "new":5, "b":6, "c":7}""", """
            {"a":8, "b":9, "c":10, "new":11}""", """
            {"a":12, "new":13, "b":14, "c":15}""", """
            {"a":16, "b":17, "c":18, "last":19}""");
    }

    /** A field written twice in a row is rejected, however the prediction for it turns out. */
    public void testDuplicateFieldIsRejected() throws IOException {
        // Empty schema: the field is added by the first write and found by the second.
        try (EscfBatchBuilder builder = newBuilder()) {
            EscfRowBuffer row = builder.beginRow();
            row.longField("a", 1L);
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> row.longField("a", 2L));
            assertEquals("Duplicate field [a]", e.getMessage());
        }
        // Known schema: after "a" the predicted field is "b", so the duplicate is found by the lookup.
        // Likewise for a duplicate that is not adjacent.
        for (boolean adjacent : new boolean[] { true, false }) {
            try (EscfBatchBuilder builder = newBuilder()) {
                EscfRowBuffer first = builder.beginRow();
                first.longField("a", 1L);
                first.longField("b", 2L);
                first.finishRow();
                builder.commit(0);

                EscfRowBuffer row = builder.beginRow();
                row.longField("a", 3L);
                if (adjacent == false) {
                    row.longField("b", 4L);
                }
                IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> row.longField("a", 5L));
                assertEquals("Duplicate field [a]", e.getMessage());
            }
        }
    }

    /**
     * Rows that repeat a shape, drop fields, reorder fields, or reuse a field name under another parent must all come
     * back as written: the row buffer's guess at which schema field comes next may be wrong, but never changes the result.
     * The cases above pin the interesting shapes; this one mixes them at random.
     */
    public void testRowsWithChangingShapesRoundTrip() throws IOException {
        String[] top = { "a", "b", "c", "d", "name", "id" };
        String[] nested = { "name", "id", "x" };
        int rows = randomIntBetween(50, 200);
        List<Map<String, Object>> expected = new ArrayList<>();
        try (EscfBatchBuilder builder = newBuilder()) {
            for (int r = 0; r < rows; r++) {
                // Per row: whether to keep the usual order, drop some fields, or shuffle.
                int mode = randomIntBetween(0, 3);
                List<String> topNames = new ArrayList<>(Arrays.asList(top));
                List<String> nestedNames = new ArrayList<>(Arrays.asList(nested));
                if (mode == 1) {
                    topNames.removeIf(n -> randomBoolean());
                    nestedNames.removeIf(n -> randomBoolean());
                } else if (mode == 2) {
                    Collections.shuffle(topNames, random());
                    Collections.shuffle(nestedNames, random());
                } else if (mode == 3) {
                    Collections.reverse(topNames);
                }
                boolean withNested = mode != 1 || randomBoolean();
                Map<String, Object> doc = new LinkedHashMap<>();
                EscfRowBuffer row = builder.beginRow();
                int insertNestedAt = randomIntBetween(0, topNames.size());
                for (int i = 0; i <= topNames.size(); i++) {
                    if (i == insertNestedAt && withNested && nestedNames.isEmpty() == false) {
                        Map<String, Object> inner = new LinkedHashMap<>();
                        row.startObject("obj");
                        for (String n : nestedNames) {
                            long v = randomLong();
                            row.longField(n, v);
                            inner.put(n, v);
                        }
                        row.endObject();
                        doc.put("obj", inner);
                    }
                    if (i < topNames.size()) {
                        String n = topNames.get(i);
                        long v = randomLong();
                        row.longField(n, v);
                        doc.put(n, v);
                    }
                }
                row.finishRow();
                builder.commit(0);
                expected.add(normalize(doc));
            }
            try (EscfBatch batch = builder.buildPartition(0)) {
                assertEquals(rows, batch.docCount());
                for (int r = 0; r < rows; r++) {
                    assertEquals("row " + r, expected.get(r), reconstruct(batch, r));
                }
            }
        }
    }

    private static EscfBatchBuilder newBuilder() {
        Recycler<BytesRef> recycler = new BytesRefRecycler(new MockPageCacheRecycler(Settings.EMPTY));
        return new EscfBatchBuilder(recycler);
    }

    private static Map<String, Object> reconstruct(EscfBatch batch, int row) throws IOException {
        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            SourceRowToXContent.writeRow(batch.row(row), batch.schema(), builder);
            return XContentHelper.convertToMap(BytesReference.bytes(builder), false, XContentType.JSON).v2();
        }
    }

    /**
     * Writes each JSON document as a row, in the order its fields appear, to one partition and checks that every row
     * reconstructs to the same document.
     */
    private static void assertRoundTrip(String... docs) throws IOException {
        try (EscfBatchBuilder builder = newBuilder()) {
            for (String doc : docs) {
                EscfRowBuffer row = builder.beginRow();
                writeFields(row, asOrderedMap(doc));
                row.finishRow();
                builder.commit(0);
            }
            try (EscfBatch batch = builder.buildPartition(0)) {
                assertEquals(docs.length, batch.docCount());
                for (int r = 0; r < docs.length; r++) {
                    assertEquals("row " + r, asMap(docs[r]), reconstruct(batch, r));
                }
            }
        }
    }

    /**
     * Writes the entries of a parsed document: integers as longs, doubles, strings, and maps as nested (or empty)
     * objects. Any other value type fails the test rather than being written as something else.
     */
    private static void writeFields(EscfRowBuffer row, Map<?, ?> fields) {
        for (Map.Entry<?, ?> entry : fields.entrySet()) {
            String name = (String) entry.getKey();
            Object value = entry.getValue();
            if (value instanceof Map<?, ?> nested) {
                if (nested.isEmpty()) {
                    row.emptyObject(name);
                } else {
                    row.startObject(name);
                    writeFields(row, nested);
                    row.endObject();
                }
            } else if (value instanceof Double number) {
                row.doubleField(name, number);
            } else if (value instanceof Integer || value instanceof Long) {
                row.longField(name, ((Number) value).longValue());
            } else if (value instanceof String string) {
                row.stringField(name, utf8(string));
            } else {
                throw new AssertionError("unsupported test value [" + value + "] for [" + name + "]");
            }
        }
    }

    /**
     * Round-trips an expected document through JSON so its numbers get the same {@code Integer}/{@code Long} typing the
     * parser gives {@link #reconstruct}: a {@code Long} within the int range would otherwise never equal the {@code Integer}.
     */
    private static Map<String, Object> normalize(Map<String, Object> doc) throws IOException {
        try (XContentBuilder builder = JsonXContent.contentBuilder().map(doc)) {
            return XContentHelper.convertToMap(BytesReference.bytes(builder), false, XContentType.JSON).v2();
        }
    }

    private static Map<String, Object> asOrderedMap(String json) {
        return XContentHelper.convertToMap(new BytesArray(json), true, XContentType.JSON).v2();
    }

    private static Map<String, Object> asMap(String json) {
        return XContentHelper.convertToMap(new BytesArray(json), false, XContentType.JSON).v2();
    }

    private static XContentString.UTF8Bytes utf8(String s) {
        return new XContentString.UTF8Bytes(s.getBytes(StandardCharsets.UTF_8));
    }
}
