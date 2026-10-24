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

    /**
     * {@link EscfRowBuffer} remembers which column each leaf resolved to by its position in the row. Rows that
     * repeat a shape, reorder it, add or drop fields, reuse a name under a different parent, or present an
     * equal-but-not-identical name must all land in the right columns.
     */
    public void testLeafColumnsStayCorrectWhenRowShapesVary() throws IOException {
        String a = "a";
        String b = "b";
        String c = "c";
        try (EscfBatchBuilder builder = newBuilder()) {
            int[] shapes = new int[] { 0, 0, 1, 0, 2, 0, 3, 4, 0, 4 };
            String[] expected = new String[] {
                "{\"a\":1,\"b\":2,\"c\":3}",
                "{\"a\":1,\"b\":2,\"c\":3}",
                "{\"c\":3,\"b\":2,\"a\":1}",
                "{\"a\":1,\"b\":2,\"c\":3}",
                "{\"a\":1,\"c\":3}",
                "{\"a\":1,\"b\":2,\"c\":3}",
                "{\"a\":1,\"o\":{\"b\":2},\"c\":3}",
                "{\"a\":1,\"b\":2,\"c\":3}",
                "{\"a\":1,\"b\":2,\"c\":3}",
                "{\"a\":1,\"b\":2,\"c\":3}" };
            for (int shape : shapes) {
                EscfRowBuffer row = builder.beginRow();
                switch (shape) {
                    case 0 -> {
                        row.longField(a, 1);
                        row.longField(b, 2);
                        row.longField(c, 3);
                    }
                    case 1 -> {
                        row.longField(c, 3);
                        row.longField(b, 2);
                        row.longField(a, 1);
                    }
                    case 2 -> {
                        row.longField(a, 1);
                        row.longField(c, 3);
                    }
                    case 3 -> {
                        row.longField(a, 1);
                        row.startObject("o");
                        row.longField(b, 2);
                        row.endObject();
                        row.longField(c, 3);
                    }
                    case 4 -> {
                        // Equal content, different String instances: must resolve to the same columns.
                        row.longField(new String("a"), 1);
                        row.longField(new String("b"), 2);
                        row.longField(new String("c"), 3);
                    }
                    default -> throw new AssertionError(shape);
                }
                row.finishRow();
                builder.commit(0);
            }
            try (EscfBatch batch = builder.buildPartition(0)) {
                assertEquals(shapes.length, batch.docCount());
                for (int i = 0; i < shapes.length; i++) {
                    assertEquals("row " + i, asMap(expected[i]), reconstruct(batch, i));
                }
            }
        }
    }

    /** A duplicate field is still rejected when its column is found through the position memo. */
    public void testDuplicateLeafRejectedWithMemoWarm() throws IOException {
        String a = "a";
        String b = "b";
        try (EscfBatchBuilder builder = newBuilder()) {
            EscfRowBuffer row = builder.beginRow();
            row.longField(a, 1);
            row.longField(b, 2);
            row.finishRow();
            builder.commit(0);

            EscfRowBuffer second = builder.beginRow();
            second.longField(a, 1);
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> second.longField(a, 2));
            assertEquals("Duplicate field [a]", e.getMessage());
        }
    }

    /** Rows with more leaves than the position memo holds still resolve every leaf correctly. */
    public void testRowsWithMoreLeavesThanTheMemoHolds() throws IOException {
        int leaves = 5000;
        String[] names = new String[leaves];
        for (int i = 0; i < leaves; i++) {
            names[i] = "f" + i;
        }
        try (EscfBatchBuilder builder = newBuilder()) {
            for (int r = 0; r < 3; r++) {
                EscfRowBuffer row = builder.beginRow();
                for (int i = 0; i < leaves; i++) {
                    row.longField(names[i], i + r);
                }
                row.finishRow();
                builder.commit(0);
            }
            try (EscfBatch batch = builder.buildPartition(0)) {
                for (int r = 0; r < 3; r++) {
                    Map<String, Object> got = reconstruct(batch, r);
                    assertEquals(leaves, got.size());
                    for (int i = 0; i < leaves; i += 499) {
                        assertEquals(i + r, ((Number) got.get(names[i])).intValue());
                    }
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

    private static Map<String, Object> asMap(String json) {
        return XContentHelper.convertToMap(new BytesArray(json), false, XContentType.JSON).v2();
    }

    private static XContentString.UTF8Bytes utf8(String s) {
        return new XContentString.UTF8Bytes(s.getBytes(StandardCharsets.UTF_8));
    }
}
