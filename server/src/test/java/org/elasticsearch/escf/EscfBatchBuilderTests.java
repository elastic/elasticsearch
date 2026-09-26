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
            builder.beginRow();
            builder.longField("i", 42L);
            builder.longField("l", 10_000_000_000L);
            builder.doubleField("d", 1.5);
            builder.stringField("s", utf8("hello"));
            builder.booleanField("b", true);
            builder.booleanField("f", false);
            builder.finishRow();
            try (EscfBatch batch = builder.build()) {
                assertEquals(1, batch.docCount());
                assertEquals(asMap("{\"i\":42,\"l\":10000000000,\"d\":1.5,\"s\":\"hello\",\"b\":true,\"f\":false}"), reconstruct(batch, 0));
            }
        }
    }

    public void testNestedObjects() throws IOException {
        try (EscfBatchBuilder builder = newBuilder()) {
            builder.beginRow();
            builder.startObject("user");
            builder.stringField("name", utf8("alice"));
            builder.longField("age", 30L);
            builder.endObject();
            builder.stringField("status", utf8("active"));
            builder.finishRow();
            try (EscfBatch batch = builder.build()) {
                assertEquals(asMap("{\"user\":{\"name\":\"alice\",\"age\":30},\"status\":\"active\"}"), reconstruct(batch, 0));
            }
        }
    }

    public void testEmptyObject() throws IOException {
        try (EscfBatchBuilder builder = newBuilder()) {
            builder.beginRow();
            builder.emptyObject("empty");
            builder.longField("x", 1L);
            builder.finishRow();
            try (EscfBatch batch = builder.build()) {
                assertEquals(asMap("{\"empty\":{},\"x\":1}"), reconstruct(batch, 0));
            }
        }
    }

    public void testFixedLongArray() throws IOException {
        long[] longs = new long[] { 1L, 2L, 3L, 4L };
        byte[] packed = SourceBatchEncodeHelper.packFixedArray(SourceValueType.LONG, longs, new Object[4], 4);
        try (EscfBatchBuilder builder = newBuilder()) {
            builder.beginRow();
            builder.arrayField("vals", SourceValueType.FIXED_ARRAY, packed);
            builder.finishRow();
            try (EscfBatch batch = builder.build()) {
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
            builder.beginRow();
            builder.arrayField("vals", SourceValueType.FIXED_ARRAY, packed);
            builder.finishRow();
            try (EscfBatch batch = builder.build()) {
                assertEquals(asMap("{\"vals\":[1.5,2.5,-3.25]}"), reconstruct(batch, 0));
            }
        }
    }

    /**
     * A late-discovered column (first seen in row 1, absent in row 0) must back-fill row 0 with
     * an absent entry so every column builder holds exactly {@code docCount} values.
     */
    public void testLateDiscoveredColumnBackfillsAbsentForPriorRows() throws IOException {
        try (EscfBatchBuilder builder = newBuilder()) {
            // Row 0: "a" present, "b" absent (first seen later)
            builder.beginRow();
            builder.longField("a", 1L);
            builder.finishRow();

            // Row 1: both "a" and "b" present — "b" is discovered here
            builder.beginRow();
            builder.longField("a", 2L);
            builder.stringField("b", utf8("hello"));
            builder.finishRow();

            try (EscfBatch batch = builder.build()) {
                assertEquals(2, batch.docCount());
                // row 0: "b" must be absent (not present), reconstructed map has no "b"
                assertEquals(asMap("{\"a\":1}"), reconstruct(batch, 0));
                assertEquals(asMap("{\"a\":2,\"b\":\"hello\"}"), reconstruct(batch, 1));
            }
        }
    }

    public void testAbsentBackfill() throws IOException {
        try (EscfBatchBuilder builder = newBuilder()) {
            // Row 0: both "a" and "b" present
            builder.beginRow();
            builder.longField("a", 1L);
            builder.stringField("b", utf8("hello"));
            builder.finishRow();

            // Row 1: only "a" present; "b" must be absent (not null) in the batch
            builder.beginRow();
            builder.longField("a", 2L);
            builder.finishRow();

            try (EscfBatch batch = builder.build()) {
                assertEquals(2, batch.docCount());
                assertEquals(asMap("{\"a\":1,\"b\":\"hello\"}"), reconstruct(batch, 0));
                assertEquals(asMap("{\"a\":2}"), reconstruct(batch, 1));
            }
        }
    }

    public void testNullField() throws IOException {
        try (EscfBatchBuilder builder = newBuilder()) {
            builder.beginRow();
            builder.nullField("a");
            builder.longField("b", 5L);
            builder.finishRow();
            try (EscfBatch batch = builder.build()) {
                assertEquals(asMap("{\"a\":null,\"b\":5}"), reconstruct(batch, 0));
            }
        }
    }

    /**
     * Duplicate field name in one row must throw {@link IllegalArgumentException} and must not
     * corrupt the column builders.
     */
    public void testDuplicateFieldInOneRowThrows() {
        try (EscfBatchBuilder builder = newBuilder()) {
            builder.beginRow();
            builder.longField("x", 1L);
            IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> builder.longField("x", 2L));
            assertEquals("Duplicate field [x]", ex.getMessage());
        }
    }

    /**
     * {@link EscfBatchBuilder#abortRow()} seals a partial row as an orphan (absent-fills + increments
     * docCount) so the column builders stay well-formed. The next row is unaffected.
     */
    public void testAbortRowLeavesWellFormedBatch() throws IOException {
        try (EscfBatchBuilder builder = newBuilder()) {
            // Start a row but abort it (simulates a failed SIMD parse).
            builder.beginRow();
            builder.longField("a", 999L);
            builder.abortRow();
            assertEquals(1, builder.docCount()); // orphan row counted

            // Next row is the "real" document.
            builder.beginRow();
            builder.longField("a", 1L);
            builder.stringField("b", utf8("hi"));
            builder.finishRow();
            assertEquals(2, builder.docCount()); // orphan + real

            try (EscfBatch batch = builder.build()) {
                assertEquals(2, batch.docCount());
                // row 1 (the real row) is well-formed
                assertEquals(asMap("{\"a\":1,\"b\":\"hi\"}"), reconstruct(batch, 1));
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
