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
