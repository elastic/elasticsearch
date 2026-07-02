/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.escf;

import org.elasticsearch.sourcebatch.SourceValueType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentString;

import java.nio.charset.StandardCharsets;

/**
 * Unit tests for {@link ElasticsearchColumnBuilder}: kind selection, the lazily-allocated validity
 * (absent) bitset, and fixed→union promotion preserving the accumulated values.
 */
public class ElasticsearchColumnBuilderTests extends ESTestCase {

    public void testLongKindSelectionAndValues() {
        ElasticsearchColumnBuilder b = new ElasticsearchColumnBuilder();
        b.addLong(1);
        b.addLong(2);
        b.addLong(3);
        ElasticsearchColumnData data = b.finish(3);
        assertEquals(ElasticsearchColumnKind.LONG, data.kind());
        assertNull("dense column has no validity bitset", data.absentBitset());
        ElasticsearchColumn col = ElasticsearchColumn.from(0, data);
        assertEquals(1L, col.getLongValue(0));
        assertEquals(2L, col.getLongValue(1));
        assertEquals(3L, col.getLongValue(2));
        assertEquals(SourceValueType.LONG, col.getTypeByte(0));
    }

    public void testValidityBitsetOnlyWhenAbsent() {
        ElasticsearchColumnBuilder b = new ElasticsearchColumnBuilder();
        b.addLong(10);
        b.addAbsent();
        b.addLong(30);
        ElasticsearchColumnData data = b.finish(3);
        assertEquals(ElasticsearchColumnKind.LONG, data.kind());
        assertNotNull("a column with an absent row carries a validity bitset", data.absentBitset());
        ElasticsearchColumn col = ElasticsearchColumn.from(0, data);
        assertFalse(col.isAbsent(0));
        assertTrue(col.isAbsent(1));
        assertFalse(col.isAbsent(2));
        assertEquals(10L, col.getLongValue(0));
        assertEquals(30L, col.getLongValue(2));
    }

    public void testStringKind() {
        ElasticsearchColumnBuilder b = new ElasticsearchColumnBuilder();
        b.addString(utf8("alpha"));
        b.addString(utf8("gamma"));
        ElasticsearchColumnData data = b.finish(2);
        assertEquals(ElasticsearchColumnKind.STRING, data.kind());
        ElasticsearchColumn col = ElasticsearchColumn.from(0, data);
        assertEquals("alpha", col.getStringValue(0).string());
        assertEquals("gamma", col.getStringValue(1).string());
    }

    public void testBoolKind() {
        ElasticsearchColumnBuilder b = new ElasticsearchColumnBuilder();
        b.addBoolean(true);
        b.addBoolean(false);
        b.addBoolean(true);
        ElasticsearchColumnData data = b.finish(3);
        assertEquals(ElasticsearchColumnKind.BOOL, data.kind());
        ElasticsearchColumn col = ElasticsearchColumn.from(0, data);
        assertTrue(col.getBooleanValue(0));
        assertFalse(col.getBooleanValue(1));
        assertTrue(col.getBooleanValue(2));
        assertEquals(SourceValueType.TRUE, col.getTypeByte(0));
        assertEquals(SourceValueType.FALSE, col.getTypeByte(1));
    }

    public void testPromoteOnTypeConflictPreservesValues() {
        ElasticsearchColumnBuilder b = new ElasticsearchColumnBuilder();
        b.addLong(7);
        b.addString(utf8("text"));
        b.addDouble(2.5);
        ElasticsearchColumnData data = b.finish(3);
        assertEquals(ElasticsearchColumnKind.UNION, data.kind());
        ElasticsearchColumn col = ElasticsearchColumn.from(0, data);
        assertEquals(SourceValueType.LONG, col.getTypeByte(0));
        assertEquals(7L, col.getLongValue(0));
        assertEquals(SourceValueType.STRING, col.getTypeByte(1));
        assertEquals("text", col.getStringValue(1).string());
        assertEquals(SourceValueType.DOUBLE, col.getTypeByte(2));
        assertEquals(2.5, col.getDoubleValue(2), 0.0);
    }

    public void testExplicitNullPromotesToUnion() {
        ElasticsearchColumnBuilder b = new ElasticsearchColumnBuilder();
        b.addLong(1);
        b.addNull();
        ElasticsearchColumnData data = b.finish(2);
        assertEquals(ElasticsearchColumnKind.UNION, data.kind());
        ElasticsearchColumn col = ElasticsearchColumn.from(0, data);
        assertEquals(1L, col.getLongValue(0));
        assertTrue(col.isNull(1));
        assertEquals(SourceValueType.NULL, col.getTypeByte(1));
    }

    public void testAllAbsentFinishesAsLong() {
        ElasticsearchColumnBuilder b = new ElasticsearchColumnBuilder();
        b.addAbsent();
        b.addAbsent();
        ElasticsearchColumnData data = b.finish(2);
        assertEquals(ElasticsearchColumnKind.LONG, data.kind());
        ElasticsearchColumn col = ElasticsearchColumn.from(0, data);
        assertTrue(col.isAbsent(0));
        assertTrue(col.isAbsent(1));
        assertEquals(SourceValueType.ABSENT, col.getTypeByte(0));
    }

    private static XContentString.UTF8Bytes utf8(String s) {
        byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
        return new XContentString.UTF8Bytes(bytes, 0, bytes.length);
    }
}
