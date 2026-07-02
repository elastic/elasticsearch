/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.sourcebatch;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.eirf.EirfEncoder;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;

public class KeyValueReaderTests extends ESTestCase {

    private static byte[] kvPayload(String json) throws IOException {
        try (
            XContentParser parser = XContentHelper.createParserNotCompressed(
                XContentParserConfiguration.EMPTY,
                new BytesArray(json),
                XContentType.JSON
            )
        ) {
            parser.nextToken(); // START_OBJECT
            return EirfEncoder.serializeKeyValue(parser);
        }
    }

    public void testEmpty() throws IOException {
        KeyValueReader kv = new KeyValueReader(kvPayload("{}"));
        assertFalse(kv.next());
    }

    public void testSingleIntEntry() throws IOException {
        KeyValueReader kv = new KeyValueReader(kvPayload("{\"x\": 42}"));
        assertTrue(kv.next());
        assertEquals("x", kv.key());
        assertEquals(SourceValueType.INT, kv.type());
        assertEquals(42, kv.intValue());
        assertFalse(kv.next());
    }

    public void testSingleFloatEntry() throws IOException {
        // 1.5 is exactly representable as float so the encoder chooses FLOAT over DOUBLE
        KeyValueReader kv = new KeyValueReader(kvPayload("{\"pi\": 1.5}"));
        assertTrue(kv.next());
        assertEquals("pi", kv.key());
        assertEquals(SourceValueType.FLOAT, kv.type());
        assertEquals(1.5f, kv.floatValue(), 0.0f);
        assertFalse(kv.next());
    }

    public void testSingleLongEntry() throws IOException {
        // Value outside int range forces LONG encoding
        KeyValueReader kv = new KeyValueReader(kvPayload("{\"ts\": 10000000000}"));
        assertTrue(kv.next());
        assertEquals("ts", kv.key());
        assertEquals(SourceValueType.LONG, kv.type());
        assertEquals(10_000_000_000L, kv.longValue());
        assertFalse(kv.next());
    }

    public void testSingleDoubleEntry() throws IOException {
        // 3.14 is not exactly representable as float so the encoder chooses DOUBLE
        KeyValueReader kv = new KeyValueReader(kvPayload("{\"d\": 3.14}"));
        assertTrue(kv.next());
        assertEquals("d", kv.key());
        assertEquals(SourceValueType.DOUBLE, kv.type());
        assertEquals(3.14, kv.doubleValue(), 0.0);
        assertFalse(kv.next());
    }

    public void testSingleStringEntry() throws IOException {
        KeyValueReader kv = new KeyValueReader(kvPayload("{\"name\": \"hello\"}"));
        assertTrue(kv.next());
        assertEquals("name", kv.key());
        assertEquals(SourceValueType.STRING, kv.type());
        assertEquals("hello", kv.stringValue());
        assertFalse(kv.next());
    }

    public void testBooleanEntries() throws IOException {
        KeyValueReader kv = new KeyValueReader(kvPayload("{\"a\": true, \"b\": false}"));

        assertTrue(kv.next());
        assertEquals("a", kv.key());
        assertEquals(SourceValueType.TRUE, kv.type());

        assertTrue(kv.next());
        assertEquals("b", kv.key());
        assertEquals(SourceValueType.FALSE, kv.type());

        assertFalse(kv.next());
    }

    public void testNullEntry() throws IOException {
        KeyValueReader kv = new KeyValueReader(kvPayload("{\"missing\": null}"));
        assertTrue(kv.next());
        assertEquals("missing", kv.key());
        assertEquals(SourceValueType.NULL, kv.type());
        assertFalse(kv.next());
    }

    public void testMultipleEntries() throws IOException {
        KeyValueReader kv = new KeyValueReader(kvPayload("{\"a\": 1, \"b\": \"two\", \"c\": true}"));

        assertTrue(kv.next());
        assertEquals("a", kv.key());
        assertEquals(1, kv.intValue());

        assertTrue(kv.next());
        assertEquals("b", kv.key());
        assertEquals("two", kv.stringValue());

        assertTrue(kv.next());
        assertEquals("c", kv.key());
        assertEquals(SourceValueType.TRUE, kv.type());

        assertFalse(kv.next());
    }

    public void testSkipsUnconsumedVariableLengthValues() throws IOException {
        // next() must advance past variable-length data even when the value accessor is not called
        KeyValueReader kv = new KeyValueReader(kvPayload("{\"a\": \"skipped\", \"b\": 99}"));

        assertTrue(kv.next()); // reads "a" / STRING — intentionally skip calling stringValue()
        assertTrue(kv.next()); // must land on "b" at the correct offset
        assertEquals("b", kv.key());
        assertEquals(99, kv.intValue());
        assertFalse(kv.next());
    }

    public void testWithOffset() throws IOException {
        byte[] payload = kvPayload("{\"k\": 7}");
        byte[] withPrefix = new byte[10 + payload.length];
        System.arraycopy(payload, 0, withPrefix, 10, payload.length);

        KeyValueReader kv = new KeyValueReader(withPrefix, 10, payload.length);
        assertTrue(kv.next());
        assertEquals("k", kv.key());
        assertEquals(7, kv.intValue());
        assertFalse(kv.next());
    }

    public void testNestedKeyValue() throws IOException {
        KeyValueReader kv = new KeyValueReader(kvPayload("{\"outer\": {\"inner\": 100}}"));
        assertTrue(kv.next());
        assertEquals("outer", kv.key());
        assertEquals(SourceValueType.KEY_VALUE, kv.type());

        KeyValueReader nested = kv.nestedKeyValue();
        assertTrue(nested.next());
        assertEquals("inner", nested.key());
        assertEquals(SourceValueType.INT, nested.type());
        assertEquals(100, nested.intValue());
        assertFalse(nested.next());

        assertFalse(kv.next());
    }

    public void testNestedFixedArray() throws IOException {
        // All-int array encodes as FIXED_ARRAY
        KeyValueReader kv = new KeyValueReader(kvPayload("{\"nums\": [10, 20, 30]}"));
        assertTrue(kv.next());
        assertEquals("nums", kv.key());
        assertEquals(SourceValueType.FIXED_ARRAY, kv.type());

        InlineArrayReader arr = kv.nestedArray();
        assertTrue(arr.next());
        assertEquals(10, arr.intValue());
        assertTrue(arr.next());
        assertEquals(20, arr.intValue());
        assertTrue(arr.next());
        assertEquals(30, arr.intValue());
        assertFalse(arr.next());

        assertFalse(kv.next());
    }

    public void testNestedUnionArray() throws IOException {
        // Mixed-type array encodes as UNION_ARRAY
        KeyValueReader kv = new KeyValueReader(kvPayload("{\"mixed\": [42, \"hello\"]}"));
        assertTrue(kv.next());
        assertEquals("mixed", kv.key());
        assertEquals(SourceValueType.UNION_ARRAY, kv.type());

        InlineArrayReader arr = kv.nestedArray();
        assertTrue(arr.next());
        assertEquals(SourceValueType.INT, arr.type());
        assertEquals(42, arr.intValue());
        assertTrue(arr.next());
        assertEquals(SourceValueType.STRING, arr.type());
        assertEquals("hello", arr.stringValue());
        assertFalse(arr.next());

        assertFalse(kv.next());
    }

    public void testEntryAfterNestedCompound() throws IOException {
        // Verifies that entries following a compound value are still reachable
        KeyValueReader kv = new KeyValueReader(kvPayload("{\"kv\": {\"i\": 1}, \"after\": 2}"));
        assertTrue(kv.next());
        assertEquals("kv", kv.key());
        // intentionally skip reading the nested KV
        assertTrue(kv.next());
        assertEquals("after", kv.key());
        assertEquals(2, kv.intValue());
        assertFalse(kv.next());
    }
}
