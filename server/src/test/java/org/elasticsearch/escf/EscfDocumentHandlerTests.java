/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.escf;

import org.apache.lucene.document.column.ObjectTupleCursor;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.util.ByteUtils;
import org.elasticsearch.common.util.MockPageCacheRecycler;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.simdjson.JsonDocumentParser;
import org.elasticsearch.simdjson.SimdJsonParserPool;
import org.elasticsearch.sourcebatch.SourceBatchEncodeHelper;
import org.elasticsearch.sourcebatch.SourceValueType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.BytesRefRecycler;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentString;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * Unit tests for {@link EscfDocumentHandler} routing and KEY_VALUE integration.
 * Wire-format correctness for KV blobs is covered by {@link org.elasticsearch.sourcebatch.KeyValueWriterTests}.
 */
public class EscfDocumentHandlerTests extends ESTestCase {

    private static byte[] expectedObjectKv(String objectJson) throws IOException {
        try (
            XContentParser parser = XContentHelper.createParserNotCompressed(
                XContentParserConfiguration.EMPTY,
                new BytesArray(objectJson),
                XContentType.JSON
            )
        ) {
            parser.nextToken();
            return SourceBatchEncodeHelper.serializeKeyValue(parser);
        }
    }

    private static byte[] encodeItemsArrayViaHandler(String objectJson) throws IOException {
        EscfBatchBuilder backend = newBackend();
        backend.beginRow();
        EscfDocumentHandler handler = new EscfDocumentHandler(backend);

        handler.startArray("items");
        handler.arrayElemStartObject();
        try (
            XContentParser parser = XContentHelper.createParserNotCompressed(
                XContentParserConfiguration.EMPTY,
                new BytesArray(objectJson),
                XContentType.JSON
            )
        ) {
            parser.nextToken();
            walkObjectFields(parser, handler);
        }
        handler.arrayElemEndObject();
        handler.endArray();
        backend.finishRow();

        try (EscfBatch batch = backend.build()) {
            int colIdx = batch.schema().findLeaf("items", 0);
            assertNotEquals(-1, colIdx);
            EscfColumn col = batch.column(colIdx);
            ObjectTupleCursor<BytesRef> cursor = col.bytesRefCursor(false);
            int r = cursor.nextDoc();
            assertEquals(0, r);
            BytesRef packed = cursor.value();
            return firstKeyValueBytes(Arrays.copyOfRange(packed.bytes, packed.offset, packed.offset + packed.length));
        }
    }

    private static byte[] encodeItemsArrayViaSimdWalk(String doc, String innerObjectJson) throws IOException {
        assumeTrue("simdjson ESCF encoding required", EscfEncoder.isSimdEnabled());
        byte[] bytes = doc.getBytes(StandardCharsets.UTF_8);
        EscfBatchBuilder backend = newBackend();
        backend.beginRow();
        EscfDocumentHandler handler = new EscfDocumentHandler(backend);

        JsonDocumentParser docParser = SimdJsonParserPool.getDefault().forCurrentThread();
        docParser.parseDocument(bytes, 0, bytes.length, handler);
        docParser.publishFieldNames();
        backend.finishRow();

        try (EscfBatch batch = backend.build()) {
            int colIdx = batch.schema().findLeaf("items", 0);
            assertNotEquals(-1, colIdx);
            EscfColumn col = batch.column(colIdx);
            ObjectTupleCursor<BytesRef> cursor = col.bytesRefCursor(false);
            int r = cursor.nextDoc();
            assertEquals(0, r);
            BytesRef packed = cursor.value();
            byte[] actual = firstKeyValueBytes(Arrays.copyOfRange(packed.bytes, packed.offset, packed.offset + packed.length));
            assertArrayEquals(expectedObjectKv(innerObjectJson), actual);
            return actual;
        }
    }

    /** First element of a UNION inline array must be KEY_VALUE; returns its payload bytes. */
    private static byte[] firstKeyValueBytes(byte[] packedUnionArray) {
        assertEquals(SourceValueType.KEY_VALUE, packedUnionArray[0]);
        int len = ByteUtils.readIntLE(packedUnionArray, 1);
        byte[] kv = new byte[len];
        System.arraycopy(packedUnionArray, 5, kv, 0, len);
        return kv;
    }

    private static EscfBatchBuilder newBackend() {
        return new EscfBatchBuilder(new BytesRefRecycler(new MockPageCacheRecycler(org.elasticsearch.common.settings.Settings.EMPTY)));
    }

    private static void walkObjectFields(XContentParser parser, EscfDocumentHandler handler) throws IOException {
        assert parser.currentToken() == XContentParser.Token.START_OBJECT;
        if (parser.nextToken() == XContentParser.Token.END_OBJECT) {
            return;
        }
        walkObjectFieldsContent(parser, handler);
    }

    private static void walkObjectFieldsContent(XContentParser parser, EscfDocumentHandler handler) throws IOException {
        while (parser.currentToken() != XContentParser.Token.END_OBJECT) {
            walkField(parser, handler);
            parser.nextToken();
        }
    }

    private static void walkField(XContentParser parser, EscfDocumentHandler handler) throws IOException {
        if (parser.currentToken() != XContentParser.Token.FIELD_NAME) {
            throw new IllegalStateException("Expected FIELD_NAME but got " + parser.currentToken());
        }
        String name = parser.currentName();
        XContentParser.Token token = parser.nextToken();
        switch (token) {
            case VALUE_STRING -> {
                XContentString.UTF8Bytes str = parser.optimizedText().bytes();
                handler.stringField(name, str.bytes(), str.offset(), str.length());
            }
            case VALUE_NUMBER -> {
                long val = parser.longValue();
                handler.longField(name, val, val >= Integer.MIN_VALUE && val <= Integer.MAX_VALUE, new byte[0], 0, 0);
            }
            case VALUE_BOOLEAN -> handler.booleanField(name, parser.booleanValue(), new byte[0], 0, 0);
            case VALUE_NULL -> handler.nullField(name);
            case START_OBJECT -> {
                handler.startObject(name);
                if (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                    walkObjectFieldsContent(parser, handler);
                }
                handler.endObject();
            }
            case START_ARRAY -> {
                handler.startArray(name);
                walkArrayElements(parser, handler);
                handler.endArray();
            }
            default -> throw new IllegalStateException("Unexpected token " + token);
        }
    }

    private static void walkArrayElements(XContentParser parser, EscfDocumentHandler handler) throws IOException {
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
            switch (token) {
                case VALUE_STRING -> {
                    XContentString.UTF8Bytes str = parser.optimizedText().bytes();
                    handler.arrayElemString(str.bytes(), str.offset(), str.length());
                }
                case VALUE_NUMBER -> {
                    long val = parser.longValue();
                    handler.arrayElemLong(val, val >= Integer.MIN_VALUE && val <= Integer.MAX_VALUE);
                }
                case VALUE_BOOLEAN -> handler.arrayElemBoolean(parser.booleanValue());
                case VALUE_NULL -> handler.arrayElemNull();
                case START_OBJECT -> {
                    handler.arrayElemStartObject();
                    if (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                        walkObjectFieldsContent(parser, handler);
                    }
                    handler.arrayElemEndObject();
                }
                case START_ARRAY -> {
                    handler.arrayElemStartArray();
                    walkArrayElements(parser, handler);
                    handler.arrayElemEndArray();
                }
                default -> throw new IllegalStateException("Unexpected token " + token);
            }
        }
    }

    public void testObjectInArrayKvMatchesHelper() throws IOException {
        String inner = """
            {"a":1,"b":"x"}""";
        assertArrayEquals(expectedObjectKv(inner), encodeItemsArrayViaHandler(inner));
    }

    public void testNestedObjectInArrayKvMatchesHelper() throws IOException {
        String inner = """
            {"outer":{"inner":42}}""";
        assertArrayEquals(expectedObjectKv(inner), encodeItemsArrayViaHandler(inner));
    }

    public void testArrayInsideObjectInArrayKvMatchesHelper() throws IOException {
        String inner = """
            {"tags":["a","b"],"n":1}""";
        assertArrayEquals(expectedObjectKv(inner), encodeItemsArrayViaHandler(inner));
    }

    public void testRootScalarsEncodedToColumns() {
        EscfBatchBuilder backend = newBackend();
        backend.beginRow();
        EscfDocumentHandler handler = new EscfDocumentHandler(backend);

        byte[] hello = "hello".getBytes(StandardCharsets.UTF_8);
        handler.longField("n", 42, true, new byte[] { '4', '2' }, 0, 2);
        handler.stringField("s", hello, 0, hello.length);
        backend.finishRow();

        try (EscfBatch batch = backend.build()) {
            assertEquals("n", batch.schema().getLeafName(0));
            assertEquals("s", batch.schema().getLeafName(1));
            assertEquals(EscfColumnKind.LONG, batch.column(0).kind());
            assertEquals(EscfColumnKind.STRING, batch.column(1).kind());
        }
    }

    public void testSimdWalkObjectInArrayMatchesHelper() throws IOException {
        String inner = """
            {"tags":["a","b"],"n":1}""";
        encodeItemsArrayViaSimdWalk("""
            {"items":[{"tags":["a","b"],"n":1}]}""", inner);
    }
}
