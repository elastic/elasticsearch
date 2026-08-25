/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.escf;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.util.ByteUtils;
import org.elasticsearch.simdjson.JsonDocumentHandler;
import org.elasticsearch.sourcebatch.LeafSink;
import org.elasticsearch.sourcebatch.SourceValueType;
import org.elasticsearch.xcontent.XContentString;

import java.io.IOException;
import java.math.BigInteger;
import java.util.Arrays;

/**
 * Implements {@link JsonDocumentHandler} by delegating to {@link EscfRowBuffer}
 * for column storage and to {@link LeafSink} for routing/extraction callbacks.
 *
 * <p>Array elements are accumulated into temporary buffers and packed into
 * the ESCF inline binary format on {@link #endArray()}.
 *
 * <p>Nested objects within arrays are serialized into KEY_VALUE binary format.
 *
 * <p><strong>Not thread-safe.</strong> One instance per document walk.
 */
final class EscfDocumentHandler implements JsonDocumentHandler {

    private static final int ARRAY_INIT_CAP = 16;

    private final EscfRowBuffer row;
    private final EscfBatchBuilder backend;
    private final LeafSink sink;
    private final boolean rawTextMode;
    private final boolean firePathSink;

    // ---- Array accumulation state ----
    private byte[] elemTypes;
    private long[] elemNumeric;
    private Object[] elemVar;
    private int elemCount;
    private boolean forceUnion;
    private String arrayFieldName;

    // Stack for nested arrays within arrays
    private byte[][] nestedElemTypes;
    private long[][] nestedElemNumeric;
    private Object[][] nestedElemVar;
    private int[] nestedElemCounts;
    private boolean[] nestedForceUnion;
    private int arrayDepth;

    // KV serialization for nested objects within arrays
    private BytesStreamOutput kvOut;
    private int kvDepth;
    private BytesStreamOutput[] kvOutStack;
    private int kvOutStackDepth;
    /** True while serializing an inline array field value inside a KEY_VALUE blob. */
    private boolean kvInlineArrayBuild;
    /** {@link #arrayDepth} at which the current {@link #kvInlineArrayBuild} started. */
    private int kvInlineArrayDepth;

    EscfDocumentHandler(EscfRowBuffer row, EscfBatchBuilder backend, LeafSink sink, boolean rawTextMode) {
        this.row = row;
        this.backend = backend;
        this.sink = sink;
        this.rawTextMode = rawTextMode;
        this.firePathSink = sink != LeafSink.NO_OP;
    }

    // ---- Object field events ----

    @Override
    public void startObject(String fieldName) {
        if (kvDepth > 0) {
            writeKvStartObject(fieldName);
            return;
        }
        row.startObject(fieldName);
    }

    @Override
    public void endObject() {
        if (kvDepth > 0) {
            writeKvEndObject();
            return;
        }
        row.endObject();
    }

    @Override
    public void emptyObject(String fieldName) {
        if (kvDepth > 0) {
            writeKvEmptyObject(fieldName);
            return;
        }
        row.emptyObject(fieldName);
    }

    @Override
    public void stringField(String fieldName, byte[] buf, int off, int len) {
        if (kvDepth > 0) {
            writeKvStringField(fieldName, buf, off, len);
            return;
        }
        int colIdx = row.stringField(fieldName, buf, off, len);
        if (firePathSink) {
            sink.onTextPrimitive(colIdx, backend.columnPath(colIdx), SourceValueType.STRING, new XContentString.UTF8Bytes(buf, off, len));
        }
    }

    @Override
    public void longField(String fieldName, long value, boolean fitsInt, byte[] srcBuf, int srcOff, int srcLen) {
        if (kvDepth > 0) {
            writeKvLongField(fieldName, value, fitsInt);
            return;
        }
        byte type = fitsInt ? SourceValueType.INT : SourceValueType.LONG;
        int colIdx = row.longField(fieldName, value);
        if (rawTextMode) {
            sink.onTextPrimitive(colIdx, backend.columnPath(colIdx), type, new XContentString.UTF8Bytes(srcBuf, srcOff, srcLen));
        } else if (firePathSink) {
            sink.onLongPrimitive(colIdx, backend.columnPath(colIdx), type, value);
        }
    }

    @Override
    public void bigIntegerField(String fieldName, BigInteger value, byte[] srcBuf, int srcOff, int srcLen) {
        if (kvDepth > 0) {
            writeKvStringField(fieldName, srcBuf, srcOff, srcLen);
            return;
        }
        int colIdx = row.stringField(fieldName, srcBuf, srcOff, srcLen);
        if (firePathSink) {
            sink.onTextPrimitive(
                colIdx,
                backend.columnPath(colIdx),
                SourceValueType.STRING,
                new XContentString.UTF8Bytes(srcBuf, srcOff, srcLen)
            );
        }
    }

    @Override
    public void doubleField(String fieldName, double value, boolean fitsFloat, byte[] srcBuf, int srcOff, int srcLen) {
        if (kvDepth > 0) {
            writeKvDoubleField(fieldName, value, fitsFloat);
            return;
        }
        byte type = fitsFloat ? SourceValueType.FLOAT : SourceValueType.DOUBLE;
        int colIdx = row.doubleField(fieldName, value);
        if (rawTextMode) {
            sink.onTextPrimitive(colIdx, backend.columnPath(colIdx), type, new XContentString.UTF8Bytes(srcBuf, srcOff, srcLen));
        } else if (firePathSink) {
            sink.onDoublePrimitive(colIdx, backend.columnPath(colIdx), type, value);
        }
    }

    @Override
    public void booleanField(String fieldName, boolean value, byte[] srcBuf, int srcOff, int srcLen) {
        if (kvDepth > 0) {
            writeKvBooleanField(fieldName, value);
            return;
        }
        int colIdx = row.booleanField(fieldName, value);
        if (rawTextMode) {
            byte type = value ? SourceValueType.TRUE : SourceValueType.FALSE;
            sink.onTextPrimitive(colIdx, backend.columnPath(colIdx), type, new XContentString.UTF8Bytes(srcBuf, srcOff, srcLen));
        } else if (firePathSink) {
            sink.onBooleanPrimitive(colIdx, backend.columnPath(colIdx), value);
        }
    }

    @Override
    public void nullField(String fieldName) {
        if (kvDepth > 0) {
            writeKvNullField(fieldName);
            return;
        }
        row.nullField(fieldName);
    }

    // ---- Array events ----

    @Override
    public void startArray(String fieldName) {
        if (kvDepth > 0) {
            writeKvStartArray(fieldName);
            return;
        }
        if (arrayDepth > 0) {
            pushArrayState();
        }
        arrayFieldName = fieldName;
        elemTypes = new byte[ARRAY_INIT_CAP];
        elemNumeric = new long[ARRAY_INIT_CAP];
        elemVar = new Object[ARRAY_INIT_CAP];
        elemCount = 0;
        forceUnion = false;
        arrayDepth++;
    }

    @Override
    public void endArray() {
        if (kvInlineArrayBuild && arrayDepth == kvInlineArrayDepth) {
            writeKvEndArray();
            return;
        }
        byte[] packed;
        byte arrayType;

        boolean useFixed = false;
        byte sharedType = 0;
        if (forceUnion == false && elemCount > 0) {
            sharedType = elemTypes[0];
            useFixed = true;
            for (int i = 1; i < elemCount; i++) {
                if (elemTypes[i] != sharedType) {
                    useFixed = false;
                    break;
                }
            }
            if (useFixed && SourceValueType.elemDataSize(sharedType) == 0) {
                useFixed = false;
            }
        }

        if (useFixed) {
            packed = packFixedArray(sharedType, elemNumeric, elemVar, elemCount);
            arrayType = SourceValueType.FIXED_ARRAY;
        } else {
            packed = packUnionArray(elemTypes, elemNumeric, elemVar, elemCount);
            arrayType = SourceValueType.UNION_ARRAY;
        }
        Arrays.fill(elemVar, 0, elemCount, null);

        arrayDepth--;

        if (arrayDepth > 0) {
            byte savedArrayType = arrayType;
            byte[] savedPacked = packed;
            popArrayState();
            ensureArrayCapacity();
            elemTypes[elemCount] = savedArrayType;
            elemVar[elemCount] = savedPacked;
            forceUnion = true;
            elemCount++;
        } else {
            row.arrayField(arrayFieldName, arrayType, packed);
        }
    }

    @Override
    public void arrayElemString(byte[] buf, int off, int len) {
        ensureArrayCapacity();
        elemTypes[elemCount] = SourceValueType.STRING;
        elemVar[elemCount] = new XContentString.UTF8Bytes(buf, off, len);
        elemCount++;
    }

    @Override
    public void arrayElemLong(long value, boolean fitsInt) {
        ensureArrayCapacity();
        if (fitsInt) {
            elemTypes[elemCount] = SourceValueType.INT;
            elemNumeric[elemCount] = value;
        } else {
            elemTypes[elemCount] = SourceValueType.LONG;
            elemNumeric[elemCount] = value;
        }
        elemCount++;
    }

    @Override
    public void arrayElemBigInteger(BigInteger value) {
        ensureArrayCapacity();
        byte[] text = value.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8);
        elemTypes[elemCount] = SourceValueType.STRING;
        elemVar[elemCount] = new XContentString.UTF8Bytes(text, 0, text.length);
        elemCount++;
    }

    @Override
    public void arrayElemDouble(double value, boolean fitsFloat) {
        ensureArrayCapacity();
        if (fitsFloat) {
            elemTypes[elemCount] = SourceValueType.FLOAT;
            elemNumeric[elemCount] = Float.floatToRawIntBits((float) value);
        } else {
            elemTypes[elemCount] = SourceValueType.DOUBLE;
            elemNumeric[elemCount] = Double.doubleToRawLongBits(value);
        }
        elemCount++;
    }

    @Override
    public void arrayElemBoolean(boolean value) {
        ensureArrayCapacity();
        elemTypes[elemCount] = value ? SourceValueType.TRUE : SourceValueType.FALSE;
        elemCount++;
    }

    @Override
    public void arrayElemNull() {
        ensureArrayCapacity();
        elemTypes[elemCount] = SourceValueType.NULL;
        elemCount++;
    }

    @Override
    public void arrayElemStartObject() {
        if (kvDepth > 0) {
            ensureKvOutStackCapacity();
            kvOutStack[kvOutStackDepth++] = kvOut;
        }
        ensureArrayCapacity();
        kvOut = new BytesStreamOutput(64);
        kvDepth = kvDepth == 0 ? 1 : kvDepth + 1;
    }

    @Override
    public void arrayElemEndObject() {
        if (kvDepth > 1) {
            byte[] nested = BytesReference.toBytes(kvOut.bytes());
            kvOut = kvOutStack[--kvOutStackDepth];
            kvDepth--;
            ensureArrayCapacity();
            elemTypes[elemCount] = SourceValueType.KEY_VALUE;
            elemVar[elemCount] = nested;
            forceUnion = true;
            elemCount++;
            return;
        }
        elemTypes[elemCount] = SourceValueType.KEY_VALUE;
        elemVar[elemCount] = BytesReference.toBytes(kvOut.bytes());
        forceUnion = true;
        elemCount++;
        kvOut = null;
        kvDepth = 0;
        kvOutStackDepth = 0;
    }

    @Override
    public void arrayElemStartArray() {
        pushArrayState();
        elemTypes = new byte[ARRAY_INIT_CAP];
        elemNumeric = new long[ARRAY_INIT_CAP];
        elemVar = new Object[ARRAY_INIT_CAP];
        elemCount = 0;
        forceUnion = false;
        arrayDepth++;
    }

    @Override
    public void arrayElemEndArray() {
        endArray();
    }

    // ---- Array capacity ----

    private void ensureArrayCapacity() {
        if (elemCount >= elemTypes.length) {
            int newCap = elemTypes.length * 2;
            elemTypes = Arrays.copyOf(elemTypes, newCap);
            elemNumeric = Arrays.copyOf(elemNumeric, newCap);
            elemVar = Arrays.copyOf(elemVar, newCap);
        }
    }

    private void pushArrayState() {
        if (nestedElemTypes == null) {
            nestedElemTypes = new byte[4][];
            nestedElemNumeric = new long[4][];
            nestedElemVar = new Object[4][];
            nestedElemCounts = new int[4];
            nestedForceUnion = new boolean[4];
        }
        int depth = arrayDepth - 1;
        if (depth >= nestedElemTypes.length) {
            int newCap = nestedElemTypes.length * 2;
            nestedElemTypes = Arrays.copyOf(nestedElemTypes, newCap);
            nestedElemNumeric = Arrays.copyOf(nestedElemNumeric, newCap);
            nestedElemVar = Arrays.copyOf(nestedElemVar, newCap);
            nestedElemCounts = Arrays.copyOf(nestedElemCounts, newCap);
            nestedForceUnion = Arrays.copyOf(nestedForceUnion, newCap);
        }
        nestedElemTypes[depth] = elemTypes;
        nestedElemNumeric[depth] = elemNumeric;
        nestedElemVar[depth] = elemVar;
        nestedElemCounts[depth] = elemCount;
        nestedForceUnion[depth] = forceUnion;
    }

    private void popArrayState() {
        int depth = arrayDepth - 1;
        elemTypes = nestedElemTypes[depth];
        elemNumeric = nestedElemNumeric[depth];
        elemVar = nestedElemVar[depth];
        elemCount = nestedElemCounts[depth];
        forceUnion = nestedForceUnion[depth];
        nestedElemTypes[depth] = null;
        nestedElemNumeric[depth] = null;
        nestedElemVar[depth] = null;
    }

    // ---- Array packing (mirrors SourceBatchEncodeHelper) ----

    private static byte[] packUnionArray(byte[] types, long[] numeric, Object[] var, int count) {
        int size = 0;
        for (int i = 0; i < count; i++) {
            size += 1 + elemDataSize(types[i], var[i]);
        }
        byte[] packed = new byte[size];
        int pos = 0;
        for (int i = 0; i < count; i++) {
            packed[pos++] = types[i];
            pos = writeElemData(packed, pos, types[i], numeric[i], var[i]);
        }
        return packed;
    }

    private static byte[] packFixedArray(byte sharedType, long[] numeric, Object[] var, int count) {
        int size = 1;
        for (int i = 0; i < count; i++) {
            size += elemDataSize(sharedType, var[i]);
        }
        byte[] packed = new byte[size];
        packed[0] = sharedType;
        int pos = 1;
        for (int i = 0; i < count; i++) {
            pos = writeElemData(packed, pos, sharedType, numeric[i], var[i]);
        }
        return packed;
    }

    private static int elemDataSize(byte type, Object varData) {
        return switch (type) {
            case SourceValueType.INT, SourceValueType.FLOAT -> 4;
            case SourceValueType.LONG, SourceValueType.DOUBLE -> 8;
            case SourceValueType.STRING -> {
                XContentString.UTF8Bytes str = (XContentString.UTF8Bytes) varData;
                yield 4 + (str != null ? str.length() : 0);
            }
            case SourceValueType.KEY_VALUE, SourceValueType.UNION_ARRAY, SourceValueType.FIXED_ARRAY -> {
                byte[] bytes = (byte[]) varData;
                yield 4 + bytes.length;
            }
            default -> 0;
        };
    }

    private static int writeElemData(byte[] packed, int pos, byte type, long numeric, Object var) {
        switch (type) {
            case SourceValueType.INT, SourceValueType.FLOAT -> {
                ByteUtils.writeIntLE((int) numeric, packed, pos);
                pos += 4;
            }
            case SourceValueType.LONG, SourceValueType.DOUBLE -> {
                ByteUtils.writeLongLE(numeric, packed, pos);
                pos += 8;
            }
            case SourceValueType.STRING -> {
                XContentString.UTF8Bytes str = (XContentString.UTF8Bytes) var;
                int len = str.length();
                ByteUtils.writeIntLE(len, packed, pos);
                pos += 4;
                System.arraycopy(str.bytes(), str.offset(), packed, pos, len);
                pos += len;
            }
            case SourceValueType.KEY_VALUE, SourceValueType.UNION_ARRAY, SourceValueType.FIXED_ARRAY -> {
                byte[] bytes = (byte[]) var;
                ByteUtils.writeIntLE(bytes.length, packed, pos);
                pos += 4;
                System.arraycopy(bytes, 0, packed, pos, bytes.length);
                pos += bytes.length;
            }
        }
        return pos;
    }

    // ---- KV serialization helpers (for nested objects within arrays) ----

    private void writeKvKey(String fieldName) {
        try {
            byte[] keyBytes = fieldName.getBytes(java.nio.charset.StandardCharsets.UTF_8);
            kvOut.writeIntLE(keyBytes.length);
            kvOut.writeBytes(keyBytes, 0, keyBytes.length);
        } catch (IOException e) {
            throw new org.elasticsearch.simdjson.JsonParsingException("IO error serializing key: " + e.getMessage());
        }
    }

    private void writeKvStringField(String fieldName, byte[] buf, int off, int len) {
        writeKvKey(fieldName);
        writeKvStringValue(buf, off, len);
    }

    private void writeKvStringValue(byte[] buf, int off, int len) {
        try {
            kvOut.writeByte(SourceValueType.STRING);
            kvOut.writeIntLE(len);
            kvOut.writeBytes(buf, off, len);
        } catch (IOException e) {
            throw new org.elasticsearch.simdjson.JsonParsingException("IO error serializing string: " + e.getMessage());
        }
    }

    private void writeKvLongField(String fieldName, long value, boolean fitsInt) {
        writeKvKey(fieldName);
        writeKvLongValue(value, fitsInt);
    }

    private void writeKvLongValue(long value, boolean fitsInt) {
        try {
            if (fitsInt) {
                kvOut.writeByte(SourceValueType.INT);
                kvOut.writeIntLE((int) value);
            } else {
                kvOut.writeByte(SourceValueType.LONG);
                kvOut.writeLongLE(value);
            }
        } catch (IOException e) {
            throw new org.elasticsearch.simdjson.JsonParsingException("IO error serializing long: " + e.getMessage());
        }
    }

    private void writeKvDoubleField(String fieldName, double value, boolean fitsFloat) {
        writeKvKey(fieldName);
        writeKvDoubleValue(value, fitsFloat);
    }

    private void writeKvDoubleValue(double value, boolean fitsFloat) {
        try {
            if (fitsFloat) {
                kvOut.writeByte(SourceValueType.FLOAT);
                kvOut.writeIntLE(Float.floatToRawIntBits((float) value));
            } else {
                kvOut.writeByte(SourceValueType.DOUBLE);
                kvOut.writeLongLE(Double.doubleToRawLongBits(value));
            }
        } catch (IOException e) {
            throw new org.elasticsearch.simdjson.JsonParsingException("IO error serializing double: " + e.getMessage());
        }
    }

    private void writeKvBooleanField(String fieldName, boolean value) {
        writeKvKey(fieldName);
        writeKvBooleanValue(value);
    }

    private void writeKvBooleanValue(boolean value) {
        kvOut.writeByte(value ? SourceValueType.TRUE : SourceValueType.FALSE);
    }

    private void writeKvNullField(String fieldName) {
        writeKvKey(fieldName);
        writeKvNullValue();
    }

    private void writeKvNullValue() {
        kvOut.writeByte(SourceValueType.NULL);
    }

    private void writeKvStartObject(String fieldName) {
        writeKvKey(fieldName);
        writeKvStartObjectValue();
    }

    private void writeKvStartObjectValue() {
        ensureKvOutStackCapacity();
        kvOutStack[kvOutStackDepth++] = kvOut;
        kvOut = new BytesStreamOutput(64);
        kvDepth++;
    }

    private void writeKvEndObject() {
        kvDepth--;
        byte[] nested = BytesReference.toBytes(kvOut.bytes());
        kvOut = kvOutStack[--kvOutStackDepth];
        try {
            kvOut.writeByte(SourceValueType.KEY_VALUE);
            kvOut.writeIntLE(nested.length);
            kvOut.writeBytes(nested, 0, nested.length);
        } catch (IOException e) {
            throw new org.elasticsearch.simdjson.JsonParsingException("IO error serializing nested object: " + e.getMessage());
        }
    }

    private void ensureKvOutStackCapacity() {
        if (kvOutStack == null) {
            kvOutStack = new BytesStreamOutput[4];
        } else if (kvOutStackDepth >= kvOutStack.length) {
            kvOutStack = Arrays.copyOf(kvOutStack, kvOutStack.length * 2);
        }
    }

    private void writeKvEmptyObject(String fieldName) {
        writeKvKey(fieldName);
        try {
            kvOut.writeByte(SourceValueType.KEY_VALUE);
            kvOut.writeIntLE(0);
        } catch (IOException e) {
            throw new org.elasticsearch.simdjson.JsonParsingException("IO error serializing empty object: " + e.getMessage());
        }
    }

    private void writeKvStartArray(String fieldName) {
        writeKvKey(fieldName);
        writeKvStartArrayValue();
    }

    private void writeKvStartArrayValue() {
        pushArrayState();
        elemTypes = new byte[ARRAY_INIT_CAP];
        elemNumeric = new long[ARRAY_INIT_CAP];
        elemVar = new Object[ARRAY_INIT_CAP];
        elemCount = 0;
        forceUnion = false;
        arrayDepth++;
        kvInlineArrayBuild = true;
        kvInlineArrayDepth = arrayDepth;
    }

    private void writeKvEndArray() {
        byte[] packed;
        byte arrayType;

        boolean useFixed = false;
        byte sharedType = 0;
        if (forceUnion == false && elemCount > 0) {
            sharedType = elemTypes[0];
            useFixed = true;
            for (int i = 1; i < elemCount; i++) {
                if (elemTypes[i] != sharedType) {
                    useFixed = false;
                    break;
                }
            }
            if (useFixed && SourceValueType.elemDataSize(sharedType) == 0) {
                useFixed = false;
            }
        }

        if (useFixed) {
            packed = packFixedArray(sharedType, elemNumeric, elemVar, elemCount);
            arrayType = SourceValueType.FIXED_ARRAY;
        } else {
            packed = packUnionArray(elemTypes, elemNumeric, elemVar, elemCount);
            arrayType = SourceValueType.UNION_ARRAY;
        }
        Arrays.fill(elemVar, 0, elemCount, null);

        arrayDepth--;
        popArrayState();

        try {
            kvOut.writeByte(arrayType);
            kvOut.writeIntLE(packed.length);
            kvOut.writeBytes(packed, 0, packed.length);
        } catch (IOException e) {
            throw new org.elasticsearch.simdjson.JsonParsingException("IO error serializing array: " + e.getMessage());
        }
        kvInlineArrayBuild = false;
        kvInlineArrayDepth = 0;
    }
}
