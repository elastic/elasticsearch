/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.escf;

import org.elasticsearch.simdjson.JsonDocumentHandler;
import org.elasticsearch.sourcebatch.KeyValueWriter;
import org.elasticsearch.sourcebatch.LeafSink;
import org.elasticsearch.sourcebatch.SourceBatchEncodeHelper;
import org.elasticsearch.sourcebatch.SourceBatchEncodeHelper.PackedArray;
import org.elasticsearch.sourcebatch.SourceValueType;
import org.elasticsearch.xcontent.XContentString;

import java.math.BigInteger;
import java.util.Arrays;

/**
 * Implements {@link JsonDocumentHandler} by delegating to {@link EscfRowBuffer}
 * for column storage and to {@link LeafSink} for routing/extraction callbacks.
 *
 * <p>Array elements are accumulated into temporary buffers and packed via
 * {@link SourceBatchEncodeHelper}. Nested objects within arrays are serialized
 * through {@link KeyValueWriter}.
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
    private KeyValueWriter kvWriter;
    private int kvDepth;
    private KeyValueWriter[] kvWriterStack;
    private int kvWriterStackDepth;
    /** True while serializing an inline array field value inside a KEY_VALUE blob. */
    private boolean kvInlineArrayBuild;
    /** {@link #arrayDepth} at which the current {@link #kvInlineArrayBuild} started. */
    private int kvInlineArrayDepth;
    /** Field name for a deferred {@link KeyValueWriter#writeArrayField} call. */
    private String pendingKvArrayFieldName;

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
            kvWriter.beginObjectField(fieldName);
            kvDepth++;
            return;
        }
        row.startObject(fieldName);
    }

    @Override
    public void endObject() {
        if (kvDepth > 0) {
            kvWriter.endObjectField();
            kvDepth--;
            return;
        }
        row.endObject();
    }

    @Override
    public void emptyObject(String fieldName) {
        if (kvDepth > 0) {
            kvWriter.writeEmptyObjectField(fieldName);
            return;
        }
        row.emptyObject(fieldName);
    }

    @Override
    public void stringField(String fieldName, byte[] buf, int off, int len) {
        if (kvDepth > 0) {
            kvWriter.writeStringField(fieldName, buf, off, len);
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
            if (fitsInt) {
                kvWriter.writeIntField(fieldName, (int) value);
            } else {
                kvWriter.writeLongField(fieldName, value);
            }
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
            kvWriter.writeStringField(fieldName, srcBuf, srcOff, srcLen);
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
            if (fitsFloat) {
                kvWriter.writeFloatField(fieldName, (float) value);
            } else {
                kvWriter.writeDoubleField(fieldName, value);
            }
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
            kvWriter.writeBooleanField(fieldName, value);
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
            kvWriter.writeNullField(fieldName);
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
        initArrayAccumulators();
        arrayDepth++;
    }

    @Override
    public void endArray() {
        if (kvInlineArrayBuild && arrayDepth == kvInlineArrayDepth) {
            writeKvEndArray();
            return;
        }
        finishArrayAccumulation();
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
            ensureKvWriterStackCapacity();
            kvWriterStack[kvWriterStackDepth++] = kvWriter;
        }
        ensureArrayCapacity();
        kvWriter = KeyValueWriter.forObjectPayload();
        kvDepth = kvDepth == 0 ? 1 : kvDepth + 1;
    }

    @Override
    public void arrayElemEndObject() {
        byte[] nested = kvWriter.toBytes();
        if (kvDepth > 1) {
            kvWriter = kvWriterStack[--kvWriterStackDepth];
            kvDepth--;
            ensureArrayCapacity();
            elemTypes[elemCount] = SourceValueType.KEY_VALUE;
            elemVar[elemCount] = nested;
            forceUnion = true;
            elemCount++;
            return;
        }
        elemTypes[elemCount] = SourceValueType.KEY_VALUE;
        elemVar[elemCount] = nested;
        forceUnion = true;
        elemCount++;
        kvWriter = null;
        kvDepth = 0;
        kvWriterStackDepth = 0;
    }

    @Override
    public void arrayElemStartArray() {
        pushArrayState();
        initArrayAccumulators();
        arrayDepth++;
    }

    @Override
    public void arrayElemEndArray() {
        endArray();
    }

    // ---- Array capacity ----

    private void initArrayAccumulators() {
        elemTypes = new byte[ARRAY_INIT_CAP];
        elemNumeric = new long[ARRAY_INIT_CAP];
        elemVar = new Object[ARRAY_INIT_CAP];
        elemCount = 0;
        forceUnion = false;
    }

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

    private void finishArrayAccumulation() {
        PackedArray packed = SourceBatchEncodeHelper.packAccumulatedElements(elemTypes, elemNumeric, elemVar, elemCount, forceUnion);
        Arrays.fill(elemVar, 0, elemCount, null);

        arrayDepth--;

        if (arrayDepth > 0) {
            popArrayState();
            ensureArrayCapacity();
            elemTypes[elemCount] = packed.arrayType();
            elemVar[elemCount] = packed.packed();
            forceUnion = true;
            elemCount++;
        } else {
            row.arrayField(arrayFieldName, packed.arrayType(), packed.packed());
        }
    }

    private void writeKvStartArray(String fieldName) {
        pendingKvArrayFieldName = fieldName;
        pushArrayState();
        initArrayAccumulators();
        arrayDepth++;
        kvInlineArrayBuild = true;
        kvInlineArrayDepth = arrayDepth;
    }

    private void writeKvEndArray() {
        PackedArray packed = SourceBatchEncodeHelper.packAccumulatedElements(elemTypes, elemNumeric, elemVar, elemCount, forceUnion);
        Arrays.fill(elemVar, 0, elemCount, null);

        arrayDepth--;
        popArrayState();

        kvWriter.writeArrayField(pendingKvArrayFieldName, packed);
        pendingKvArrayFieldName = null;
        kvInlineArrayBuild = false;
        kvInlineArrayDepth = 0;
    }

    private void ensureKvWriterStackCapacity() {
        if (kvWriterStack == null) {
            kvWriterStack = new KeyValueWriter[4];
        } else if (kvWriterStackDepth >= kvWriterStack.length) {
            kvWriterStack = Arrays.copyOf(kvWriterStack, kvWriterStack.length * 2);
        }
    }
}
