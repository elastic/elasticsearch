/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.sourcebatch;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * Incremental serializer for KEY_VALUE binary blobs ({@code key_length(i32) + key + type(u8) + data}).
 * Wire format matches {@link SourceBatchEncodeHelper#serializeKeyValue}.
 */
public final class KeyValueWriter {

    private BytesStreamOutput out;
    private BytesStreamOutput[] nestedOutStack;
    private int nestedOutDepth;

    private KeyValueWriter(BytesStreamOutput out) {
        this.out = out;
    }

    /** Creates a writer for a standalone object payload (no enclosing field key). */
    public static KeyValueWriter forObjectPayload() {
        // TODO: Eventually expose a recycler here and use a recycling instance
        return new KeyValueWriter(new BytesStreamOutput(64));
    }

    public byte[] toBytes() {
        return BytesReference.toBytes(out.bytes());
    }

    public void writeStringField(String fieldName, byte[] buf, int off, int len) {
        writeKey(fieldName);
        writeStringValue(buf, off, len);
    }

    public void writeIntField(String fieldName, int value) {
        writeKey(fieldName);
        writeIntValue(value);
    }

    public void writeLongField(String fieldName, long value) {
        writeKey(fieldName);
        writeLongValue(value);
    }

    public void writeFloatField(String fieldName, float value) {
        writeKey(fieldName);
        writeFloatValue(value);
    }

    public void writeDoubleField(String fieldName, double value) {
        writeKey(fieldName);
        writeDoubleValue(value);
    }

    public void writeBooleanField(String fieldName, boolean value) {
        writeKey(fieldName);
        writeBooleanValue(value);
    }

    public void writeNullField(String fieldName) {
        writeKey(fieldName);
        writeNullValue();
    }

    public void writeEmptyObjectField(String fieldName) {
        writeKey(fieldName);
        writeEmptyObjectValue();
    }

    public void writeArrayField(String fieldName, SourceBatchEncodeHelper.PackedArray array) {
        writeKey(fieldName);
        writeArrayValue(array);
    }

    public void writeNestedObjectField(String fieldName, byte[] nestedKvBytes) {
        writeKey(fieldName);
        writeNestedObjectValue(nestedKvBytes);
    }

    /** Opens a nested object field; call {@link #endObjectField()} when done. */
    public void beginObjectField(String fieldName) {
        writeKey(fieldName);
        pushNestedOut();
    }

    public void endObjectField() {
        byte[] nested = BytesReference.toBytes(out.bytes());
        out = popNestedOut();
        writeNestedObjectValue(nested);
    }

    private void writeKey(String fieldName) {
        byte[] keyBytes = fieldName.getBytes(StandardCharsets.UTF_8);
        try {
            out.writeIntLE(keyBytes.length);
            out.writeBytes(keyBytes, 0, keyBytes.length);
        } catch (IOException e) {
            throw new IllegalStateException("IO error serializing key", e);
        }
    }

    void writeStringValue(byte[] buf, int off, int len) {
        try {
            out.writeByte(SourceValueType.STRING);
            out.writeIntLE(len);
            out.writeBytes(buf, off, len);
        } catch (IOException e) {
            throw new IllegalStateException("IO error serializing string", e);
        }
    }

    void writeIntValue(int value) {
        try {
            out.writeByte(SourceValueType.INT);
            out.writeIntLE(value);
        } catch (IOException e) {
            throw new IllegalStateException("IO error serializing int", e);
        }
    }

    void writeLongValue(long value) {
        try {
            out.writeByte(SourceValueType.LONG);
            out.writeLongLE(value);
        } catch (IOException e) {
            throw new IllegalStateException("IO error serializing long", e);
        }
    }

    void writeFloatValue(float value) {
        try {
            out.writeByte(SourceValueType.FLOAT);
            out.writeIntLE(Float.floatToRawIntBits(value));
        } catch (IOException e) {
            throw new IllegalStateException("IO error serializing float", e);
        }
    }

    void writeDoubleValue(double value) {
        try {
            out.writeByte(SourceValueType.DOUBLE);
            out.writeLongLE(Double.doubleToRawLongBits(value));
        } catch (IOException e) {
            throw new IllegalStateException("IO error serializing double", e);
        }
    }

    void writeBooleanValue(boolean value) {
        out.writeByte(value ? SourceValueType.TRUE : SourceValueType.FALSE);
    }

    void writeNullValue() {
        out.writeByte(SourceValueType.NULL);
    }

    void writeEmptyObjectValue() {
        try {
            out.writeByte(SourceValueType.KEY_VALUE);
            out.writeIntLE(0);
        } catch (IOException e) {
            throw new IllegalStateException("IO error serializing empty object", e);
        }
    }

    void writeNestedObjectValue(byte[] nestedKvBytes) {
        try {
            out.writeByte(SourceValueType.KEY_VALUE);
            out.writeIntLE(nestedKvBytes.length);
            out.writeBytes(nestedKvBytes, 0, nestedKvBytes.length);
        } catch (IOException e) {
            throw new IllegalStateException("IO error serializing nested object", e);
        }
    }

    void writeArrayValue(SourceBatchEncodeHelper.PackedArray array) {
        try {
            out.writeByte(array.arrayType());
            out.writeIntLE(array.packed().length);
            out.writeBytes(array.packed(), 0, array.packed().length);
        } catch (IOException e) {
            throw new IllegalStateException("IO error serializing array", e);
        }
    }

    private void pushNestedOut() {
        if (nestedOutStack == null) {
            nestedOutStack = new BytesStreamOutput[4];
        } else if (nestedOutDepth >= nestedOutStack.length) {
            nestedOutStack = Arrays.copyOf(nestedOutStack, nestedOutStack.length * 2);
        }
        nestedOutStack[nestedOutDepth++] = out;
        out = new BytesStreamOutput(64);
    }

    private BytesStreamOutput popNestedOut() {
        return nestedOutStack[--nestedOutDepth];
    }
}
