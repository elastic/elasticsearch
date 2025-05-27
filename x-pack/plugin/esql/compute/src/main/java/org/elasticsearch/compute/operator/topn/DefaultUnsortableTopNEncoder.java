/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.topn;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.operator.BreakingBytesRefBuilder;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;

/**
 * A {@link TopNEncoder} that doesn't encode values so they are sortable but is
 * capable of encoding any values.
 */
public final class DefaultUnsortableTopNEncoder implements TopNEncoder {
    public static final VarHandle LONG = MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.nativeOrder());
    public static final VarHandle INT = MethodHandles.byteArrayViewVarHandle(int[].class, ByteOrder.nativeOrder());
    public static final VarHandle FLOAT = MethodHandles.byteArrayViewVarHandle(float[].class, ByteOrder.nativeOrder());
    public static final VarHandle DOUBLE = MethodHandles.byteArrayViewVarHandle(double[].class, ByteOrder.nativeOrder());

    @Override
    public void encodeLong(long value, BreakingBytesRefBuilder bytesRefBuilder) {
        bytesRefBuilder.grow(bytesRefBuilder.length() + Long.BYTES);
        LONG.set(bytesRefBuilder.bytes(), bytesRefBuilder.length(), value);
        bytesRefBuilder.setLength(bytesRefBuilder.length() + Long.BYTES);
    }

    @Override
    public long decodeLong(BytesRef bytes) {
        if (bytes.length < Long.BYTES) {
            throw new IllegalArgumentException("not enough bytes");
        }
        long v = (long) LONG.get(bytes.bytes, bytes.offset);
        bytes.offset += Long.BYTES;
        bytes.length -= Long.BYTES;
        return v;
    }

    /**
     * Writes an int in a variable-length format. Writes between one and
     * five bytes. Smaller values take fewer bytes. Negative numbers
     * will always use all 5 bytes.
     */
    public void encodeVInt(int value, BreakingBytesRefBuilder bytesRefBuilder) {
        while ((value & ~0x7F) != 0) {
            bytesRefBuilder.append(((byte) ((value & 0x7f) | 0x80)));
            value >>>= 7;
        }
        bytesRefBuilder.append((byte) value);
    }

    /**
     * Reads an int stored in variable-length format. Reads between one and
     * five bytes. Smaller values take fewer bytes. Negative numbers
     * will always use all 5 bytes.
     */
    public int decodeVInt(BytesRef bytes) {
        /*
         * The loop for this is unrolled because we unrolled the loop in StreamInput.
         * I presume it's a decent choice here because it was a good choice there.
         */
        byte b = bytes.bytes[bytes.offset];
        if (b >= 0) {
            bytes.offset += 1;
            bytes.length -= 1;
            return b;
        }
        int i = b & 0x7F;
        b = bytes.bytes[bytes.offset + 1];
        i |= (b & 0x7F) << 7;
        if (b >= 0) {
            bytes.offset += 2;
            bytes.length -= 2;
            return i;
        }
        b = bytes.bytes[bytes.offset + 2];
        i |= (b & 0x7F) << 14;
        if (b >= 0) {
            bytes.offset += 3;
            bytes.length -= 3;
            return i;
        }
        b = bytes.bytes[bytes.offset + 3];
        i |= (b & 0x7F) << 21;
        if (b >= 0) {
            bytes.offset += 4;
            bytes.length -= 4;
            return i;
        }
        b = bytes.bytes[bytes.offset + 4];
        i |= (b & 0x0F) << 28;
        if ((b & 0xF0) != 0) {
            throw new IllegalStateException("Invalid last byte for a vint [" + Integer.toHexString(b) + "]");
        }
        bytes.offset += 5;
        bytes.length -= 5;
        return i;
    }

    @Override
    public void encodeInt(int value, BreakingBytesRefBuilder bytesRefBuilder) {
        bytesRefBuilder.grow(bytesRefBuilder.length() + Integer.BYTES);
        INT.set(bytesRefBuilder.bytes(), bytesRefBuilder.length(), value);
        bytesRefBuilder.setLength(bytesRefBuilder.length() + Integer.BYTES);
    }

    @Override
    public int decodeInt(BytesRef bytes) {
        if (bytes.length < Integer.BYTES) {
            throw new IllegalArgumentException("not enough bytes");
        }
        int v = (int) INT.get(bytes.bytes, bytes.offset);
        bytes.offset += Integer.BYTES;
        bytes.length -= Integer.BYTES;
        return v;
    }

    @Override
    public void encodeFloat(float value, BreakingBytesRefBuilder bytesRefBuilder) {
        bytesRefBuilder.grow(bytesRefBuilder.length() + Float.BYTES);
        FLOAT.set(bytesRefBuilder.bytes(), bytesRefBuilder.length(), value);
        bytesRefBuilder.setLength(bytesRefBuilder.length() + Float.BYTES);
    }

    @Override
    public float decodeFloat(BytesRef bytes) {
        if (bytes.length < Float.BYTES) {
            throw new IllegalArgumentException("not enough bytes");
        }
        float v = (float) FLOAT.get(bytes.bytes, bytes.offset);
        bytes.offset += Float.BYTES;
        bytes.length -= Float.BYTES;
        return v;
    }

    @Override
    public void encodeDouble(double value, BreakingBytesRefBuilder bytesRefBuilder) {
        bytesRefBuilder.grow(bytesRefBuilder.length() + Double.BYTES);
        DOUBLE.set(bytesRefBuilder.bytes(), bytesRefBuilder.length(), value);
        bytesRefBuilder.setLength(bytesRefBuilder.length() + Long.BYTES);
    }

    @Override
    public double decodeDouble(BytesRef bytes) {
        if (bytes.length < Double.BYTES) {
            throw new IllegalArgumentException("not enough bytes");
        }
        double v = (double) DOUBLE.get(bytes.bytes, bytes.offset);
        bytes.offset += Double.BYTES;
        bytes.length -= Double.BYTES;
        return v;
    }

    @Override
    public void encodeBoolean(boolean value, BreakingBytesRefBuilder bytesRefBuilder) {
        bytesRefBuilder.append(value ? (byte) 1 : (byte) 0);
    }

    @Override
    public boolean decodeBoolean(BytesRef bytes) {
        if (bytes.length < Byte.BYTES) {
            throw new IllegalArgumentException("not enough bytes");
        }
        boolean v = bytes.bytes[bytes.offset] == 1;
        bytes.offset += Byte.BYTES;
        bytes.length -= Byte.BYTES;
        return v;
    }

    @Override
    public int encodeBytesRef(BytesRef value, BreakingBytesRefBuilder bytesRefBuilder) {
        final int offset = bytesRefBuilder.length();
        encodeVInt(value.length, bytesRefBuilder);
        bytesRefBuilder.append(value);
        return bytesRefBuilder.length() - offset;
    }

    @Override
    public BytesRef decodeBytesRef(BytesRef bytes, BytesRef scratch) {
        final int len = decodeVInt(bytes);
        scratch.bytes = bytes.bytes;
        scratch.offset = bytes.offset;
        scratch.length = len;
        bytes.offset += len;
        bytes.length -= len;
        return scratch;
    }

    @Override
    public TopNEncoder toSortable() {
        return TopNEncoder.DEFAULT_SORTABLE;
    }

    @Override
    public TopNEncoder toUnsortable() {
        return this;
    }

    @Override
    public String toString() {
        return "DefaultUnsortable";
    }
}
