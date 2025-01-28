/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.topn;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.compute.operator.BreakingBytesRefBuilder;

/**
 * A {@link TopNEncoder} that encodes values to byte arrays that may be sorted directly.
 */
public abstract class SortableTopNEncoder implements TopNEncoder {
    @Override
    public final void encodeLong(long value, BreakingBytesRefBuilder bytesRefBuilder) {
        bytesRefBuilder.grow(bytesRefBuilder.length() + Long.BYTES);
        NumericUtils.longToSortableBytes(value, bytesRefBuilder.bytes(), bytesRefBuilder.length());
        bytesRefBuilder.setLength(bytesRefBuilder.length() + Long.BYTES);
    }

    @Override
    public final long decodeLong(BytesRef bytes) {
        if (bytes.length < Long.BYTES) {
            throw new IllegalArgumentException("not enough bytes");
        }
        long v = NumericUtils.sortableBytesToLong(bytes.bytes, bytes.offset);
        bytes.offset += Long.BYTES;
        bytes.length -= Long.BYTES;
        return v;
    }

    @Override
    public final void encodeInt(int value, BreakingBytesRefBuilder bytesRefBuilder) {
        bytesRefBuilder.grow(bytesRefBuilder.length() + Integer.BYTES);
        NumericUtils.intToSortableBytes(value, bytesRefBuilder.bytes(), bytesRefBuilder.length());
        bytesRefBuilder.setLength(bytesRefBuilder.length() + Integer.BYTES);
    }

    @Override
    public final int decodeInt(BytesRef bytes) {
        if (bytes.length < Integer.BYTES) {
            throw new IllegalArgumentException("not enough bytes");
        }
        int v = NumericUtils.sortableBytesToInt(bytes.bytes, bytes.offset);
        bytes.offset += Integer.BYTES;
        bytes.length -= Integer.BYTES;
        return v;
    }

    @Override
    public final void encodeFloat(float value, BreakingBytesRefBuilder bytesRefBuilder) {
        bytesRefBuilder.grow(bytesRefBuilder.length() + Integer.BYTES);
        NumericUtils.intToSortableBytes(NumericUtils.floatToSortableInt(value), bytesRefBuilder.bytes(), bytesRefBuilder.length());
        bytesRefBuilder.setLength(bytesRefBuilder.length() + Integer.BYTES);
    }

    @Override
    public final float decodeFloat(BytesRef bytes) {
        if (bytes.length < Float.BYTES) {
            throw new IllegalArgumentException("not enough bytes");
        }
        float v = NumericUtils.sortableIntToFloat(NumericUtils.sortableBytesToInt(bytes.bytes, bytes.offset));
        bytes.offset += Float.BYTES;
        bytes.length -= Float.BYTES;
        return v;
    }

    @Override
    public final void encodeDouble(double value, BreakingBytesRefBuilder bytesRefBuilder) {
        bytesRefBuilder.grow(bytesRefBuilder.length() + Long.BYTES);
        NumericUtils.longToSortableBytes(NumericUtils.doubleToSortableLong(value), bytesRefBuilder.bytes(), bytesRefBuilder.length());
        bytesRefBuilder.setLength(bytesRefBuilder.length() + Long.BYTES);
    }

    @Override
    public final double decodeDouble(BytesRef bytes) {
        if (bytes.length < Double.BYTES) {
            throw new IllegalArgumentException("not enough bytes");
        }
        double v = NumericUtils.sortableLongToDouble(NumericUtils.sortableBytesToLong(bytes.bytes, bytes.offset));
        bytes.offset += Double.BYTES;
        bytes.length -= Double.BYTES;
        return v;
    }

    @Override
    public final void encodeBoolean(boolean value, BreakingBytesRefBuilder bytesRefBuilder) {
        bytesRefBuilder.append(value ? (byte) 1 : (byte) 0);
    }

    @Override
    public final boolean decodeBoolean(BytesRef bytes) {
        if (bytes.length < Byte.BYTES) {
            throw new IllegalArgumentException("not enough bytes");
        }
        boolean v = bytes.bytes[bytes.offset] == 1;
        bytes.offset += Byte.BYTES;
        bytes.length -= Byte.BYTES;
        return v;
    }
}
