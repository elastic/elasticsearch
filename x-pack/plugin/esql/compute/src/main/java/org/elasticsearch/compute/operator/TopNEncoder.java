/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.NumericUtils;

/**
 * Defines a default BytesRef encoding behavior for all block types, leaving text based types for concrete implementations.
 */
public interface TopNEncoder {

    default void encodeLong(long value, BytesRefBuilder bytesRefBuilder) {
        bytesRefBuilder.grow(bytesRefBuilder.length() + Long.BYTES);
        NumericUtils.longToSortableBytes(value, bytesRefBuilder.bytes(), bytesRefBuilder.length());
        bytesRefBuilder.setLength(bytesRefBuilder.length() + Long.BYTES);
    }

    default void encodeInteger(int value, BytesRefBuilder bytesRefBuilder) {
        bytesRefBuilder.grow(bytesRefBuilder.length() + Integer.BYTES);
        NumericUtils.intToSortableBytes(value, bytesRefBuilder.bytes(), bytesRefBuilder.length());
        bytesRefBuilder.setLength(bytesRefBuilder.length() + Integer.BYTES);
    }

    default void encodeDouble(double value, BytesRefBuilder bytesRefBuilder) {
        bytesRefBuilder.grow(bytesRefBuilder.length() + Long.BYTES);
        NumericUtils.longToSortableBytes(NumericUtils.doubleToSortableLong(value), bytesRefBuilder.bytes(), bytesRefBuilder.length());
        bytesRefBuilder.setLength(bytesRefBuilder.length() + Long.BYTES);
    }

    default void encodeBoolean(boolean value, BytesRefBuilder bytesRefBuilder) {
        var bytes = new byte[] { value ? (byte) 1 : (byte) 0 };
        bytesRefBuilder.append(bytes, 0, 1);
    }

    void encodeBytesRef(BytesRef value, BytesRefBuilder bytesRefBuilder);

}
