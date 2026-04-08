/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.topn;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.bytes.PagedBytesBuilder;
import org.elasticsearch.common.bytes.PagedBytesCursor;

/**
 * A {@link TopNEncoder} that doesn't encode values so they are sortable but is
 * capable of encoding any values.
 */
public class DefaultUnsortableTopNEncoder implements TopNEncoder {
    @Override
    public void encodeLong(long value, PagedBytesBuilder builder) {
        builder.append(value);
    }

    @Override
    public long decodeLong(PagedBytesCursor bytes) {
        return bytes.readLong();
    }

    /**
     * Writes an int in variable-length format. Writes between one and
     * five bytes. Smaller values take fewer bytes. Negative numbers
     * will always use all 5 bytes.
     */
    public void encodeVInt(int value, PagedBytesBuilder builder) {
        builder.appendVInt(value);
    }

    /**
     * Reads an int stored in variable-length format.
     */
    public int decodeVInt(PagedBytesCursor cursor) {
        return cursor.readVInt();
    }

    @Override
    public void encodeInt(int value, PagedBytesBuilder builder) {
        builder.append(value);
    }

    @Override
    public int decodeInt(PagedBytesCursor bytes) {
        return bytes.readInt();
    }

    @Override
    public void encodeFloat(float value, PagedBytesBuilder builder) {
        builder.append(Float.floatToRawIntBits(value));
    }

    @Override
    public float decodeFloat(PagedBytesCursor bytes) {
        return Float.intBitsToFloat(bytes.readInt());
    }

    @Override
    public void encodeDouble(double value, PagedBytesBuilder builder) {
        builder.append(Double.doubleToRawLongBits(value));
    }

    @Override
    public double decodeDouble(PagedBytesCursor bytes) {
        return Double.longBitsToDouble(bytes.readLong());
    }

    @Override
    public void encodeBoolean(boolean value, PagedBytesBuilder builder) {
        builder.append(value ? (byte) 1 : (byte) 0);
    }

    @Override
    public boolean decodeBoolean(PagedBytesCursor bytes) {
        return bytes.readByte() == 1;
    }

    @Override
    public void encodeBytesRef(BytesRef value, PagedBytesBuilder builder) {
        builder.appendVInt(value.length);
        builder.append(value);
    }

    @Override
    public PagedBytesCursor decodeBytesRef(PagedBytesCursor cursor, PagedBytesCursor scratch) {
        return cursor.slice(cursor.readVInt(), scratch);
    }

    @Override
    public TopNEncoder toSortable(boolean asc) {
        return TopNEncoder.DEFAULT_SORTABLE.toSortable(asc);
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
