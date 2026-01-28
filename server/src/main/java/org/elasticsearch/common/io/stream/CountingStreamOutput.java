/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.io.stream;

import org.apache.lucene.util.BitUtil;
import org.elasticsearch.core.Nullable;

import java.io.IOException;

/**
 * A reusable @link {@link StreamOutput} that just count how many bytes are written.
 */
public class CountingStreamOutput extends StreamOutput {
    private long size;

    /** reset the written byes to 0 */
    public void reset() {
        size = 0L;
    }

    /** returns how many bytes would have been written  */
    public long size() {
        return size;
    }

    @Override
    public void writeByte(byte b) {
        ++size;
    }

    @Override
    public void writeBytes(byte[] b, int offset, int length) {
        size += length;
    }

    @Override
    public void writeShort(short v) throws IOException {
        size += Short.BYTES;
    }

    @Override
    public void writeInt(int i) {
        size += Integer.BYTES;
    }

    @Override
    public void writeIntLE(int i) throws IOException {
        size += Integer.BYTES;
    }

    @Override
    public void writeIntArray(int[] values) {
        writeVInt(values.length);
        size += (long) values.length * Integer.BYTES;
    }

    @Override
    public void writeLong(long i) {
        size += Long.BYTES;
    }

    @Override
    public void writeLongLE(long i) {
        size += Long.BYTES;
    }

    @Override
    public void writeLongArray(long[] values) {
        writeVInt(values.length);
        size += (long) values.length * Long.BYTES;
    }

    @Override
    public void writeFloat(float v) {
        size += Float.BYTES;
    }

    @Override
    public void writeFloatArray(float[] values) {
        writeVInt(values.length);
        size += (long) values.length * Float.BYTES;
    }

    @Override
    public void writeDouble(double v) {
        size += Double.BYTES;
    }

    @Override
    public void writeDoubleArray(double[] values) {
        writeVInt(values.length);
        size += (long) values.length * Double.BYTES;
    }

    @Override
    public void writeVInt(int v) {
        // set LSB because 0 takes 1 byte
        size += (38 - Integer.numberOfLeadingZeros(v | 1)) / 7;
    }

    @Override
    void writeVLongNoCheck(long v) {
        // set LSB because 0 takes 1 byte
        size += (70 - Long.numberOfLeadingZeros(v | 1L)) / 7;
    }

    @Override
    public void writeZLong(long i) {
        writeVLongNoCheck(BitUtil.zigZagEncode(i));
    }

    @Override
    public void writeString(String str) {
        final int charCount = str.length();
        writeVInt(charCount);
        size += charCount;
        for (int i = 0; i < charCount; i++) {
            final int c = str.charAt(i);
            if (c > 0x007F) {
                size += c > 0x07FF ? 2 : 1;
            }
        }
    }

    @Override
    public void writeOptionalString(@Nullable String str) {
        size += 1;
        if (str != null) {
            writeString(str);
        }
    }

    @Override
    public void writeGenericString(String value) {
        size += 1;
        writeString(value);
    }

    @Override
    public void flush() {}

    @Override
    public void close() {}
}
