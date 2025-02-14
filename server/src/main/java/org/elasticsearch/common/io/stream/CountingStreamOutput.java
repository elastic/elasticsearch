/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.io.stream;

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
    public void writeInt(int i) {
        size += Integer.BYTES;
    }

    @Override
    public void writeIntArray(int[] values) throws IOException {
        writeVInt(values.length);
        size += (long) values.length * Integer.BYTES;
    }

    @Override
    public void writeLong(long i) {
        size += Long.BYTES;
    }

    @Override
    public void writeLongArray(long[] values) throws IOException {
        writeVInt(values.length);
        size += (long) values.length * Long.BYTES;
    }

    @Override
    public void writeFloat(float v) {
        size += Float.BYTES;
    }

    @Override
    public void writeFloatArray(float[] values) throws IOException {
        writeVInt(values.length);
        size += (long) values.length * Float.BYTES;
    }

    @Override
    public void writeDouble(double v) {
        size += Double.BYTES;
    }

    @Override
    public void writeDoubleArray(double[] values) throws IOException {
        writeVInt(values.length);
        size += (long) values.length * Double.BYTES;
    }

    @Override
    public void flush() {}

    @Override
    public void close() {}
}
