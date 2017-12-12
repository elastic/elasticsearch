/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.protocol.shared;

import java.io.DataOutput;
import java.io.IOException;

/**
 * {@linkplain DataOutput} customized for SQL. It has:
 * <ul>
 * <li>{@link #version}. This allows us to add new fields
 * to the protocol in a backwards compatible way by bumping
 * the version number.</li>
 * </ul>
 */
public final class SqlDataOutput implements DataOutput {
    private final DataOutput delegate;
    private final int version;

    public SqlDataOutput(DataOutput delegate, int version) {
        this.delegate = delegate;
        this.version = version;
    }

    /**
     * Version of the protocol to use. When new fields are added
     * to the protocol we bump the maximum version. Requests and
     * responses use the minimum version understood by both the
     * client and the server.
     */
    public int version() {
        return version;
    }

    /**
     * The maximum size of a string to submit to {@link #delegate}'s
     * {@link DataOutput#writeUTF(String)}. The {@code 65535} is the
     * number of bytes that the string can be encoded to. The {@code 3}
     * is the "worst case" for the number of bytes used to encode each
     * char.
     */
    static final int WORST_CASE_SPLIT = 65535 / 3;
    /**
     * Override the built-in {@link DataOutput#writeUTF(String)}
     * to support strings that need more than 65535 charcters.
     */
    @Override
    public void writeUTF(String s) throws IOException {
        int splits = s.length() / WORST_CASE_SPLIT + 1;
        delegate.writeInt(splits);

        int start = 0;
        while (true) {
            int end = Math.min(s.length(), start + WORST_CASE_SPLIT);
            delegate.writeUTF(s.substring(start, end));
            if (end == s.length()) {
                break;
            }
            start = end;
        }
    }

    @Override
    public void write(int b) throws IOException {
        delegate.write(b);
    }

    @Override
    public void write(byte[] b) throws IOException {
        delegate.write(b);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        delegate.write(b, off, len);
    }

    @Override
    public void writeBoolean(boolean v) throws IOException {
        delegate.writeBoolean(v);
    }

    @Override
    public void writeByte(int v) throws IOException {
        delegate.writeByte(v);
    }

    @Override
    public void writeShort(int v) throws IOException {
        delegate.writeShort(v);
    }

    @Override
    public void writeChar(int v) throws IOException {
        delegate.writeChar(v);
    }

    @Override
    public void writeInt(int v) throws IOException {
        delegate.writeInt(v);
    }

    @Override
    public void writeLong(long v) throws IOException {
        delegate.writeLong(v);
    }

    @Override
    public void writeFloat(float v) throws IOException {
        delegate.writeFloat(v);
    }

    @Override
    public void writeDouble(double v) throws IOException {
        delegate.writeDouble(v);
    }

    @Override
    public void writeBytes(String s) throws IOException {
        delegate.writeBytes(s);
    }

    @Override
    public void writeChars(String s) throws IOException {
        delegate.writeChars(s);
    }
}
