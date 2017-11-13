/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.protocol.shared;

import java.io.DataInput;
import java.io.IOException;

/**
 * {@linkplain DataInput} customized for SQL. It has:
 * <ul>
 * <li>{@link #version}. This allows us to add new fields
 * to the protocol in a backwards compatible way by bumping
 * the version number.</li>
 * </ul>
 */public final class SqlDataInput implements DataInput {
    private final DataInput delegate;
    private final int version;

    public SqlDataInput(DataInput delegate, int version) {
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

    @Override
    public void readFully(byte[] b) throws IOException {
        delegate.readFully(b);
    }

    @Override
    public void readFully(byte[] b, int off, int len) throws IOException {
        delegate.readFully(b, off, len);
    }

    @Override
    public int skipBytes(int n) throws IOException {
        return delegate.skipBytes(n);
    }

    @Override
    public boolean readBoolean() throws IOException {
        return delegate.readBoolean();
    }

    @Override
    public byte readByte() throws IOException {
        return delegate.readByte();
    }

    @Override
    public int readUnsignedByte() throws IOException {
        return delegate.readUnsignedByte();
    }

    @Override
    public short readShort() throws IOException {
        return delegate.readShort();
    }

    @Override
    public int readUnsignedShort() throws IOException {
        return delegate.readUnsignedShort();
    }

    @Override
    public char readChar() throws IOException {
        return delegate.readChar();
    }

    @Override
    public int readInt() throws IOException {
        return delegate.readInt();
    }

    @Override
    public long readLong() throws IOException {
        return delegate.readLong();
    }

    @Override
    public float readFloat() throws IOException {
        return delegate.readFloat();
    }

    @Override
    public double readDouble() throws IOException {
        return delegate.readDouble();
    }

    @Override
    public String readLine() throws IOException {
        return delegate.readLine();
    }

    @Override
    public String readUTF() throws IOException {
        return delegate.readUTF();
    }
}