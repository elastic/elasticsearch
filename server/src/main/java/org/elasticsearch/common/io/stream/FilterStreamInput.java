/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.io.stream;

import org.elasticsearch.Version;
import org.elasticsearch.common.bytes.ReleasableBytesReference;

import java.io.EOFException;
import java.io.IOException;

/**
 * Wraps a {@link StreamInput} and delegates to it. To be used to add functionality to an existing stream by subclassing.
 */
public abstract class FilterStreamInput extends StreamInput {

    protected final StreamInput delegate;

    protected FilterStreamInput(StreamInput delegate) {
        this.delegate = delegate;
    }

    @Override
    public byte readByte() throws IOException {
        return delegate.readByte();
    }

    @Override
    public void readBytes(byte[] b, int offset, int len) throws IOException {
        delegate.readBytes(b, offset, len);
    }

    @Override
    public ReleasableBytesReference readReleasableBytesReference() throws IOException {
        return delegate.readReleasableBytesReference();
    }

    @Override
    public short readShort() throws IOException {
        return delegate.readShort();
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
    public int readVInt() throws IOException {
        return delegate.readVInt();
    }

    @Override
    public long readVLong() throws IOException {
        return delegate.readVLong();
    }

    @Override
    public void reset() throws IOException {
        delegate.reset();
    }

    @Override
    public int read() throws IOException {
        return delegate.read();
    }

    @Override
    public void close() throws IOException {
        delegate.close();
    }

    @Override
    public int available() throws IOException {
        return delegate.available();
    }

    @Override
    public Version getVersion() {
        return delegate.getVersion();
    }

    @Override
    public void setVersion(Version version) {
        delegate.setVersion(version);
    }

    @Override
    protected void ensureCanReadBytes(int length) throws EOFException {
        delegate.ensureCanReadBytes(length);
    }

    @Override
    public NamedWriteableRegistry namedWriteableRegistry() {
        return delegate.namedWriteableRegistry();
    }
}
