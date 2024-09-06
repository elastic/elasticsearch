/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.io.stream;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.bytes.BytesReference;
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
    public String readString() throws IOException {
        return delegate.readString();
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
    public boolean supportReadAllToReleasableBytesReference() {
        return delegate.supportReadAllToReleasableBytesReference();
    }

    @Override
    public ReleasableBytesReference readAllToReleasableBytesReference() throws IOException {
        assert supportReadAllToReleasableBytesReference() : "This InputStream doesn't support readAllToReleasableBytesReference";
        return delegate.readAllToReleasableBytesReference();
    }

    @Override
    public BytesReference readSlicedBytesReference() throws IOException {
        return delegate.readSlicedBytesReference();
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
    public int read(byte[] b, int off, int len) throws IOException {
        return delegate.read(b, off, len);
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
    public TransportVersion getTransportVersion() {
        return delegate.getTransportVersion();
    }

    @Override
    public void setTransportVersion(TransportVersion version) {
        delegate.setTransportVersion(version);
        // also set the version on this stream directly, so that any uses of this.version are still correct
        super.setTransportVersion(version);
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
