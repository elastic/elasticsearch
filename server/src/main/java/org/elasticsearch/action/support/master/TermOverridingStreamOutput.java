/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.support.master;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * Wrapper around a {@link StreamOutput} for use when sending a {@link MasterNodeRequest} to another node, exposing the {@link
 * MasterNodeRequest#masterTerm()} to send out over the wire.
 */
class TermOverridingStreamOutput extends StreamOutput {

    private final StreamOutput delegate;
    final long masterTerm;

    TermOverridingStreamOutput(StreamOutput delegate, long masterTerm) {
        this.delegate = delegate;
        this.masterTerm = masterTerm;
    }

    @Override
    public void writeByte(byte b) throws IOException {
        delegate.writeByte(b);
    }

    @Override
    public void writeBytes(byte[] b, int offset, int length) throws IOException {
        delegate.writeBytes(b, offset, length);
    }

    @Override
    public void flush() throws IOException {
        delegate.flush();
    }

    @Override
    public void close() throws IOException {
        delegate.close();
    }

    @Override
    public TransportVersion getTransportVersion() {
        return delegate.getTransportVersion();
    }

    @Override
    public void setTransportVersion(TransportVersion version) {
        assert false : version;
        delegate.setTransportVersion(version);
    }
}
