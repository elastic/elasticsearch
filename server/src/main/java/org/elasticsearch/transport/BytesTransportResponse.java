/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.transport;

import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * A specialized, bytes only response, that can potentially be optimized on the network layer.
 */
public class BytesTransportResponse extends TransportResponse implements BytesTransportMessage {

    private final ReleasableBytesReference bytes;

    public BytesTransportResponse(ReleasableBytesReference bytes) {
        this.bytes = bytes;
    }

    @Override
    public ReleasableBytesReference bytes() {
        return this.bytes;
    }

    @Override
    public void writeThin(StreamOutput out) throws IOException {}

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        bytes.writeTo(out);
    }

    @Override
    public void incRef() {
        bytes.incRef();
    }

    @Override
    public boolean tryIncRef() {
        return bytes.tryIncRef();
    }

    @Override
    public boolean decRef() {
        return bytes.decRef();
    }

    @Override
    public boolean hasReferences() {
        return bytes.hasReferences();
    }
}
