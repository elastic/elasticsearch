/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.transport;

import org.elasticsearch.Version;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * A specialized, bytes only request, that can potentially be optimized on the network
 * layer, specifically for the same large buffer send to several nodes.
 */
public class BytesTransportRequest extends TransportRequest {

    BytesReference bytes;
    Version version;

    public BytesTransportRequest(StreamInput in) throws IOException {
        super(in);
        bytes = in.readBytesReference();
        version = in.getVersion();
    }

    public BytesTransportRequest(BytesReference bytes, Version version) {
        this.bytes = bytes;
        this.version = version;
    }

    public Version version() {
        return this.version;
    }

    public BytesReference bytes() {
        return this.bytes;
    }

    /**
     * Writes the data in a "thin" manner, without the actual bytes, assumes
     * the actual bytes will be appended right after this content.
     */
    public void writeThin(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVInt(bytes.length());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeBytesReference(bytes);
    }
}
