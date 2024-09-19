/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.cluster.node.hotthreads;

import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.support.nodes.BaseNodeResponse;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;

public class NodeHotThreads extends BaseNodeResponse {

    private final ReleasableBytesReference bytes;

    NodeHotThreads(StreamInput in) throws IOException {
        super(in);
        if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_13_0)) {
            bytes = in.readReleasableBytesReference();
        } else {
            bytes = ReleasableBytesReference.wrap(new BytesArray(in.readString().getBytes(StandardCharsets.UTF_8)));
        }
    }

    public NodeHotThreads(DiscoveryNode node, ReleasableBytesReference hotThreadsUtf8Bytes) {
        super(node);
        assert hotThreadsUtf8Bytes.hasReferences();
        bytes = hotThreadsUtf8Bytes; // takes ownership of the original ref, no need to .retain()
    }

    public String getHotThreads() {
        return bytes.utf8ToString();
    }

    public java.io.Reader getHotThreadsReader() {
        try {
            return new InputStreamReader(bytes.streamInput(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            assert false : e; // all in-memory, no IO takes place
            return new StringReader("ERROR:" + e.toString());
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_13_0)) {
            out.writeBytesReference(bytes);
        } else {
            out.writeString(bytes.utf8ToString());
        }
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
