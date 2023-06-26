/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.cluster.coordination;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.CheckedSupplier;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.compress.CompressorFactory;
import org.elasticsearch.common.io.stream.InputStreamStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.RefCounted;
import org.elasticsearch.transport.TransportRequest;

import java.io.IOException;

public class ValidateJoinRequest extends TransportRequest {
    private final CheckedSupplier<ClusterState, IOException> stateSupplier;
    private final RefCounted refCounted;

    public ValidateJoinRequest(StreamInput in) throws IOException {
        super(in);
        if (in.getTransportVersion().onOrAfter(TransportVersion.V_8_3_0)) {
            // recent versions send a BytesTransportRequest containing a compressed representation of the state
            final var bytes = in.readReleasableBytesReference();
            final var version = in.getTransportVersion();
            final var namedWriteableRegistry = in.namedWriteableRegistry();
            this.stateSupplier = () -> readCompressed(version, bytes, namedWriteableRegistry);
            this.refCounted = bytes;
        } else {
            // older versions just contain the bare state
            final var state = ClusterState.readFrom(in, null);
            this.stateSupplier = () -> state;
            this.refCounted = null;
        }
    }

    private static ClusterState readCompressed(
        TransportVersion version,
        BytesReference bytes,
        NamedWriteableRegistry namedWriteableRegistry
    ) throws IOException {
        try (
            var bytesStreamInput = bytes.streamInput();
            var in = new NamedWriteableAwareStreamInput(
                new InputStreamStreamInput(CompressorFactory.COMPRESSOR.threadLocalInputStream(bytesStreamInput)),
                namedWriteableRegistry
            )
        ) {
            in.setTransportVersion(version);
            return ClusterState.readFrom(in, null);
        }
    }

    public ValidateJoinRequest(ClusterState state) {
        this.stateSupplier = () -> state;
        this.refCounted = null;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        assert out.getTransportVersion().before(TransportVersion.V_8_3_0);
        super.writeTo(out);
        stateSupplier.get().writeTo(out);
    }

    public ClusterState getOrReadState() throws IOException {
        return stateSupplier.get();
    }

    @Override
    public void incRef() {
        if (refCounted != null) {
            refCounted.incRef();
        }
    }

    @Override
    public boolean tryIncRef() {
        return refCounted == null || refCounted.tryIncRef();
    }

    @Override
    public boolean decRef() {
        return refCounted != null && refCounted.decRef();
    }

    @Override
    public boolean hasReferences() {
        return refCounted == null || refCounted.hasReferences();
    }
}
