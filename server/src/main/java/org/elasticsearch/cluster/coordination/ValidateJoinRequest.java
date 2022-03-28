/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.cluster.coordination;

import org.elasticsearch.Version;
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
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.transport.TransportRequest;

import java.io.IOException;

public class ValidateJoinRequest extends TransportRequest {
    private final CheckedSupplier<ClusterState, IOException> stateSupplier;
    private final RefCounted refCounted;

    public ValidateJoinRequest(StreamInput in) throws IOException {
        super(in);
        if (in.getVersion().onOrAfter(Version.V_8_2_0)) {
            // recent versions send a BytesTransportRequest containing the compressed state
            final var bytes = in.readReleasableBytesReference();
            final var version = in.getVersion();
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

    private static ClusterState readCompressed(Version version, BytesReference bytes, NamedWriteableRegistry namedWriteableRegistry)
        throws IOException {
        final var compressor = CompressorFactory.compressor(bytes);
        StreamInput in = bytes.streamInput();
        try {
            if (compressor != null) {
                in = new InputStreamStreamInput(compressor.threadLocalInputStream(in));
            }
            in = new NamedWriteableAwareStreamInput(in, namedWriteableRegistry);
            in.setVersion(version);
            try (StreamInput input = in) {
                return ClusterState.readFrom(input, null);
            } catch (Exception e) {
                assert false : e;
                throw e;
            }
        } finally {
            IOUtils.close(in);
        }
    }

    public ValidateJoinRequest(ClusterState state) {
        this.stateSupplier = () -> state;
        this.refCounted = null;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        assert out.getVersion().before(Version.V_8_2_0);
        super.writeTo(out);
        stateSupplier.get().writeTo(out);
    }

    public ClusterState getState() throws IOException {
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
