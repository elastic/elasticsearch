/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.action;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.transport.BytesTransportMessage;
import org.elasticsearch.transport.BytesTransportResponse;
import org.elasticsearch.transport.OutboundHandler;
import org.elasticsearch.xpack.stateless.commits.GetVirtualBatchedCompoundCommitChunksPressure;

import java.io.IOException;

/**
 * Response carrying a chunk of virtual batched compound commit data.
 * <p>
 * Implements {@link BytesTransportMessage} so {@link OutboundHandler} can send the chunk without copying: it writes
 * {@link #writeThin(StreamOutput)} into the transport header and appends {@link #bytes()} as a zero-copy suffix. The
 * {@link GetVirtualBatchedCompoundCommitChunksPressure} releasable is wrapped in the chunk bytes reference, so pressure
 * stays held until the outbound send completes rather than when the response is handed to the transport layer.
 * <p>
 * This does not use {@link BytesTransportResponse} because that class extends {@link org.elasticsearch.transport.TransportResponse}
 * rather than {@link ActionResponse}, and its wire format also differs (it puts the fully serialized body in bytes, whereas
 * here we split the length into writeThin and the payload into bytes). Also, we do not need its {@link org.elasticsearch.TransportVersion}
 * since the response is opaque bytes, sent directly from the indexing node to the search node, without a proxy in-between.
 * <p>
 * Wire format for {@link #writeThin(StreamOutput)}:
 * <ul>
 *     <li>Legacy: a VInt length prefix before the zero-copy payload (for BwC with nodes that shipped the initial
 *     {@link BytesTransportMessage} implementation).</li>
 *     <li>From {@link #VBCC_CHUNK_RESPONSE_WITHOUT_LENGTH_PREFIX}: empty {@code writeThin}; payload length is taken from the
 *     transport message header.</li>
 * </ul>
 */
public class GetVirtualBatchedCompoundCommitChunkResponse extends ActionResponse implements BytesTransportMessage {

    public static final TransportVersion VBCC_CHUNK_RESPONSE_WITHOUT_LENGTH_PREFIX = TransportVersion.fromName(
        "vbcc_chunk_response_without_length_prefix"
    );

    private final ReleasableBytesReference data;

    public GetVirtualBatchedCompoundCommitChunkResponse(ReleasableBytesReference data) {
        assert data.hasReferences();
        this.data = data; // takes ownership of the original ref, no need to .retain()
    }

    public GetVirtualBatchedCompoundCommitChunkResponse(StreamInput in) throws IOException {
        if (in.getTransportVersion().supports(VBCC_CHUNK_RESPONSE_WITHOUT_LENGTH_PREFIX)) {
            assert in.supportReadAllToReleasableBytesReference() : "StreamInput must support readAllToReleasableBytesReference";
            if (in.supportReadAllToReleasableBytesReference() == false) {
                throw new IllegalStateException("StreamInput does not support readAllToReleasableBytesReference");
            }
            data = in.readAllToReleasableBytesReference();
        } else {
            data = in.readReleasableBytesReference();
        }
    }

    @Override
    public ReleasableBytesReference bytes() {
        return data;
    }

    @Override
    public void writeThin(StreamOutput out) throws IOException {
        if (out.getTransportVersion().supports(VBCC_CHUNK_RESPONSE_WITHOUT_LENGTH_PREFIX) == false) {
            out.writeVInt(data.length());
        }
    }

    @Override
    public final void writeTo(StreamOutput out) {
        assert false : "should not be called";
        throw new UnsupportedOperationException("writeTo() should not be used");
    }

    public ReleasableBytesReference getData() {
        return data;
    }

    @Override
    public void incRef() {
        data.incRef();
    }

    @Override
    public boolean tryIncRef() {
        return data.tryIncRef();
    }

    @Override
    public boolean decRef() {
        return data.decRef();
    }

    @Override
    public boolean hasReferences() {
        return data.hasReferences();
    }
}
