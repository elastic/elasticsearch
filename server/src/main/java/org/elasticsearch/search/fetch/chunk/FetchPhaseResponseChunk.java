/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.fetch.chunk;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.CompositeBytesReference;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.RecyclerBytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.SearchHit;

import java.io.IOException;

/**
 * A single chunk of fetch results streamed from a data node to the coordinator.
 * Contains sequence information to maintain correct ordering when chunks arrive out of order.
 *
 * <p>Supports zero-copy transport by separating header metadata from serialized hits.
 * The header is created after hits are serialized (since we don't know hit count until
 * the buffer is full), then combined using {@link CompositeBytesReference} to avoid copying.
 */
public class FetchPhaseResponseChunk implements Writeable, Releasable {

    private final long timestampMillis;
    private final Type type;
    private final ShardId shardId;
    private final int hitCount;
    private final int from;
    private final int expectedDocs;
    private final long sequenceStart;

    private BytesReference serializedHits;
    private SearchHit[] deserializedHits;
    private NamedWriteableRegistry namedWriteableRegistry;

    /**
     * The type of chunk being sent.
     */
    public enum Type {
        HITS
    }

    /**
     * Creates a chunk with pre-serialized hits.
     * Takes ownership of serializedHits - caller must not release it.
     *
     * @param timestampMillis  creation timestamp
     * @param type             chunk type
     * @param shardId          source shard
     * @param serializedHits   pre-serialized hit bytes
     * @param hitCount         number of hits in the serialized bytes
     * @param from             index of first hit in the overall result set
     * @param expectedDocs     total documents expected across all chunks
     * @param sequenceStart    sequence number of first hit for ordering
     */
    public FetchPhaseResponseChunk(
        long timestampMillis,
        Type type,
        ShardId shardId,
        BytesReference serializedHits,
        int hitCount,
        int from,
        int expectedDocs,
        long sequenceStart
    ) {
        if (shardId.getId() < -1) {
            throw new IllegalArgumentException("invalid shardId: " + shardId);
        }
        this.timestampMillis = timestampMillis;
        this.type = type;
        this.shardId = shardId;
        this.serializedHits = serializedHits;
        this.hitCount = hitCount;
        this.from = from;
        this.expectedDocs = expectedDocs;
        this.sequenceStart = sequenceStart;
    }

    /**
     * Deserializes from stream (receiving side).
     */
    public FetchPhaseResponseChunk(StreamInput in) throws IOException {
        this.timestampMillis = in.readVLong();
        this.type = in.readEnum(Type.class);
        this.shardId = new ShardId(in);
        this.hitCount = in.readVInt();
        this.from = in.readVInt();
        this.expectedDocs = in.readVInt();
        this.sequenceStart = in.readVLong();
        this.serializedHits = in.readBytesReference();
        this.namedWriteableRegistry = in.namedWriteableRegistry();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(timestampMillis);
        out.writeEnum(type);
        shardId.writeTo(out);
        out.writeVInt(hitCount);
        out.writeVInt(from);
        out.writeVInt(expectedDocs);
        out.writeVLong(sequenceStart);
        out.writeBytesReference(serializedHits);
    }

    public ReleasableBytesReference toReleasableBytesReference(long coordinatingTaskId) throws IOException {
        final ReleasableBytesReference result;
        try (BytesStreamOutput header = new BytesStreamOutput(16)) {
            header.writeVLong(coordinatingTaskId);

            BytesReference composite = CompositeBytesReference.of(header.copyBytes(), toBytesReference());
            if (serializedHits instanceof ReleasableBytesReference releasableHits) {
                result = new ReleasableBytesReference(composite, releasableHits::decRef);
            } else {
                result = ReleasableBytesReference.wrap(composite);
            }
            this.serializedHits = null;
        }
        return result;
    }

    private BytesReference toBytesReference() throws IOException {
        try (BytesStreamOutput out = new BytesStreamOutput(128)) {
            writeTo(out);
            return out.copyBytes();
        }
    }

    public long getBytesLength() {
        return serializedHits == null ? 0 : serializedHits.length();
    }

    public SearchHit[] getHits() throws IOException {
        if (deserializedHits == null && serializedHits != null && hitCount > 0) {
            deserializedHits = new SearchHit[hitCount];
            try (StreamInput in = createStreamInput()) {
                for (int i = 0; i < hitCount; i++) {
                    deserializedHits[i] = SearchHit.readFrom(in, false);
                }
            }
        }
        return deserializedHits != null ? deserializedHits : new SearchHit[0];
    }

    private StreamInput createStreamInput() throws IOException {
        StreamInput in = serializedHits.streamInput();
        if (namedWriteableRegistry != null) {
            in = new NamedWriteableAwareStreamInput(in, namedWriteableRegistry);
        }
        return in;
    }

    public Type type() {
        return type;
    }

    public ShardId shardId() {
        return shardId;
    }

    public int hitCount() {
        return hitCount;
    }

    public int from() {
        return from;
    }

    public int expectedDocs() {
        return expectedDocs;
    }

    public long sequenceStart() {
        return sequenceStart;
    }

    @Override
    public void close() {
        if (serializedHits instanceof Releasable) {
            Releasables.closeWhileHandlingException((Releasable) serializedHits);
        }
        serializedHits = null;

        if (deserializedHits != null) {
            for (SearchHit hit : deserializedHits) {
                if (hit != null) {
                    hit.decRef();
                }
            }
            deserializedHits = null;
        }
    }

    /**
     * Interface for sending chunk responses from the data node to the coordinator.
     * <p>
     * Implementations handle network transport using {@link org.elasticsearch.transport.BytesTransportRequest}
     * for zero-copy transmission, and provide buffer allocation using Netty's pooled allocator.
     */
    public interface Writer {

        /**
         * Sends a chunk to the coordinator using zero-copy transport.
         *
         * @param responseChunk the chunk to send
         * @param listener      called when the chunk is acknowledged or fails
         */
        void writeResponseChunk(FetchPhaseResponseChunk responseChunk, ActionListener<Void> listener);

        /**
         * Creates a new byte stream for serializing hits.
         * <p>
         * Uses {@link org.elasticsearch.transport.TransportService#newNetworkBytesStream()}
         * which allocates buffers from Netty's pooled allocator.
         *
         * @return a new RecyclerBytesStreamOutput from the network buffer pool
         */
        RecyclerBytesStreamOutput newNetworkBytesStream();
    }
}
