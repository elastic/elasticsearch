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

    /**
     * Initial capacity hint for chunk metadata serialization.
     * <p>
     * The metadata contains a few fields plus a reference to the already serialized
     * hit payload. The payload size dominates and the stream can grow if needed, so this is
     * intentionally a small preallocation to avoid over-reserving per chunk.
     */
    private static final int INITIAL_CHUNK_SERIALIZATION_CAPACITY = 128;

    private final ShardId shardId;
    private final int hitCount;
    private final int expectedTotalDocs;
    private final long sequenceStart;

    private BytesReference serializedHits;
    private SearchHit[] deserializedHits;
    private NamedWriteableRegistry namedWriteableRegistry;

    /**
     * Creates a chunk with pre-serialized hits.
     * Takes ownership of serializedHits - caller must not release it.
     *
     * @param shardId          source shard
     * @param serializedHits   pre-serialized hit bytes
     * @param hitCount         number of hits in the serialized bytes
     * @param expectedTotalDocs total number of documents requested for this shard fetch operation
     *                          across all chunks (derived from requested doc IDs, not an observed
     *                          count of docs received so far)
     * @param sequenceStart    sequence number of first hit for ordering
     */
    public FetchPhaseResponseChunk(
        ShardId shardId,
        BytesReference serializedHits,
        int hitCount,
        int expectedTotalDocs,
        long sequenceStart
    ) {
        this.shardId = shardId;
        this.serializedHits = serializedHits;
        this.hitCount = hitCount;
        this.expectedTotalDocs = expectedTotalDocs;
        this.sequenceStart = sequenceStart;
    }

    /**
     * Deserializes from stream (receiving side).
     */
    public FetchPhaseResponseChunk(StreamInput in) throws IOException {
        this.shardId = new ShardId(in);
        this.hitCount = in.readVInt();
        this.expectedTotalDocs = in.readVInt();
        this.sequenceStart = in.readVLong();
        this.serializedHits = in.readBytesReference();
        this.namedWriteableRegistry = in.namedWriteableRegistry();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        shardId.writeTo(out);
        out.writeVInt(hitCount);
        out.writeVInt(expectedTotalDocs);
        out.writeVLong(sequenceStart);
        out.writeBytesReference(serializedHits);
    }

    public ReleasableBytesReference toReleasableBytesReference(long coordinatingTaskId) throws IOException {
        final ReleasableBytesReference result;
        try (BytesStreamOutput header = new BytesStreamOutput(INITIAL_CHUNK_SERIALIZATION_CAPACITY)) {
            header.writeVLong(coordinatingTaskId);
            shardId.writeTo(header);
            header.writeVInt(hitCount);
            header.writeVInt(expectedTotalDocs);
            header.writeVLong(sequenceStart);
            header.writeVInt(serializedHits.length());

            BytesReference composite = CompositeBytesReference.of(header.copyBytes(), serializedHits);
            if (serializedHits instanceof ReleasableBytesReference releasableHits) {
                result = new ReleasableBytesReference(composite, releasableHits::decRef);
            } else {
                result = ReleasableBytesReference.wrap(composite);
            }
            this.serializedHits = null;
        }
        return result;
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

    public ShardId shardId() {
        return shardId;
    }

    public int hitCount() {
        return hitCount;
    }

    public int expectedTotalDocs() {
        return expectedTotalDocs;
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
         * Creates a new byte stream for serializing hits. Uses a network buffer pool for efficient allocation.
         *
         * @return a new RecyclerBytesStreamOutput from the network buffer pool
         */
        RecyclerBytesStreamOutput newNetworkBytesStream();
    }
}
