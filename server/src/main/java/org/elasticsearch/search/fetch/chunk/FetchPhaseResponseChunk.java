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
import java.util.function.ObjIntConsumer;

/**
 * A single chunk of fetch results streamed from a data node to the coordinator.
 * Contains sequence information to maintain correct ordering when chunks arrive out of order.
 *
 * <p>Supports zero-copy transport by separating header metadata from serialized hits.
 * The header is created after hits are serialized (since we don't know hit count until
 * the buffer is full), then combined using {@link CompositeBytesReference} to avoid copying.
 *
 * <p>Lifecycle: on the coordinator path the stream constructor reads {@code serializedHits} as a
 * {@link ReleasableBytesReference} retained slice of the transport buffer, enabling zero-copy
 * deserialization of hit {@code _source} bytes in {@link #getHits()}. On the data-node path the
 * direct constructor accepts any {@link BytesReference}. {@link #close()} releases
 * {@code serializedHits} when it is {@link Releasable}, then {@link SearchHit#decRef()}s any
 * cached deserialized hits so pooled sources are released in a refcount-safe way (hits retain
 * their own refs until {@code decRef}).
 *
 * <p>Thread-safety: instances are single-owner. A chunk is constructed, drained via
 * {@link #consumeHits} or {@link #toReleasableBytesReference}, and closed all on one thread;
 * it is never shared between threads concurrently. Cross-thread publication of the produced
 * {@link SearchHit}s happens downstream through {@link FetchPhaseResponseStream}'s queue.
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
    private int[] hitPositions;
    private NamedWriteableRegistry namedWriteableRegistry;

    /**
     * Creates a chunk with pre-serialized hits.
     * Takes ownership of {@code serializedHits}; the caller must not release it.
     * {@link #close()} releases that buffer when it is {@link Releasable}, and also
     * {@link SearchHit#decRef() release}s any hits produced by {@link #getHits()}.
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
     * Deserializes from stream (receiving side). When {@code in} is backed by a pooled buffer
     * (e.g. a Netty transport frame), {@code serializedHits} is stored as a retained
     * {@link ReleasableBytesReference} slice — no copy — so that subsequent {@link #getHits()}
     * calls can in turn slice hit {@code _source} bytes directly from the same buffer.
     */
    public FetchPhaseResponseChunk(StreamInput in) throws IOException {
        this.shardId = new ShardId(in);
        this.hitCount = in.readVInt();
        this.expectedTotalDocs = in.readVInt();
        this.sequenceStart = in.readVLong();
        this.serializedHits = in.readReleasableBytesReference();
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

    /**
     * Iterates the hits in this chunk, invoking {@code consumer} with each hit and its position.
     * */
    public void consumeHits(HitConsumer consumer) throws IOException {
        ensureDeserialized();
        drainDeserializedHits((hit, i) -> consumer.accept(hitPositions[i], hit));
    }

    /**
     * Walks {@code deserializedHits}, invoking {@code visitor} for each non-null slot and clearing
     * the slot afterwards so the same hit is never handed out twice. Shared by {@link #consumeHits}
     * and {@link #close}.
     */
    private void drainDeserializedHits(ObjIntConsumer<SearchHit> visitor) {
        if (deserializedHits == null) {
            return;
        }
        for (int i = 0; i < deserializedHits.length; i++) {
            SearchHit hit = deserializedHits[i];
            if (hit != null) {
                visitor.accept(hit, i);
                deserializedHits[i] = null;
            }
        }
    }

    /* Visibility for tests */
    SearchHit[] getHits() throws IOException {
        ensureDeserialized();
        return deserializedHits != null ? deserializedHits : new SearchHit[0];
    }

    /* Visibility for tests */
    int[] getHitPositions() throws IOException {
        ensureDeserialized();
        return hitPositions;
    }

    /**
     * Deserializes the chunk's hits on first use.
     * */
    private void ensureDeserialized() throws IOException {
        if (deserializedHits != null || serializedHits == null || hitCount == 0) {
            return;
        }
        deserializedHits = new SearchHit[hitCount];
        hitPositions = new int[hitCount];
        try (StreamInput in = createStreamInput()) {
            for (int i = 0; i < hitCount; i++) {
                hitPositions[i] = in.readVInt();
                deserializedHits[i] = SearchHit.readFrom(in);
            }
        }
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

    /**
     * Releases {@code serializedHits} when releasable (always true on the coordinator path after
     * the stream constructor), then releases deserialized hits.
     * <p>
     * Order is safe: {@link SearchHit#readFrom(StreamInput)} uses
     * {@link StreamInput#readReleasableBytesReference()}, which on a pooled stream retains the
     * underlying buffer (via {@link ReleasableBytesReference#retainedSlice(int, int)}), so
     * releasing {@code serializedHits} first cannot free bytes that hits still own. On plain
     * heap-backed streams {@code _source} is an independent copy and no ordering constraint applies.
     */
    @Override
    public void close() {
        if (serializedHits instanceof Releasable) {
            Releasables.closeWhileHandlingException((Releasable) serializedHits);
        }
        serializedHits = null;

        drainDeserializedHits((hit, i) -> hit.decRef());
        deserializedHits = null;
    }

    /**
     * Callback invoked for each hit in a chunk together with its position.
     * */
    @FunctionalInterface
    public interface HitConsumer {
        void accept(int position, SearchHit hit);
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
