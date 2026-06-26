/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.fetch.chunk;

import org.apache.lucene.search.TotalHits;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.core.AbstractRefCounted;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.fetch.FetchSearchResult;
import org.elasticsearch.search.internal.ShardSearchContextId;
import org.elasticsearch.search.profile.ProfileResult;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Accumulates {@link SearchHit} chunks sent from a data node during a chunked fetch operation.
 * Runs on the coordinator node and maintains an in-memory buffer of hits received
 * from a single shard on a data node. The data node sends hits in small chunks to
 * avoid large network messages and memory pressure.
 *
 * Uses sequence numbers to maintain correct ordering when chunks arrive out of order.
 **/
class FetchPhaseResponseStream extends AbstractRefCounted {

    private static final Logger logger = LogManager.getLogger(FetchPhaseResponseStream.class);

    private final int shardIndex;
    private final int expectedTotalDocs;

    // Accumulate hits with sequence numbers for ordering. Populated by the embedded last chunk path
    // (already-deserialized hits) and by deferred deserialization of pending chunks in buildFinalResult.
    private final Queue<SequencedHit> queue = new ConcurrentLinkedQueue<>();

    // Raw, not-yet-deserialized streamed chunks. Hits are decoded lazily in buildFinalResult so that
    // chunk reception can be acknowledged to the data node without paying per-hit deserialization cost
    // on the chunk-ACK critical path.
    private final Queue<PendingChunk> pendingChunks = new ConcurrentLinkedQueue<>();

    // Registry needed to deserialize NamedWriteable fields (e.g. LookupField) within serialized hits.
    private final NamedWriteableRegistry namedWriteableRegistry;

    // Circuit breaker accounting
    private final CircuitBreaker circuitBreaker;
    private final AtomicLong totalBreakerBytes = new AtomicLong(0);

    /**
     * Creates a new response stream for accumulating hits from a single shard.
     *
     * @param shardIndex the shard ID this stream is collecting hits for
     * @param expectedTotalDocs total number of documents requested for this shard fetch operation
     *                          across all chunks (target/requested count, not guaranteed delivered count)
     * @param circuitBreaker circuit breaker to check memory usage during accumulation (typically REQUEST breaker)
     * @param namedWriteableRegistry registry used to deserialize NamedWriteable hit fields when chunks are
     *                          decoded lazily in {@link #buildFinalResult}
     */
    FetchPhaseResponseStream(
        int shardIndex,
        int expectedTotalDocs,
        CircuitBreaker circuitBreaker,
        NamedWriteableRegistry namedWriteableRegistry
    ) {
        this.shardIndex = shardIndex;
        this.expectedTotalDocs = expectedTotalDocs;
        this.circuitBreaker = circuitBreaker;
        this.namedWriteableRegistry = namedWriteableRegistry;
    }

    /**
     * Accumulates a chunk of hits into this stream.
     *
     * @param chunk the chunk containing hits to accumulate
     * @param releasable closed after a successful write
     */
    void writeChunk(FetchPhaseResponseChunk chunk, Releasable releasable) {
        // Track memory usage. Reserved before acknowledging; may trip the breaker and fail the chunk,
        // in which case the bytes are not taken and the chunk is released by the caller.
        long bytesSize = chunk.getBytesLength();
        circuitBreaker.addEstimateBytesAndMaybeBreak(bytesSize, "fetch_chunk_accumulation");
        totalBreakerBytes.addAndGet(bytesSize);

        // Defer hit deserialization off the chunk-ACK critical path: retain the raw serialized bytes
        // now and decode them later in buildFinalResult. This releases the data node's in-flight chunk
        // permit as soon as the bytes are received and accounted, rather than after per-hit decoding.
        final ReleasableBytesReference rawHits = chunk.takeSerializedHits();
        boolean enqueued = false;
        try {
            if (rawHits != null && chunk.hitCount() > 0) {
                pendingChunks.add(new PendingChunk(rawHits, chunk.hitCount()));
                enqueued = true;
            }
        } finally {
            if (enqueued == false && rawHits != null) {
                Releasables.closeWhileHandlingException(rawHits);
            }
        }

        if (logger.isDebugEnabled()) {
            logger.debug(
                "Received chunk [{}] docs for shard [{}]: [{}/{}] chunks pending, [{}] breaker bytes, used breaker bytes [{}]",
                chunk.hitCount(),
                shardIndex,
                pendingChunks.size(),
                expectedTotalDocs,
                totalBreakerBytes.get(),
                circuitBreaker.getUsed()
            );
        }

        // Acknowledge only after the bytes are retained and accounted.
        releasable.close();
    }

    /**
     * Builds the final {@link FetchSearchResult} from all accumulated hits.
     * Sorts hits by sequence number to restore correct order.
     *
     * @param ctxId the shard search context ID
     * @param shardTarget the shard target information
     * @param profileResult the profile result from the data node (may be null)
     * @return a complete {@link FetchSearchResult} containing all accumulated hits in correct order
     */
    FetchSearchResult buildFinalResult(ShardSearchContextId ctxId, SearchShardTarget shardTarget, @Nullable ProfileResult profileResult) {
        if (logger.isDebugEnabled()) {
            logger.debug("Building final result for shard [{}] with [{}] hits", shardIndex, queue.size());
        }

        // Drain the queue: ownership of every hit moves to the final result. Contains any
        // already-deserialized hits from the embedded last chunk path.
        List<SequencedHit> sequencedHits = drainQueue();

        // Deserialize the deferred streamed chunks now, off the chunk-ACK critical path.
        PendingChunk pending;
        while ((pending = pendingChunks.poll()) != null) {
            try (
                PendingChunk toClose = pending;
                StreamInput in = new NamedWriteableAwareStreamInput(pending.bytes().streamInput(), namedWriteableRegistry)
            ) {
                for (int i = 0; i < pending.hitCount(); i++) {
                    int position = in.readVInt();
                    sequencedHits.add(new SequencedHit(SearchHit.readFrom(in), position));
                }
            } catch (IOException e) {
                // Release everything collected so far and any remaining undeserialized chunks.
                for (SequencedHit sh : sequencedHits) {
                    sh.hit.decRef();
                }
                PendingChunk rest;
                while ((rest = pendingChunks.poll()) != null) {
                    rest.close();
                }
                throw new RuntimeException("Failed to deserialize hits from chunk", e);
            }
        }

        // Restore correct order (chunks may have arrived out of order).
        sequencedHits.sort(Comparator.comparingLong(sh -> sh.sequence));

        List<SearchHit> orderedHits = new ArrayList<>(sequencedHits.size());
        float maxScore = Float.NEGATIVE_INFINITY;

        for (SequencedHit sequencedHit : sequencedHits) {
            SearchHit hit = sequencedHit.hit;
            orderedHits.add(hit);

            if (Float.isNaN(hit.getScore()) == false) {
                maxScore = Math.max(maxScore, hit.getScore());
            }
        }

        if (maxScore == Float.NEGATIVE_INFINITY) {
            maxScore = Float.NaN;
        }

        SearchHits searchHits = new SearchHits(
            orderedHits.toArray(SearchHit[]::new),
            new TotalHits(orderedHits.size(), TotalHits.Relation.EQUAL_TO),
            maxScore
        );

        FetchSearchResult result = new FetchSearchResult(ctxId, shardTarget);
        result.shardResult(searchHits, profileResult);
        return result;
    }

    /**
     * Adds a single hit with explicit sequence number to the accumulated result.
     * Used for processing the last chunk embedded in FetchSearchResult where sequence is known.
     *
     * @param hit the hit to add
     * @param sequence the sequence number for this hit
     */
    void addHitWithSequence(SearchHit hit, long sequence) {
        queue.add(new SequencedHit(hit, sequence));
    }

    /**
     * Tracks circuit breaker bytes without checking. Used when coordinator processes the embedded last chunk.
     */
    void trackBreakerBytes(int bytes) {
        totalBreakerBytes.addAndGet(bytes);
    }

    /**
     * Releases accumulated hits and circuit breaker bytes when hits are released from memory.
     */
    @Override
    protected void closeInternal() {
        if (logger.isDebugEnabled()) {
            logger.debug(
                "Closing response stream for shard [{}], releasing [{}] hits, [{}] breaker bytes",
                shardIndex,
                queue.size(),
                totalBreakerBytes.get()
            );
        }

        // Release any hits still queued
        for (SequencedHit pending : drainQueue()) {
            pending.hit.decRef();
        }

        // Release any raw chunks that were never deserialized
        PendingChunk pendingChunk;
        while ((pendingChunk = pendingChunks.poll()) != null) {
            pendingChunk.close();
        }

        // Release circuit breaker bytes added during accumulation when hits are released from memory
        if (totalBreakerBytes.get() > 0) {
            circuitBreaker.addWithoutBreaking(-totalBreakerBytes.get());
            if (logger.isDebugEnabled()) {
                logger.debug(
                    "Released [{}] breaker bytes for shard [{}], used breaker bytes [{}]",
                    totalBreakerBytes.get(),
                    shardIndex,
                    circuitBreaker.getUsed()
                );
            }
            totalBreakerBytes.set(0);
        }
    }

    /**
     * Polls and returns every hit currently in the queue.
     */
    private List<SequencedHit> drainQueue() {
        List<SequencedHit> drained = new ArrayList<>();
        SequencedHit polled;
        while ((polled = queue.poll()) != null) {
            drained.add(polled);
        }
        return drained;
    }

    /**
     * Wrapper class that pairs a SearchHit with its sequence number.
     * This ensures we can restore the correct order even if chunks arrive out of order.
     */
    private static class SequencedHit {
        final SearchHit hit;
        final long sequence;

        SequencedHit(SearchHit hit, long sequence) {
            this.hit = hit;
            this.sequence = sequence;
        }
    }

    /**
     * A streamed chunk whose hits have been received and accounted but not yet deserialized.
     * Holds a retained reference to the raw serialized bytes; {@link #close()} releases it.
     */
    private record PendingChunk(ReleasableBytesReference bytes, int hitCount) implements Releasable {
        @Override
        public void close() {
            Releasables.closeWhileHandlingException(bytes);
        }
    }
}
