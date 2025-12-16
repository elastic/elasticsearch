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
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.core.AbstractRefCounted;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.fetch.FetchSearchResult;
import org.elasticsearch.search.internal.ShardSearchContextId;
import org.elasticsearch.search.profile.ProfileResult;

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
    private final int expectedDocs;

    // Accumulate hits with sequence numbers for ordering
    private final Queue<SequencedHit> queue = new ConcurrentLinkedQueue<>();
    private volatile boolean ownershipTransferred = false;

    // Circuit breaker accounting
    private final CircuitBreaker circuitBreaker;
    private final AtomicLong totalBreakerBytes = new AtomicLong(0);

    /**
     * Creates a new response stream for accumulating hits from a single shard.
     *
     * @param shardIndex the shard ID this stream is collecting hits for
     * @param expectedDocs the total number of documents expected to be fetched from this shard
     * @param circuitBreaker circuit breaker to check memory usage during accumulation (typically REQUEST breaker)
     */
    FetchPhaseResponseStream(int shardIndex, int expectedDocs, CircuitBreaker circuitBreaker) {
        this.shardIndex = shardIndex;
        this.expectedDocs = expectedDocs;
        this.circuitBreaker = circuitBreaker;
    }

    /**
     * Adds a chunk of hits to the accumulated result.
     *
     * This method increments the reference count of each {@link SearchHit}
     * via {@link SearchHit#incRef()} to take ownership. The hits will be released in {@link #closeInternal()}.
     *
     * @param chunk the chunk containing hits to accumulate
     * @param releasable a releasable to close after processing (typically releases the acquired stream reference)
     */
    void writeChunk(FetchPhaseResponseChunk chunk, Releasable releasable) {
        boolean success = false;
        try {
            if (chunk.hits() != null) {
                SearchHit[] chunkHits = chunk.hits().getHits();
                long sequenceStart = chunk.sequenceStart();

                for (int i = 0; i < chunkHits.length; i++) {
                    SearchHit hit = chunkHits[i];
                    hit.incRef();

                    // Calculate sequence: chunk start + index within chunk
                    long hitSequence = sequenceStart + i;
                    queue.add(new SequencedHit(hit, hitSequence));

                    // Estimate memory usage from source size
                    BytesReference sourceRef = hit.getSourceRef();
                    if (sourceRef != null) {
                        int hitBytes = sourceRef.length() * 2;
                        circuitBreaker.addEstimateBytesAndMaybeBreak(hitBytes, "fetch_chunk_accumulation");
                        totalBreakerBytes.addAndGet(hitBytes);
                    }
                }
            }

            if (logger.isTraceEnabled()) {
                logger.info(
                    "Received chunk [{}] docs for shard [{}]: [{}/{}] hits accumulated, [{}] breaker bytes, used breaker bytes [{}]",
                    chunk.hits() == null ? 0 : chunk.hits().getHits().length,
                    shardIndex,
                    queue.size(),
                    expectedDocs,
                    totalBreakerBytes.get(),
                    circuitBreaker.getUsed()
                );
            }
            success = true;
        } finally {
            if (success) {
                releasable.close();
            }
        }
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
        if (logger.isTraceEnabled()) {
            logger.info("Building final result for shard [{}] with [{}] hits", shardIndex, queue.size());
        }

        // Convert queue to list and sort by sequence number to restore correct order
        List<SequencedHit> sequencedHits = new ArrayList<>(queue);
        sequencedHits.sort(Comparator.comparingLong(sh -> sh.sequence));

        // Extract hits in correct order and calculate maxScore
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

        // Hits have refCount=1, SearchHits constructor will increment to 2
        ownershipTransferred = true;

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
     * Gets the current size of the queue. Used for debugging and monitoring.
     */
    int getCurrentQueueSize() {
        return queue.size();
    }

    /**
     * Releases accumulated hits and circuit breaker bytes when hits are released from memory.
     */
    @Override
    protected void closeInternal() {
        if (logger.isTraceEnabled()) {
            logger.info(
                "Closing response stream for shard [{}], releasing [{}] hits, [{}] breaker bytes",
                shardIndex,
                queue.size(),
                totalBreakerBytes.get()
            );
        }

        if (ownershipTransferred == false) {
            for (SequencedHit sequencedHit : queue) {
                sequencedHit.hit.decRef();
            }
        }
        queue.clear();

        // Release circuit breaker bytes added during accumulation when hits are released from memory
        if (totalBreakerBytes.get() > 0) {
            circuitBreaker.addWithoutBreaking(-totalBreakerBytes.get());
            if (logger.isTraceEnabled()) {
                logger.info(
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
}
