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
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Accumulates {@link SearchHit} chunks sent from a data node during a chunked fetch operation.
 * Runs on the coordinator node and maintains an in-memory buffer of hits received
 * from a single shard on a data node. The data node sends hits in small chunks to
 * avoid large network messages and memory pressure.
 **/
class FetchPhaseResponseStream extends AbstractRefCounted {

    private static final Logger logger = LogManager.getLogger(FetchPhaseResponseStream.class);

    /**
     * Buffer size before checking circuit breaker. Same as SearchContext's memAccountingBufferSize.
     * This reduces contention by batching circuit breaker checks.
     */
    private static final int MEM_ACCOUNTING_BUFFER_SIZE = 512; // 512KB

    private final int shardIndex;
    private final int expectedDocs;

    // Accumulate hits
    private final Queue<SearchHit> queue = new ConcurrentLinkedQueue<>();
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
                for (SearchHit hit : chunk.hits().getHits()) {
                    hit.incRef();
                    queue.add(hit);

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
                    "Received [{}] chunk [{}] docs for shard [{}]: [{}/{}] hits accumulated, [{}] breaker bytes, used breaker bytes [{}]",
                    chunk.timestampMillis(),
                    chunk.hits().getHits().length,
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
     *
     * @param ctxId the shard search context ID
     * @param shardTarget the shard target information
     * @param profileResult the profile result from the data node (may be null)
     * @return a complete {@link FetchSearchResult} containing all accumulated hits
     */
    FetchSearchResult buildFinalResult(ShardSearchContextId ctxId, SearchShardTarget shardTarget, @Nullable ProfileResult profileResult) {
        if (logger.isTraceEnabled()) {
            logger.info("Building final result for shard [{}] with [{}] hits", shardIndex, queue.size());
        }

        float maxScore = Float.NEGATIVE_INFINITY;
        for (SearchHit hit : queue) {
            if (Float.isNaN(hit.getScore()) == false) {
                maxScore = Math.max(maxScore, hit.getScore());
            }
        }
        if (maxScore == Float.NEGATIVE_INFINITY) {
            maxScore = Float.NaN;
        }

        // Hits have refCount=1, SearchHits constructor will increment to 2
        ownershipTransferred = true;

        List<SearchHit> hits = new ArrayList<>(queue);
        SearchHits searchHits = new SearchHits(
            hits.toArray(SearchHit[]::new),
            new TotalHits(hits.size(), TotalHits.Relation.EQUAL_TO),
            maxScore
        );

        FetchSearchResult result = new FetchSearchResult(ctxId, shardTarget);
        result.shardResult(searchHits, profileResult);
        return result;
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
            for (SearchHit hit : queue) {
                hit.decRef();
            }
        }
        queue.clear();

        // Release circuit breaker bytes added during accumulation when hits are released from memory
        if (totalBreakerBytes.get() > 0) {
            //if(circuitBreaker.getUsed() >= totalBreakerBytes) {
            circuitBreaker.addWithoutBreaking(-totalBreakerBytes.get());
            //}
            if (logger.isTraceEnabled()) {
                logger.info("Released [{}] breaker bytes for shard [{}], used breaker bytes [{}]",
                    totalBreakerBytes.get(), shardIndex, circuitBreaker.getUsed());
            }
            totalBreakerBytes.set(0);
        }
    }
}
