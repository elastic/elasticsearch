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
    private static final int MEM_ACCOUNTING_BUFFER_SIZE = 512 * 1024; // 512KB

    private final int shardIndex;
    private final int expectedDocs;
    private final List<SearchHit> hits = new ArrayList<>();
    private volatile boolean responseStarted = false;
    private volatile boolean ownershipTransferred = false;
    private final CircuitBreaker circuitBreaker;

    // Buffered circuit breaker accounting
    private int locallyAccumulatedBytes = 0;
    private long totalBreakerBytes = 0;

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
     * Marks the start of the response stream. Must be called before any {@link #writeChunk} calls.
     *
     * This method is invoked when the data node sends the START_RESPONSE chunk, indicating
     * that fetch processing has begun and hit chunks will follow.
     *
     * @param releasable a releasable to close after processing (typically releases the acquired stream reference)
     * @throws IllegalStateException if called more than once
     */
    void startResponse(Releasable releasable) {
        try (releasable) {
            if (responseStarted) {
                throw new IllegalStateException("response already started for shard " + shardIndex);
            }
            responseStarted = true;
            if(logger.isTraceEnabled()) {
                logger.debug("Started response stream for shard [{}], expecting [{}] docs", shardIndex, expectedDocs);
            }
        }
    }

    /**
     * Adds a chunk of hits to the accumulated result.
     *
     * This method increments the reference count of each {@link SearchHit}
     * via {@link SearchHit#incRef()} to take ownership. The hits will be released in {@link #closeInternal()}.
     *
     * @param chunk the chunk containing hits to accumulate
     * @param releasable a releasable to close after processing (typically releases the acquired stream reference)
     * @throws IllegalStateException if {@link #startResponse} has not been called first
     */
    void writeChunk(FetchPhaseResponseChunk chunk, Releasable releasable) {
        try (releasable) {
            if (responseStarted == false) {
                throw new IllegalStateException("must call startResponse first for shard " + shardIndex);
            }

            if (chunk.hits() != null) {
                for (SearchHit hit : chunk.hits().getHits()) {
                    hit.incRef();
                    hits.add(hit);

                    // Estimate memory usage from source size
                    BytesReference sourceRef = hit.getSourceRef();
                    if (sourceRef != null) {
                        // Multiply by 2 as empirical estimate (source in memory + HTTP serialization)
                        int hitBytes = sourceRef.length() * 2;
                        locallyAccumulatedBytes += hitBytes;

                        if (checkCircuitBreaker(locallyAccumulatedBytes, "fetch_chunk_accumulation")) {
                            addToBreakerTracking(locallyAccumulatedBytes);
                            locallyAccumulatedBytes = 0;
                        }
                    }
                }

                // Flush any remaining bytes in buffer at end of chunk
                if (locallyAccumulatedBytes > 0) {
                    checkCircuitBreaker(locallyAccumulatedBytes, "fetch_chunk_accumulation");
                    addToBreakerTracking(locallyAccumulatedBytes);
                    locallyAccumulatedBytes = 0;
                }
            }

            if (logger.isTraceEnabled()) {
                logger.trace(
                    "Received chunk for shard [{}]: [{}/{}] hits accumulated, {} breaker bytes",
                    shardIndex,
                    hits.size(),
                    expectedDocs,
                    totalBreakerBytes
                );
            }
        }
    }

    /**
     * Checks circuit breaker if accumulated bytes exceed threshold.
     * Similar to {@code SearchContext.checkCircuitBreaker()}.
     *
     * @param accumulatedBytes bytes accumulated in local buffer
     * @param label label for circuit breaker error messages
     * @return true if circuit breaker was checked (buffer flushed), false otherwise
     */
    private boolean checkCircuitBreaker(int accumulatedBytes, String label) {
        if (accumulatedBytes >= MEM_ACCOUNTING_BUFFER_SIZE) {
            circuitBreaker.addEstimateBytesAndMaybeBreak(accumulatedBytes, label);
            return true;
        }
        return false;
    }

    /**
     * Tracks bytes that were added to circuit breaker for later cleanup.
     * Similar to {@code SearchContext.addRequestBreakerBytes()}.
     *
     * @param bytes bytes that were added to circuit breaker
     */
    private void addToBreakerTracking(int bytes) {
        totalBreakerBytes += bytes;
    }

    /**
     * Builds the final {@link FetchSearchResult} from all accumulated hits.
     *
     * @param ctxId the shard search context ID
     * @param shardTarget the shard target information
     * @param profileResult the profile result from the data node (may be null)
     * @return a complete {@link FetchSearchResult} containing all accumulated hits
     */
    FetchSearchResult buildFinalResult(ShardSearchContextId ctxId,
                                       SearchShardTarget shardTarget,
                                       @Nullable ProfileResult profileResult) {
        if(logger.isTraceEnabled()) {
            logger.debug("Building final result for shard [{}] with [{}] hits", shardIndex, hits.size());
        }

        float maxScore = Float.NEGATIVE_INFINITY;
        for (SearchHit hit : hits) {
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
            logger.trace(
                "Closing response stream for shard [{}], releasing [{}] hits, {} breaker bytes",
                shardIndex,
                hits.size(),
                totalBreakerBytes
            );
        }

        if (ownershipTransferred == false) {
            for (SearchHit hit : hits) {
                hit.decRef();
            }
        }
        hits.clear();

        // Release circuit breaker bytes added during accumulation when hits are released from memory
        if (totalBreakerBytes > 0) {
            circuitBreaker.addWithoutBreaking(-totalBreakerBytes);
            if (logger.isTraceEnabled()) {
                logger.trace("Released {} breaker bytes for shard [{}]", totalBreakerBytes, shardIndex);
            }
            totalBreakerBytes = 0;
        }

        // Reset local buffer (should already be 0, but defensive)
        locallyAccumulatedBytes = 0;
    }
}
