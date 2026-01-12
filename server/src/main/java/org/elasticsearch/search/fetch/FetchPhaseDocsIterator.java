/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.fetch;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.search.TotalHits;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.RefCountingListener;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.fetch.chunk.FetchPhaseResponseChunk;
import org.elasticsearch.search.internal.ContextIndexSearcher;
import org.elasticsearch.search.query.QuerySearchResult;
import org.elasticsearch.search.query.SearchTimeoutException;
import org.elasticsearch.tasks.TaskCancelledException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

/**
 * Iterates through a set of document IDs, fetching each document and collecting
 * the resulting {@link SearchHit}s.
 * <p>
 * Supports two modes of operation:
 * <ul>
 *   <li><b>Non-streaming mode</b> ({@link #iterate}): Documents are sorted by doc ID for
 *       efficient sequential Lucene access, then results are mapped back to their original
 *       score-based order. All hits are collected in memory and returned at once.</li>
 *   <li><b>Streaming mode</b> ({@link #iterateAsync}): Documents are fetched in chunks and
 *       streamed to the coordinator as they become ready. A semaphore-based backpressure
 *       mechanism limits in-flight chunks to bound memory usage. Sequence numbers track
 *       hit ordering for reassembly at the coordinator.</li>
 * </ul>
 * <p>
 * In both modes, the iterator splits documents by leaf reader and calls
 * {@link #setNextReader(LeafReaderContext, int[])} when crossing segment boundaries,
 * then {@link #nextDoc(int)} for each document.
 * <p>
 * <b>Threading:</b> All Lucene operations execute on a single thread to satisfy
 * Lucene's thread-affinity requirements. In streaming mode, only network transmission
 * and ACK handling occur asynchronously.
 * <p>
 * <b>Cancellation:</b> Streaming mode supports responsive task cancellation by polling
 * a cancellation flag at chunk boundaries and during backpressure waits.
 */
abstract class FetchPhaseDocsIterator {

    // Timeout interval
    private static final long CANCELLATION_CHECK_INTERVAL_MS = 200;

    /**
     * Accounts for FetchPhase memory usage.
     * It gets cleaned up after each fetch phase and should not be accessed/modified by subclasses.
     */
    private long requestBreakerBytes;

    /**
     * Sequence counter for tracking hit order in streaming mode.
     * Each hit gets a unique sequence number allowing the coordinator to restore correct order
     * even if chunks arrive out of order.
     */
    private final AtomicLong hitSequenceCounter = new AtomicLong(0);

    public void addRequestBreakerBytes(long delta) {
        requestBreakerBytes += delta;
    }

    public long getRequestBreakerBytes() {
        return requestBreakerBytes;
    }

    /**
     * Called when a new leaf reader is reached
     * @param ctx           the leaf reader for this set of doc ids
     * @param docsInLeaf    the reader-specific docids to be fetched in this leaf reader
     */
    protected abstract void setNextReader(LeafReaderContext ctx, int[] docsInLeaf) throws IOException;

    /**
     * Called for each document within a leaf reader
     * @param doc   the global doc id
     * @return a {@link SearchHit} for the document
     */
    protected abstract SearchHit nextDoc(int doc) throws IOException;

    /**
     * Synchronous iteration for non-streaming mode.
     * Documents are sorted by doc ID for efficient sequential Lucene access,
     * then results are mapped back to their original (score-based) order.
     *
     * @param shardTarget         the shard being fetched from
     * @param indexReader         the index reader for accessing documents
     * @param docIds              document IDs to fetch (in score order)
     * @param allowPartialResults if true, return partial results on timeout instead of failing
     * @param querySearchResult   query result for recording timeout state
     * @return IterateResult containing fetched hits in original score order
     * @throws SearchTimeoutException if timeout occurs and partial results not allowed
     * @throws FetchPhaseExecutionException if fetch fails for a document
     */
    public final IterateResult iterate(
        SearchShardTarget shardTarget,
        IndexReader indexReader,
        int[] docIds,
        boolean allowPartialResults,
        QuerySearchResult querySearchResult
    ) {
        SearchHit[] searchHits = new SearchHit[docIds.length];
        DocIdToIndex[] docs = new DocIdToIndex[docIds.length];
        for (int index = 0; index < docIds.length; index++) {
            docs[index] = new DocIdToIndex(docIds[index], index);
        }
        // make sure that we iterate in doc id order
        Arrays.sort(docs);
        int currentDoc = docs[0].docId;
        try {
            if (docs.length == 0) {
                return new IterateResult(searchHits, null, -1);
            }

            int leafOrd = ReaderUtil.subIndex(docs[0].docId, indexReader.leaves());
            LeafReaderContext ctx = indexReader.leaves().get(leafOrd);
            int endReaderIdx = endReaderIdx(ctx, 0, docs);
            int[] docsInLeaf = docIdsInLeaf(0, endReaderIdx, docs, ctx.docBase);
            try {
                setNextReader(ctx, docsInLeaf);
            } catch (ContextIndexSearcher.TimeExceededException e) {
                SearchTimeoutException.handleTimeout(allowPartialResults, shardTarget, querySearchResult);
                assert allowPartialResults;
                return new IterateResult(new SearchHit[0], null, -1);
            }

            for (int i = 0; i < docs.length; i++) {
                try {
                    if (i >= endReaderIdx) {
                        leafOrd = ReaderUtil.subIndex(docs[i].docId, indexReader.leaves());
                        ctx = indexReader.leaves().get(leafOrd);
                        endReaderIdx = endReaderIdx(ctx, i, docs);
                        docsInLeaf = docIdsInLeaf(i, endReaderIdx, docs, ctx.docBase);
                        setNextReader(ctx, docsInLeaf);
                    }
                    currentDoc = docs[i].docId;
                    assert searchHits[docs[i].index] == null;
                    searchHits[docs[i].index] = nextDoc(docs[i].docId);
                } catch (ContextIndexSearcher.TimeExceededException e) {
                    if (allowPartialResults == false) {
                        purgeSearchHits(searchHits);
                    }
                    SearchTimeoutException.handleTimeout(allowPartialResults, shardTarget, querySearchResult);
                    assert allowPartialResults;
                    SearchHit[] partialSearchHits = new SearchHit[i];
                    System.arraycopy(searchHits, 0, partialSearchHits, 0, i);
                    return new IterateResult(partialSearchHits, null, -1);
                }
            }
        } catch (SearchTimeoutException e) {
            throw e;
        } catch (CircuitBreakingException e) {
            purgeSearchHits(searchHits);
            throw e;
        } catch (Exception e) {
            purgeSearchHits(searchHits);
            throw new FetchPhaseExecutionException(shardTarget, "Error running fetch phase for doc [" + currentDoc + "]", e);
        }
        return new IterateResult(searchHits, null, -1);
    }

    /**
     * Asynchronous iteration for streaming mode with backpressure.
     * <p>
     * Fetches documents in chunks and streams them to the coordinator as they're ready.
     * Uses a semaphore-based backpressure mechanism to limit in-flight chunks, preventing
     * memory exhaustion when the coordinator is slow to acknowledge.
     * <p>
     * <b>Threading model:</b> All Lucene operations (setNextReader, nextDoc) execute on the
     * calling thread to maintain Lucene's thread-affinity requirements. Only the network
     * send and ACK handling occur asynchronously.
     * <p>
     * <b>Chunk handling:</b>
     * <ul>
     *   <li>Non-last chunks are sent immediately and tracked via semaphore permits</li>
     *   <li>The last chunk is held back and returned via the listener for the caller to send</li>
     *   <li>Each chunk includes a sequence number for reassembly at the coordinator</li>
     * </ul>
     * <p>
     * <b>Cancellation:</b> The method periodically checks the cancellation flag between chunks
     * and while waiting for backpressure permits, ensuring responsive cancellation even under
     * heavy backpressure.
     *
     * @param shardTarget         the shard being fetched from
     * @param indexReader         the index reader for accessing documents
     * @param docIds              document IDs to fetch (in score order, not modified)
     * @param chunkWriter         writer for sending chunks to the coordinator
     * @param chunkSize           number of hits per chunk
     * @param chunkCompletionRefs ref-counting listener for tracking outstanding chunk ACKs;
     *                            caller uses this to know when all chunks are acknowledged
     * @param maxInFlightChunks   maximum concurrent unacknowledged chunks (backpressure limit)
     * @param sendFailure         atomic reference to capture the first send failure;
     *                            checked before each chunk to fail fast
     * @param totalHits           total hits count for SearchHits metadata
     * @param maxScore            maximum score for SearchHits metadata
     * @param isCancelled         supplier that returns true if the task has been cancelled;
     *                            checked periodically to support responsive cancellation
     * @param listener            receives the result: empty hits array plus the last chunk
     *                            (which caller must send) with its sequence start position
     */
    public void iterateAsync(
        SearchShardTarget shardTarget,
        IndexReader indexReader,
        int[] docIds,
        FetchPhaseResponseChunk.Writer chunkWriter,
        int chunkSize,
        RefCountingListener chunkCompletionRefs,
        int maxInFlightChunks,
        AtomicReference<Throwable> sendFailure,
        TotalHits totalHits,
        float maxScore,
        Supplier<Boolean> isCancelled,
        ActionListener<IterateResult> listener
    ) {
        if (docIds == null || docIds.length == 0) {
            listener.onResponse(new IterateResult(new SearchHit[0], null, -1));
            return;
        }

        // Semaphore controls backpressure, each in-flight chunk holds one permit.
        // When maxInFlightChunks are in flight, we block until an ACK releases a permit.
        Semaphore transmitPermits = new Semaphore(maxInFlightChunks);

        SearchHits lastChunk = null;
        long lastChunkSeqStart = -1;
        ShardId shardId = shardTarget.getShardId();
        int totalDocs = docIds.length;

        // Leaf reader state - maintained across iterations for efficiency.
        // Only changes when we cross into a new segment.
        int currentLeafOrd = -1;
        LeafReaderContext currentCtx = null;

        try {
            for (int start = 0; start < docIds.length; start += chunkSize) {
                int end = Math.min(start + chunkSize, docIds.length);
                boolean isLast = (end == docIds.length);

                // Check cancellation at chunk boundaries for responsive task cancellation
                if (isCancelled.get()) {
                    throw new TaskCancelledException("cancelled");
                }

                // Check for prior send failure
                Throwable failure = sendFailure.get();
                if (failure != null) {
                    throw failure instanceof Exception ? (Exception) failure : new RuntimeException(failure);
                }

                List<SearchHit> hits = new ArrayList<>(end - start);
                for (int i = start; i < end; i++) {
                    int docId = docIds[i];

                    int leafOrd = ReaderUtil.subIndex(docId, indexReader.leaves());
                    if (leafOrd != currentLeafOrd) {
                        currentLeafOrd = leafOrd;
                        currentCtx = indexReader.leaves().get(leafOrd);
                        int[] docsInLeaf = computeDocsInLeaf(docIds, i, end, currentCtx);
                        setNextReader(currentCtx, docsInLeaf);
                    }

                    SearchHit hit = nextDoc(docId);
                    hits.add(hit);
                }

                SearchHits chunk = createSearchHits(hits, totalHits, maxScore);
                long sequenceStart = hitSequenceCounter.getAndAdd(chunk.getHits().length);

                if (isLast) {
                    // Hold back last chunk - caller sends it after all ACKs received
                    lastChunk = chunk;
                    lastChunkSeqStart = sequenceStart;
                } else {
                    // Wait for permit before sending
                    // This blocks if maxInFlightChunks are already in flight,
                    // with periodic cancellation checks to remain responsive
                    acquirePermitWithCancellationCheck(transmitPermits, isCancelled);

                    // Send chunk asynchronously - permit released when ACK arrives
                    sendChunk(
                        chunk,
                        chunkWriter,
                        shardId,
                        sequenceStart,
                        start,
                        totalDocs,
                        sendFailure,
                        chunkCompletionRefs.acquire(),
                        transmitPermits
                    );
                }
            }

            // Wait for all in-flight chunks to be acknowledged
            // Ensures we don't return until all chunks are safely received
            waitForAllPermits(transmitPermits, maxInFlightChunks, isCancelled);

            // Final failure check after all chunks sent
            Throwable failure = sendFailure.get();
            if (failure != null) {
                if (lastChunk != null) {
                    lastChunk.decRef();
                }
                throw failure instanceof Exception ? (Exception) failure : new RuntimeException(failure);
            }

            // Return last chunk for caller to send (completes the streaming response)
            listener.onResponse(new IterateResult(new SearchHit[0], lastChunk, lastChunkSeqStart));
        } catch (Exception e) {
            // Clean up last chunk on any failure
            if (lastChunk != null) {
                lastChunk.decRef();
            }
            listener.onFailure(e);
        }
    }

    /**
     * Sends a chunk of search hits to the coordinator.
     * <p>
     * Wraps the hits in a {@link FetchPhaseResponseChunk} message and writes it via the
     * chunk writer. Handles reference counting and permit management for both success
     * and failure cases.
     *
     * @param chunk          the search hits to send (reference will be released)
     * @param writer         the chunk writer for network transmission
     * @param shardId        the source shard identifier
     * @param sequenceStart  starting sequence number for hit ordering at coordinator
     * @param fromIndex      index of first doc in this chunk (for progress tracking)
     * @param totalDocs      total documents being fetched (for progress tracking)
     * @param sendFailure    atomic reference to capture first failure
     * @param ackListener    listener to signal when ACK received (for RefCountingListener)
     * @param transmitPermits semaphore to release when ACK received (backpressure control)
     */
    private void sendChunk(
        SearchHits chunk,
        FetchPhaseResponseChunk.Writer writer,
        ShardId shardId,
        long sequenceStart,
        int fromIndex,
        int totalDocs,
        AtomicReference<Throwable> sendFailure,
        ActionListener<Void> ackListener,
        Semaphore transmitPermits
    ) {
        try {
            FetchPhaseResponseChunk chunkMsg = new FetchPhaseResponseChunk(
                System.currentTimeMillis(),
                FetchPhaseResponseChunk.Type.HITS,
                shardId,
                chunk,
                fromIndex,
                chunk.getHits().length,
                totalDocs,
                sequenceStart
            );

            writer.writeResponseChunk(chunkMsg, ActionListener.wrap(ack -> {
                // Success: clean up and signal completion
                chunk.decRef();
                ackListener.onResponse(null);
                transmitPermits.release();  // Allow next chunk to proceed
            }, e -> {
                // Failure: clean up, record error, and release permit
                chunk.decRef();
                sendFailure.compareAndSet(null, e);
                ackListener.onFailure(e);
                transmitPermits.release();  // Release even on failure
            }));
        } catch (Exception e) {
            chunk.decRef();
            sendFailure.compareAndSet(null, e);
            ackListener.onFailure(e);
            transmitPermits.release();
        }
    }

    /**
     * Acquires a single permit from the semaphore, polling for task cancellation
     * between acquisition attempts.
     */
    private void acquirePermitWithCancellationCheck(Semaphore semaphore, Supplier<Boolean> isCancelled) throws InterruptedException {
        while (semaphore.tryAcquire(CANCELLATION_CHECK_INTERVAL_MS, TimeUnit.MILLISECONDS) == false) {
            if (isCancelled.get()) {
                throw new TaskCancelledException("cancelled");
            }
        }
    }

    /**
     * Waits for all permits to become available (indicating all chunks have been ACKed),
     * polling for task cancellation between attempts. Permits are re-released after acquisition
     * since we're just checking that all async work has completed.
     */
    private void waitForAllPermits(Semaphore semaphore, int totalPermits, Supplier<Boolean> isCancelled) throws InterruptedException {
        int acquired = 0;
        try {
            while (acquired < totalPermits) {
                while (semaphore.tryAcquire(CANCELLATION_CHECK_INTERVAL_MS, TimeUnit.MILLISECONDS) == false) {
                    if (isCancelled.get()) {
                        throw new TaskCancelledException("cancelled");
                    }
                }
                acquired++;
            }
        } finally {
            // Release all acquired permits - we were just checking completion
            if (acquired > 0) {
                semaphore.release(acquired);
            }
        }
    }

    private int[] computeDocsInLeaf(int[] docIds, int fromIndex, int toIndex, LeafReaderContext ctx) {
        int docBase = ctx.docBase;
        int leafEndDoc = docBase + ctx.reader().maxDoc();

        List<Integer> docsInLeaf = new ArrayList<>();
        for (int i = fromIndex; i < toIndex; i++) {
            int docId = docIds[i];
            if (docId >= docBase && docId < leafEndDoc) {
                docsInLeaf.add(docId - docBase);
            }
        }
        return docsInLeaf.stream().mapToInt(Integer::intValue).toArray();
    }

    private SearchHits createSearchHits(List<SearchHit> hits, TotalHits totalHits, float maxScore) {
        if (hits.isEmpty()) {
            return SearchHits.empty(totalHits, maxScore);
        }
        SearchHit[] hitsArray = hits.toArray(new SearchHit[0]);
        return new SearchHits(hitsArray, totalHits, maxScore);
    }

    private static void purgeSearchHits(SearchHit[] searchHits) {
        for (SearchHit searchHit : searchHits) {
            if (searchHit != null) {
                searchHit.decRef();
            }
        }
    }

    private static int endReaderIdx(LeafReaderContext currentReaderContext, int index, DocIdToIndex[] docs) {
        int firstInNextReader = currentReaderContext.docBase + currentReaderContext.reader().maxDoc();
        int i = index + 1;
        while (i < docs.length) {
            if (docs[i].docId >= firstInNextReader) {
                return i;
            }
            i++;
        }
        return i;
    }

    private static int[] docIdsInLeaf(int index, int endReaderIdx, DocIdToIndex[] docs, int docBase) {
        int[] result = new int[endReaderIdx - index];
        int d = 0;
        for (int i = index; i < endReaderIdx; i++) {
            assert docs[i].docId >= docBase;
            result[d++] = docs[i].docId - docBase;
        }
        return result;
    }

    private static class DocIdToIndex implements Comparable<DocIdToIndex> {
        final int docId;
        final int index;

        DocIdToIndex(int docId, int index) {
            this.docId = docId;
            this.index = index;
        }

        @Override
        public int compareTo(DocIdToIndex o) {
            return Integer.compare(docId, o.docId);
        }
    }

    /**
     * Result class that carries hits array, last chunk, and sequence information.
     * The lastChunkSequenceStart is used by the coordinator to properly order the last chunk's hits.
     */
    static class IterateResult implements AutoCloseable {
        final SearchHit[] hits;
        final SearchHits lastChunk;  // null for non-streaming mode
        final long lastChunkSequenceStart;  // -1 if no last chunk
        private boolean closed = false;

        IterateResult(SearchHit[] hits, SearchHits lastChunk, long lastChunkSequenceStart) {
            this.hits = hits;
            this.lastChunk = lastChunk;
            this.lastChunkSequenceStart = lastChunkSequenceStart;
        }

        @Override
        public void close() {
            if (closed) {
                return;
            }
            closed = true;

            if (lastChunk != null) {
                lastChunk.decRef();
            }
        }
    }
}
