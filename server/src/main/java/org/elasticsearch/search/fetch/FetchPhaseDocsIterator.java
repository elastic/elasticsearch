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
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.RefCountingListener;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.io.stream.RecyclerBytesStreamOutput;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.fetch.chunk.FetchPhaseResponseChunk;
import org.elasticsearch.search.internal.ContextIndexSearcher;
import org.elasticsearch.search.query.QuerySearchResult;
import org.elasticsearch.search.query.SearchTimeoutException;
import org.elasticsearch.tasks.TaskCancelledException;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
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
 *   <li><b>Streaming mode</b> ({@link #iterateAsync}): Documents are fetched in small batches,
 *       serialized immediately to byte buffers from Netty's pool, and streamed when the buffer
 *       exceeds a byte threshold. SearchHit objects are released immediately after serialization
 *       to minimize heap usage.</li>
 * </ul>
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
     * Default target chunk size in bytes (256KB).
     * Chunks may slightly exceed this as we complete the current hit before checking.
     */
    static final int DEFAULT_TARGET_CHUNK_BYTES = 256 * 1024;

    /**
     * Accounts for FetchPhase memory usage
     * It gets cleaned up after each fetch phase and should not be accessed/modified by subclasses.
     */
    private long requestBreakerBytes;

    public void addRequestBreakerBytes(long delta) {
        requestBreakerBytes += delta;
    }

    public long getRequestBreakerBytes() {
        return requestBreakerBytes;
    }

    /**
     * Called when a new leaf reader is reached.
     *
     * @param ctx        the leaf reader for this set of doc ids
     * @param docsInLeaf the reader-specific docids to be fetched in this leaf reader
     */
    protected abstract void setNextReader(LeafReaderContext ctx, int[] docsInLeaf) throws IOException;

    /**
     * Called for each document within a leaf reader.
     *
     * @param doc the global doc id
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
     * @throws SearchTimeoutException       if timeout occurs and partial results not allowed
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
        int currentDoc = docs.length > 0 ? docs[0].docId : -1;

        try {
            if (docs.length == 0) {
                return new IterateResult(searchHits);
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
                return new IterateResult(new SearchHit[0]);
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
                    return new IterateResult(partialSearchHits);
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
        return new IterateResult(searchHits);
    }

    /**
     * Asynchronous iteration with byte-based chunking for streaming mode.
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
     * @param indexReader         the index reader
     * @param docIds              document IDs to fetch (in score order)
     * @param chunkWriter         writer for sending chunks (also provides buffer allocation)
     * @param targetChunkBytes    target size in bytes for each chunk
     * @param chunkCompletionRefs ref-counting listener for tracking chunk ACKs
     * @param maxInFlightChunks   maximum concurrent unacknowledged chunks
     * @param sendFailure         atomic reference to capture send failures
     * @param isCancelled         supplier for cancellation checking
     * @param listener            receives the result with the last chunk bytes
     */
    void iterateAsync(
        SearchShardTarget shardTarget,
        IndexReader indexReader,
        int[] docIds,
        FetchPhaseResponseChunk.Writer chunkWriter,
        int targetChunkBytes,
        RefCountingListener chunkCompletionRefs,
        int maxInFlightChunks,
        AtomicReference<Throwable> sendFailure,
        Supplier<Boolean> isCancelled,
        ActionListener<IterateResult> listener
    ) {
        if (docIds == null || docIds.length == 0) {
            listener.onResponse(new IterateResult(new SearchHit[0]));
            return;
        }

        // Semaphore controls backpressure, each in-flight chunk holds one permit.
        // When maxInFlightChunks are in flight, we block until an ACK releases a permit.
        Semaphore transmitPermits = new Semaphore(maxInFlightChunks);
        ShardId shardId = shardTarget.getShardId();
        int totalDocs = docIds.length;

        // Last chunk state
        ReleasableBytesReference lastChunkBytes = null;
        int lastChunkHitCount = 0;
        long lastChunkSeqStart = -1;

        RecyclerBytesStreamOutput chunkBuffer = null;
        try {
            // Allocate from Netty's pool via the writer
            chunkBuffer = chunkWriter.newNetworkBytesStream();
            int chunkStartIndex = 0;
            int hitsInChunk = 0;

            for (int scoreIndex = 0; scoreIndex < totalDocs; scoreIndex++) {
                // Periodic cancellation check
                if (scoreIndex % 64 == 0) {
                    if (isCancelled.get()) {
                        throw new TaskCancelledException("cancelled");
                    }
                    Throwable failure = sendFailure.get();
                    if (failure != null) {
                        throw failure instanceof Exception ? (Exception) failure : new RuntimeException(failure);
                    }
                }

                int docId = docIds[scoreIndex];

                // Set up the correct leaf reader for this doc
                int leafOrd = ReaderUtil.subIndex(docId, indexReader.leaves());
                LeafReaderContext ctx = indexReader.leaves().get(leafOrd);
                int leafDocId = docId - ctx.docBase;
                setNextReader(ctx, new int[] { leafDocId });

                // Fetch and serialize immediately
                SearchHit hit = nextDoc(docId);
                try {
                    hit.writeTo(chunkBuffer);
                } finally {
                    hit.decRef();
                }
                hitsInChunk++;

                // Check if chunk is ready to send
                boolean isLast = (scoreIndex == totalDocs - 1);
                boolean bufferFull = chunkBuffer.size() >= targetChunkBytes;

                if (bufferFull || isLast) {
                    if (isLast == false) {
                        acquirePermitWithCancellationCheck(transmitPermits, isCancelled);
                    }

                    ReleasableBytesReference chunkBytes = null;
                    try {
                        chunkBytes = chunkBuffer.moveToBytesReference();
                        chunkBuffer = null;

                        if (isLast) {
                            lastChunkBytes = chunkBytes;
                            lastChunkHitCount = hitsInChunk;
                            lastChunkSeqStart = chunkStartIndex;
                            chunkBytes = null; // ownership transferred to lastChunkBytes
                        } else {
                            sendChunk(
                                chunkBytes,
                                hitsInChunk,
                                chunkStartIndex,
                                chunkStartIndex,
                                totalDocs,
                                chunkWriter,
                                shardId,
                                sendFailure,
                                chunkCompletionRefs.acquire(),
                                transmitPermits
                            );
                            chunkBytes = null;
                        }
                    } finally {
                        Releasables.closeWhileHandlingException(chunkBytes);
                    }

                    if (isLast == false) {
                        chunkBuffer = chunkWriter.newNetworkBytesStream();
                        chunkStartIndex = scoreIndex + 1;
                        hitsInChunk = 0;
                    }
                }
            }

            // Wait for all in-flight chunks to be acknowledged
            waitForAllPermits(transmitPermits, maxInFlightChunks, isCancelled);

            // Final failure check after all chunks sent
            Throwable failure = sendFailure.get();
            if (failure != null) {
                Releasables.closeWhileHandlingException(lastChunkBytes);
                throw failure instanceof Exception ? (Exception) failure : new RuntimeException(failure);
            }

            listener.onResponse(new IterateResult(lastChunkBytes, lastChunkHitCount, lastChunkSeqStart));
        } catch (Exception e) {
            if (chunkBuffer != null) {
                Releasables.closeWhileHandlingException(chunkBuffer);
            }
            Releasables.closeWhileHandlingException(lastChunkBytes);
            listener.onFailure(e);
        }
    }

    /**
     * Sends a chunk of search hits to the coordinator.
     * <p>
     * Wraps the hits in a {@link FetchPhaseResponseChunk} message and writes it via the
     * chunk writer. Handles reference counting and permit management for both success
     * and failure cases.
     */
    private void sendChunk(
        ReleasableBytesReference chunkBytes,
        int hitCount,
        long sequenceStart,
        int fromIndex,
        int totalDocs,
        FetchPhaseResponseChunk.Writer writer,
        ShardId shardId,
        AtomicReference<Throwable> sendFailure,
        ActionListener<Void> ackListener,
        Semaphore transmitPermits
    ) {
        FetchPhaseResponseChunk chunk = null;
        try {
            chunk = new FetchPhaseResponseChunk(
                System.currentTimeMillis(),
                FetchPhaseResponseChunk.Type.HITS,
                shardId,
                chunkBytes,
                hitCount,
                fromIndex,
                totalDocs,
                sequenceStart
            );

            final FetchPhaseResponseChunk chunkToClose = chunk;
            writer.writeResponseChunk(chunk, ActionListener.wrap(ack -> {
                chunkToClose.close();
                ackListener.onResponse(null);
                transmitPermits.release();
            }, e -> {
                chunkToClose.close();
                sendFailure.compareAndSet(null, e);
                ackListener.onFailure(e);
                transmitPermits.release();
            }));

            chunk = null;
        } catch (Exception e) {
            if (chunk != null) {
                chunk.close();
            } else {
                Releasables.closeWhileHandlingException(chunkBytes);
            }
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
            if (acquired > 0) {
                semaphore.release(acquired);
            }
        }
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
     * Result of iteration.
     * For non-streaming: contains hits array.
     * For streaming: contains last chunk bytes to be sent after all ACKs.
     */
    static class IterateResult implements AutoCloseable {
        final SearchHit[] hits;  // Non-streaming mode only
        final ReleasableBytesReference lastChunkBytes;  // Streaming mode only
        final int lastChunkHitCount;
        final long lastChunkSequenceStart;
        private boolean closed = false;
        private boolean bytesOwnershipTransferred = false;

        // Non-streaming constructor
        IterateResult(SearchHit[] hits) {
            this.hits = hits;
            this.lastChunkBytes = null;
            this.lastChunkHitCount = 0;
            this.lastChunkSequenceStart = -1;
        }

        // Streaming constructor
        IterateResult(ReleasableBytesReference lastChunkBytes, int hitCount, long seqStart) {
            this.hits = null;
            this.lastChunkBytes = lastChunkBytes;
            this.lastChunkHitCount = hitCount;
            this.lastChunkSequenceStart = seqStart;
        }

        /**
         * Takes ownership of the last chunk bytes.
         * After calling, close() will not release the bytes.
         */
        ReleasableBytesReference takeLastChunkBytes() {
            bytesOwnershipTransferred = true;
            return lastChunkBytes;
        }

        @Override
        public void close() {
            if (closed) return;
            closed = true;

            if (bytesOwnershipTransferred == false) {
                Releasables.closeWhileHandlingException(lastChunkBytes);
            }
        }
    }
}
