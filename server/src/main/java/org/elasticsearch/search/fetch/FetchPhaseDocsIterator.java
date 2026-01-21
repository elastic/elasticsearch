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
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.io.stream.RecyclerBytesStreamOutput;
import org.elasticsearch.common.util.concurrent.ThrottledIterator;
import org.elasticsearch.core.Releasable;
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
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
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
 *   <li><b>Streaming mode</b> ({@link #iterateAsync}): Uses a producer-consumer pattern where:
 *       <ul>
 *         <li><b>Producer (Lucene thread)</b>: Reads documents, serializes hits into byte chunks,
 *             and enqueues them. Reserves memory on the circuit breaker for each chunk.</li>
 *         <li><b>Consumer (sender)</b>: Drains the queue and sends chunks with backpressure via
 *             {@link ThrottledIterator}. Releases circuit breaker memory when chunks are acknowledged.</li>
 *       </ul>
 *   </li>
 * </ul>
 * <b>Threading:</b> All Lucene operations execute on the calling thread to satisfy
 * Lucene's thread-affinity requirements. Network transmission and ACK handling occur
 * asynchronously via the consumer.
 * <p>
 * <b>Memory Management:</b> The circuit breaker tracks accumulated chunk bytes (data node). If the
 * breaker trips, the producer fails immediately with a {@link CircuitBreakingException},
 * preventing unbounded memory growth when the consumer is slow.
 * <p>
 * <b>Cancellation:</b> Both producer and consumer check the cancellation flag, ensuring
 * responsive cancellation even under heavy load.
 */
abstract class FetchPhaseDocsIterator {

    /**
     * Default target chunk size in bytes (256KB).
     * Chunks may slightly exceed this as we complete the current hit before checking.
     */
    static final int DEFAULT_TARGET_CHUNK_BYTES = 256 * 1024;

    /**
     * Label for circuit breaker reservations.
     */
    static final String CIRCUIT_BREAKER_LABEL = "fetch_phase_streaming_chunks";

    /**
     * Accounts for FetchPhase memory usage.
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
     *
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
     * Asynchronous iteration using producer-consumer pattern for streaming mode.
     * <p>
     * <b>Architecture:</b>
     * <ul>
     *   <li><b>Producer (this thread)</b>: Iterates through documents, fetches and serializes
     *       each hit into chunks, and enqueues them. Runs synchronously on the calling thread
     *       to maintain Lucene's thread-affinity requirements.</li>
     *   <li><b>Consumer (separate thread)</b>: Uses {@link ThrottledIterator} to drain the queue
     *       and send chunks with backpressure, limiting to {@code maxInFlightChunks} concurrent sends.</li>
     * </ul>
     * <p>
     * <b>Memory Management:</b>
     * The producer reserves memory on the circuit breaker when creating chunks (dataNode). The consumer
     * releases memory when chunks are acknowledged. If the circuit breaker trips, the producer
     * fails immediately with a {@link CircuitBreakingException}.
     * <p>
     * <b>Coordination:</b>
     * <ul>
     *   <li>A COMPLETE chunk signals producer completion to the consumer</li>
     *   <li>Send failures are captured and checked by both producer and consumer</li>
     *   <li>The last chunk is held back and returned via the listener</li>
     * </ul>
     *
     * @param shardTarget         the shard being fetched from
     * @param indexReader         the index reader
     * @param docIds              document IDs to fetch (in score order)
     * @param chunkWriter         writer for sending chunks (also provides buffer allocation)
     * @param targetChunkBytes    target size in bytes for each chunk
     * @param chunkCompletionRefs ref-counting listener for tracking chunk ACKs
     * @param maxInFlightChunks   maximum concurrent unacknowledged chunks
     * @param circuitBreaker      circuit breaker for memory management (trips if accumulated chunks exceed threshold)
     * @param sendFailure         atomic reference to capture send failures
     * @param isCancelled         supplier for cancellation checking
     * @param executor            executor for running the consumer thread
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
        CircuitBreaker circuitBreaker,
        AtomicReference<Throwable> sendFailure,
        Supplier<Boolean> isCancelled,
        Executor executor,
        ActionListener<IterateResult> listener
    ) {
        if (docIds == null || docIds.length == 0) {
            listener.onResponse(new IterateResult(new SearchHit[0]));
            return;
        }

        ShardId shardId = shardTarget.getShardId();
        int totalDocs = docIds.length;

        BlockingQueue<PendingChunk> chunkQueue = new LinkedBlockingQueue<>();
        AtomicReference<PendingChunk> lastChunkHolder = new AtomicReference<>();
        AtomicReference<Throwable> producerError = new AtomicReference<>();

        ChunkConsumer consumer = new ChunkConsumer(
            chunkQueue,
            chunkWriter,
            shardId,
            totalDocs,
            maxInFlightChunks,
            circuitBreaker,
            sendFailure,
            chunkCompletionRefs,
            isCancelled
        );

        // Consumer completion handler
        ActionListener<Void> consumerListener = ActionListener.wrap(v -> {
            // Check for producer
            Throwable pError = producerError.get();
            if (pError != null) {
                cleanupLastChunk(lastChunkHolder, circuitBreaker);
                listener.onFailure(pError instanceof Exception ? (Exception) pError : new RuntimeException(pError));
                return;
            }

            // Check for send failure
            Throwable sError = sendFailure.get();
            if (sError != null) {
                cleanupLastChunk(lastChunkHolder, circuitBreaker);
                listener.onFailure(sError instanceof Exception ? (Exception) sError : new RuntimeException(sError));
                return;
            }

            // Return the last chunk
            PendingChunk lastChunk = lastChunkHolder.get();
            if (lastChunk != null && lastChunk.bytes != null) {
                listener.onResponse(
                    new IterateResult(lastChunk.bytes, lastChunk.hitCount, lastChunk.sequenceStart, lastChunk.byteSize, circuitBreaker)
                );
            } else {
                listener.onResponse(new IterateResult(new SearchHit[0]));
            }
        }, e -> {
            cleanupLastChunk(lastChunkHolder, circuitBreaker);
            listener.onFailure(e);
        });

        // Start consumer on a separate thread
        executor.execute(() -> consumer.execute(consumerListener));

        try {
            produceChunks(
                indexReader,
                docIds,
                chunkWriter,
                targetChunkBytes,
                chunkQueue,
                lastChunkHolder,
                circuitBreaker,
                sendFailure,
                isCancelled
            );
        } catch (Exception e) {
            producerError.set(e);
            drainAndCleanup(chunkQueue, circuitBreaker);
        } finally {
            // Signal consumer that production is complete
            chunkQueue.offer(PendingChunk.COMPLETE);
        }
    }

    /**
     * Producer: Iterates through documents, fetches hits, serializes into chunks, and enqueues.
     * Runs on the Lucene thread to maintain thread-affinity.
     * <p>
     * Reserves memory on the circuit breaker for each chunk. If the breaker trips,
     * throws {@link CircuitBreakingException} to fail fast.
     */
    private void produceChunks(
        IndexReader indexReader,
        int[] docIds,
        FetchPhaseResponseChunk.Writer chunkWriter,
        int targetChunkBytes,
        BlockingQueue<PendingChunk> chunkQueue,
        AtomicReference<PendingChunk> lastChunkHolder,
        CircuitBreaker circuitBreaker,
        AtomicReference<Throwable> sendFailure,
        Supplier<Boolean> isCancelled
    ) throws Exception {
        int totalDocs = docIds.length;
        RecyclerBytesStreamOutput chunkBuffer = null;

        try {
            chunkBuffer = chunkWriter.newNetworkBytesStream();
            int chunkStartIndex = 0;
            int hitsInChunk = 0;

            for (int scoreIndex = 0; scoreIndex < totalDocs; scoreIndex++) {
                // Periodic checks - every 64 docs
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

                // Check if chunk is ready
                boolean isLast = (scoreIndex == totalDocs - 1);
                boolean bufferFull = chunkBuffer.size() >= targetChunkBytes;

                if (bufferFull || isLast) {
                    final ReleasableBytesReference chunkBytes = chunkBuffer.moveToBytesReference();
                    chunkBuffer = null;

                    final long byteSize = chunkBytes.length();
                    boolean reserved = false;

                    try {
                        circuitBreaker.addEstimateBytesAndMaybeBreak(byteSize, CIRCUIT_BREAKER_LABEL);
                        reserved = true;

                        PendingChunk chunk = new PendingChunk(chunkBytes, hitsInChunk, chunkStartIndex, chunkStartIndex, byteSize, isLast);

                        if (isLast) {
                            lastChunkHolder.set(chunk);
                        } else {
                            chunkQueue.put(chunk);
                        }

                        if (isLast == false) {
                            chunkBuffer = chunkWriter.newNetworkBytesStream();
                            chunkStartIndex = scoreIndex + 1;
                            hitsInChunk = 0;
                        }
                    } catch (Exception e) {
                        Releasables.closeWhileHandlingException(chunkBytes);
                        if (reserved) {
                            circuitBreaker.addWithoutBreaking(-byteSize);
                        }
                        throw e;
                    }
                }
            }
        } finally {
            if (chunkBuffer != null) {
                Releasables.closeWhileHandlingException(chunkBuffer);
            }
        }
    }

    /**
     * Consumer: Drains chunks from the queue and sends them with backpressure.
     * Uses {@link ThrottledIterator} to limit concurrent in-flight chunks.
     * Releases circuit breaker memory when chunks are acknowledged.
     */
    private static class ChunkConsumer {
        private final BlockingQueue<PendingChunk> queue;
        private final FetchPhaseResponseChunk.Writer writer;
        private final ShardId shardId;
        private final int totalDocs;
        private final int maxInFlightChunks;
        private final CircuitBreaker circuitBreaker;
        private final AtomicReference<Throwable> sendFailure;
        private final RefCountingListener chunkCompletionRefs;
        private final Supplier<Boolean> isCancelled;

        ChunkConsumer(
            BlockingQueue<PendingChunk> queue,
            FetchPhaseResponseChunk.Writer writer,
            ShardId shardId,
            int totalDocs,
            int maxInFlightChunks,
            CircuitBreaker circuitBreaker,
            AtomicReference<Throwable> sendFailure,
            RefCountingListener chunkCompletionRefs,
            Supplier<Boolean> isCancelled
        ) {
            this.queue = queue;
            this.writer = writer;
            this.shardId = shardId;
            this.totalDocs = totalDocs;
            this.maxInFlightChunks = maxInFlightChunks;
            this.circuitBreaker = circuitBreaker;
            this.sendFailure = sendFailure;
            this.chunkCompletionRefs = chunkCompletionRefs;
            this.isCancelled = isCancelled;
        }

        void execute(ActionListener<Void> listener) {
            // Iterator that pulls from the queue
            Iterator<PendingChunk> chunkIterator = new QueueDrainingIterator(queue, isCancelled);

            // ThrottledIterator for backpressure control
            ThrottledIterator.run(chunkIterator, (releasable, chunk) -> sendChunk(chunk, releasable), maxInFlightChunks, () -> {
                drainAndCleanup(queue, circuitBreaker);

                // Completion callback - check for errors and notify listener
                Throwable failure = sendFailure.get();
                if (failure != null) {
                    listener.onFailure(failure instanceof Exception ? (Exception) failure : new RuntimeException(failure));
                } else if (isCancelled.get()) {
                    listener.onFailure(new TaskCancelledException("cancelled"));
                } else {
                    listener.onResponse(null);
                }
            });
        }

        private void sendChunk(PendingChunk chunk, Releasable releasable) {
            if (isCancelled.get()) {
                releaseChunk(chunk);
                releasable.close();
                return;
            }

            Throwable failure = sendFailure.get();
            if (failure != null) {
                releaseChunk(chunk);
                releasable.close();
                return;
            }

            FetchPhaseResponseChunk responseChunk = null;
            ActionListener<Void> ackListener = null;
            try {
                responseChunk = new FetchPhaseResponseChunk(
                    System.currentTimeMillis(),
                    FetchPhaseResponseChunk.Type.HITS,
                    shardId,
                    chunk.bytes,
                    chunk.hitCount,
                    chunk.fromIndex,
                    totalDocs,
                    chunk.sequenceStart
                );

                final FetchPhaseResponseChunk chunkToClose = responseChunk;
                final long chunkByteSize = chunk.byteSize;
                ackListener = chunkCompletionRefs.acquire();
                final ActionListener<Void> finalAckListener = ackListener;

                writer.writeResponseChunk(responseChunk, ActionListener.wrap(ack -> {
                    chunkToClose.close();
                    circuitBreaker.addWithoutBreaking(-chunkByteSize);
                    finalAckListener.onResponse(null);
                    releasable.close();
                }, e -> {
                    chunkToClose.close();
                    circuitBreaker.addWithoutBreaking(-chunkByteSize);
                    sendFailure.compareAndSet(null, e);
                    finalAckListener.onFailure(e);
                    releasable.close();
                }));

                responseChunk = null; // ownership transferred
            } catch (Exception e) {
                if (responseChunk != null) {
                    responseChunk.close();
                    circuitBreaker.addWithoutBreaking(-chunk.byteSize);
                } else {
                    releaseChunk(chunk);
                }
                sendFailure.compareAndSet(null, e);

                if (ackListener != null) {
                    ackListener.onFailure(e);
                }

                releasable.close();
            }
        }

        private void releaseChunk(PendingChunk chunk) {
            chunk.close();
            circuitBreaker.addWithoutBreaking(-chunk.byteSize);
        }
    }

    /**
     * Iterator that drains chunks from the queue, blocking when empty but not done.
     * Returns null (ending iteration) when the COMPLETE signal is received, or cancelled.
     */
    private static class QueueDrainingIterator implements Iterator<PendingChunk> {
        private final BlockingQueue<PendingChunk> queue;
        private final Supplier<Boolean> isCancelled;
        private PendingChunk nextChunk;
        private boolean exhausted = false;

        QueueDrainingIterator(BlockingQueue<PendingChunk> queue, Supplier<Boolean> isCancelled) {
            this.queue = queue;
            this.isCancelled = isCancelled;
        }

        @Override
        public boolean hasNext() {
            if (exhausted) {
                return false;
            }
            if (nextChunk != null) {
                return true;
            }

            try {
                // Poll with timeout to allow cancellation checks
                while (nextChunk == null) {
                    if (isCancelled.get()) {
                        exhausted = true;
                        return false;
                    }

                    nextChunk = queue.poll(100, TimeUnit.MILLISECONDS);

                    if (nextChunk == PendingChunk.COMPLETE) {
                        exhausted = true;
                        nextChunk = null;
                        return false;
                    }
                }
                return true;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                exhausted = true;
                return false;
            }
        }

        @Override
        public PendingChunk next() {
            if (hasNext() == false) {
                throw new NoSuchElementException();
            }
            PendingChunk result = nextChunk;
            nextChunk = null;
            return result;
        }
    }

    /**
     * Represents a chunk ready to be sent. Tracks byte size for circuit breaker accounting.
     */
    private static class PendingChunk implements AutoCloseable {
        final ReleasableBytesReference bytes;
        final int hitCount;
        final int fromIndex;
        final long sequenceStart;
        final long byteSize;
        final boolean isLast;

        // This is a completion signal for the consumer
        public static PendingChunk COMPLETE = new PendingChunk(null, 0, 0, 0, 0, true);

        PendingChunk(ReleasableBytesReference bytes, int hitCount, int fromIndex, long sequenceStart, long byteSize, boolean isLast) {
            this.bytes = bytes;
            this.hitCount = hitCount;
            this.fromIndex = fromIndex;
            this.sequenceStart = sequenceStart;
            this.byteSize = byteSize;
            this.isLast = isLast;
        }

        @Override
        public void close() {
            Releasables.closeWhileHandlingException(bytes);
        }
    }

    private static void cleanupLastChunk(AtomicReference<PendingChunk> lastChunkHolder, CircuitBreaker circuitBreaker) {
        PendingChunk lastChunk = lastChunkHolder.getAndSet(null);
        if (lastChunk != null) {
            lastChunk.close();
            if (lastChunk.byteSize > 0) {
                circuitBreaker.addWithoutBreaking(-lastChunk.byteSize);
            }
        }
    }

    private static void drainAndCleanup(BlockingQueue<PendingChunk> queue, CircuitBreaker circuitBreaker) {
        PendingChunk chunk;
        while ((chunk = queue.poll()) != null) {
            if (chunk != PendingChunk.COMPLETE) {
                chunk.close();
                if (chunk.byteSize > 0) {
                    circuitBreaker.addWithoutBreaking(-chunk.byteSize);
                }
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
        final ReleasableBytesReference lastChunkBytes;
        final int lastChunkHitCount;
        final long lastChunkSequenceStart;
        final long lastChunkByteSize;
        final CircuitBreaker circuitBreaker;
        private boolean closed = false;
        private boolean bytesOwnershipTransferred = false;

        // Non-streaming constructor
        IterateResult(SearchHit[] hits) {
            this.hits = hits;
            this.lastChunkBytes = null;
            this.lastChunkHitCount = 0;
            this.lastChunkSequenceStart = -1;
            this.lastChunkByteSize = 0;
            this.circuitBreaker = null;
        }

        // Streaming constructor
        IterateResult(ReleasableBytesReference lastChunkBytes, int hitCount, long seqStart, long byteSize, CircuitBreaker circuitBreaker) {
            this.hits = null;
            this.lastChunkBytes = lastChunkBytes;
            this.lastChunkHitCount = hitCount;
            this.lastChunkSequenceStart = seqStart;
            this.lastChunkByteSize = byteSize;
            this.circuitBreaker = circuitBreaker;
        }

        /**
         * Takes ownership of the last chunk bytes.
         * After calling, close() will not release the bytes, but the caller
         * becomes responsible for releasing circuit breaker memory.
         *
         * @return the last chunk bytes, or null if none
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
                if (circuitBreaker != null && lastChunkByteSize > 0) {
                    circuitBreaker.addWithoutBreaking(-lastChunkByteSize);
                }
            }
        }
    }
}
