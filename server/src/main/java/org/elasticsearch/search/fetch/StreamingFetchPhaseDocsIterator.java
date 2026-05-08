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
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.io.stream.RecyclerBytesStreamOutput;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.ThrottledIterator;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.fetch.chunk.FetchPhaseResponseChunk;
import org.elasticsearch.tasks.TaskCancelledException;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

/**
 * Extends {@link FetchPhaseDocsIterator} with asynchronous chunked iteration
 * via {@link #iterateAsync}. The synchronous {@link #iterate} method from the
 * parent class remains available for non-streaming use.
 * <p>
 * Uses {@link ThrottledIterator} to process chunks of documents across threads:
 * <ul>
 *   <li>A {@link ChunkProducingIterator} yields serialized chunks by performing Lucene I/O</li>
 *   <li>A send consumer initiates asynchronous network sends for each chunk</li>
 *   <li>The {@link ThrottledIterator} releasable is closed on send ACK, triggering the next chunk</li>
 * </ul>
 * <b>Threading:</b> The search thread produces up to {@code maxInFlightChunks} chunks in the
 * initial burst, then returns to the thread pool. Subsequent chunks are produced on whatever
 * thread ACKs a previous send. Per-leaf Lucene readers are re-acquired via {@link #setNextReader}
 * (clone-per-chunk) whenever production crosses a leaf boundary or moves to a different thread,
 * satisfying Lucene's thread-affinity assertions. If consecutive chunks stay within the same leaf
 * on the same producer thread, the previous leaf setup is reused to avoid rebuilding stored-field
 * loaders, doc-values, and sub-phase processor state on every chunk.
 * <p>
 * <b>Memory Management:</b> The circuit breaker tracks recycler page allocations via the
 * {@link RecyclerBytesStreamOutput} passed from the chunk writer. If the breaker trips
 * during serialization, the producer fails immediately with a
 * {@link org.elasticsearch.common.breaker.CircuitBreakingException}, preventing unbounded
 * memory growth. Pages are released (and the breaker decremented) when the
 * {@link ReleasableBytesReference} from {@link RecyclerBytesStreamOutput#moveToBytesReference()}
 * is closed -- either on ACK for intermediate chunks or when the last chunk is consumed.
 * <p>
 * <b>Backpressure:</b> {@link ThrottledIterator}'s {@code maxConcurrency} limits concurrent
 * in-flight sends to {@code maxInFlightChunks}. The circuit breaker provides the memory limit.
 * <p>
 * <b>Cancellation:</b> The producer checks the cancellation flag periodically and in
 * {@link ChunkProducingIterator#hasNext()}.
 */
abstract class StreamingFetchPhaseDocsIterator extends FetchPhaseDocsIterator {

    /**
     * Default target chunk size in bytes (256KB).
     * Chunks may slightly exceed this as we complete the current hit before checking.
     */
    static final int DEFAULT_TARGET_CHUNK_BYTES = 256 * 1024;

    /**
     * Asynchronous iteration using {@link ThrottledIterator} for streaming mode.
     *
     * @param shardTarget         the shard being fetched from
     * @param indexReader         the index reader
     * @param docIds              document IDs to fetch (in score order)
     * @param chunkWriter         writer for sending chunks (also provides buffer allocation with CB tracking)
     * @param targetChunkBytes    target size in bytes for each chunk
     * @param chunkCompletionRefs ref-counting listener for tracking chunk ACKs
     * @param maxInFlightChunks   maximum concurrent unacknowledged chunks
     * @param sendFailure         atomic reference to capture send failures
     * @param isCancelled         supplier for cancellation checking
     * @param continuationExecutor executor for dispatching chunk production after ACKs
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
        Executor continuationExecutor,
        ActionListener<IterateResult> listener
    ) {
        if (docIds == null || docIds.length == 0) {
            listener.onResponse(new IterateResult(new SearchHit[0]));
            return;
        }

        final AtomicReference<PendingChunk> lastChunkHolder = new AtomicReference<>();
        final AtomicReference<Throwable> producerError = new AtomicReference<>();

        DocIdToIndex[] docs = sortDocsByDocId(docIds);
        Iterator<PendingChunk> chunkIterator = new ChunkProducingIterator(
            docs,
            indexReader,
            chunkWriter,
            targetChunkBytes,
            isCancelled,
            sendFailure,
            producerError
        );

        BiConsumer<Releasable, PendingChunk> sendConsumer = createSendConsumer(
            chunkWriter,
            shardTarget.getShardId(),
            docIds.length,
            sendFailure,
            chunkCompletionRefs,
            isCancelled,
            lastChunkHolder
        );

        // Dispatch rawCompletion off the ACK / producer thread; it fires the listener itself and never throws.
        Runnable rawCompletion = createCompletionHandler(listener, producerError, sendFailure, isCancelled, lastChunkHolder);
        Runnable onCompletion = () -> continuationExecutor.execute(new AbstractRunnable() {
            @Override
            protected void doRun() {
                rawCompletion.run();
            }

            @Override
            public void onRejection(Exception e) {
                rawCompletion.run();
            }

            @Override
            public void onFailure(Exception e) {
                // rawCompletion never throws.
                assert false : e;
            }
        });

        ThrottledIterator.run(
            chunkIterator,
            sendConsumer,
            maxInFlightChunks,
            onCompletion,
            continuationExecutor,
            e -> producerError.compareAndSet(null, e)
        );
    }

    private static DocIdToIndex[] sortDocsByDocId(int[] docIds) {
        DocIdToIndex[] docs = new DocIdToIndex[docIds.length];
        for (int i = 0; i < docIds.length; i++) {
            docs[i] = new DocIdToIndex(docIds[i], i);
        }
        Arrays.sort(docs);
        return docs;
    }

    private static BiConsumer<Releasable, PendingChunk> createSendConsumer(
        FetchPhaseResponseChunk.Writer chunkWriter,
        ShardId shardId,
        int totalDocs,
        AtomicReference<Throwable> sendFailure,
        RefCountingListener chunkCompletionRefs,
        Supplier<Boolean> isCancelled,
        AtomicReference<PendingChunk> lastChunkHolder
    ) {
        return (releasable, chunk) -> {
            try {
                if (chunk.isLast) {
                    lastChunkHolder.set(chunk);
                    releasable.close();
                    return;
                }
                sendChunk(chunk, releasable, chunkWriter, shardId, totalDocs, sendFailure, chunkCompletionRefs, isCancelled);
            } catch (Exception e) {
                sendFailure.compareAndSet(null, e);
                chunk.close();
                releasable.close();
            }
        };
    }

    private static Runnable createCompletionHandler(
        ActionListener<IterateResult> listener,
        AtomicReference<Throwable> producerError,
        AtomicReference<Throwable> sendFailure,
        Supplier<Boolean> isCancelled,
        AtomicReference<PendingChunk> lastChunkHolder
    ) {
        return () -> {
            final Throwable pError = producerError.get();
            if (pError != null) {
                cleanupLastChunk(lastChunkHolder);
                listener.onFailure(pError instanceof Exception ? (Exception) pError : new RuntimeException(pError));
                return;
            }

            final Throwable sError = sendFailure.get();
            if (sError != null) {
                cleanupLastChunk(lastChunkHolder);
                listener.onFailure(sError instanceof Exception ? (Exception) sError : new RuntimeException(sError));
                return;
            }

            if (isCancelled.get()) {
                cleanupLastChunk(lastChunkHolder);
                listener.onFailure(new TaskCancelledException("cancelled"));
                return;
            }

            final PendingChunk lastChunk = lastChunkHolder.getAndSet(null);
            if (lastChunk == null) {
                listener.onResponse(new IterateResult(new SearchHit[0]));
                return;
            }

            try {
                listener.onResponse(new IterateResult(lastChunk.bytes, lastChunk.hitCount, lastChunk.sequenceStart));
            } catch (Exception e) {
                lastChunk.close();
            }
        };
    }

    /**
     * Yields serialized chunks by fetching documents from Lucene in doc-ID order.
     * <p>
     * Documents are sorted by doc ID for efficient sequential Lucene access, matching
     * the non-streaming {@link FetchPhaseDocsIterator#iterate} path. Each serialized
     * hit is prefixed with its original score-order position (as a vInt) so the
     * coordinator can reassemble results in the correct order.
     * <p>
     * <b>Clone-per-chunk:</b> Per-leaf Lucene readers are re-acquired via
     * {@link #setNextReaderAndGetLeafEndIndex} whenever production crosses a leaf boundary or
     * moves to a different thread than the one that last set up the current leaf. This creates
     * fresh clones of {@code StoredFields}, {@code DocValues}, etc. bound to the calling thread,
     * satisfying Lucene's thread-affinity assertions when chunks are produced across different
     * threads. When consecutive chunks stay within the same leaf on the same thread the previous
     * setup is reused, avoiding redundant loader and sub-phase processor rebuilds.
     * <p>
     * <b>Error safety:</b> All exceptions from Lucene I/O are caught and stored in
     * {@code producerError}. This is critical because {@link ThrottledIterator} does not
     * catch exceptions from {@code iterator.next()} -- an uncaught exception in
     * {@code onItemRelease -> run -> next} would propagate unhandled on an ACK thread.
     * <p>
     * <b>Iterator contract:</b> Uses a read-ahead pattern: {@link #hasNext} eagerly
     * produces the next chunk (buffered in {@code pendingNext}) and returns {@code false}
     * if production fails, is cancelled, or no docs remain. {@link #next} returns the
     * buffered chunk or throws {@link NoSuchElementException}, honouring the
     * {@link Iterator} contract (never returns {@code null}).
     */
    private class ChunkProducingIterator implements Iterator<PendingChunk> {
        private final DocIdToIndex[] docs;
        private final IndexReader indexReader;
        private final FetchPhaseResponseChunk.Writer chunkWriter;
        private final int targetChunkBytes;
        private final Supplier<Boolean> isCancelled;
        private final AtomicReference<Throwable> sendFailure;
        private final AtomicReference<Throwable> producerError;

        private int currentIdx;
        private int endReaderIdx;
        // Thread that last ran setNextReaderAndGetLeafEndIndex; null until the first leaf setup.
        // Used to skip redundant per-leaf rebuilds when consecutive chunks stay on the same
        // thread within the same leaf (Lucene's thread-affinity invariant still holds).
        private Thread leafSetupThread;
        private PendingChunk pendingNext;

        ChunkProducingIterator(
            DocIdToIndex[] docs,
            IndexReader indexReader,
            FetchPhaseResponseChunk.Writer chunkWriter,
            int targetChunkBytes,
            Supplier<Boolean> isCancelled,
            AtomicReference<Throwable> sendFailure,
            AtomicReference<Throwable> producerError
        ) {
            this.docs = docs;
            this.indexReader = indexReader;
            this.chunkWriter = chunkWriter;
            this.targetChunkBytes = targetChunkBytes;
            this.isCancelled = isCancelled;
            this.sendFailure = sendFailure;
            this.producerError = producerError;
        }

        @Override
        public boolean hasNext() {
            if (pendingNext != null) {
                return true;
            }
            if (currentIdx >= docs.length || producerError.get() != null || sendFailure.get() != null || isCancelled.get()) {
                return false;
            }
            pendingNext = produceNext();
            return pendingNext != null;
        }

        @Override
        public PendingChunk next() {
            if (hasNext() == false) {
                throw new NoSuchElementException();
            }
            PendingChunk chunk = pendingNext;
            pendingNext = null;
            return chunk;
        }

        private PendingChunk produceNext() {
            RecyclerBytesStreamOutput chunkBuffer = null;
            try {
                chunkBuffer = chunkWriter.newNetworkBytesStream();
                int chunkStartIndex = currentIdx;
                int hitsInChunk = 0;

                while (currentIdx < docs.length) {
                    if (hitsInChunk > 0) {
                        if (isCancelled.get()) {
                            throw new TaskCancelledException("cancelled");
                        }
                        Throwable failure = sendFailure.get();
                        if (failure != null) {
                            throw failure instanceof Exception ? (Exception) failure : new RuntimeException(failure);
                        }
                    }

                    if (currentIdx >= endReaderIdx || (hitsInChunk == 0 && Thread.currentThread() != leafSetupThread)) {
                        endReaderIdx = setNextReaderAndGetLeafEndIndex(indexReader, docs, currentIdx);
                        leafSetupThread = Thread.currentThread();
                    }

                    SearchHit hit = nextDoc(docs[currentIdx].docId);
                    try {
                        chunkBuffer.writeVInt(docs[currentIdx].index);
                        hit.writeTo(chunkBuffer);
                    } finally {
                        hit.decRef();
                    }
                    currentIdx++;
                    hitsInChunk++;

                    if (currentIdx == docs.length || chunkBuffer.size() >= targetChunkBytes) {
                        break;
                    }
                }

                ReleasableBytesReference chunkBytes = chunkBuffer.moveToBytesReference();
                chunkBuffer = null;
                boolean isLast = (currentIdx == docs.length);
                return new PendingChunk(chunkBytes, hitsInChunk, chunkStartIndex, isLast);
            } catch (Exception e) {
                producerError.compareAndSet(null, e);
                if (chunkBuffer != null) {
                    Releasables.closeWhileHandlingException(chunkBuffer);
                }
                return null;
            }
        }
    }

    private int setNextReaderAndGetLeafEndIndex(IndexReader indexReader, DocIdToIndex[] docs, int index) throws IOException {
        int leafOrd = ReaderUtil.subIndex(docs[index].docId, indexReader.leaves());
        LeafReaderContext ctx = indexReader.leaves().get(leafOrd);
        int endReaderIdx = endReaderIdx(ctx, index, docs);
        int[] docsInLeaf = docIdsInLeaf(index, endReaderIdx, docs, ctx.docBase);
        setNextReader(ctx, docsInLeaf);
        return endReaderIdx;
    }

    /**
     * Sends a single intermediate chunk. Initiates an asynchronous network write and
     * closes the {@link ThrottledIterator} releasable on ACK (or failure), which triggers
     * production of the next chunk.
     */
    private static void sendChunk(
        PendingChunk chunk,
        Releasable iteratorReleasable,
        FetchPhaseResponseChunk.Writer writer,
        ShardId shardId,
        int totalDocs,
        AtomicReference<Throwable> sendFailure,
        RefCountingListener chunkCompletionRefs,
        Supplier<Boolean> isCancelled
    ) {
        if (isCancelled.get()) {
            chunk.close();
            iteratorReleasable.close();
            return;
        }

        final Throwable failure = sendFailure.get();
        if (failure != null) {
            chunk.close();
            iteratorReleasable.close();
            return;
        }

        FetchPhaseResponseChunk responseChunk = null;
        ActionListener<Void> ackRef = null;
        try {
            responseChunk = new FetchPhaseResponseChunk(shardId, chunk.bytes, chunk.hitCount, totalDocs, chunk.sequenceStart);

            final FetchPhaseResponseChunk chunkToClose = responseChunk;

            ackRef = chunkCompletionRefs.acquire();
            final ActionListener<Void> finalAckRef = ackRef;

            writer.writeResponseChunk(responseChunk, ActionListener.wrap(v -> {
                chunkToClose.close();
                finalAckRef.onResponse(null);
                iteratorReleasable.close();
            }, e -> {
                chunkToClose.close();
                sendFailure.compareAndSet(null, e);
                finalAckRef.onFailure(e);
                iteratorReleasable.close();
            }));

            responseChunk = null;
        } catch (Exception e) {
            if (responseChunk != null) {
                responseChunk.close();
            } else {
                chunk.close();
            }
            sendFailure.compareAndSet(null, e);
            if (ackRef != null) {
                ackRef.onFailure(e);
            }
            iteratorReleasable.close();
        }
    }

    private static void cleanupLastChunk(AtomicReference<PendingChunk> lastChunkHolder) {
        PendingChunk lastChunk = lastChunkHolder.getAndSet(null);
        if (lastChunk != null) {
            lastChunk.close();
        }
    }

    /**
     * Represents a chunk ready to be sent. The underlying {@link ReleasableBytesReference} carries
     * the page-level circuit breaker release callback from {@link RecyclerBytesStreamOutput#moveToBytesReference()}.
     */
    static class PendingChunk implements AutoCloseable {
        ReleasableBytesReference bytes;
        final int hitCount;
        final int sequenceStart;
        final boolean isLast;

        PendingChunk(ReleasableBytesReference bytes, int hitCount, int sequenceStart, boolean isLast) {
            this.bytes = bytes;
            this.hitCount = hitCount;
            this.sequenceStart = sequenceStart;
            this.isLast = isLast;
        }

        @Override
        public void close() {
            ReleasableBytesReference toClose = bytes;
            bytes = null;
            if (toClose != null) {
                Releasables.closeWhileHandlingException(toClose);
            }
        }
    }
}
