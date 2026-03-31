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
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.ThrottledTaskRunner;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.fetch.chunk.FetchPhaseResponseChunk;
import org.elasticsearch.tasks.TaskCancelledException;

import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

/**
 * Extends {@link FetchPhaseDocsIterator} with asynchronous chunked iteration
 * via {@link #iterateAsync}. The synchronous {@link #iterate} method from the
 * parent class remains available for non-streaming use.
 * <p>
 * Uses {@link ThrottledTaskRunner} with {@link EsExecutors#DIRECT_EXECUTOR_SERVICE} to
 * manage chunk sends:
 * <ul>
 *   <li>Fetches documents and creates chunks</li>
 *   <li>Send tasks are enqueued directly to ThrottledTaskRunner</li>
 *   <li>Tasks run inline when under maxInFlightChunks capacity</li>
 *   <li>When at capacity, tasks queue internally until ACKs arrive</li>
 *   <li>ACK callbacks signal task completion, triggering queued tasks</li>
 * </ul>
 * <b>Threading:</b> All Lucene operations execute on the calling thread to satisfy
 * Lucene's thread-affinity requirements. Send tasks run inline (DIRECT_EXECUTOR) when
 * under capacity; ACK handling occurs asynchronously on network threads.
 * <p>
 * <b>Memory Management:</b> The circuit breaker tracks recycler page allocations via the
 * {@link RecyclerBytesStreamOutput} passed from the chunk writer. If the breaker trips
 * during serialization, the producer fails immediately with a
 * {@link org.elasticsearch.common.breaker.CircuitBreakingException}, preventing unbounded
 * memory growth. Pages are released (and the breaker decremented) when the
 * {@link ReleasableBytesReference} from {@link RecyclerBytesStreamOutput#moveToBytesReference()}
 * is closed — either on ACK for intermediate chunks or when the last chunk is consumed.
 * <p>
 * <b>Backpressure:</b> {@link ThrottledTaskRunner} limits concurrent in-flight sends to
 * {@code maxInFlightChunks}. The circuit breaker provides the memory limit.
 * <p>
 * <b>Cancellation:</b> The producer checks the cancellation flag periodically.
 */
abstract class StreamingFetchPhaseDocsIterator extends FetchPhaseDocsIterator {

    /**
     * Default target chunk size in bytes (256KB).
     * Chunks may slightly exceed this as we complete the current hit before checking.
     */
    static final int DEFAULT_TARGET_CHUNK_BYTES = 256 * 1024;

    /**
     * Asynchronous iteration using {@link ThrottledTaskRunner} for streaming mode.
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

        final AtomicReference<PendingChunk> lastChunkHolder = new AtomicReference<>();
        final AtomicReference<Throwable> producerError = new AtomicReference<>();

        // ThrottledTaskRunner manages send concurrency
        final ThrottledTaskRunner sendRunner = new ThrottledTaskRunner("fetch", maxInFlightChunks, EsExecutors.DIRECT_EXECUTOR_SERVICE);

        // RefCountingListener fires completion callback when all refs are released.
        final RefCountingListener completionRefs = new RefCountingListener(ActionListener.wrap(ignored -> {

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
                throw e;
            }
        }, e -> {
            cleanupLastChunk(lastChunkHolder);
            listener.onFailure(e);
        }));

        try {
            produceChunks(
                shardTarget.getShardId(),
                indexReader,
                docIds,
                chunkWriter,
                targetChunkBytes,
                sendRunner,
                completionRefs,
                lastChunkHolder,
                sendFailure,
                chunkCompletionRefs,
                isCancelled
            );
        } catch (Exception e) {
            producerError.set(e);
        } finally {
            completionRefs.close();
        }
    }

    /**
     * Produces chunks and enqueues send tasks to ThrottledTaskRunner.
     * <p>
     * For each chunk:
     * <ol>
     *   <li>Fetch documents and serialize to bytes (page allocations tracked by the CB in the stream)</li>
     *   <li>For intermediate chunks: acquire ref and enqueue send task to ThrottledTaskRunner</li>
     *   <li>For last chunk: store in lastChunkHolder (returned via listener after all ACKs)</li>
     * </ol>
     */
    private void produceChunks(
        ShardId shardId,
        IndexReader indexReader,
        int[] docIds,
        FetchPhaseResponseChunk.Writer chunkWriter,
        int targetChunkBytes,
        ThrottledTaskRunner sendRunner,
        RefCountingListener completionRefs,
        AtomicReference<PendingChunk> lastChunkHolder,
        AtomicReference<Throwable> sendFailure,
        RefCountingListener chunkCompletionRefs,
        Supplier<Boolean> isCancelled
    ) throws Exception {
        int totalDocs = docIds.length;
        RecyclerBytesStreamOutput chunkBuffer = null;

        try {
            chunkBuffer = chunkWriter.newNetworkBytesStream();
            int chunkStartIndex = 0;
            int hitsInChunk = 0;

            for (int scoreIndex = 0; scoreIndex < totalDocs; scoreIndex++) {
                if (scoreIndex % 32 == 0) {
                    if (isCancelled.get()) {
                        throw new TaskCancelledException("cancelled");
                    }
                    Throwable failure = sendFailure.get();
                    if (failure != null) {
                        throw failure instanceof Exception ? (Exception) failure : new RuntimeException(failure);
                    }
                }

                int docId = docIds[scoreIndex];

                int leafOrd = ReaderUtil.subIndex(docId, indexReader.leaves());
                LeafReaderContext ctx = indexReader.leaves().get(leafOrd);
                int leafDocId = docId - ctx.docBase;
                setNextReader(ctx, new int[] { leafDocId });

                SearchHit hit = nextDoc(docId);
                try {
                    hit.writeTo(chunkBuffer);
                } finally {
                    hit.decRef();
                }
                hitsInChunk++;

                boolean isLast = (scoreIndex == totalDocs - 1);
                boolean bufferFull = chunkBuffer.size() >= targetChunkBytes;

                if (bufferFull || isLast) {
                    final ReleasableBytesReference chunkBytes = chunkBuffer.moveToBytesReference();
                    chunkBuffer = null;

                    try {
                        PendingChunk chunk = new PendingChunk(chunkBytes, hitsInChunk, chunkStartIndex, isLast);

                        if (isLast) {
                            lastChunkHolder.set(chunk);
                        } else {
                            ActionListener<Void> completionRef = null;
                            try {
                                completionRef = completionRefs.acquire();
                                sendRunner.enqueueTask(
                                    new SendChunkTask(
                                        chunk,
                                        completionRef,
                                        chunkWriter,
                                        shardId,
                                        totalDocs,
                                        sendFailure,
                                        chunkCompletionRefs,
                                        isCancelled
                                    )
                                );
                                completionRef = null;
                            } finally {
                                if (completionRef != null) {
                                    completionRef.onResponse(null);
                                    chunk.close();
                                }
                            }
                        }

                        if (isLast == false) {
                            chunkBuffer = chunkWriter.newNetworkBytesStream();
                            chunkStartIndex = scoreIndex + 1;
                            hitsInChunk = 0;
                        }
                    } catch (Exception e) {
                        Releasables.closeWhileHandlingException(chunkBytes);
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
     * Task that sends a single chunk. Implements {@link ActionListener} to receive
     * the throttle releasable from {@link ThrottledTaskRunner}.
     */
    private static final class SendChunkTask implements ActionListener<Releasable> {
        private final PendingChunk chunk;
        private final ActionListener<Void> completionRef;
        private final FetchPhaseResponseChunk.Writer writer;
        private final ShardId shardId;
        private final int totalDocs;
        private final AtomicReference<Throwable> sendFailure;
        private final RefCountingListener chunkCompletionRefs;
        private final Supplier<Boolean> isCancelled;

        private SendChunkTask(
            PendingChunk chunk,
            ActionListener<Void> completionRef,
            FetchPhaseResponseChunk.Writer writer,
            ShardId shardId,
            int totalDocs,
            AtomicReference<Throwable> sendFailure,
            RefCountingListener chunkCompletionRefs,
            Supplier<Boolean> isCancelled
        ) {
            this.chunk = chunk;
            this.completionRef = completionRef;
            this.writer = writer;
            this.shardId = shardId;
            this.totalDocs = totalDocs;
            this.sendFailure = sendFailure;
            this.chunkCompletionRefs = chunkCompletionRefs;
            this.isCancelled = isCancelled;
        }

        @Override
        public void onResponse(Releasable throttleReleasable) {
            sendChunk(chunk, throttleReleasable, completionRef, writer, shardId, totalDocs, sendFailure, chunkCompletionRefs, isCancelled);
        }

        @Override
        public void onFailure(Exception e) {
            chunk.close();
            sendFailure.compareAndSet(null, e);
            completionRef.onFailure(e);
        }
    }

    /**
     * Sends a single chunk. Called by ThrottledTaskRunner.
     * <p>
     * The send is asynchronous - this method initiates the network write and returns immediately.
     * The ACK callback handles cleanup and signals task completion to ThrottledTaskRunner.
     * Page-level CB tracking is released when the {@link ReleasableBytesReference} is closed.
     */
    private static void sendChunk(
        PendingChunk chunk,
        Releasable throttleReleasable,
        ActionListener<Void> completionRef,
        FetchPhaseResponseChunk.Writer writer,
        ShardId shardId,
        int totalDocs,
        AtomicReference<Throwable> sendFailure,
        RefCountingListener chunkCompletionRefs,
        Supplier<Boolean> isCancelled
    ) {
        if (isCancelled.get()) {
            chunk.close();
            completionRef.onResponse(null);
            throttleReleasable.close();
            return;
        }

        // Check for prior failure before sending
        final Throwable failure = sendFailure.get();
        if (failure != null) {
            chunk.close();
            completionRef.onResponse(null);
            throttleReleasable.close();
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
                completionRef.onResponse(null);
                throttleReleasable.close();
            }, e -> {
                chunkToClose.close();
                sendFailure.compareAndSet(null, e);
                finalAckRef.onFailure(e);
                completionRef.onFailure(e);
                throttleReleasable.close();
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
            completionRef.onFailure(e);
            throttleReleasable.close();
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
        final ReleasableBytesReference bytes;
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
            if (bytes != null) {
                Releasables.closeWhileHandlingException(bytes);
            }
        }
    }
}
