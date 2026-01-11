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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Given a set of doc ids and an index reader, sorts the docs by id (when not streaming),
 * splits the sorted docs by leaf reader, and iterates through them calling abstract methods
 * {@link #setNextReader(LeafReaderContext, int[])} for each new leaf reader and
 * {@link #nextDoc(int)} for each document; then collects the resulting {@link SearchHit}s
 * into an array and returns them in the order of the original doc ids.
 * <p>
 * Optionally supports streaming hits in chunks if a {@link FetchPhaseResponseChunk.Writer}
 * is provided, reducing memory footprint for large result sets.
 * <p>
 * ORDERING: When streaming is disabled, docs are sorted by doc ID for efficient index access,
 * but the original score-based order is restored via index mapping. When streaming is enabled,
 * docs are NOT sorted to preserve score order, and sequence numbers track ordering across chunks.
 */
abstract class FetchPhaseDocsIterator {

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
     * Iterate over a set of docsIds within a particular shard and index reader.
     *
     * Streaming mode: When {@code chunkWriter} is non-null, hits are buffered and sent
     * in chunks. Docs are kept in original order (score-based) and sequence numbers track
     * position to handle out-of-order chunk arrival.
     *
     * Non-streaming mode: Docs are sorted by doc ID for efficiency, and original order
     * is restored via index mapping.
     *
     * @param shardTarget         the shard being fetched from
     * @param indexReader         the index reader
     * @param docIds              the document IDs to fetch
     * @param allowPartialResults whether partial results are allowed on timeout
     * @param querySearchResult   the query result
     * @param chunkWriter         if non-null, enables streaming mode and sends hits in chunks
     * @param chunkSize           number of hits per chunk (only used if chunkWriter is non-null)
     * @param chunkCompletionRefs RefCountingListener for tracking chunk ACKs.
     *                            Each chunk acquires a listener; when ACK is received, it signals completion.
     * @param maxInFlightChunks   maximum number of chunks to send before backpressure
     * @param sendFailure         reference to capture first chunk send failure
     * @param totalHits           total hits for building SearchHits objects
     * @param maxScore            max score for building SearchHits objects
     * @return IterateResult containing hits array and optional last chunk with sequence info
     */
    public final IterateResult iterate(
        SearchShardTarget shardTarget,
        IndexReader indexReader,
        int[] docIds,
        boolean allowPartialResults,
        QuerySearchResult querySearchResult,
        FetchPhaseResponseChunk.Writer chunkWriter,
        int chunkSize,
        RefCountingListener chunkCompletionRefs,
        int maxInFlightChunks,
        AtomicReference<Throwable> sendFailure,
        TotalHits totalHits,
        float maxScore
    ) {
        SearchHit[] searchHits = new SearchHit[docIds.length];
        DocIdToIndex[] docs = new DocIdToIndex[docIds.length];

        final boolean streamingEnabled = chunkWriter != null && chunkSize > 0;
        List<SearchHit> chunkBuffer = streamingEnabled ? new ArrayList<>(chunkSize) : null;
        ShardId shardId = streamingEnabled ? shardTarget.getShardId() : null;
        SearchHits lastChunk = null;
        long lastChunkSequenceStart = -1;

        for (int index = 0; index < docIds.length; index++) {
            docs[index] = new DocIdToIndex(docIds[index], index);
        }

        if (streamingEnabled == false) {
            Arrays.sort(docs);
        }

        int currentDoc = docs[0].docId;

        try {
            if (streamingEnabled) {
                iterateStreaming(
                    docs,
                    indexReader,
                    shardTarget,
                    allowPartialResults,
                    querySearchResult,
                    chunkWriter,
                    chunkSize,
                    chunkBuffer,
                    shardId,
                    chunkCompletionRefs,
                    maxInFlightChunks,
                    sendFailure,
                    docIds.length,
                    totalHits,
                    maxScore
                );

                // Handle final chunk
                if (chunkBuffer != null && chunkBuffer.isEmpty() == false) {
                    lastChunkSequenceStart = hitSequenceCounter.get() - chunkBuffer.size();
                    SearchHit[] lastHitsArray = chunkBuffer.toArray(new SearchHit[0]);

                    for (SearchHit hit : lastHitsArray) {
                        hit.decRef();
                    }
                    lastChunk = new SearchHits(lastHitsArray, totalHits, maxScore);
                    chunkBuffer.clear();
                }
                return new IterateResult(SearchHits.EMPTY_WITHOUT_TOTAL_HITS.getHits(), lastChunk, lastChunkSequenceStart);
            } else {
                int leafOrd = ReaderUtil.subIndex(docs[0].docId, indexReader.leaves());
                LeafReaderContext ctx = indexReader.leaves().get(leafOrd);
                int endReaderIdx = endReaderIdx(ctx, 0, docs);
                int[] docsInLeaf = docIdsInLeaf(0, endReaderIdx, docs, ctx.docBase);

                try {
                    setNextReader(ctx, docsInLeaf);
                } catch (ContextIndexSearcher.TimeExceededException e) {
                    SearchTimeoutException.handleTimeout(allowPartialResults, shardTarget, querySearchResult);
                    assert allowPartialResults;
                    return new IterateResult(SearchHits.EMPTY, lastChunk, lastChunkSequenceStart);
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
                        SearchHit hit = nextDoc(docs[i].docId);
                        searchHits[docs[i].index] = hit;

                    } catch (ContextIndexSearcher.TimeExceededException e) {
                        if (allowPartialResults == false) {
                            purgeSearchHits(searchHits);
                        }
                        SearchTimeoutException.handleTimeout(allowPartialResults, shardTarget, querySearchResult);
                        assert allowPartialResults;
                        SearchHit[] partialSearchHits = new SearchHit[i];
                        System.arraycopy(searchHits, 0, partialSearchHits, 0, i);
                        return new IterateResult(partialSearchHits, lastChunk, lastChunkSequenceStart);
                    }
                }
            }
        } catch (SearchTimeoutException e) {
            if (lastChunk != null) {
                lastChunk.decRef();
            }
            throw e;
        } catch (CircuitBreakingException e) {
            purgeSearchHits(searchHits);

            if (lastChunk != null) {
                lastChunk.decRef();
            }
            throw e;
        } catch (Exception e) {
            purgeSearchHits(searchHits);

            if (lastChunk != null) {
                lastChunk.decRef();
            }
            throw new FetchPhaseExecutionException(shardTarget, "Error running fetch phase for doc [" + currentDoc + "]", e);
        }

        return new IterateResult(searchHits, lastChunk, lastChunkSequenceStart);
    }

    /**
     * Streaming iteration: Fetches docs in sorted order (per reader) but preserves
     * score order for chunk streaming. Tracks successfully sent hits in sentIndices
     * to prevent double-decRef during cleanup if circuit breaker trips after some chunks
     * have been successfully transmitted.
     */
    private void iterateStreaming(
        DocIdToIndex[] docs,
        IndexReader indexReader,
        SearchShardTarget shardTarget,
        boolean allowPartialResults,
        QuerySearchResult querySearchResult,
        FetchPhaseResponseChunk.Writer chunkWriter,
        int chunkSize,
        List<SearchHit> chunkBuffer,
        ShardId shardId,
        RefCountingListener chunkCompletionRefs,
        int maxInFlightChunks,
        AtomicReference<Throwable> sendFailure,
        int totalDocs,
        TotalHits totalHits,
        float maxScore
    ) throws IOException {
        List<LeafReaderContext> leaves = indexReader.leaves();
        long currentChunkSequenceStart = -1;

        // Semaphore with maxInFlightChunks permits
        Semaphore transmitPermits = new Semaphore(maxInFlightChunks);

        // Track indices of hits that have been successfully sent to prevent double-cleanup
        // if circuit breaker trips after some chunks are transmitted.
        Set<Integer> sentIndices = new HashSet<>();

        // Store hits with their original score position
        SearchHit[] hitsInScoreOrder = new SearchHit[docs.length];

        try {
            // Process one reader at a time
            for (int leafOrd = 0; leafOrd < leaves.size(); leafOrd++) {
                LeafReaderContext ctx = leaves.get(leafOrd);
                int docBase = ctx.docBase;
                int maxDoc = ctx.reader().maxDoc();
                int leafEndDoc = docBase + maxDoc;

                // Collect docs that belong to this reader with their original positions
                List<DocPosition> docsInReader = new ArrayList<>();
                for (int i = 0; i < docs.length; i++) {
                    if (docs[i].docId >= docBase && docs[i].docId < leafEndDoc) {
                        docsInReader.add(new DocPosition(docs[i].docId, i));
                    }
                }

                if (docsInReader.isEmpty()) {
                    continue;
                }

                // Sort by doc ID for Lucene
                docsInReader.sort(Comparator.comparingInt(a -> a.docId));

                // Prepare array for setNextReader
                int[] docsArray = docsInReader.stream().mapToInt(dp -> dp.docId - docBase).toArray();

                try {
                    setNextReader(ctx, docsArray);
                } catch (ContextIndexSearcher.TimeExceededException e) {
                    if (leafOrd == 0) {
                        SearchTimeoutException.handleTimeout(allowPartialResults, shardTarget, querySearchResult);
                        assert allowPartialResults;
                        return;
                    }
                    if (allowPartialResults == false) {
                        purgePartialHits(hitsInScoreOrder, Collections.emptySet());
                    }
                    SearchTimeoutException.handleTimeout(allowPartialResults, shardTarget, querySearchResult);
                    assert allowPartialResults;
                    return;
                }

                // Fetch docs in sorted order
                for (DocPosition dp : docsInReader) {
                    try {
                        SearchHit hit = nextDoc(dp.docId);
                        hitsInScoreOrder[dp.scorePosition] = hit;
                    } catch (ContextIndexSearcher.TimeExceededException e) {
                        if (allowPartialResults == false) {
                            purgePartialHits(hitsInScoreOrder, Collections.emptySet());
                        }
                        SearchTimeoutException.handleTimeout(allowPartialResults, shardTarget, querySearchResult);
                        assert allowPartialResults;
                        return;
                    }
                }
            }

            // Now stream hits in score order
            int processedCount = 0;
            for (int i = 0; i < hitsInScoreOrder.length; i++) {
                SearchHit hit = hitsInScoreOrder[i];
                if (hit == null) {
                    continue; // Defensive
                }

                hit.incRef();

                if (chunkBuffer.isEmpty()) {
                    currentChunkSequenceStart = hitSequenceCounter.get();
                }
                hitSequenceCounter.getAndIncrement();

                chunkBuffer.add(hit);
                processedCount++;

                // Send chunk if full (but not on last doc)
                boolean isLastDoc = (i == hitsInScoreOrder.length - 1);
                if (chunkBuffer.size() >= chunkSize && isLastDoc == false) {
                    Throwable knownFailure = sendFailure.get();
                    if (knownFailure != null) {
                        throw new RuntimeException("Fetch chunk failed", knownFailure);
                    }

                    try {
                        transmitPermits.acquire();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException("Interrupted while waiting for transmit permit", e);
                    }

                    // Track which indices are being sent in this chunk
                    int chunkStartIdx = i - chunkBuffer.size() + 1;
                    int chunkEndIdx = i + 1;

                    // Mark indices as sent BEFORE the async call, since sendChunk immediately decRefs them
                    for (int idx = chunkStartIdx; idx < chunkEndIdx; idx++) {
                        sentIndices.add(idx);
                    }

                    sendChunk(
                        chunkWriter,
                        chunkBuffer,
                        shardId,
                        currentChunkSequenceStart,
                        processedCount - chunkBuffer.size(),
                        totalDocs,
                        totalHits,
                        maxScore,
                        sendFailure,
                        transmitPermits,
                        chunkCompletionRefs.acquire()
                    );
                    chunkBuffer.clear();
                }
            }
        } catch (Exception e) {
            for (SearchHit bufferHit : chunkBuffer) {
                if (bufferHit != null) {
                    for (int j = 0; j < hitsInScoreOrder.length; j++) {
                        if (hitsInScoreOrder[j] == bufferHit) {
                            // DecRef twice: once for buffer incRef, once for base reference
                            bufferHit.decRef();
                            bufferHit.decRef();
                            sentIndices.add(j);
                            break;
                        }
                    }
                }
            }
            chunkBuffer.clear();

            purgePartialHits(hitsInScoreOrder, sentIndices);
            throw e;
        }
    }

    /**
     * Helper to store doc ID with its original score position
     */
    private static class DocPosition {
        final int docId;
        final int scorePosition;

        DocPosition(int docId, int scorePosition) {
            this.docId = docId;
            this.scorePosition = scorePosition;
        }
    }

    /**
     * Clean up partially fetched hits, skipping hits that were successfully sent in chunks.
     * This prevents double-decRef when circuit breaker trips after some chunks were transmitted.
     */
    private static void purgePartialHits(SearchHit[] hits, Set<Integer> sentIndices) {
        for (int i = 0; i < hits.length; i++) {
            if (hits[i] != null && sentIndices.contains(i) == false) {
                hits[i].decRef();
            }
        }
    }

    /**
     * Sends a chunk of hits to the coordinator with sequence information for ordering.
     * Releases a transmit permit when complete (success or failure). On successful transmission,
     * adds the sent hit indices to sentIndices to prevent double-cleanup if a later circuit breaker trip occurs.
     */
    private static void sendChunk(
        FetchPhaseResponseChunk.Writer writer,
        List<SearchHit> buffer,
        ShardId shardId,
        long sequenceStart,
        int fromIndex,
        int totalDocs,
        TotalHits totalHits,
        float maxScore,
        AtomicReference<Throwable> sendFailure,
        Semaphore transmitPermits,
        ActionListener<Void> chunkListener
    ) {

        // Release if nothing to send
        if (buffer.isEmpty()) {
            transmitPermits.release();
            chunkListener.onResponse(null); // Signal completion to RefCountingListener
            return;
        }

        SearchHit[] hitsArray = buffer.toArray(new SearchHit[0]);

        // We incremented when adding to buffer, SearchHits constructor will increment again
        // So decRef to get back to refCount=1 before passing to SearchHits
        for (SearchHit hit : hitsArray) {
            hit.decRef();
        }

        SearchHits chunkHits = null;
        try {
            chunkHits = new SearchHits(hitsArray, totalHits, maxScore);
            final SearchHits finalChunkHits = chunkHits;

            FetchPhaseResponseChunk chunk = new FetchPhaseResponseChunk(
                System.currentTimeMillis(),
                FetchPhaseResponseChunk.Type.HITS,
                shardId,
                chunkHits,
                fromIndex,
                hitsArray.length,
                totalDocs,
                sequenceStart
            );

            writer.writeResponseChunk(chunk, ActionListener.wrap(ack -> {
                // Success: coordinator received the chunk
                transmitPermits.release();
                finalChunkHits.decRef();
                chunkListener.onResponse(null);
            }, ex -> {
                // Failure: transmission failed, we still own the hits
                transmitPermits.release();
                finalChunkHits.decRef();
                sendFailure.compareAndSet(null, ex);
                chunkListener.onFailure(ex);
            }));
        } catch (Exception e) {
            transmitPermits.release();
            sendFailure.compareAndSet(null, e);

            // If chunk creation failed after SearchHits was created, clean up
            if (chunkHits != null) {
                chunkHits.decRef();
            }

            // Signal failure to RefCountingListener
            chunkListener.onFailure(e);
        }
    }

    private static void purgeSearchHits(SearchHit[] searchHits) {
        for (SearchHit searchHit : searchHits) {
            if (searchHit != null) {
                searchHit.decRef();
            }
        }
    }

    /**
     * Releases hits in the chunk buffer during error cleanup.
     * Only called when streaming mode is enabled.
     */
    private static void purgeChunkBuffer(List<SearchHit> buffer) {
        for (SearchHit hit : buffer) {
            if (hit != null) {
                hit.decRef();
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
