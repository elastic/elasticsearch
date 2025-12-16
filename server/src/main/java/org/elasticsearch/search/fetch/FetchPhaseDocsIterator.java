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
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

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
     * @param pendingChunks       list to track pending chunk acknowledgments
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
        List<CompletableFuture<Void>> pendingChunks
    ) {
        SearchHit[] searchHits = new SearchHit[docIds.length];
        DocIdToIndex[] docs = new DocIdToIndex[docIds.length];

        final boolean streamingEnabled = chunkWriter != null && chunkSize > 0;
        List<SearchHit> chunkBuffer = streamingEnabled ? new ArrayList<>(chunkSize) : null;
        ShardId shardId = streamingEnabled ? shardTarget.getShardId() : null;
        SearchHits lastChunk = null;

        // Track sequence numbers for ordering
        long currentChunkSequenceStart = -1;
        long lastChunkSequenceStart = -1;

        for (int index = 0; index < docIds.length; index++) {
            docs[index] = new DocIdToIndex(docIds[index], index);
        }

        // Only sort by doc ID if NOT streaming
        // Sorting by doc ID is an optimization for sequential index access,
        // but streaming mode needs to preserve score order from query phase
        if (streamingEnabled == false) {
            Arrays.sort(docs);
        }

        int currentDoc = docs[0].docId;

        try {
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

                    if (streamingEnabled) {
                        hit.incRef();

                        // Mark sequence start when starting new chunk
                        if (chunkBuffer.isEmpty()) {
                            currentChunkSequenceStart = hitSequenceCounter.get();
                        }

                        // Assign sequence to this hit and increment counter
                        hitSequenceCounter.getAndIncrement();

                        chunkBuffer.add(hit);

                        // Send intermediate chunks - not when it's the last iteration
                        if (chunkBuffer.size() >= chunkSize && i < docs.length - 1) {
                            // Send chunk with sequence information
                            pendingChunks.add(
                                sendChunk(
                                    chunkWriter,
                                    chunkBuffer,
                                    shardId,
                                    currentChunkSequenceStart,  // Pass sequence start for ordering
                                    i - chunkBuffer.size() + 1,
                                    docIds.length,
                                    Float.NaN
                                )
                            );
                            chunkBuffer.clear();
                        }
                    } else {
                        searchHits[docs[i].index] = hit;
                    }
                } catch (ContextIndexSearcher.TimeExceededException e) {
                    if (allowPartialResults == false) {
                        purgeSearchHits(searchHits);
                        if (streamingEnabled) {
                            purgeChunkBuffer(chunkBuffer);
                        }
                    }
                    SearchTimeoutException.handleTimeout(allowPartialResults, shardTarget, querySearchResult);
                    assert allowPartialResults;
                    SearchHit[] partialSearchHits = new SearchHit[i];
                    System.arraycopy(searchHits, 0, partialSearchHits, 0, i);
                    return new IterateResult(partialSearchHits, lastChunk, lastChunkSequenceStart);
                }
            }

            // Return the final partial chunk if streaming is enabled and buffer has remaining hits
            if (streamingEnabled && chunkBuffer.isEmpty() == false) {
                // Remember the sequence start for the last chunk
                lastChunkSequenceStart = currentChunkSequenceStart;

                SearchHit[] lastHitsArray = chunkBuffer.toArray(new SearchHit[0]);

                // DecRef for SearchHits constructor (will increment)
                for (SearchHit hit : lastHitsArray) {
                    hit.decRef();
                }

                lastChunk = new SearchHits(
                    lastHitsArray,
                    new TotalHits(lastHitsArray.length, TotalHits.Relation.EQUAL_TO),
                    Float.NaN
                );
                chunkBuffer.clear();
            }
        } catch (SearchTimeoutException e) {
            throw e;
        } catch (CircuitBreakingException e) {
            purgeSearchHits(searchHits);
            if (streamingEnabled) {
                purgeChunkBuffer(chunkBuffer);
            }
            throw e;
        } catch (Exception e) {
            purgeSearchHits(searchHits);
            if (streamingEnabled) {
                purgeChunkBuffer(chunkBuffer);
            }
            throw new FetchPhaseExecutionException(shardTarget, "Error running fetch phase for doc [" + currentDoc + "]", e);
        }

        return new IterateResult(searchHits, lastChunk, lastChunkSequenceStart);
    }

    /**
     * Sends a chunk of hits to the coordinator with sequence information for ordering.
     */
    private static CompletableFuture<Void> sendChunk(
        FetchPhaseResponseChunk.Writer writer,
        List<SearchHit> buffer,
        ShardId shardId,
        long sequenceStart,
        int fromIndex,
        int totalDocs,
        float maxScore
    ) {
        CompletableFuture<Void> future = new CompletableFuture<>();

        if (buffer.isEmpty()) {
            future.complete(null);
            return future;
        }

        SearchHit[] hitsArray = buffer.toArray(new SearchHit[0]);

        // We incremented when adding to buffer, SearchHits constructor will increment again
        // So decRef to get back to refCount=1 before passing to SearchHits
        for (SearchHit hit : hitsArray) {
            hit.decRef();
        }

        SearchHits chunkHits = null;
        try {
            chunkHits = new SearchHits(
                hitsArray,
                new TotalHits(hitsArray.length, TotalHits.Relation.EQUAL_TO),
                maxScore
            );
            final SearchHits finalChunkHits = chunkHits;

            FetchPhaseResponseChunk chunk = new FetchPhaseResponseChunk(
                System.currentTimeMillis(),
                FetchPhaseResponseChunk.Type.HITS,
                shardId,
                chunkHits,
                fromIndex,
                hitsArray.length,
                totalDocs,
                sequenceStart  // Include sequence start in chunk metadata
            );

            // Send the chunk - coordinator will take ownership of the hits
            writer.writeResponseChunk(chunk, ActionListener.wrap(
                ack -> {
                    // Coordinator now owns the hits, decRef to release local reference
                    finalChunkHits.decRef();
                    future.complete(null);
                },
                ex -> {
                    // Failed to send - we still own the hits, must clean up
                    finalChunkHits.decRef();
                    future.completeExceptionally(ex);
                }
            ));
        } catch (Exception e) {
            future.completeExceptionally(e);
            // If chunk creation failed after SearchHits was created, clean up
            if (chunkHits != null) {
                chunkHits.decRef();
            }
        }

        return future;
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
    static class IterateResult {
        final SearchHit[] hits;
        final SearchHits lastChunk;  // null for non-streaming mode
        final long lastChunkSequenceStart;  // -1 if no last chunk

        IterateResult(SearchHit[] hits, SearchHits lastChunk, long lastChunkSequenceStart) {
            this.hits = hits;
            this.lastChunk = lastChunk;
            this.lastChunkSequenceStart = lastChunkSequenceStart;
        }
    }
}
