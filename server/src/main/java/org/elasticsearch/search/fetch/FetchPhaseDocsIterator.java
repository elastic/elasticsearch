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
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.fetch.chunk.FetchPhaseResponseChunk;
import org.elasticsearch.search.internal.ContextIndexSearcher;
import org.elasticsearch.search.internal.ShardSearchContextId;
import org.elasticsearch.search.query.QuerySearchResult;
import org.elasticsearch.search.query.SearchTimeoutException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Given a set of doc ids and an index reader, sorts the docs by id, splits the sorted
 * docs by leaf reader, and iterates through them calling abstract methods
 * {@link #setNextReader(LeafReaderContext, int[])} for each new leaf reader and
 * {@link #nextDoc(int)} for each document; then collects the resulting {@link SearchHit}s
 * into an array and returns them in the order of the original doc ids.
 * <p>
 * Optionally supports streaming hits in chunks if a {@link FetchPhaseResponseChunk.Writer}
 * is provided, reducing memory footprint for large result sets.
 */
abstract class FetchPhaseDocsIterator {

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
     */
    public final SearchHit[] iterate(
        SearchShardTarget shardTarget,
        IndexReader indexReader,
        int[] docIds,
        boolean allowPartialResults,
        QuerySearchResult querySearchResult
    ) {
        // Delegate to new method with null writer to maintain backward compatibility
        // When writer is null, no streaming chunks are sent (original behavior)
        return iterate(shardTarget, indexReader, docIds, allowPartialResults, querySearchResult, null, 0);
    }

    /**
     * Iterate over a set of docsIds within a particular shard and index reader.
     * If a writer is provided, hits are sent in chunks as they are produced (streaming mode).
     * Streaming mode: When {@code chunkWriter} is non-null, hits are buffered and sent
     * in chunks of size {@code chunkSize}. This reduces memory footprint for large result sets
     * by streaming results to the coordinator as they are produced.
     * Legacy mode: When {@code chunkWriter} is null, behaves exactly like the original
     * {@link #iterate(SearchShardTarget, IndexReader, int[], boolean, QuerySearchResult)} method.
     *
     * @param shardTarget         the shard being fetched from
     * @param indexReader         the index reader
     * @param docIds              the document IDs to fetch
     * @param allowPartialResults whether partial results are allowed on timeout
     * @param querySearchResult   the query result
     * @param chunkWriter         if non-null, enables streaming mode and sends hits in chunks
     * @param chunkSize           number of hits per chunk (only used if chunkWriter is non-null)
     * @return array of SearchHits in the order of the original docIds
     */
    public final SearchHit[] iterate(
        SearchShardTarget shardTarget,
        IndexReader indexReader,
        int[] docIds,
        boolean allowPartialResults,
        QuerySearchResult querySearchResult,
        FetchPhaseResponseChunk.Writer chunkWriter,
        int chunkSize
    ) {
        SearchHit[] searchHits = new SearchHit[docIds.length];
        DocIdToIndex[] docs = new DocIdToIndex[docIds.length];

        final boolean streamingEnabled = chunkWriter != null && chunkSize > 0;
        List<SearchHit> chunkBuffer = streamingEnabled ? new ArrayList<>(chunkSize) : null;
        int shardIndex = -1;
        ShardSearchContextId ctxId = null;

        // Initialize streaming context if enabled
        if (streamingEnabled) {
            shardIndex = shardTarget.getShardId().id();

            // Send START_RESPONSE chunk
            FetchPhaseResponseChunk startChunk = new FetchPhaseResponseChunk(
                System.currentTimeMillis(),
                FetchPhaseResponseChunk.Type.START_RESPONSE,
                shardIndex,
                null,
                0,
                0,
                docIds.length
            );
            chunkWriter.writeResponseChunk(startChunk, ActionListener.running(() -> {}));
        }

        for (int index = 0; index < docIds.length; index++) {
            docs[index] = new DocIdToIndex(docIds[index], index);
        }
        // make sure that we iterate in doc id order
        Arrays.sort(docs);
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
                return SearchHits.EMPTY;
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
                        chunkBuffer.add(hit);

                        if (chunkBuffer.size() >= chunkSize) {
                            // Send HIT chunk
                            sendChunk(
                                chunkWriter,
                                chunkBuffer,
                                shardIndex,
                                i - chunkBuffer.size() + 1,  // from index
                                docIds.length,
                                Float.NaN  // maxScore not meaningful for individual chunks
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
                    return partialSearchHits;
                }
            }

            // Send final partial chunk if streaming is enabled and buffer has remaining hits
            if (streamingEnabled && chunkBuffer.isEmpty() == false) {
                sendChunk(chunkWriter, chunkBuffer, shardIndex, docs.length - chunkBuffer.size(), docIds.length, Float.NaN);
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
        return searchHits;
    }

    /**
     * Sends a chunk of hits to the coordinator.
     */
    private static void sendChunk(
        FetchPhaseResponseChunk.Writer writer,
        List<SearchHit> buffer,
        int shardIndex,
        int fromIndex,
        int totalDocs,
        float maxScore
    ) {
        if (buffer.isEmpty()) {
            return;
        }

        SearchHit[] hitsArray = buffer.toArray(new SearchHit[0]);

        // We incremented when adding to buffer, SearchHits constructor will increment again
        for (SearchHit hit : hitsArray) {
            hit.decRef();
        }

        SearchHits chunkHits = null;
        try {
            chunkHits = new SearchHits(hitsArray, new TotalHits(hitsArray.length, TotalHits.Relation.EQUAL_TO), maxScore);

            FetchPhaseResponseChunk chunk = new FetchPhaseResponseChunk(
                System.currentTimeMillis(),
                FetchPhaseResponseChunk.Type.HITS,
                shardIndex,
                chunkHits,
                fromIndex,
                hitsArray.length,
                totalDocs
            );

            writer.writeResponseChunk(chunk, ActionListener.running(() -> {}));
        } finally {
            if (chunkHits != null) {
                chunkHits.decRef();
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
}
