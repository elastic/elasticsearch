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
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.internal.ContextIndexSearcher;
import org.elasticsearch.search.query.QuerySearchResult;
import org.elasticsearch.search.query.SearchTimeoutException;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

/**
 * Iterates through a set of document IDs, fetching each document and collecting
 * the resulting {@link SearchHit}s.
 * <p>
 * Documents are sorted by doc ID for efficient sequential Lucene access, then results
 * are mapped back to their original score-based order. All hits are collected in memory
 * and returned at once.
 *
 * @see StreamingFetchPhaseDocsIterator
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
                    return new IterateResult(stripNulls(searchHits));
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

    private static SearchHit[] stripNulls(SearchHit[] searchHits) {
        for (SearchHit hit : searchHits) {
            if (hit == null) {
                return Arrays.stream(searchHits).filter(Objects::nonNull).toArray(SearchHit[]::new);
            }
        }
        return searchHits;
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
     * For streaming: contains last chunk bytes to be sent after all ACKs. The bytes carry
     * page-level circuit breaker tracking from the {@link org.elasticsearch.common.io.stream.RecyclerBytesStreamOutput};
     * releasing the bytes automatically decrements the breaker.
     */
    static class IterateResult implements AutoCloseable {
        final SearchHit[] hits;  // Non-streaming mode only
        final ReleasableBytesReference lastChunkBytes;
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
         * After calling, close() will not release the bytes. The caller becomes responsible
         * for eventually releasing the {@link ReleasableBytesReference} (which decrements the circuit breaker).
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
            }
        }
    }
}
