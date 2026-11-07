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
import org.apache.lucene.index.StoredFields;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.store.DirectoryMetrics;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.internal.ContextIndexSearcher;
import org.elasticsearch.search.query.QuerySearchResult;
import org.elasticsearch.search.query.SearchTimeoutException;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

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

    private static final Logger LOGGER = LogManager.getLogger(FetchPhaseDocsIterator.class);

    /**
     * Accounts for FetchPhase memory usage.
     * It gets cleaned up after each fetch phase and should not be accessed/modified by subclasses.
     */
    private long requestBreakerBytes;

    private final AtomicReference<DirectoryMetrics> fetchMetricsDelta = new AtomicReference<>(DirectoryMetrics.EMPTY);

    private final DirectoryMetrics.Capture metricsCapture;

    protected FetchPhaseDocsIterator(DirectoryMetrics.Capture metricsCapture) {
        this.metricsCapture = metricsCapture;
    }

    public void addRequestBreakerBytes(long delta) {
        requestBreakerBytes += delta;
    }

    public long getRequestBreakerBytes() {
        return requestBreakerBytes;
    }

    protected DirectoryMetrics getFetchMetricsDelta() {
        return fetchMetricsDelta.get();
    }

    protected final <T> T measure(Supplier<T> readOperation) {
        return metricsCapture.measure(readOperation, fetchMetricsDelta);
    }

    /**
     * Called when a new leaf reader is reached.
     *
     * @param ctx        the leaf reader for this set of doc ids
     * @param docsInLeaf the reader-specific docids to be fetched in this leaf reader
     */
    protected abstract void setNextReader(LeafReaderContext ctx, int[] docsInLeaf) throws IOException;

    /**
     * Whether to ask the codec to prefetch the data needed to load the hits of every segment before loading the first hit, see
     * {@link SearchService#FETCH_PHASE_PREFETCH_ENABLED}. Defaults to {@code false}.
     */
    protected boolean prefetch() {
        return false;
    }

    /**
     * Prefetches the doc values that {@link #setNextReader} reads for a segment that has hits, such as the {@code _id}. Called once for
     * every such segment before the first {@link #setNextReader} call, when {@link #prefetch()} is {@code true}. Defaults to a no-op.
     */
    protected void prefetchDocValues(LeafReaderContext ctx) throws IOException {}

    /**
     * Whether loading the hits reads their stored fields, so that it is worth prefetching them. Defaults to {@code true}.
     */
    protected boolean loadsStoredFields() {
        return true;
    }

    /**
     * The maximum number of hits whose stored fields are prefetched ahead of the hit being loaded, see
     * {@link SearchService#FETCH_PHASE_PREFETCH_MAX_DOCS}. Only used when {@link #prefetch()} is {@code true}.
     */
    protected int prefetchMaxDocs() {
        return SearchService.FETCH_PHASE_PREFETCH_MAX_DOCS.get(Settings.EMPTY);
    }

    /**
     * Returns a {@link HitsPrefetcher} for these hits that has prefetched what loading the first hits reads, or {@code null} if
     * {@link #prefetch()} is {@code false}.
     *
     * @param docs the hits, sorted by doc id
     */
    final HitsPrefetcher startPrefetching(IndexReader indexReader, DocIdToIndex[] docs) {
        if (prefetch() == false) {
            return null;
        }
        HitsPrefetcher prefetcher = new HitsPrefetcher(indexReader, docs, prefetchMaxDocs(), loadsStoredFields());
        prefetcher.start();
        return prefetcher;
    }

    /**
     * Keeps what loading the next hits reads prefetched, so that a directory that reads from slow storage can fetch the data of many
     * hits concurrently rather than one hit after the other, while bounding how much is prefetched at once. Best effort: failures stop
     * the prefetching, and loading the hits reads the data anyway.
     * <p>
     * The doc values of every segment that has hits are prefetched first, once per segment. Then the stored fields of at most
     * {@code maxDocs} hits ahead of the hit being loaded are prefetched: the first {@code maxDocs} hits before the first one is loaded,
     * and the next half window each time loading reaches the middle of the prefetched hits. The reads of the next hits are then in flight
     * while the current ones are loaded, and a fetch of {@code n} hits waits for about {@code 2n / maxDocs} rounds of reads instead of
     * {@code n}.
     * <p>
     * The doc values go first because {@link StoredFields#prefetch} reads the stored fields index synchronously to locate the block of a
     * document, which would otherwise delay the doc values prefetch of the following segments.
     * <p>
     * Not thread-safe: a fetch calls it from one thread at a time.
     */
    final class HitsPrefetcher {
        private final IndexReader indexReader;
        private final DocIdToIndex[] docs;
        private final int maxDocs;
        private final boolean storedFields;
        private boolean failed;
        // index in docs of the first hit whose stored fields are not prefetched yet
        private int prefetchedUpTo;
        private LeafReaderContext leaf;
        private StoredFields leafStoredFields;
        // StoredFields instances are not shared between threads: a streaming fetch can produce its chunks on different threads
        private Thread leafThread;

        private HitsPrefetcher(IndexReader indexReader, DocIdToIndex[] docs, int maxDocs, boolean storedFields) {
            this.indexReader = indexReader;
            this.docs = docs;
            this.maxDocs = Math.max(1, maxDocs);
            this.storedFields = storedFields;
        }

        private void start() {
            try {
                for (int i = 0; i < docs.length;) {
                    LeafReaderContext ctx = leaf(indexReader, docs[i].docId);
                    prefetchDocValues(ctx);
                    i = endReaderIdx(ctx, i, docs);
                }
            } catch (Exception e) {
                onFailure(e);
            }
            beforeHit(0);
        }

        /**
         * Called before loading {@code docs[index]}. Prefetches the stored fields of the next half window of hits once loading reaches
         * the middle of the prefetched hits.
         */
        void beforeHit(int index) {
            if (failed || storedFields == false || prefetchedUpTo >= docs.length || prefetchedUpTo - index > maxDocs / 2) {
                return;
            }
            prefetchedUpTo = Math.max(prefetchedUpTo, index);
            int end = Math.min(docs.length, index + maxDocs);
            try {
                if (leafThread != Thread.currentThread()) {
                    leaf = null;
                    leafThread = Thread.currentThread();
                }
                for (; prefetchedUpTo < end; prefetchedUpTo++) {
                    int docId = docs[prefetchedUpTo].docId;
                    if (leaf == null || docId >= leaf.docBase + leaf.reader().maxDoc()) {
                        leaf = leaf(indexReader, docId);
                        leafStoredFields = leaf.reader().storedFields();
                    }
                    leafStoredFields.prefetch(docId - leaf.docBase);
                }
            } catch (Exception e) {
                onFailure(e);
            }
        }

        private void onFailure(Exception e) {
            failed = true;
            // only a hint: loading the hits reads the same data and reports any failure
            LOGGER.debug("failed to prefetch the hits to fetch", e);
        }
    }

    private static LeafReaderContext leaf(IndexReader indexReader, int docId) {
        return indexReader.leaves().get(ReaderUtil.subIndex(docId, indexReader.leaves()));
    }

    /**
     * Called for each document within a leaf reader.
     *
     * @param doc the global doc id
     * @return a {@link SearchHit} for the document
     */
    protected abstract SearchHit nextDoc(int doc) throws IOException;

    /**
     * Called after all documents have been successfully iterated in non-streaming mode.
     * Subclasses can override this to flush any tail-buffered bytes to the circuit breaker.
     * May throw {@link org.elasticsearch.common.breaker.CircuitBreakingException} — the caller
     * ensures that already-fetched hits are released before propagating the exception.
     */
    protected void onAllHitsIterated() {}

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
        return measure(() -> doIterate(shardTarget, indexReader, docIds, allowPartialResults, querySearchResult));
    }

    private IterateResult doIterate(
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
            HitsPrefetcher prefetcher = startPrefetching(indexReader, docs);

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
                    if (prefetcher != null) {
                        prefetcher.beforeHit(i);
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
                    onAllHitsIterated();
                    return new IterateResult(stripNulls(searchHits));
                }
            }
            onAllHitsIterated();
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

    static int endReaderIdx(LeafReaderContext currentReaderContext, int index, DocIdToIndex[] docs) {
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

    static int[] docIdsInLeaf(int index, int endReaderIdx, DocIdToIndex[] docs, int docBase) {
        int[] result = new int[endReaderIdx - index];
        int d = 0;
        for (int i = index; i < endReaderIdx; i++) {
            assert docs[i].docId >= docBase;
            result[d++] = docs[i].docId - docBase;
        }
        return result;
    }

    static class DocIdToIndex implements Comparable<DocIdToIndex> {
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
