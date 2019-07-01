/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.security.authz.accesscontrol;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FilterDirectoryReader;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.BitSetIterator;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.CombinedBitSet;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.cache.Cache;
import org.elasticsearch.common.cache.CacheBuilder;
import org.elasticsearch.common.logging.LoggerMessageFormat;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;

/**
 * A reader that only exposes documents via {@link #getLiveDocs()} that matches with the provided role query.
 */
public final class DocumentSubsetReader extends FilterLeafReader {

    public static DocumentSubsetDirectoryReader wrap(DirectoryReader in, DocumentSubsetBitsetCache bitsetCache,
            Query roleQuery) throws IOException {
        return new DocumentSubsetDirectoryReader(in, bitsetCache, roleQuery);
    }

    /**
     * Cache of the number of live docs for a given (segment, role query) pair.
     * This is useful because numDocs() is called eagerly by BaseCompositeReader so computing
     * numDocs() lazily doesn't help. Plus it helps reuse the result of the computation either
     * between refreshes, or across refreshes if no more documents were deleted in the
     * considered segment. The size of the top-level map is bounded by the number of segments
     * on the node.
     */
    static final Map<IndexReader.CacheKey, Cache<Query, Integer>> NUM_DOCS_CACHE = new ConcurrentHashMap<>();

    /**
     * Compute the number of live documents. This method is SLOW.
     */
    private static int computeNumDocs(LeafReader reader, Query roleQuery, BitSet roleQueryBits) {
        final Bits liveDocs = reader.getLiveDocs();
        if (roleQueryBits == null) {
            return 0;
        } else if (liveDocs == null) {
            // slow
            return roleQueryBits.cardinality();
        } else {
            // very slow, but necessary in order to be correct
            int numDocs = 0;
            DocIdSetIterator it = new BitSetIterator(roleQueryBits, 0L); // we don't use the cost
            try {
                for (int doc = it.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = it.nextDoc()) {
                    if (liveDocs.get(doc)) {
                        numDocs++;
                    }
                }
                return numDocs;
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    /**
     * Like {@link #computeNumDocs} but caches results.
     */
    private static int getNumDocs(LeafReader reader, Query roleQuery, BitSet roleQueryBits) throws IOException, ExecutionException {
        IndexReader.CacheHelper cacheHelper = reader.getReaderCacheHelper(); // this one takes deletes into account
        if (cacheHelper == null) {
            throw new IllegalStateException("Reader " + reader + " does not support caching");
        }
        final boolean[] added = new boolean[] { false };
        Cache<Query, Integer> perReaderCache = NUM_DOCS_CACHE.computeIfAbsent(cacheHelper.getKey(),
                key -> {
                    added[0] = true;
                    return CacheBuilder.<Query, Integer>builder()
                            // Not configurable, this limit only exists so that if a role query is updated
                            // then we won't risk OOME because of old role queries that are not used anymore
                            .setMaximumWeight(1000)
                            .weigher((k, v) -> 1) // just count
                            .build();
                });
        if (added[0]) {
            IndexReader.ClosedListener closedListener = NUM_DOCS_CACHE::remove;
            try {
                cacheHelper.addClosedListener(closedListener);
            } catch (AlreadyClosedException e) {
                closedListener.onClose(cacheHelper.getKey());
                throw e;
            }
        }
        return perReaderCache.computeIfAbsent(roleQuery, q -> computeNumDocs(reader, roleQuery, roleQueryBits));
    }

    public static final class DocumentSubsetDirectoryReader extends FilterDirectoryReader {

        private final Query roleQuery;
        private final DocumentSubsetBitsetCache bitsetCache;

        DocumentSubsetDirectoryReader(final DirectoryReader in, final DocumentSubsetBitsetCache bitsetCache,
                                      final Query roleQuery) throws IOException {
            super(in, new SubReaderWrapper() {
                @Override
                public LeafReader wrap(LeafReader reader) {
                    try {
                        return new DocumentSubsetReader(reader, bitsetCache, roleQuery);
                    } catch (Exception e) {
                        throw ExceptionsHelper.convertToElastic(e);
                    }
                }
            });
            this.bitsetCache = bitsetCache;
            this.roleQuery = roleQuery;

            verifyNoOtherDocumentSubsetDirectoryReaderIsWrapped(in);
        }

        @Override
        protected DirectoryReader doWrapDirectoryReader(DirectoryReader in) throws IOException {
            return new DocumentSubsetDirectoryReader(in, bitsetCache, roleQuery);
        }

        private static void verifyNoOtherDocumentSubsetDirectoryReaderIsWrapped(DirectoryReader reader) {
            if (reader instanceof FilterDirectoryReader) {
                FilterDirectoryReader filterDirectoryReader = (FilterDirectoryReader) reader;
                if (filterDirectoryReader instanceof DocumentSubsetDirectoryReader) {
                    throw new IllegalArgumentException(LoggerMessageFormat.format("Can't wrap [{}] twice",
                            DocumentSubsetDirectoryReader.class));
                } else {
                    verifyNoOtherDocumentSubsetDirectoryReaderIsWrapped(filterDirectoryReader.getDelegate());
                }
            }
        }

        @Override
        public CacheHelper getReaderCacheHelper() {
            return in.getReaderCacheHelper();
        }
    }

    private final BitSet roleQueryBits;
    private final int numDocs;

    private DocumentSubsetReader(final LeafReader in, DocumentSubsetBitsetCache bitsetCache, final Query roleQuery) throws Exception {
        super(in);
        this.roleQueryBits = bitsetCache.getBitSet(roleQuery, in.getContext());
        this.numDocs = getNumDocs(in, roleQuery, roleQueryBits);
    }

    @Override
    public Bits getLiveDocs() {
        final Bits actualLiveDocs = in.getLiveDocs();
        if (roleQueryBits == null) {
            // If we would a <code>null</code> liveDocs then that would mean that no docs are marked as deleted,
            // but that isn't the case. No docs match with the role query and therefor all docs are marked as deleted
            return new Bits.MatchNoBits(in.maxDoc());
        } else if (actualLiveDocs == null) {
            return roleQueryBits;
        } else {
            // apply deletes when needed:
            return new CombinedBitSet(roleQueryBits, actualLiveDocs);
        }
    }

    @Override
    public int numDocs() {
        return numDocs;
    }

    @Override
    public boolean hasDeletions() {
        // we always return liveDocs and hide docs:
        return true;
    }

    @Override
    public CacheHelper getCoreCacheHelper() {
        return in.getCoreCacheHelper();
    }

    @Override
    public CacheHelper getReaderCacheHelper() {
        // Not delegated since we change the live docs
        return null;
    }
}
