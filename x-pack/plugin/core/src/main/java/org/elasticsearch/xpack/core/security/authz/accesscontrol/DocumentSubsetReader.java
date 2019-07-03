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
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.BitSetIterator;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.cache.Cache;
import org.elasticsearch.common.cache.CacheBuilder;
import org.elasticsearch.common.logging.LoggerMessageFormat;
import org.elasticsearch.index.cache.bitset.BitsetFilterCache;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;

/**
 * A reader that only exposes documents via {@link #getLiveDocs()} that matches with the provided role query.
 */
public final class DocumentSubsetReader extends FilterLeafReader {

    public static DocumentSubsetDirectoryReader wrap(DirectoryReader in, BitsetFilterCache bitsetFilterCache,
            Query roleQuery, boolean strictTermsEnum) throws IOException {
        return new DocumentSubsetDirectoryReader(in, bitsetFilterCache, roleQuery, strictTermsEnum);
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
        private final BitsetFilterCache bitsetFilterCache;
        private boolean strictTermsEnum;

        DocumentSubsetDirectoryReader(final DirectoryReader in, final BitsetFilterCache bitsetFilterCache, final Query roleQuery,
                                      boolean strictTermsEnum)
                throws IOException {
            super(in, new SubReaderWrapper() {
                @Override
                public LeafReader wrap(LeafReader reader) {
                    try {
                        return new DocumentSubsetReader(reader, bitsetFilterCache, roleQuery, strictTermsEnum);
                    } catch (Exception e) {
                        throw ExceptionsHelper.convertToElastic(e);
                    }
                }
            });
            this.bitsetFilterCache = bitsetFilterCache;
            this.roleQuery = roleQuery;
            this.strictTermsEnum = strictTermsEnum;

            verifyNoOtherDocumentSubsetDirectoryReaderIsWrapped(in);
        }

        @Override
        protected DirectoryReader doWrapDirectoryReader(DirectoryReader in) throws IOException {
            return new DocumentSubsetDirectoryReader(in, bitsetFilterCache, roleQuery, strictTermsEnum);
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
    private final boolean strictTermsEnum;

    private DocumentSubsetReader(final LeafReader in, BitsetFilterCache bitsetFilterCache, final Query roleQuery,
                                 final boolean strictTermsEnum ) throws Exception {
        super(in);
        this.roleQueryBits = bitsetFilterCache.getBitSetProducer(roleQuery).getBitSet(in.getContext());
        this.numDocs = getNumDocs(in, roleQuery, roleQueryBits);
        this.strictTermsEnum = strictTermsEnum;
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
            return new Bits() {

                @Override
                public boolean get(int index) {
                    return roleQueryBits.get(index) && actualLiveDocs.get(index);
                }

                @Override
                public int length() {
                    return roleQueryBits.length();
                }
            };
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

    BitSet getRoleQueryBits() {
        return roleQueryBits;
    }

    Bits getWrappedLiveDocs() {
        return in.getLiveDocs();
    }

    @Override
    public Terms terms(String field) throws IOException {
        Terms t = super.terms(field);
        if (strictTermsEnum == false || null == t) {
            return t;
        } else{
            return new DocumentSafeTerms(t);
        }
    }

    @Override
    public SortedDocValues getSortedDocValues(String field) throws IOException {
        final SortedDocValues sortedDocValues = in.getSortedDocValues(field);
        if (strictTermsEnum == false) {
            return sortedDocValues;
        } else {
            final Bits liveDocs = getLiveDocs();
            final TermsEnum invertedIndexTermsEnum = getInvertedIndexTermsEnum(field);
            return new DocumentSafeSortedDocValues(sortedDocValues, invertedIndexTermsEnum, liveDocs);
        }
    }

    @Override
    public SortedSetDocValues getSortedSetDocValues(String field) throws IOException {
        final SortedSetDocValues sortedSetDocValues = in.getSortedSetDocValues(field);
        if (strictTermsEnum == false) {
            return sortedSetDocValues;
        } else {
            final Bits liveDocs = getLiveDocs();
            final TermsEnum invertedIndexTermsEnum = getInvertedIndexTermsEnum(field);
            return new DocumentSafeSortedSetDocValues(sortedSetDocValues, invertedIndexTermsEnum, liveDocs);
        }
    }

    private TermsEnum getInvertedIndexTermsEnum(String field) throws IOException{
        final Terms terms = in.terms(field);
        final TermsEnum invertedIndexTermsEnum;
        if (terms == null) {
            invertedIndexTermsEnum = null;
        } else {
            invertedIndexTermsEnum = terms.iterator();
        }
        return invertedIndexTermsEnum;
    }

    static class DocumentSafeTerms extends FilterTerms {

        DocumentSafeTerms(Terms in) {
            super(in);
        }

        @Override
        public TermsEnum iterator() throws IOException {
            return new DocumentSafeTermsEnum(in.iterator());
        }
    }

    static class DocumentSafeTermsEnum extends FilterTermsEnum {

        DocumentSafeTermsEnum(TermsEnum in) {
            super(in);
        }

        @Override
        public SeekStatus seekCeil(BytesRef term) {
            throw new UnsupportedOperationException("This query type is disallowed when " +
                IndexMetaData.INDEX_PRIORITY_SETTING.getKey() +
                " is set to true, as it can inadvertently leak terms that DLS would not permit.");
        }

        @Override
        public BytesRef next() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void seekExact(long ord) throws IOException {
            throw new UnsupportedOperationException("This query type is disallowed when " +
                IndexMetaData.INDEX_PRIORITY_SETTING.getKey() +
                " is set to true, as it can inadvertently leak terms that DLS would not permit.");
        }
    }

    /**
     * A {@link TermsEnum} for doc-values that cross-checks lookups by ord with
     * the terms dictionary of the inverted index to prevent lookup by ord on
     * terms that are only contained by deleted documents.
     */
    static class DocumentSafeDocValuesTermsEnum extends DocumentSafeTermsEnum {

        private final TermsEnum invertedIndexTermsEnum;
        private final Bits liveDocs;
        private PostingsEnum postings;

        DocumentSafeDocValuesTermsEnum(TermsEnum in, TermsEnum invertedIndexTermsEnum, Bits liveDocs) {
            super(in);
            this.liveDocs = liveDocs;
            this.invertedIndexTermsEnum = Objects.requireNonNull(invertedIndexTermsEnum);
        }

        @Override
        public void seekExact(long ord) throws IOException {
            in.seekExact(ord);
            // invertedIndexTermsEnum usually doesn't support lookup by ord
            // so we are looking up by term
            BytesRef term = in.term();
            boolean termIsLive = false;
            if (invertedIndexTermsEnum.seekExact(term)) {
                postings = invertedIndexTermsEnum.postings(postings, PostingsEnum.NONE);
                for (int doc = postings.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = postings.nextDoc()) {
                    if (liveDocs == null || liveDocs.get(doc)) {
                        termIsLive = true;
                        break;
                    }
                }
            }
            if (termIsLive == false) {
                throw new UnsupportedOperationException("Lookup by ord on random ords is disallowed");
            }
        }

    }

    static class DocumentSafeSortedDocValues extends SortedDocValues {

        private final SortedDocValues in;
        private final TermsEnum invertedIndexTermsEnum;
        private final Bits liveDocs;
        private final TermsEnum termsEnum;

        DocumentSafeSortedDocValues(SortedDocValues in, TermsEnum invertedIndexTermsEnum, Bits liveDocs) throws IOException {
            this.in = in;
            this.invertedIndexTermsEnum = invertedIndexTermsEnum;
            this.liveDocs = liveDocs;
            this.termsEnum = termsEnum();
        }

        @Override
        public int ordValue() throws IOException {
            return in.ordValue();
        }

        @Override
        public BytesRef lookupOrd(int ord) throws IOException {
            termsEnum.seekExact(ord);
            return termsEnum.term();
        }

        @Override
        public int getValueCount() {
            return in.getValueCount();
        }

        @Override
        public boolean advanceExact(int target) throws IOException {
            return in.advanceExact(target);
        }

        @Override
        public int docID() {
            return in.docID();
        }

        @Override
        public int nextDoc() throws IOException {
            return in.nextDoc();
        }

        @Override
        public int advance(int target) throws IOException {
            return in.advance(target);
        }

        @Override
        public long cost() {
            return in.cost();
        }

        @Override
        public int lookupTerm(BytesRef key) throws IOException {
            return in.lookupTerm(key);
        }

        @Override
        public TermsEnum termsEnum() throws IOException {
            // this needs to be a fresh new TermsEnum
            if (invertedIndexTermsEnum == null) {
                // we can't cross check
                return new DocumentSafeTermsEnum(in.termsEnum());
            } else {
                return new DocumentSafeDocValuesTermsEnum(in.termsEnum(), invertedIndexTermsEnum, liveDocs);
            }
        }
    }

    static class DocumentSafeSortedSetDocValues extends SortedSetDocValues {

        private final SortedSetDocValues in;
        private final TermsEnum invertedIndexTermsEnum;
        private final Bits liveDocs;
        private final TermsEnum termsEnum;

        DocumentSafeSortedSetDocValues(SortedSetDocValues in, TermsEnum invertedIndexTermsEnum, Bits liveDocs) throws IOException {
            this.in = in;
            this.invertedIndexTermsEnum = invertedIndexTermsEnum;
            this.liveDocs = liveDocs;
            this.termsEnum = termsEnum();
        }

        @Override
        public long nextOrd() throws IOException {
            return in.nextOrd();
        }

        @Override
        public BytesRef lookupOrd(long ord) throws IOException {
            termsEnum.seekExact(ord);
            return termsEnum.term();
        }

        @Override
        public long getValueCount() {
            return in.getValueCount();
        }

        @Override
        public boolean advanceExact(int target) throws IOException {
            return in.advanceExact(target);
        }

        @Override
        public int docID() {
            return in.docID();
        }

        @Override
        public int nextDoc() throws IOException {
            return in.nextDoc();
        }

        @Override
        public int advance(int target) throws IOException {
            return in.advance(target);
        }

        @Override
        public long cost() {
            return in.cost();
        }

        @Override
        public long lookupTerm(BytesRef key) throws IOException {
            return in.lookupTerm(key);
        }

        @Override
        public TermsEnum termsEnum() throws IOException {
            // this needs to be a fresh new TermsEnum
            if (invertedIndexTermsEnum == null) {
                // we can't cross check
                return new DocumentSafeTermsEnum(in.termsEnum());
            } else {
                return new DocumentSafeDocValuesTermsEnum(in.termsEnum(), invertedIndexTermsEnum, liveDocs);
            }
        }

    }
}
