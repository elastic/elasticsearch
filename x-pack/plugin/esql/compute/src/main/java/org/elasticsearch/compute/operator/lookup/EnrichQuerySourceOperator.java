/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.lookup;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FilterDirectoryReader;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.DocVector;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.lucene.IndexedByShardId;
import org.elasticsearch.compute.lucene.ShardContext;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.core.Releasables;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.IntFunction;

/**
 * Lookup document IDs for the input queries.
 * This operator will emit Pages consisting of a {@link DocVector} and {@link IntBlock} of positions for each query of the input queries.
 * The position block will be used as keys to combine the extracted values by {@link MergePositionsOperator}.
 */
public final class EnrichQuerySourceOperator extends SourceOperator {
    private final BlockFactory blockFactory;
    private final LookupEnrichQueryGenerator queryList;
    private int queryPosition = -1;
    private final IndexedByShardId<? extends ShardContext> shardContexts;
    private final ShardContext shardContext;
    private final IndexReader indexReader;
    private final IndexSearcher searcher;
    private final Warnings warnings;
    private final int maxPageSize;

    // using smaller pages enables quick cancellation and reduces sorting costs
    public static final int DEFAULT_MAX_PAGE_SIZE = 256;

    public EnrichQuerySourceOperator(
        BlockFactory blockFactory,
        int maxPageSize,
        LookupEnrichQueryGenerator queryList,
        IndexedByShardId<? extends ShardContext> shardContexts,
        int shardId,
        Warnings warnings
    ) {
        this.blockFactory = blockFactory;
        this.maxPageSize = maxPageSize;
        this.queryList = queryList;
        this.shardContexts = shardContexts;
        this.shardContext = shardContexts.get(shardId);
        this.shardContext.incRef();
        try {
            this.indexReader = new CachedDirectoryReader((DirectoryReader) shardContext.searcher().getIndexReader());
            this.searcher = new IndexSearcher(this.indexReader);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        this.warnings = warnings;
    }

    static class CachedDirectoryReader extends FilterDirectoryReader {
        CachedDirectoryReader(DirectoryReader in) throws IOException {
            super(in, new FilterDirectoryReader.SubReaderWrapper() {
                @Override
                public LeafReader wrap(LeafReader reader) {
                    return new CachedLeafReader(reader);
                }
            });
        }

        @Override
        protected DirectoryReader doWrapDirectoryReader(DirectoryReader in) throws IOException {
            return new CachedDirectoryReader(in);
        }

        @Override
        public CacheHelper getReaderCacheHelper() {
            return in.getReaderCacheHelper();
        }
    }

    static class CachedLeafReader extends FilterLeafReader {
        final Map<String, NumericDocValues> docValues = new HashMap<>();
        final Map<String, Terms> termsCache = new HashMap<>();

        CachedLeafReader(LeafReader in) {
            super(in);
        }

        @Override
        public NumericDocValues getNumericDocValues(String field) throws IOException {
            NumericDocValues dv = super.getNumericDocValues(field);
            if (dv == null) {
                return null;
            }
            return new CachedNumericDocValues(docId -> docValues.compute(field, (k, curr) -> {
                if (curr == null || curr.docID() > docId) {
                    return dv;
                }
                return curr;
            }));
        }

        @Override
        public Terms terms(String field) throws IOException {
            Terms terms = termsCache.computeIfAbsent(field, k -> {
                try {
                    return super.terms(k);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            });
            if (terms == null) {
                return null;
            }
            // Return a FilterTerms that always creates a fresh TermsEnum iterator
            // We cache the Terms object itself for performance, but always create fresh TermsEnum
            // instances because TermsEnum maintains position state and reusing it causes incorrect
            // results when the same field is accessed multiple times with different conditions
            // (e.g., in OR NOT expressions like: OR NOT (other1 != "omicron" AND other1 != "nu"))
            return new FilterTerms(terms) {
                @Override
                public TermsEnum iterator() throws IOException {
                    return in.iterator();
                }
            };
        }

        @Override
        public CacheHelper getCoreCacheHelper() {
            return in.getCoreCacheHelper();
        }

        @Override
        public CacheHelper getReaderCacheHelper() {
            return in.getCoreCacheHelper();
        }
    }

    static class CachedNumericDocValues extends NumericDocValues {
        private NumericDocValues delegate = null;
        private final IntFunction<NumericDocValues> fromCache;

        CachedNumericDocValues(IntFunction<NumericDocValues> fromCache) {
            this.fromCache = fromCache;
        }

        NumericDocValues getDelegate(int docID) {
            if (delegate == null) {
                delegate = fromCache.apply(docID);
            }
            return delegate;
        }

        @Override
        public long longValue() throws IOException {
            return getDelegate(-1).longValue();
        }

        @Override
        public boolean advanceExact(int target) throws IOException {
            return getDelegate(target).advanceExact(target);
        }

        @Override
        public int advance(int target) throws IOException {
            return getDelegate(target).nextDoc();
        }

        @Override
        public int docID() {
            return getDelegate(-1).docID();
        }

        @Override
        public int nextDoc() throws IOException {
            return getDelegate(-1).nextDoc();
        }

        @Override
        public long cost() {
            return fromCache.apply(DocIdSetIterator.NO_MORE_DOCS).cost();
        }
    }

    @Override
    public void finish() {}

    @Override
    public boolean isFinished() {
        return queryPosition >= queryList.getPositionCount();
    }

    @Override
    public Page getOutput() {
        int estimatedSize = Math.min(maxPageSize, queryList.getPositionCount() - queryPosition);
        IntVector.Builder positionsBuilder = null;
        IntVector.Builder docsBuilder = null;
        IntVector.Builder segmentsBuilder = null;
        try {
            positionsBuilder = blockFactory.newIntVectorBuilder(estimatedSize);
            docsBuilder = blockFactory.newIntVectorBuilder(estimatedSize);
            if (indexReader.leaves().size() > 1) {
                segmentsBuilder = blockFactory.newIntVectorBuilder(estimatedSize);
            }
            int totalMatches = 0;
            do {
                Query query;
                try {
                    query = nextQuery();
                    if (query == null) {
                        assert isFinished();
                        break;
                    }
                    query = searcher.rewrite(new ConstantScoreQuery(query));
                } catch (Exception e) {
                    warnings.registerException(e);
                    continue;
                }
                final var weight = searcher.createWeight(query, ScoreMode.COMPLETE_NO_SCORES, 1.0f);
                if (weight == null) {
                    continue;
                }
                for (LeafReaderContext leaf : indexReader.leaves()) {
                    var scorer = weight.bulkScorer(leaf);
                    if (scorer == null) {
                        continue;
                    }
                    final DocCollector collector = new DocCollector(docsBuilder);
                    scorer.score(collector, leaf.reader().getLiveDocs(), 0, DocIdSetIterator.NO_MORE_DOCS);
                    int matches = collector.matches;

                    if (segmentsBuilder != null) {
                        for (int i = 0; i < matches; i++) {
                            segmentsBuilder.appendInt(leaf.ord);
                        }
                    }
                    for (int i = 0; i < matches; i++) {
                        positionsBuilder.appendInt(queryPosition);
                    }
                    totalMatches += matches;
                }
            } while (totalMatches < maxPageSize);

            return buildPage(totalMatches, positionsBuilder, segmentsBuilder, docsBuilder);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } finally {
            Releasables.close(docsBuilder, segmentsBuilder, positionsBuilder);
        }
    }

    Page buildPage(int positions, IntVector.Builder positionsBuilder, IntVector.Builder segmentsBuilder, IntVector.Builder docsBuilder) {
        IntVector positionsVector = null;
        IntVector shardsVector = null;
        IntVector segmentsVector = null;
        IntVector docsVector = null;
        Page page = null;
        try {
            positionsVector = positionsBuilder.build();
            shardsVector = blockFactory.newConstantIntVector(0, positions);
            if (segmentsBuilder == null) {
                segmentsVector = blockFactory.newConstantIntVector(0, positions);
            } else {
                segmentsVector = segmentsBuilder.build();
            }
            docsVector = docsBuilder.build();
            page = new Page(
                new DocVector(shardContexts, shardsVector, segmentsVector, docsVector, null).asBlock(),
                positionsVector.asBlock()
            );
        } finally {
            if (page == null) {
                Releasables.close(positionsBuilder, segmentsVector, docsBuilder, positionsVector, shardsVector, docsVector);
            }
        }
        return page;
    }

    private Query nextQuery() {
        ++queryPosition;
        while (isFinished() == false) {
            Query query = queryList.getQuery(queryPosition);
            if (query != null) {
                return query;
            }
            ++queryPosition;
        }
        return null;
    }

    private static class DocCollector implements LeafCollector {
        final IntVector.Builder docIds;
        int matches = 0;

        DocCollector(IntVector.Builder docIds) {
            this.docIds = docIds;
        }

        @Override
        public void setScorer(Scorable scorer) {

        }

        @Override
        public void collect(int doc) {
            ++matches;
            docIds.appendInt(doc);
        }
    }

    @Override
    public void close() {
        this.shardContext.decRef();
    }
}
