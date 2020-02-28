/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.search.internal;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FilterDirectoryReader;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.BulkScorer;
import org.apache.lucene.search.CollectionStatistics;
import org.apache.lucene.search.CollectionTerminatedException;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.ConjunctionDISI;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryCache;
import org.apache.lucene.search.QueryCachingPolicy;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.TermStatistics;
import org.apache.lucene.search.TopFieldDocs;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.search.suggest.document.CompletionTerms;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.BitSetIterator;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CombinedBitSet;
import org.apache.lucene.util.SparseFixedBitSet;
import org.apache.lucene.util.automaton.CompiledAutomaton;
import org.elasticsearch.common.lucene.search.TopDocsAndMaxScore;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.dfs.AggregatedDfs;
import org.elasticsearch.search.profile.Timer;
import org.elasticsearch.search.profile.query.ProfileWeight;
import org.elasticsearch.search.profile.query.QueryProfileBreakdown;
import org.elasticsearch.search.profile.query.QueryProfiler;
import org.elasticsearch.search.profile.query.QueryTimingType;
import org.elasticsearch.search.query.QuerySearchResult;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

/**
 * Context-aware extension of {@link IndexSearcher}.
 */
public class ContextIndexSearcher extends IndexSearcher {
    /**
     * The interval at which we check for search cancellation when we cannot use
     * a {@link CancellableBulkScorer}. See {@link #intersectScorerAndBitSet}.
     */
    private static int CHECK_CANCELLED_SCORER_INTERVAL = 1 << 11;

    private AggregatedDfs aggregatedDfs;
    private QueryProfiler profiler;
    private Holder<Cancellable> cancellable;

    public ContextIndexSearcher(IndexReader reader, Similarity similarity,
                                QueryCache queryCache, QueryCachingPolicy queryCachingPolicy) throws IOException {
        this(reader, similarity, queryCache, queryCachingPolicy, new Holder<>(null));
    }

    // TODO: Make the 2nd constructor private so that the dirCancellable is never null and the IndexReader is always wrapped.
    // Some issues must be fixed regarding tests deriving from AggregatorTestCase and more specifically
    // the use of searchAndReduce and the ShardSearcher sub-searchers.
    public ContextIndexSearcher(IndexReader reader, Similarity similarity,
                                QueryCache queryCache, QueryCachingPolicy queryCachingPolicy,
                                Holder<Cancellable> cancellable) throws IOException {
        super(cancellable!= null? new CancellableDirectoryReader((DirectoryReader) reader, cancellable) : reader);
        this.cancellable = cancellable;
        setSimilarity(similarity);
        setQueryCache(queryCache);
        setQueryCachingPolicy(queryCachingPolicy);
    }

    public void setProfiler(QueryProfiler profiler) {
        this.profiler = profiler;
    }

    /**
     * Set a {@link Runnable} that will be run on a regular basis while
     * collecting documents and check for query cancellation or timeout
     */
    public void setCancellable(Cancellable cancellable) {
        this.cancellable.set(cancellable);
    }

    public void unsetCheckTimeout() {
        this.cancellable.get().unsetCheckTimeout();
    }

    public void setAggregatedDfs(AggregatedDfs aggregatedDfs) {
        this.aggregatedDfs = aggregatedDfs;
    }

    @Override
    public Query rewrite(Query original) throws IOException {
        if (profiler != null) {
            profiler.startRewriteTime();
        }

        try {
            return super.rewrite(original);
        } finally {
            if (profiler != null) {
                profiler.stopAndAddRewriteTime();
            }
        }
    }

    @Override
    public Weight createWeight(Query query, ScoreMode scoreMode, float boost) throws IOException {
        if (profiler != null) {
            // createWeight() is called for each query in the tree, so we tell the queryProfiler
            // each invocation so that it can build an internal representation of the query
            // tree
            QueryProfileBreakdown profile = profiler.getQueryBreakdown(query);
            Timer timer = profile.getTimer(QueryTimingType.CREATE_WEIGHT);
            timer.start();
            final Weight weight;
            try {
                weight = query.createWeight(this, scoreMode, boost);
            } finally {
                timer.stop();
                profiler.pollLastElement();
            }
            return new ProfileWeight(query, weight, profile);
        } else {
            return super.createWeight(query, scoreMode, boost);
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    public void search(List<LeafReaderContext> leaves, Weight weight, CollectorManager manager,
            QuerySearchResult result, DocValueFormat[] formats, TotalHits totalHits) throws IOException {
        final List<Collector> collectors = new ArrayList<>(leaves.size());
        for (LeafReaderContext ctx : leaves) {
            final Collector collector = manager.newCollector();
            searchLeaf(ctx, weight, collector);
            collectors.add(collector);
        }
        TopFieldDocs mergedTopDocs = (TopFieldDocs) manager.reduce(collectors);
        // Lucene sets shards indexes during merging of topDocs from different collectors
        // We need to reset shard index; ES will set shard index later during reduce stage
        for (ScoreDoc scoreDoc : mergedTopDocs.scoreDocs) {
            scoreDoc.shardIndex = -1;
        }
        if (totalHits != null) { // we have already precalculated totalHits for the whole index
            mergedTopDocs = new TopFieldDocs(totalHits, mergedTopDocs.scoreDocs, mergedTopDocs.fields);
        }
        result.topDocs(new TopDocsAndMaxScore(mergedTopDocs, Float.NaN), formats);
    }

    @Override
    protected void search(List<LeafReaderContext> leaves, Weight weight, Collector collector) throws IOException {
        for (LeafReaderContext ctx : leaves) { // search each subreader
            searchLeaf(ctx, weight, collector);
        }
    }

    /**
     * Lower-level search API.
     *
     * {@link LeafCollector#collect(int)} is called for every matching document in
     * the provided <code>ctx</code>.
     */
    private void searchLeaf(LeafReaderContext ctx, Weight weight, Collector collector) throws IOException {
        cancellable.get().checkCancelled();
        weight = wrapWeight(weight);
        final LeafCollector leafCollector;
        try {
            leafCollector = collector.getLeafCollector(ctx);
        } catch (CollectionTerminatedException e) {
            // there is no doc of interest in this reader context
            // continue with the following leaf
            return;
        }
        Bits liveDocs = ctx.reader().getLiveDocs();
        BitSet liveDocsBitSet = getSparseBitSetOrNull(liveDocs);
        if (liveDocsBitSet == null) {
            BulkScorer bulkScorer = weight.bulkScorer(ctx);
            if (bulkScorer != null) {
                try {
                    bulkScorer.score(leafCollector, liveDocs);
                } catch (CollectionTerminatedException e) {
                    // collection was terminated prematurely
                    // continue with the following leaf
                }
            }
        } else {
            // if the role query result set is sparse then we should use the SparseFixedBitSet for advancing:
            Scorer scorer = weight.scorer(ctx);
            if (scorer != null) {
                try {
                    intersectScorerAndBitSet(scorer, liveDocsBitSet, leafCollector, () -> cancellable.get().checkCancelled());
                } catch (CollectionTerminatedException e) {
                    // collection was terminated prematurely
                    // continue with the following leaf
                }
            }
        }
    }

    private Weight wrapWeight(Weight weight) {
        if (cancellable.get().isEnabled()) {
            return new Weight(weight.getQuery()) {
                @Override
                public void extractTerms(Set<Term> terms) {
                    throw new UnsupportedOperationException();
                }

                @Override
                public Explanation explain(LeafReaderContext context, int doc) throws IOException {
                    throw new UnsupportedOperationException();
                }

                @Override
                public boolean isCacheable(LeafReaderContext ctx) {
                    throw new UnsupportedOperationException();
                }

                @Override
                public Scorer scorer(LeafReaderContext context) throws IOException {
                    return weight.scorer(context);
                }

                @Override
                public BulkScorer bulkScorer(LeafReaderContext context) throws IOException {
                    BulkScorer in = weight.bulkScorer(context);
                    if (in != null) {
                        return new CancellableBulkScorer(in, () -> cancellable.get().checkCancelled());
                    } else {
                        return null;
                    }
                }
            };
        } else {
            return weight;
        }
    }


    private static BitSet getSparseBitSetOrNull(Bits liveDocs) {
        if (liveDocs instanceof SparseFixedBitSet) {
            return (BitSet) liveDocs;
        } else if (liveDocs instanceof CombinedBitSet
                        // if the underlying role bitset is sparse
                        && ((CombinedBitSet) liveDocs).getFirst() instanceof SparseFixedBitSet) {
            return (BitSet) liveDocs;
        } else {
            return null;
        }

    }

    static void intersectScorerAndBitSet(Scorer scorer, BitSet acceptDocs,
                                         LeafCollector collector, Runnable checkCancelled) throws IOException {
        collector.setScorer(scorer);
        // ConjunctionDISI uses the DocIdSetIterator#cost() to order the iterators, so if roleBits has the lowest cardinality it should
        // be used first:
        DocIdSetIterator iterator = ConjunctionDISI.intersectIterators(Arrays.asList(new BitSetIterator(acceptDocs,
            acceptDocs.approximateCardinality()), scorer.iterator()));
        int seen = 0;
        checkCancelled.run();
        for (int docId = iterator.nextDoc(); docId < DocIdSetIterator.NO_MORE_DOCS; docId = iterator.nextDoc()) {
            if (++seen % CHECK_CANCELLED_SCORER_INTERVAL == 0) {
                checkCancelled.run();
            }
            collector.collect(docId);
        }
        checkCancelled.run();
    }

    @Override
    public TermStatistics termStatistics(Term term, int docFreq, long totalTermFreq) throws IOException {
        if (aggregatedDfs == null) {
            // we are either executing the dfs phase or the search_type doesn't include the dfs phase.
            return super.termStatistics(term, docFreq, totalTermFreq);
        }
        TermStatistics termStatistics = aggregatedDfs.termStatistics().get(term);
        if (termStatistics == null) {
            // we don't have stats for this - this might be a must_not clauses etc. that doesn't allow extract terms on the query
            return super.termStatistics(term, docFreq, totalTermFreq);
        }
        return termStatistics;
    }

    @Override
    public CollectionStatistics collectionStatistics(String field) throws IOException {
        if (aggregatedDfs == null) {
            // we are either executing the dfs phase or the search_type doesn't include the dfs phase.
            return super.collectionStatistics(field);
        }
        CollectionStatistics collectionStatistics = aggregatedDfs.fieldStatistics().get(field);
        if (collectionStatistics == null) {
            // we don't have stats for this - this might be a must_not clauses etc. that doesn't allow extract terms on the query
            return super.collectionStatistics(field);
        }
        return collectionStatistics;
    }

    public DirectoryReader getDirectoryReader() {
        final IndexReader reader = getIndexReader();
        assert reader instanceof DirectoryReader : "expected an instance of DirectoryReader, got " + reader.getClass();
        return (DirectoryReader) reader;
    }

    /**
     * Wraps an {@link IndexReader} with a cancellation Runnable task.
     */
    private static class CancellableDirectoryReader extends FilterDirectoryReader {

        private CancellableDirectoryReader(DirectoryReader in, Holder<Cancellable> cancellable) throws IOException {
            super(in, new SubReaderWrapper() {
                @Override
                public LeafReader wrap(LeafReader reader) {
                    return new CancellableLeafReader(reader, cancellable);
                }
            });
        }

        @Override
        protected DirectoryReader doWrapDirectoryReader(DirectoryReader in) {
            return in;
        }

        @Override
        public CacheHelper getReaderCacheHelper() {
            return in.getReaderCacheHelper();
        }
    }

    /**
     * Wraps a leaf reader with a cancellable task
     */
    private static class CancellableLeafReader extends FilterLeafReader {

        private final Holder<Cancellable> cancellable;

        private CancellableLeafReader(LeafReader leafReader, Holder<Cancellable> cancellable)  {
            super(leafReader);
            this.cancellable = cancellable;
        }

        @Override
        public PointValues getPointValues(String field) throws IOException {
            final PointValues pointValues = in.getPointValues(field);
            if (pointValues == null) {
                return null;
            }
            return (cancellable.get().isEnabled()) ? new ExitablePointValues(pointValues, cancellable.get()) : pointValues;
        }

        @Override
        public Terms terms(String field) throws IOException {
            Terms terms = in.terms(field);
            if (terms == null) {
                return null;
            }
            return (cancellable.get().isEnabled() && terms instanceof CompletionTerms == false) ?
                    new ExitableTerms(terms, cancellable.get()) : terms;
        }

        @Override
        public CacheHelper getCoreCacheHelper() {
            return in.getCoreCacheHelper();
        }

        @Override
        public CacheHelper getReaderCacheHelper() {
            return in.getReaderCacheHelper();
        }
    }

    private static class Holder<T> {
        private T in;

        public Holder(T in) {
            this.in = in;
        }

        private void set(T in) {
            this.in = in;
        }

        private T get() {
            return in;
        }
    }

    public interface Cancellable {

        boolean isEnabled();
        void checkCancelled();
        default void checkDirReaderCancelled() {
            checkCancelled();
        }
        void unsetCheckTimeout();
    }

    public static class CancellableImpl implements Cancellable {

        private Runnable checkCancelled;
        private Runnable checkTimeout;

        public CancellableImpl(Runnable checkTimeout, Runnable checkCancelled) {
            this.checkCancelled = checkCancelled;
            this.checkTimeout = checkTimeout;
        }

        @Override
        public boolean isEnabled() {
            return checkCancelled != null || checkTimeout != null;
        }

        @Override
        public void checkCancelled() {
            if (checkTimeout != null) {
                checkTimeout.run();
            }
            if (checkCancelled != null) {
                checkCancelled.run();;
            }
        }

        @Override
        public void unsetCheckTimeout() {
            this.checkTimeout = null;
        }
    }

    /**
     * Wrapper class for terms that check for query cancellation or timeout.
     */
    public static class ExitableTerms extends FilterLeafReader.FilterTerms {

        private final Cancellable cancellable;

        /** Constructor **/
        public ExitableTerms(Terms terms, Cancellable cancellable) {
            super(terms);
            this.cancellable = cancellable;
        }

        @Override
        public TermsEnum intersect(CompiledAutomaton compiled, BytesRef startTerm) throws IOException {
            return new ExitableTermsEnum(in.intersect(compiled, startTerm), cancellable);
        }

        @Override
        public TermsEnum iterator() throws IOException {
            return new ExitableTermsEnum(in.iterator(), cancellable);
        }
    }

    /**
     * Wrapper class for TermsEnum that is used by ExitableTerms for implementing an
     * exitable enumeration of terms.
     */
    private static class ExitableTermsEnum extends FilterLeafReader.FilterTermsEnum {

        private final Cancellable cancellable;

        /** Constructor **/
        private ExitableTermsEnum(TermsEnum termsEnum, Cancellable cancellable) {
            super(termsEnum);
            this.cancellable = cancellable;
            this.cancellable.checkDirReaderCancelled();
        }

        @Override
        public BytesRef next() throws IOException {
            // Before every iteration, check if the iteration should exit
            this.cancellable.checkDirReaderCancelled();
            return in.next();
        }
    }

    /**
     * Wrapper class for PointValues that checks for cancellation and timeout.
     */
    private static class ExitablePointValues extends PointValues {

        private final PointValues in;
        private final Cancellable cancellable;

        private ExitablePointValues(PointValues in, Cancellable cancellable) {
            this.in = in;
            this.cancellable = cancellable;
            this.cancellable.checkDirReaderCancelled();
        }

        @Override
        public void intersect(IntersectVisitor visitor) throws IOException {
            cancellable.checkDirReaderCancelled();
            in.intersect(new ExitableIntersectVisitor(visitor, cancellable));
        }

        @Override
        public long estimatePointCount(IntersectVisitor visitor) {
            cancellable.checkDirReaderCancelled();
            return in.estimatePointCount(visitor);
        }

        @Override
        public byte[] getMinPackedValue() throws IOException {
            cancellable.checkDirReaderCancelled();
            return in.getMinPackedValue();
        }

        @Override
        public byte[] getMaxPackedValue() throws IOException {
            cancellable.checkDirReaderCancelled();
            return in.getMaxPackedValue();
        }

        @Override
        public int getNumDimensions() throws IOException {
            cancellable.checkDirReaderCancelled();
            return in.getNumDimensions();
        }

        @Override
        public int getNumIndexDimensions() throws IOException {
            cancellable.checkDirReaderCancelled();
            return in.getNumIndexDimensions();
        }

        @Override
        public int getBytesPerDimension() throws IOException {
            cancellable.checkDirReaderCancelled();
            return in.getBytesPerDimension();
        }

        @Override
        public long size() {
            cancellable.checkDirReaderCancelled();
            return in.size();
        }

        @Override
        public int getDocCount() {
            cancellable.checkDirReaderCancelled();
            return in.getDocCount();
        }
    }

    private static class ExitableIntersectVisitor implements PointValues.IntersectVisitor {

        private static final int MAX_CALLS_BEFORE_QUERY_TIMEOUT_CHECK = 10;

        private final PointValues.IntersectVisitor in;
        private final Cancellable cancellable;
        private int calls;

        private ExitableIntersectVisitor(PointValues.IntersectVisitor in, Cancellable cancellable) {
            this.in = in;
            this.cancellable = cancellable;
        }

        private void checkAndThrowWithSampling() {
            if (calls++ % MAX_CALLS_BEFORE_QUERY_TIMEOUT_CHECK == 0) {
                cancellable.checkDirReaderCancelled();
            }
        }

        @Override
        public void visit(int docID) throws IOException {
            checkAndThrowWithSampling();
            in.visit(docID);
        }

        @Override
        public void visit(int docID, byte[] packedValue) throws IOException {
            checkAndThrowWithSampling();
            in.visit(docID, packedValue);
        }

        @Override
        public PointValues.Relation compare(byte[] minPackedValue, byte[] maxPackedValue) {
            cancellable.checkDirReaderCancelled();
            return in.compare(minPackedValue, maxPackedValue);
        }

        @Override
        public void grow(int count) {
            cancellable.checkDirReaderCancelled();
            in.grow(count);
        }
    }
}
