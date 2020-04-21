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
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
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
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.BitSetIterator;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.CombinedBitSet;
import org.apache.lucene.util.SparseFixedBitSet;
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
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
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
    private MutableQueryTimeout cancellable;

    public ContextIndexSearcher(IndexReader reader, Similarity similarity,
                                QueryCache queryCache, QueryCachingPolicy queryCachingPolicy,
                                boolean wrapWithExitableDirectoryReader) throws IOException {
        this(reader, similarity, queryCache, queryCachingPolicy, new MutableQueryTimeout(), wrapWithExitableDirectoryReader);
    }

    private ContextIndexSearcher(IndexReader reader, Similarity similarity,
                                 QueryCache queryCache, QueryCachingPolicy queryCachingPolicy,
                                 MutableQueryTimeout cancellable,
                                 boolean wrapWithExitableDirectoryReader) throws IOException {
        super(wrapWithExitableDirectoryReader ? new ExitableDirectoryReader((DirectoryReader) reader, cancellable) : reader);
        setSimilarity(similarity);
        setQueryCache(queryCache);
        setQueryCachingPolicy(queryCachingPolicy);
        this.cancellable = cancellable;
    }

    public void setProfiler(QueryProfiler profiler) {
        this.profiler = profiler;
    }

    /**
     * Add a {@link Runnable} that will be run on a regular basis while accessing documents in the
     * DirectoryReader but also while collecting them and check for query cancellation or timeout.
     */
    public Runnable addQueryCancellation(Runnable action) {
        return this.cancellable.add(action);
    }

    /**
     * Remove a {@link Runnable} that checks for query cancellation or timeout
     * which is called while accessing documents in the DirectoryReader but also while collecting them.
     */
    public void removeQueryCancellation(Runnable action) {
        this.cancellable.remove(action);
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
        cancellable.checkCancelled();
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
                    intersectScorerAndBitSet(scorer, liveDocsBitSet, leafCollector,
                            this.cancellable.isEnabled() ? cancellable::checkCancelled: () -> {});
                } catch (CollectionTerminatedException e) {
                    // collection was terminated prematurely
                    // continue with the following leaf
                }
            }
        }
    }

    private Weight wrapWeight(Weight weight) {
        if (cancellable.isEnabled()) {
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
                        return new CancellableBulkScorer(in, cancellable::checkCancelled);
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

    private static class MutableQueryTimeout implements ExitableDirectoryReader.QueryCancellation {

        private final Set<Runnable> runnables = new HashSet<>();

        private Runnable add(Runnable action) {
            Objects.requireNonNull(action, "cancellation runnable should not be null");
            if (runnables.add(action) == false) {
                throw new IllegalArgumentException("Cancellation runnable already added");
            }
            return action;
        }

        private void remove(Runnable action) {
            runnables.remove(action);
        }

        @Override
        public void checkCancelled() {
            for (Runnable timeout : runnables) {
                timeout.run();
            }
        }

        @Override
        public boolean isEnabled() {
            return runnables.isEmpty() == false;
        }
    }
}
