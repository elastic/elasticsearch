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
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermContext;
import org.apache.lucene.search.BulkScorer;
import org.apache.lucene.search.CollectionStatistics;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryCache;
import org.apache.lucene.search.QueryCachingPolicy;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.TermStatistics;
import org.apache.lucene.search.Weight;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.search.dfs.AggregatedDfs;
import org.elasticsearch.search.profile.Timer;
import org.elasticsearch.search.profile.query.ProfileWeight;
import org.elasticsearch.search.profile.query.QueryProfileBreakdown;
import org.elasticsearch.search.profile.query.QueryProfiler;
import org.elasticsearch.search.profile.query.QueryTimingType;

import java.io.IOException;
import java.util.List;
import java.util.Set;

/**
 * Context-aware extension of {@link IndexSearcher}.
 */
public class ContextIndexSearcher extends IndexSearcher implements Releasable {

    /** The wrapped {@link IndexSearcher}. The reason why we sometimes prefer delegating to this searcher instead of <tt>super</tt> is that
     *  this instance may have more assertions, for example if it comes from MockInternalEngine which wraps the IndexSearcher into an
     *  AssertingIndexSearcher. */
    private final IndexSearcher in;

    private AggregatedDfs aggregatedDfs;

    private final Engine.Searcher engineSearcher;

    // TODO revisit moving the profiler to inheritance or wrapping model in the future
    private QueryProfiler profiler;

    private Runnable checkCancelled;

    public ContextIndexSearcher(Engine.Searcher searcher,
            QueryCache queryCache, QueryCachingPolicy queryCachingPolicy) {
        super(searcher.reader());
        in = searcher.searcher();
        engineSearcher = searcher;
        setSimilarity(searcher.searcher().getSimilarity(true));
        setQueryCache(queryCache);
        setQueryCachingPolicy(queryCachingPolicy);
    }

    @Override
    public void close() {
    }

    public void setProfiler(QueryProfiler profiler) {
        this.profiler = profiler;
    }

    /**
     * Set a {@link Runnable} that will be run on a regular basis while
     * collecting documents.
     */
    public void setCheckCancelled(Runnable checkCancelled) {
        this.checkCancelled = checkCancelled;
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
            return in.rewrite(original);
        } finally {
            if (profiler != null) {
                profiler.stopAndAddRewriteTime();
            }
        }
    }

    @Override
    public Weight createNormalizedWeight(Query query, boolean needsScores) throws IOException {
        // During tests we prefer to use the wrapped IndexSearcher, because then we use the AssertingIndexSearcher
        // it is hacky, because if we perform a dfs search, we don't use the wrapped IndexSearcher...
        if (aggregatedDfs != null && needsScores) {
            // if scores are needed and we have dfs data then use it
            return super.createNormalizedWeight(query, needsScores);
        } else if (profiler != null) {
            // we need to use the createWeight method to insert the wrappers
            return super.createNormalizedWeight(query, needsScores);
        } else {
            return in.createNormalizedWeight(query, needsScores);
        }
    }

    @Override
    public Weight createWeight(Query query, boolean needsScores, float boost) throws IOException {
        if (profiler != null) {
            // createWeight() is called for each query in the tree, so we tell the queryProfiler
            // each invocation so that it can build an internal representation of the query
            // tree
            QueryProfileBreakdown profile = profiler.getQueryBreakdown(query);
            Timer timer = profile.getTimer(QueryTimingType.CREATE_WEIGHT);
            timer.start();
            final Weight weight;
            try {
                weight = super.createWeight(query, needsScores, boost);
            } finally {
                timer.stop();
                profiler.pollLastElement();
            }
            return new ProfileWeight(query, weight, profile);
        } else {
            // needs to be 'super', not 'in' in order to use aggregated DFS
            return super.createWeight(query, needsScores, boost);
        }
    }

    @Override
    protected void search(List<LeafReaderContext> leaves, Weight weight, Collector collector) throws IOException {
        final Weight cancellableWeight;
        if (checkCancelled != null) {
            cancellableWeight = new Weight(weight.getQuery()) {

                @Override
                public void extractTerms(Set<Term> terms) {
                    throw new UnsupportedOperationException();
                }

                @Override
                public Explanation explain(LeafReaderContext context, int doc) throws IOException {
                    throw new UnsupportedOperationException();
                }

                @Override
                public Scorer scorer(LeafReaderContext context) throws IOException {
                    throw new UnsupportedOperationException();
                }

                @Override
                public BulkScorer bulkScorer(LeafReaderContext context) throws IOException {
                    BulkScorer in = weight.bulkScorer(context);
                    if (in != null) {
                        return new CancellableBulkScorer(in, checkCancelled);
                    } else {
                        return null;
                    }
                }
            };
        } else {
            cancellableWeight = weight;
        }
        super.search(leaves, cancellableWeight, collector);
    }

    @Override
    public Explanation explain(Query query, int doc) throws IOException {
        if (aggregatedDfs != null) {
            // dfs data is needed to explain the score
            return super.explain(createNormalizedWeight(query, true), doc);
        }
        return in.explain(query, doc);
    }

    @Override
    public TermStatistics termStatistics(Term term, TermContext context) throws IOException {
        if (aggregatedDfs == null) {
            // we are either executing the dfs phase or the search_type doesn't include the dfs phase.
            return super.termStatistics(term, context);
        }
        TermStatistics termStatistics = aggregatedDfs.termStatistics().get(term);
        if (termStatistics == null) {
            // we don't have stats for this - this might be a must_not clauses etc. that doesn't allow extract terms on the query
            return super.termStatistics(term, context);
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
        return engineSearcher.getDirectoryReader();
    }
}
