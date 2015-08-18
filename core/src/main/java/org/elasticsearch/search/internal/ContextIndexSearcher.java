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

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Weight;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.search.dfs.CachedDfSource;
import org.elasticsearch.search.internal.SearchContext.Lifetime;
import org.elasticsearch.common.lucene.search.ProfileQuery;
import org.elasticsearch.search.profile.InternalProfileBreakdown;

import java.io.IOException;
import java.util.*;

/**
 * Context-aware extension of {@link IndexSearcher}.
 */
public class ContextIndexSearcher extends IndexSearcher implements Releasable {

    /** The wrapped {@link IndexSearcher}. The reason why we sometimes prefer delegating to this searcher instead of <tt>super</tt> is that
     *  this instance may have more assertions, for example if it comes from MockInternalEngine which wraps the IndexSearcher into an
     *  AssertingIndexSearcher. */
    private final IndexSearcher in;

    private final SearchContext searchContext;

    private CachedDfSource dfSource;

    private boolean profile = false;

    public ContextIndexSearcher(SearchContext searchContext, Engine.Searcher searcher) {
        super(searcher.reader());
        in = searcher.searcher();
        this.searchContext = searchContext;
        setSimilarity(searcher.searcher().getSimilarity(true));
    }

    @Override
    public void close() {
    }

    public void dfSource(CachedDfSource dfSource) {
        this.dfSource = dfSource;
    }

    @Override
    public Query rewrite(Query original) throws IOException {
        try {
            return doRewrite(original);
        } catch (Throwable t) {
            searchContext.clearReleasables(Lifetime.COLLECTION);
            throw ExceptionsHelper.convertToElastic(t);
        }
    }

    /**
     * Profiling-aware wrapper of rewrite().  If profiling is enabled, the queryProfiler
     * inside the searchContext is start/stop after the rewrite operation
     *
     * @param original Query to be rewritten
     * @return         Rewritten query
     *
     * @throws IOException
     */
    private Query doRewrite(Query original) throws IOException {
        if (profile) {
            searchContext.queryProfiler().startTime(original, InternalProfileBreakdown.TimingType.REWRITE);
            Query rewritten = super.rewrite(original);      // nocommit Had to use super!  kosher?
            searchContext.queryProfiler().stopAndRecordTime(original, InternalProfileBreakdown.TimingType.REWRITE);

            searchContext.queryProfiler().reconcileRewrite(original, rewritten);

            return rewritten;
        }

        return in.rewrite(original);
    }

    @Override
    public Weight createNormalizedWeight(Query query, boolean needsScores) throws IOException {
        try {
            // if scores are needed and we have dfs data then use it
            if (dfSource != null && needsScores) {
                return doCreateNormalizedWeight(query, needsScores, true);
            }
            return  doCreateNormalizedWeight(query, needsScores, false);   // nocommit Had to use super!  kosher?
        } catch (Throwable t) {
            searchContext.clearReleasables(Lifetime.COLLECTION);
            throw ExceptionsHelper.convertToElastic(t);
        }
    }

    /**
     * Profiling-aware wrapper of createNormalizedWeight().  If profiling is enabled,
     * the queryProfiler inside the searchContext is start/stop after the normalization operation
     *
     * Note: if profiling is enabled, DFS is not used due to limitations of wrapping the
     * IndexSearcher
     *
     * @param query         Query to build weight for
     * @param needsScores   If this weight needs scores or not
     * @param useDFS        If this query should use precomputed DFS tf/df
     * @return              The normalized weight
     *
     * @throws IOException
     */
    private Weight doCreateNormalizedWeight(Query query, boolean needsScores, boolean useDFS) throws IOException {

        // NOTE: DFS won't work!
        if (profile) {
            return super.createNormalizedWeight(query, needsScores);    // nocommit Had to use super!  kosher?
        }

        return useDFS ? dfSource.createNormalizedWeight(query, needsScores) : in.createNormalizedWeight(query, needsScores);
    }

    @Override
    public Weight createWeight(Query query, boolean needsScores) throws IOException {
        if (profile) {
            // createWeight() is called for each query in the tree, so we tell the queryProfiler
            // each invocation so that it can build an internal representation of the query
            // tree
            searchContext.queryProfiler().pushQuery(query);

            searchContext.queryProfiler().startTime(query, InternalProfileBreakdown.TimingType.WEIGHT);
            Weight weight = super.createWeight(query, needsScores);
            searchContext.queryProfiler().stopAndRecordTime(query, InternalProfileBreakdown.TimingType.WEIGHT);

            searchContext.queryProfiler().pollLast();

            return new ProfileQuery.ProfileWeight(query, weight, searchContext.queryProfiler());
        }

        return super.createWeight(query, needsScores);
    }

    @Override
    public Explanation explain(Query query, int doc) throws IOException {
        try {
            return in.explain(query, doc);
        } finally {
            searchContext.clearReleasables(Lifetime.COLLECTION);
        }
    }

    @Override
    protected void search(List<LeafReaderContext> leaves, Weight weight, Collector collector) throws IOException {
        try {
            super.search(leaves, weight, collector);
        } finally {
            searchContext.clearReleasables(Lifetime.COLLECTION);
        }
    }

    /**
     * Return if this query is being profiled or not
     */
    public boolean profile() {
        return profile;
    }

    /**
     * Sets if this query should be profiled or not
     *
     * @param profile True if this query should be profiled
     */
    public void profile(boolean profile) {
        this.profile = profile;
    }
}
