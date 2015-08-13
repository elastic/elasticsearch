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

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermContext;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.*;
import org.apache.lucene.search.similarities.Similarity;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.lucene.MinimumScoreCollector;
import org.elasticsearch.common.lucene.search.FilteredCollector;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.dfs.AggregatedDfs;
import org.elasticsearch.search.internal.SearchContext.Lifetime;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Context-aware extension of {@link IndexSearcher}.
 */
public class ContextIndexSearcher extends IndexSearcher implements Releasable {

    public enum Stage {
        NA,
        MAIN_QUERY
    }

    private SearchContext searchContext;
    private AggregatedDfs aggregatedDfs;
    private Map<Class<?>, Collector> queryCollectors;
    private Stage currentState = Stage.NA;

    private QueryCache queryCache;
    private QueryCachingPolicy queryCachingPolicy;

    // for testing:
    public ContextIndexSearcher(IndexReader r) {
        super(r);
    }

    public ContextIndexSearcher(IndexReader reader, Similarity similarity, QueryCache queryCache, QueryCachingPolicy queryCachingPolicy) {
        super(reader);
        setSimilarity(similarity);
        setQueryCache(queryCache);
        setQueryCachingPolicy(queryCachingPolicy);
    }

    @Override
    public void close() {
    }

    public void setDfSource(AggregatedDfs aggregatedDfs) {
        this.aggregatedDfs = aggregatedDfs;
    }

    public void setSearchContext(SearchContext searchContext) {
        this.searchContext = searchContext;
    }

    @Override
    public void setQueryCache(QueryCache queryCache) {
        this.queryCache = queryCache;
        super.setQueryCache(queryCache);
    }

    public QueryCache getQueryCache() {
        return queryCache;
    }

    @Override
    public void setQueryCachingPolicy(QueryCachingPolicy queryCachingPolicy) {
        this.queryCachingPolicy = queryCachingPolicy;
        super.setQueryCachingPolicy(queryCachingPolicy);
    }

    public QueryCachingPolicy getQueryCachingPolicy() {
        return queryCachingPolicy;
    }

    /**
     * Adds a query level collector that runs at {@link Stage#MAIN_QUERY}. Note, supports
     * {@link Collector} allowing for a callback
     * when collection is done.
     */
    public Map<Class<?>, Collector> queryCollectors() {
        if (queryCollectors == null) {
            queryCollectors = new HashMap<>();
        }
        return queryCollectors;
    }

    public void inStage(Stage stage) {
        this.currentState = stage;
    }

    public void finishStage(Stage stage) {
        assert currentState == stage : "Expected stage " + stage + " but was stage " + currentState;
        this.currentState = Stage.NA;
    }

    @Override
    public Query rewrite(Query original) throws IOException {
        if (searchContext != null && (original == searchContext.query() || original == searchContext.parsedQuery().query())) {
            // optimize in case its the top level search query and we already rewrote it...
            if (searchContext.queryRewritten()) {
                return searchContext.query();
            }
            Query rewriteQuery = super.rewrite(original);
            searchContext.updateRewriteQuery(rewriteQuery);
            return rewriteQuery;
        } else {
            return super.rewrite(original);
        }
    }

    @Override
    public Weight createNormalizedWeight(Query query, boolean needsScores) throws IOException {
        try {
            return super.createNormalizedWeight(query, needsScores);
        } catch (Throwable t) {
            searchContext.clearReleasables(Lifetime.COLLECTION);
            throw ExceptionsHelper.convertToElastic(t);
        }
    }

    @Override
    public void search(Query query, Collector collector) throws IOException {
        if (searchContext == null) {
            super.search(query, collector);
            return;
        }

        // Wrap the caller's collector with various wrappers e.g. those used to siphon
        // matches off for aggregation or to impose a time-limit on collection.
        final boolean timeoutSet = searchContext.timeoutInMillis() != SearchService.NO_TIMEOUT.millis();
        final boolean terminateAfterSet = searchContext.terminateAfter() != SearchContext.DEFAULT_TERMINATE_AFTER;

        if (timeoutSet) {
            // TODO: change to use our own counter that uses the scheduler in ThreadPool
            // throws TimeLimitingCollector.TimeExceededException when timeout has reached
            collector = Lucene.wrapTimeLimitingCollector(collector, searchContext.timeEstimateCounter(), searchContext.timeoutInMillis());
        }
        if (terminateAfterSet) {
            // throws Lucene.EarlyTerminationException when given count is reached
            collector = Lucene.wrapCountBasedEarlyTerminatingCollector(collector, searchContext.terminateAfter());
        }
        if (currentState == Stage.MAIN_QUERY) {
            if (searchContext.parsedPostFilter() != null) {
                // this will only get applied to the actual search collector and not
                // to any scoped collectors, also, it will only be applied to the main collector
                // since that is where the filter should only work
                final Weight filterWeight = createNormalizedWeight(searchContext.parsedPostFilter().query(), false);
                collector = new FilteredCollector(collector, filterWeight);
            }
            if (queryCollectors != null && !queryCollectors.isEmpty()) {
                ArrayList<Collector> allCollectors = new ArrayList<>(queryCollectors.values());
                allCollectors.add(collector);
                collector = MultiCollector.wrap(allCollectors);
            }

            // apply the minimum score after multi collector so we filter aggs as well
            if (searchContext.minimumScore() != null) {
                collector = new MinimumScoreCollector(collector, searchContext.minimumScore());
            }
        }
        super.search(query, collector);
    }

    @Override
    public void search(List<LeafReaderContext> leaves, Weight weight, Collector collector) throws IOException {
        if (searchContext == null) {
            super.search(leaves, weight, collector);
            return;
        }

        final boolean timeoutSet = searchContext.timeoutInMillis() != SearchService.NO_TIMEOUT.millis();
        final boolean terminateAfterSet = searchContext.terminateAfter() != SearchContext.DEFAULT_TERMINATE_AFTER;
        try {
            if (timeoutSet || terminateAfterSet) {
                try {
                    super.search(leaves, weight, collector);
                } catch (TimeLimitingCollector.TimeExceededException e) {
                    assert timeoutSet : "TimeExceededException thrown even though timeout wasn't set";
                    searchContext.queryResult().searchTimedOut(true);
                } catch (Lucene.EarlyTerminationException e) {
                    assert terminateAfterSet : "EarlyTerminationException thrown even though terminateAfter wasn't set";
                    searchContext.queryResult().terminatedEarly(true);
                }
                if (terminateAfterSet && searchContext.queryResult().terminatedEarly() == null) {
                    searchContext.queryResult().terminatedEarly(false);
                }
            } else {
                super.search(leaves, weight, collector);
            }
        } finally {
            searchContext.clearReleasables(Lifetime.COLLECTION);
        }
    }

    @Override
    public Explanation explain(Query query, int doc) throws IOException {
        if (searchContext == null) {
            return super.explain(query, doc);
        }

        try {
            if (searchContext.aliasFilter() == null) {
                return super.explain(query, doc);
            }
            BooleanQuery filteredQuery = new BooleanQuery();
            filteredQuery.add(query, Occur.MUST);
            filteredQuery.add(searchContext.aliasFilter(), Occur.FILTER);
            return super.explain(filteredQuery, doc);
        } finally {
            searchContext.clearReleasables(Lifetime.COLLECTION);
        }
    }

    @Override
    public TermStatistics termStatistics(Term term, TermContext context) throws IOException {
        if (searchContext == null || aggregatedDfs == null) {
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
        if (searchContext == null || aggregatedDfs == null) {
            return super.collectionStatistics(field);
        }

        CollectionStatistics collectionStatistics = aggregatedDfs.fieldStatistics().get(field);
        if (collectionStatistics == null) {
            // we don't have stats for this - this might be a must_not clauses etc. that doesn't allow extract terms on the query
            return super.collectionStatistics(field);
        }
        return collectionStatistics;
    }
}
