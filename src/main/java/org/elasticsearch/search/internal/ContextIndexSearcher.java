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

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.*;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.lucene.MinimumScoreCollector;
import org.elasticsearch.common.lucene.MultiCollector;
import org.elasticsearch.common.lucene.search.FilteredCollector;
import org.elasticsearch.common.lucene.search.XCollector;
import org.elasticsearch.common.lucene.search.XFilteredQuery;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.search.dfs.CachedDfSource;
import org.elasticsearch.search.internal.SearchContext.Lifetime;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Context-aware extension of {@link IndexSearcher}.
 */
public class ContextIndexSearcher extends IndexSearcher implements Releasable {

    public static enum Stage {
        NA,
        MAIN_QUERY
    }

    /** The wrapped {@link IndexSearcher}. The reason why we sometimes prefer delegating to this searcher instead of <tt>super</tt> is that
     *  this instance may have more assertions, for example if it comes from MockInternalEngine which wraps the IndexSearcher into an
     *  AssertingIndexSearcher. */
    private final IndexSearcher in;

    private final SearchContext searchContext;

    private CachedDfSource dfSource;

    private List<Collector> queryCollectors;

    private Stage currentState = Stage.NA;

    private boolean enableMainDocIdSetCollector;
    private DocIdSetCollector mainDocIdSetCollector;

    public ContextIndexSearcher(SearchContext searchContext, Engine.Searcher searcher) {
        super(searcher.reader());
        in = searcher.searcher();
        this.searchContext = searchContext;
        setSimilarity(searcher.searcher().getSimilarity());
    }

    @Override
    public void close() {
        Releasables.close(mainDocIdSetCollector);
    }

    public void dfSource(CachedDfSource dfSource) {
        this.dfSource = dfSource;
    }

    /**
     * Adds a query level collector that runs at {@link Stage#MAIN_QUERY}. Note, supports
     * {@link org.elasticsearch.common.lucene.search.XCollector} allowing for a callback
     * when collection is done.
     */
    public void addMainQueryCollector(Collector collector) {
        if (queryCollectors == null) {
            queryCollectors = new ArrayList<>();
        }
        queryCollectors.add(collector);
    }

    public DocIdSetCollector mainDocIdSetCollector() {
        return this.mainDocIdSetCollector;
    }

    public void enableMainDocIdSetCollector() {
        this.enableMainDocIdSetCollector = true;
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
        if (original == searchContext.query() || original == searchContext.parsedQuery().query()) {
            // optimize in case its the top level search query and we already rewrote it...
            if (searchContext.queryRewritten()) {
                return searchContext.query();
            }
            Query rewriteQuery = in.rewrite(original);
            searchContext.updateRewriteQuery(rewriteQuery);
            return rewriteQuery;
        } else {
            return in.rewrite(original);
        }
    }

    @Override
    public Weight createNormalizedWeight(Query query) throws IOException {
        try {
            // if its the main query, use we have dfs data, only then do it
            if (dfSource != null && (query == searchContext.query() || query == searchContext.parsedQuery().query())) {
                return dfSource.createNormalizedWeight(query);
            }
            return in.createNormalizedWeight(query);
        } catch (Throwable t) {
            searchContext.clearReleasables(Lifetime.COLLECTION);
            throw new RuntimeException(t);
        }
    }

    @Override
    public void search(List<AtomicReaderContext> leaves, Weight weight, Collector collector) throws IOException {
        if (searchContext.timeoutInMillis() != -1) {
            // TODO: change to use our own counter that uses the scheduler in ThreadPool
            collector = new TimeLimitingCollector(collector, TimeLimitingCollector.getGlobalCounter(), searchContext.timeoutInMillis());
        }
        if (currentState == Stage.MAIN_QUERY) {
            if (enableMainDocIdSetCollector) {
                // TODO should we create a cache of segment->docIdSets so we won't create one each time?
                collector = this.mainDocIdSetCollector = new DocIdSetCollector(searchContext.docSetCache(), collector);
            }
            if (searchContext.parsedPostFilter() != null) {
                // this will only get applied to the actual search collector and not
                // to any scoped collectors, also, it will only be applied to the main collector
                // since that is where the filter should only work
                collector = new FilteredCollector(collector, searchContext.parsedPostFilter().filter());
            }
            if (queryCollectors != null && !queryCollectors.isEmpty()) {
                collector = new MultiCollector(collector, queryCollectors.toArray(new Collector[queryCollectors.size()]));
            }

            // apply the minimum score after multi collector so we filter facets as well
            if (searchContext.minimumScore() != null) {
                collector = new MinimumScoreCollector(collector, searchContext.minimumScore());
            }
        }

        // we only compute the doc id set once since within a context, we execute the same query always...
        try {
            if (searchContext.timeoutInMillis() != -1) {
                try {
                    super.search(leaves, weight, collector);
                } catch (TimeLimitingCollector.TimeExceededException e) {
                    searchContext.queryResult().searchTimedOut(true);
                }
            } else {
                super.search(leaves, weight, collector);
            }

            if (currentState == Stage.MAIN_QUERY) {
                if (enableMainDocIdSetCollector) {
                    enableMainDocIdSetCollector = false;
                    mainDocIdSetCollector.postCollection();
                }
                if (queryCollectors != null && !queryCollectors.isEmpty()) {
                    for (Collector queryCollector : queryCollectors) {
                        if (queryCollector instanceof XCollector) {
                            ((XCollector) queryCollector).postCollection();
                        }
                    }
                }
            }
        } finally {
            searchContext.clearReleasables(Lifetime.COLLECTION);
        }
    }

    @Override
    public Explanation explain(Query query, int doc) throws IOException {
        try {
            if (searchContext.aliasFilter() == null) {
                return super.explain(query, doc);
            }
            XFilteredQuery filteredQuery = new XFilteredQuery(query, searchContext.aliasFilter());
            return super.explain(filteredQuery, doc);
        } finally {
            searchContext.clearReleasables(Lifetime.COLLECTION);
        }
    }
}