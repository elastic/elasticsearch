/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import com.google.common.collect.ImmutableList;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.*;
import org.elasticsearch.common.lucene.MinimumScoreCollector;
import org.elasticsearch.common.lucene.MultiCollector;
import org.elasticsearch.common.lucene.search.AndFilter;
import org.elasticsearch.common.lucene.search.FilteredCollector;
import org.elasticsearch.common.lucene.search.XFilteredQuery;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.search.dfs.CachedDfSource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 *
 */
public class ContextIndexSearcher extends IndexSearcher {

    public static enum Stage {
        NA,
        MAIN_QUERY,
        REWRITE
    }

    private final SearchContext searchContext;

    private CachedDfSource dfSource;

    private List<Collector> queryCollectors;

    private Stage currentState = Stage.NA;

    public ContextIndexSearcher(SearchContext searchContext, Engine.Searcher searcher) {
        super(searcher.reader());
        this.searchContext = searchContext;
        setSimilarity(searcher.searcher().getSimilarity());
    }

    public void dfSource(CachedDfSource dfSource) {
        this.dfSource = dfSource;
    }

    /**
     * Adds a query level collector that runs at {@link Stage#MAIN_QUERY}
     */
    public void addMainQueryCollector(Collector collector) {
        if (queryCollectors == null) {
            queryCollectors = new ArrayList<Collector>();
        }
        queryCollectors.add(collector);
    }

    public void inStage(Stage stage) {
        this.currentState = stage;
    }

    public void finishStage(Stage stage) {
        assert currentState == stage;
        this.currentState = Stage.NA;
    }

    @Override
    public Query rewrite(Query original) throws IOException {
        if (original == searchContext.query() || original == searchContext.parsedQuery().query()) {
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
    public Weight createNormalizedWeight(Query query) throws IOException {
        // if its the main query, use we have dfs data, only then do it
        if (dfSource != null && (query == searchContext.query() || query == searchContext.parsedQuery().query())) {
            return dfSource.createNormalizedWeight(query);
        }
        return super.createNormalizedWeight(query);
    }

    private Filter combinedFilter(Filter filter) {
        Filter combinedFilter;
        if (filter == null) {
            combinedFilter = searchContext.aliasFilter();
        } else {
            if (searchContext.aliasFilter() != null) {
                combinedFilter = new AndFilter(ImmutableList.of(filter, searchContext.aliasFilter()));
            } else {
                combinedFilter = filter;
            }
        }
        return combinedFilter;
    }

    @Override
    public void search(Query query, Collector results) throws IOException {
        Filter filter = combinedFilter(null);
        if (filter != null) {
            super.search(wrapFilter(query, filter), results);
        } else {
            super.search(query, results);
        }
    }

    @Override
    public TopDocs search(Query query, Filter filter, int n) throws IOException {
        return super.search(query, combinedFilter(filter), n);
    }

    @Override
    public void search(List<AtomicReaderContext> leaves, Weight weight, Collector collector) throws IOException {
        if (searchContext.parsedFilter() != null && currentState == Stage.MAIN_QUERY) {
            // this will only get applied to the actual search collector and not
            // to any scoped collectors, also, it will only be applied to the main collector
            // since that is where the filter should only work
            collector = new FilteredCollector(collector, searchContext.parsedFilter());
        }
        if (searchContext.timeoutInMillis() != -1) {
            // TODO: change to use our own counter that uses the scheduler in ThreadPool
            collector = new TimeLimitingCollector(collector, TimeLimitingCollector.getGlobalCounter(), searchContext.timeoutInMillis());
        }
        if (currentState == Stage.MAIN_QUERY) {
            if (queryCollectors != null && !queryCollectors.isEmpty()) {
                collector = new MultiCollector(collector, queryCollectors.toArray(new Collector[queryCollectors.size()]));
            }
        }
        // apply the minimum score after multi collector so we filter facets as well
        if (searchContext.minimumScore() != null) {
            collector = new MinimumScoreCollector(collector, searchContext.minimumScore());
        }

        // we only compute the doc id set once since within a context, we execute the same query always...
        if (searchContext.timeoutInMillis() != -1) {
            try {
                super.search(leaves, weight, collector);
            } catch (TimeLimitingCollector.TimeExceededException e) {
                searchContext.queryResult().searchTimedOut(true);
            }
        } else {
            super.search(leaves, weight, collector);
        }
    }

    @Override
    public Explanation explain(Query query, int doc) throws IOException {
        if (searchContext.aliasFilter() == null) {
            return super.explain(query, doc);
        }

        XFilteredQuery filteredQuery = new XFilteredQuery(query, searchContext.aliasFilter());
        return super.explain(filteredQuery, doc);
    }
}