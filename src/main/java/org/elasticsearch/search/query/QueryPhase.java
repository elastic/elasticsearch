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

package org.elasticsearch.search.query;

import com.google.common.collect.ImmutableMap;
import org.apache.lucene.search.*;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.lucene.search.function.BoostScoreFunction;
import org.elasticsearch.common.lucene.search.function.FunctionScoreQuery;
import org.elasticsearch.index.query.ParsedQuery;
import org.elasticsearch.search.SearchParseElement;
import org.elasticsearch.search.SearchPhase;
import org.elasticsearch.search.facet.FacetPhase;
import org.elasticsearch.search.internal.ContextIndexSearcher;
import org.elasticsearch.search.internal.ScopePhase;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.sort.SortParseElement;
import org.elasticsearch.search.sort.TrackScoresParseElement;

import java.util.Map;

/**
 *
 */
public class QueryPhase implements SearchPhase {

    private final FacetPhase facetPhase;

    @Inject
    public QueryPhase(FacetPhase facetPhase) {
        this.facetPhase = facetPhase;
    }

    @Override
    public Map<String, ? extends SearchParseElement> parseElements() {
        ImmutableMap.Builder<String, SearchParseElement> parseElements = ImmutableMap.builder();
        parseElements.put("from", new FromParseElement()).put("size", new SizeParseElement())
                .put("indices_boost", new IndicesBoostParseElement())
                .put("indicesBoost", new IndicesBoostParseElement())
                .put("query", new QueryParseElement())
                .put("queryBinary", new QueryBinaryParseElement())
                .put("query_binary", new QueryBinaryParseElement())
                .put("filter", new FilterParseElement())
                .put("filterBinary", new FilterBinaryParseElement())
                .put("filter_binary", new FilterBinaryParseElement())
                .put("sort", new SortParseElement())
                .put("trackScores", new TrackScoresParseElement())
                .put("track_scores", new TrackScoresParseElement())
                .put("min_score", new MinScoreParseElement())
                .put("minScore", new MinScoreParseElement())
                .put("timeout", new TimeoutParseElement())
                .putAll(facetPhase.parseElements());
        return parseElements.build();
    }

    @Override
    public void preProcess(SearchContext context) {
        if (context.query() == null) {
            context.parsedQuery(ParsedQuery.MATCH_ALL_PARSED_QUERY);
        }
        if (context.queryBoost() != 1.0f) {
            context.parsedQuery(new ParsedQuery(new FunctionScoreQuery(context.query(), new BoostScoreFunction(context.queryBoost())), context.parsedQuery()));
        }
        Filter searchFilter = context.mapperService().searchFilter(context.types());
        if (searchFilter != null) {
            if (Queries.isMatchAllQuery(context.query())) {
                Query q = new DeletionAwareConstantScoreQuery(context.filterCache().cache(searchFilter));
                q.setBoost(context.query().getBoost());
                context.parsedQuery(new ParsedQuery(q, context.parsedQuery()));
            } else {
                context.parsedQuery(new ParsedQuery(new FilteredQuery(context.query(), context.filterCache().cache(searchFilter)), context.parsedQuery()));
            }
        }
        facetPhase.preProcess(context);
    }

    public void execute(SearchContext searchContext) throws QueryPhaseExecutionException {
        searchContext.queryResult().searchTimedOut(false);
        // set the filter on the searcher
        if (searchContext.scopePhases() != null) {
            // we have scoped queries, refresh the id cache
            try {
                searchContext.idCache().refresh(searchContext.searcher().subReaders());
            } catch (Exception e) {
                throw new QueryPhaseExecutionException(searchContext, "Failed to refresh id cache for child queries", e);
            }

            // the first scope level is the most nested child
            for (ScopePhase scopePhase : searchContext.scopePhases()) {
                if (scopePhase instanceof ScopePhase.TopDocsPhase) {
                    ScopePhase.TopDocsPhase topDocsPhase = (ScopePhase.TopDocsPhase) scopePhase;
                    topDocsPhase.clear();
                    int numDocs = (searchContext.from() + searchContext.size());
                    if (numDocs == 0) {
                        numDocs = 1;
                    }
                    try {
                        numDocs *= topDocsPhase.factor();
                        while (true) {
                            if (topDocsPhase.scope() != null) {
                                searchContext.searcher().processingScope(topDocsPhase.scope());
                            }
                            TopDocs topDocs = searchContext.searcher().search(topDocsPhase.query(), numDocs);
                            if (topDocsPhase.scope() != null) {
                                // we mark the scope as processed, so we don't process it again, even if we need to rerun the query...
                                searchContext.searcher().processedScope();
                            }
                            topDocsPhase.processResults(topDocs, searchContext);

                            // check if we found enough docs, if so, break
                            if (topDocsPhase.numHits() >= (searchContext.from() + searchContext.size())) {
                                break;
                            }
                            // if we did not find enough docs, check if it make sense to search further
                            if (topDocs.totalHits <= numDocs) {
                                break;
                            }
                            // if not, update numDocs, and search again
                            numDocs *= topDocsPhase.incrementalFactor();
                            if (numDocs > topDocs.totalHits) {
                                numDocs = topDocs.totalHits;
                            }
                        }
                    } catch (Exception e) {
                        throw new QueryPhaseExecutionException(searchContext, "Failed to execute child query [" + scopePhase.query() + "]", e);
                    }
                } else if (scopePhase instanceof ScopePhase.CollectorPhase) {
                    try {
                        ScopePhase.CollectorPhase collectorPhase = (ScopePhase.CollectorPhase) scopePhase;
                        // collector phase might not require extra processing, for example, when scrolling
                        if (!collectorPhase.requiresProcessing()) {
                            continue;
                        }
                        if (scopePhase.scope() != null) {
                            searchContext.searcher().processingScope(scopePhase.scope());
                        }
                        Collector collector = collectorPhase.collector();
                        searchContext.searcher().search(collectorPhase.query(), collector);
                        collectorPhase.processCollector(collector);
                        if (collectorPhase.scope() != null) {
                            // we mark the scope as processed, so we don't process it again, even if we need to rerun the query...
                            searchContext.searcher().processedScope();
                        }
                    } catch (Exception e) {
                        throw new QueryPhaseExecutionException(searchContext, "Failed to execute child query [" + scopePhase.query() + "]", e);
                    }
                }
            }
        }

        searchContext.searcher().processingScope(ContextIndexSearcher.Scopes.MAIN);
        try {
            searchContext.queryResult().from(searchContext.from());
            searchContext.queryResult().size(searchContext.size());

            Query query = searchContext.query();

            TopDocs topDocs;
            int numDocs = searchContext.from() + searchContext.size();
            if (numDocs == 0) {
                // if 0 was asked, change it to 1 since 0 is not allowed
                numDocs = 1;
            }

            if (searchContext.searchType() == SearchType.COUNT) {
                TotalHitCountCollector collector = new TotalHitCountCollector();
                searchContext.searcher().search(query, collector);
                topDocs = new TopDocs(collector.getTotalHits(), Lucene.EMPTY_SCORE_DOCS, 0);
            } else if (searchContext.searchType() == SearchType.SCAN) {
                topDocs = searchContext.scanContext().execute(searchContext);
            } else if (searchContext.sort() != null) {
                topDocs = searchContext.searcher().search(query, null, numDocs, searchContext.sort());
            } else {
                topDocs = searchContext.searcher().search(query, numDocs);
            }
            searchContext.queryResult().topDocs(topDocs);
        } catch (Exception e) {
            throw new QueryPhaseExecutionException(searchContext, "Failed to execute main query", e);
        } finally {
            searchContext.searcher().processedScope();
        }

        facetPhase.execute(searchContext);
    }
}
