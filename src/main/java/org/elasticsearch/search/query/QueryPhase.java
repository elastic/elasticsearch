/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
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

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.FilteredQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TopDocs;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.common.collect.ImmutableMap;
import org.elasticsearch.common.inject.Inject;
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;

/**
 * @author kimchy (Shay Banon)
 */
public class QueryPhase implements SearchPhase {

    private final FacetPhase facetPhase;

    @Inject public QueryPhase(FacetPhase facetPhase) {
        this.facetPhase = facetPhase;
    }

    @Override public Map<String, ? extends SearchParseElement> parseElements() {
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
                .putAll(facetPhase.parseElements());
        return parseElements.build();
    }

    @Override public void preProcess(SearchContext context) {
        if (context.query() == null) {
            context.parsedQuery(ParsedQuery.MATCH_ALL_PARSED_QUERY);
        }
        if (context.queryBoost() != 1.0f) {
            context.parsedQuery(new ParsedQuery(new FunctionScoreQuery(context.query(), new BoostScoreFunction(context.queryBoost())), context.parsedQuery()));
        }
        facetPhase.preProcess(context);
    }

    public void execute(SearchContext searchContext) throws QueryPhaseExecutionException {
        // set the filter on the searcher
        if (searchContext.scopePhases() != null) {
            // we have scoped queries, refresh the id cache
            try {
                searchContext.idCache().refresh(searchContext.searcher().subReaders());
            } catch (Exception e) {
                throw new QueryPhaseExecutionException(searchContext, "Failed to refresh id cache for child queries", e);
            }

            // process scoped queries (from the last to the first, working with the parsing option here)
            for (int i = searchContext.scopePhases().size() - 1; i >= 0; i--) {
                ScopePhase scopePhase = searchContext.scopePhases().get(i);

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
            Filter searchFilter = searchContext.mapperService().searchFilter(searchContext.types());
            if (searchFilter != null) {
                query = new FilteredQuery(query, searchContext.filterCache().cache(searchFilter));
            }

            TopDocs topDocs;
            int numDocs = searchContext.from() + searchContext.size();
            if (numDocs == 0) {
                // if 0 was asked, change it to 1 since 0 is not allowed
                numDocs = 1;
            }
            boolean sort = false;
            // try and optimize for a case where the sorting is based on score, this is how we work by default!
            if (searchContext.sort() != null) {
                if (searchContext.sort().getSort().length > 1) {
                    sort = true;
                } else {
                    SortField sortField = searchContext.sort().getSort()[0];
                    if (sortField.getType() == SortField.SCORE && !sortField.getReverse()) {
                        sort = false;
                    } else {
                        sort = true;
                    }
                }
            }

            if (searchContext.searchType() == SearchType.COUNT) {
                CountCollector countCollector = new CountCollector();
                try {
                    searchContext.searcher().search(query, countCollector);
                } catch (ScanCollector.StopCollectingException e) {
                    // all is well
                }
                topDocs = countCollector.topDocs();
            } else if (searchContext.searchType() == SearchType.SCAN) {
                ScanCollector scanCollector = new ScanCollector(searchContext.from(), searchContext.size(), searchContext.trackScores());
                try {
                    searchContext.searcher().search(query, scanCollector);
                } catch (ScanCollector.StopCollectingException e) {
                    // all is well
                }
                topDocs = scanCollector.topDocs();
            } else if (sort) {
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

    static class CountCollector extends Collector {

        private int totalHits = 0;

        @Override public void setScorer(Scorer scorer) throws IOException {
        }

        @Override public void collect(int doc) throws IOException {
            totalHits++;
        }

        @Override public void setNextReader(IndexReader reader, int docBase) throws IOException {
        }

        @Override public boolean acceptsDocsOutOfOrder() {
            return true;
        }

        public TopDocs topDocs() {
            return new TopDocs(totalHits, EMPTY, 0);
        }

        private static ScoreDoc[] EMPTY = new ScoreDoc[0];
    }

    static class ScanCollector extends Collector {

        private final int from;

        private final int to;

        private final ArrayList<ScoreDoc> docs;

        private final boolean trackScores;

        private Scorer scorer;

        private int docBase;

        private int counter;

        ScanCollector(int from, int size, boolean trackScores) {
            this.from = from;
            this.to = from + size;
            this.trackScores = trackScores;
            this.docs = new ArrayList<ScoreDoc>(size);
        }

        public TopDocs topDocs() {
            return new TopDocs(docs.size(), docs.toArray(new ScoreDoc[docs.size()]), 0f);
        }

        @Override public void setScorer(Scorer scorer) throws IOException {
            this.scorer = scorer;
        }

        @Override public void collect(int doc) throws IOException {
            if (counter >= from) {
                docs.add(new ScoreDoc(docBase + doc, trackScores ? scorer.score() : 0f));
            }
            counter++;
            if (counter >= to) {
                throw StopCollectingException;
            }
        }

        @Override public void setNextReader(IndexReader reader, int docBase) throws IOException {
            this.docBase = docBase;
        }

        @Override public boolean acceptsDocsOutOfOrder() {
            return true;
        }

        public static final RuntimeException StopCollectingException = new StopCollectingException();

        static class StopCollectingException extends RuntimeException {
            @Override public Throwable fillInStackTrace() {
                return null;
            }
        }
    }
}
