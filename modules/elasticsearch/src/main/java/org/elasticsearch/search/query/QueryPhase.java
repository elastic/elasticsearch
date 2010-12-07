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

import org.apache.lucene.search.*;
import org.elasticsearch.common.collect.ImmutableMap;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.lucene.search.function.BoostScoreFunction;
import org.elasticsearch.common.lucene.search.function.FunctionScoreQuery;
import org.elasticsearch.index.query.ParsedQuery;
import org.elasticsearch.search.SearchParseElement;
import org.elasticsearch.search.SearchParseException;
import org.elasticsearch.search.SearchPhase;
import org.elasticsearch.search.facet.FacetsPhase;
import org.elasticsearch.search.internal.ContextIndexSearcher;
import org.elasticsearch.search.internal.ScopePhase;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.sort.SortParseElement;

import java.util.Map;

/**
 * @author kimchy (Shay Banon)
 */
public class QueryPhase implements SearchPhase {

    private final FacetsPhase facetsPhase;

    @Inject public QueryPhase(FacetsPhase facetsPhase) {
        this.facetsPhase = facetsPhase;
    }

    @Override public Map<String, ? extends SearchParseElement> parseElements() {
        ImmutableMap.Builder<String, SearchParseElement> parseElements = ImmutableMap.builder();
        parseElements.put("from", new FromParseElement()).put("size", new SizeParseElement())
                .put("query_parser_name", new QueryParserNameParseElement())
                .put("queryParserName", new QueryParserNameParseElement())
                .put("indices_boost", new IndicesBoostParseElement())
                .put("indicesBoost", new IndicesBoostParseElement())
                .put("query", new QueryParseElement())
                .put("queryBinary", new QueryBinaryParseElement())
                .put("query_binary", new QueryBinaryParseElement())
                .put("sort", new SortParseElement())
                .putAll(facetsPhase.parseElements());
        return parseElements.build();
    }

    @Override public void preProcess(SearchContext context) {
        if (context.query() == null) {
            throw new SearchParseException(context, "No query specified in search request");
        }
        if (context.queryBoost() != 1.0f) {
            context.parsedQuery(new ParsedQuery(new FunctionScoreQuery(context.query(), new BoostScoreFunction(context.queryBoost())), context.parsedQuery()));
        }
        facetsPhase.preProcess(context);
    }

    public void execute(SearchContext searchContext) throws QueryPhaseExecutionException {
        if (searchContext.parsedQuery().scopePhases().length > 0) {
            // we have scoped queries, refresh the id cache
            try {
                searchContext.idCache().refresh(searchContext.searcher().subReaders());
            } catch (Exception e) {
                throw new QueryPhaseExecutionException(searchContext, "Failed to refresh id cache for child queries", e);
            }

            // process scoped queries (from the last to the first, working with the parsing option here)
            for (int i = searchContext.parsedQuery().scopePhases().length - 1; i >= 0; i--) {
                ScopePhase scopePhase = searchContext.parsedQuery().scopePhases()[i];

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
            if (searchContext.types().length > 0) {
                query = new FilteredQuery(query, searchContext.filterCache().cache(searchContext.mapperService().typesFilter(searchContext.types())));
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

            if (sort) {
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

        facetsPhase.execute(searchContext);
    }
}
