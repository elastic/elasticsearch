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
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHitCountCollector;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.search.SearchParseElement;
import org.elasticsearch.search.SearchPhase;
import org.elasticsearch.search.facet.FacetPhase;
import org.elasticsearch.search.internal.ContextIndexSearcher;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.rescore.RescorePhase;
import org.elasticsearch.search.sort.SortParseElement;
import org.elasticsearch.search.sort.TrackScoresParseElement;
import org.elasticsearch.search.suggest.SuggestPhase;

import java.util.Map;

/**
 *
 */
public class QueryPhase implements SearchPhase {

    private final FacetPhase facetPhase;
    private final SuggestPhase suggestPhase;
    private RescorePhase rescorePhase;

    @Inject
    public QueryPhase(FacetPhase facetPhase, SuggestPhase suggestPhase, RescorePhase rescorePhase) {
        this.facetPhase = facetPhase;
        this.suggestPhase = suggestPhase;
        this.rescorePhase = rescorePhase;
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
                .putAll(facetPhase.parseElements())
                .putAll(suggestPhase.parseElements())
                .putAll(rescorePhase.parseElements());
        return parseElements.build();
    }

    @Override
    public void preProcess(SearchContext context) {
        context.preProcess();
        facetPhase.preProcess(context);
    }

    public void execute(SearchContext searchContext) throws QueryPhaseExecutionException {
        searchContext.queryResult().searchTimedOut(false);

        searchContext.searcher().inStage(ContextIndexSearcher.Stage.MAIN_QUERY);
        boolean rescore = false;
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
                topDocs = searchContext.searcher().search(query, null, numDocs, searchContext.sort(),
                        searchContext.trackScores(), searchContext.trackScores());
            } else {
                if (searchContext.rescore() != null) {
                    rescore = true;
                    numDocs = Math.max(searchContext.rescore().window(), numDocs);
                }
                topDocs = searchContext.searcher().search(query, numDocs);
            }
            searchContext.queryResult().topDocs(topDocs);
        } catch (Throwable e) {
            throw new QueryPhaseExecutionException(searchContext, "Failed to execute main query", e);
        } finally {
            searchContext.searcher().finishStage(ContextIndexSearcher.Stage.MAIN_QUERY);
        }
        if (rescore) { // only if we do a regular search
            rescorePhase.execute(searchContext);
        }
        suggestPhase.execute(searchContext);
        facetPhase.execute(searchContext);
    }
}
