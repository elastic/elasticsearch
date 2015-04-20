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

package org.elasticsearch.search.query;

import com.google.common.collect.ImmutableMap;

import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.search.SearchParseElement;
import org.elasticsearch.search.SearchPhase;
import org.elasticsearch.search.aggregations.AggregationPhase;
import org.elasticsearch.search.internal.ContextIndexSearcher;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.rescore.RescorePhase;
import org.elasticsearch.search.rescore.RescoreSearchContext;
import org.elasticsearch.search.sort.SortParseElement;
import org.elasticsearch.search.sort.TrackScoresParseElement;
import org.elasticsearch.search.suggest.SuggestPhase;

import java.util.Map;

/**
 *
 */
public class QueryPhase implements SearchPhase {

    private final AggregationPhase aggregationPhase;
    private final SuggestPhase suggestPhase;
    private RescorePhase rescorePhase;

    @Inject
    public QueryPhase(AggregationPhase aggregationPhase, SuggestPhase suggestPhase, RescorePhase rescorePhase) {
        this.aggregationPhase = aggregationPhase;
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
                .put("filter", new PostFilterParseElement()) // For bw comp reason, should be removed in version 1.1
                .put("post_filter", new PostFilterParseElement())
                .put("postFilter", new PostFilterParseElement())
                .put("filterBinary", new FilterBinaryParseElement())
                .put("filter_binary", new FilterBinaryParseElement())
                .put("sort", new SortParseElement())
                .put("trackScores", new TrackScoresParseElement())
                .put("track_scores", new TrackScoresParseElement())
                .put("min_score", new MinScoreParseElement())
                .put("minScore", new MinScoreParseElement())
                .put("timeout", new TimeoutParseElement())
                .put("terminate_after", new TerminateAfterParseElement())
                .putAll(aggregationPhase.parseElements())
                .putAll(suggestPhase.parseElements())
                .putAll(rescorePhase.parseElements());
        return parseElements.build();
    }

    @Override
    public void preProcess(SearchContext context) {
        context.preProcess();
    }

    @Override
    public void execute(SearchContext searchContext) throws QueryPhaseExecutionException {
        // Pre-process aggregations as late as possible. In the case of a DFS_Q_T_F
        // request, preProcess is called on the DFS phase phase, this is why we pre-process them
        // here to make sure it happens during the QUERY phase
        aggregationPhase.preProcess(searchContext);

        searchContext.queryResult().searchTimedOut(false);

        searchContext.searcher().inStage(ContextIndexSearcher.Stage.MAIN_QUERY);
        boolean rescore = false;
        try {
            searchContext.queryResult().from(searchContext.from());
            searchContext.queryResult().size(searchContext.size());

            Query query = searchContext.query();

            final TopDocs topDocs;
            int numDocs = searchContext.from() + searchContext.size();

            if (searchContext.size() == 0) { // no matter what the value of from is
                topDocs = new TopDocs(searchContext.searcher().count(query), Lucene.EMPTY_SCORE_DOCS, 0);
            } else if (searchContext.searchType() == SearchType.SCAN) {
                topDocs = searchContext.scanContext().execute(searchContext);
            } else {
                // Perhaps have a dedicated scroll phase?
                if (searchContext.request().scroll() != null) {
                    numDocs = searchContext.size();
                    ScoreDoc lastEmittedDoc = searchContext.lastEmittedDoc();
                    if (searchContext.sort() != null) {
                        topDocs = searchContext.searcher().searchAfter(
                                lastEmittedDoc, query, null, numDocs, searchContext.sort(),
                                searchContext.trackScores(), searchContext.trackScores()
                        );
                    } else {
                        rescore = !searchContext.rescore().isEmpty();
                        for (RescoreSearchContext rescoreContext : searchContext.rescore()) {
                            numDocs = Math.max(rescoreContext.window(), numDocs);
                        }
                        topDocs = searchContext.searcher().searchAfter(lastEmittedDoc, query, numDocs);
                    }

                    int size = topDocs.scoreDocs.length;
                    if (size > 0) {
                        // In the case of *QUERY_AND_FETCH we don't get back to shards telling them which least
                        // relevant docs got emitted as hit, we can simply mark the last doc as last emitted
                        if (searchContext.searchType() == SearchType.QUERY_AND_FETCH ||
                                searchContext.searchType() == SearchType.DFS_QUERY_AND_FETCH) {
                            searchContext.lastEmittedDoc(topDocs.scoreDocs[size - 1]);
                        }
                    }
                } else {
                    if (searchContext.sort() != null) {
                        topDocs = searchContext.searcher().search(query, null, numDocs, searchContext.sort(),
                                searchContext.trackScores(), searchContext.trackScores());
                    } else {
                        rescore = !searchContext.rescore().isEmpty();
                        for (RescoreSearchContext rescoreContext : searchContext.rescore()) {
                            numDocs = Math.max(rescoreContext.window(), numDocs);
                        }
                        topDocs = searchContext.searcher().search(query, numDocs);
                    }
                }
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
        aggregationPhase.execute(searchContext);
    }
}
