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

import org.apache.lucene.queries.MinDocQuery;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MultiCollector;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TimeLimitingCollector;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopDocsCollector;
import org.apache.lucene.search.TopFieldCollector;
import org.apache.lucene.search.TopScoreDocCollector;
import org.apache.lucene.search.TotalHitCountCollector;
import org.apache.lucene.search.Weight;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.lucene.MinimumScoreCollector;
import org.elasticsearch.common.lucene.search.FilteredCollector;
import org.elasticsearch.search.SearchParseElement;
import org.elasticsearch.search.SearchPhase;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.aggregations.AggregationPhase;
import org.elasticsearch.search.internal.ScrollContext;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.rescore.RescorePhase;
import org.elasticsearch.search.rescore.RescoreSearchContext;
import org.elasticsearch.search.scan.ScanContext.ScanCollector;
import org.elasticsearch.search.sort.SortParseElement;
import org.elasticsearch.search.sort.TrackScoresParseElement;
import org.elasticsearch.search.suggest.SuggestPhase;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

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

        final SearchType searchType = searchContext.searchType();
        boolean rescore = false;
        try {
            searchContext.queryResult().from(searchContext.from());
            searchContext.queryResult().size(searchContext.size());

            final IndexSearcher searcher = searchContext.searcher();
            Query query = searchContext.query();

            final int totalNumDocs = searcher.getIndexReader().numDocs();
            int numDocs = Math.min(searchContext.from() + searchContext.size(), totalNumDocs);

            Collector collector;
            final Callable<TopDocs> topDocsCallable;

            if (searchContext.size() == 0) { // no matter what the value of from is
                final TotalHitCountCollector totalHitCountCollector = new TotalHitCountCollector();
                collector = totalHitCountCollector;
                topDocsCallable = new Callable<TopDocs>() {
                    @Override
                    public TopDocs call() throws Exception {
                        return new TopDocs(totalHitCountCollector.getTotalHits(), Lucene.EMPTY_SCORE_DOCS, 0);
                    }
                };
            } else if (searchType == SearchType.SCAN) {
                query = searchContext.scanContext().wrapQuery(query);
                final ScanCollector scanCollector = searchContext.scanContext().collector(searchContext);
                collector = scanCollector;
                topDocsCallable = new Callable<TopDocs>() {
                    @Override
                    public TopDocs call() throws Exception {
                        return scanCollector.topDocs();
                    }
                };
            } else {
                // Perhaps have a dedicated scroll phase?
                final ScrollContext scrollContext = searchContext.scrollContext();
                assert (scrollContext != null) == (searchContext.request().scroll() != null);
                final TopDocsCollector<?> topDocsCollector;
                ScoreDoc lastEmittedDoc;
                if (searchContext.request().scroll() != null) {
                    numDocs = Math.min(searchContext.size(), totalNumDocs);
                    lastEmittedDoc = scrollContext.lastEmittedDoc;

                    if (Sort.INDEXORDER.equals(searchContext.sort())) {
                        if (scrollContext.totalHits == -1) {
                            // first round
                            assert scrollContext.lastEmittedDoc == null;
                            // there is not much that we can optimize here since we want to collect all
                            // documents in order to get the total number of hits
                        } else {
                            // now this gets interesting: since we sort in index-order, we can directly
                            // skip to the desired doc and stop collecting after ${size} matches
                            if (scrollContext.lastEmittedDoc != null) {
                                BooleanQuery bq = new BooleanQuery();
                                bq.add(query, Occur.MUST);
                                bq.add(new MinDocQuery(lastEmittedDoc.doc + 1), Occur.FILTER);
                                query = bq;
                            }
                            searchContext.terminateAfter(numDocs);
                        }
                    }
                } else {
                    lastEmittedDoc = null;
                }
                if (totalNumDocs == 0) {
                    // top collectors don't like a size of 0
                    numDocs = 1;
                }
                assert numDocs > 0;
                if (searchContext.sort() != null) {
                    topDocsCollector = TopFieldCollector.create(searchContext.sort(), numDocs,
                            (FieldDoc) lastEmittedDoc, true, searchContext.trackScores(), searchContext.trackScores());
                } else {
                    rescore = !searchContext.rescore().isEmpty();
                    for (RescoreSearchContext rescoreContext : searchContext.rescore()) {
                        numDocs = Math.max(rescoreContext.window(), numDocs);
                    }
                    topDocsCollector = TopScoreDocCollector.create(numDocs, lastEmittedDoc);
                }
                collector = topDocsCollector;
                topDocsCallable = new Callable<TopDocs>() {
                    @Override
                    public TopDocs call() throws Exception {
                        TopDocs topDocs = topDocsCollector.topDocs();
                        if (scrollContext != null) {
                            if (scrollContext.totalHits == -1) {
                                // first round
                                scrollContext.totalHits = topDocs.totalHits;
                                scrollContext.maxScore = topDocs.getMaxScore();
                            } else {
                                // subsequent round: the total number of hits and
                                // the maximum score were computed on the first round
                                topDocs.totalHits = scrollContext.totalHits;
                                topDocs.setMaxScore(scrollContext.maxScore);
                            }
                            switch (searchType) {
                            case QUERY_AND_FETCH:
                            case DFS_QUERY_AND_FETCH:
                                // for (DFS_)QUERY_AND_FETCH, we already know the last emitted doc
                                if (topDocs.scoreDocs.length > 0) {
                                    // set the last emitted doc
                                    scrollContext.lastEmittedDoc = topDocs.scoreDocs[topDocs.scoreDocs.length - 1];
                                }
                            default:
                                break;
                            }
                        }
                        return topDocs;
                    }
                };
            }

            final boolean terminateAfterSet = searchContext.terminateAfter() != SearchContext.DEFAULT_TERMINATE_AFTER;
            if (terminateAfterSet) {
                // throws Lucene.EarlyTerminationException when given count is reached
                collector = Lucene.wrapCountBasedEarlyTerminatingCollector(collector, searchContext.terminateAfter());
            }

            if (searchContext.parsedPostFilter() != null) {
                // this will only get applied to the actual search collector and not
                // to any scoped collectors, also, it will only be applied to the main collector
                // since that is where the filter should only work
                final Weight filterWeight = searcher.createNormalizedWeight(searchContext.parsedPostFilter().query(), false);
                collector = new FilteredCollector(collector, filterWeight);
            }

            // plug in additional collectors, like aggregations
            List<Collector> allCollectors = new ArrayList<>();
            allCollectors.add(collector);
            allCollectors.addAll(searchContext.queryCollectors().values());
            collector = MultiCollector.wrap(allCollectors);

            // apply the minimum score after multi collector so we filter aggs as well
            if (searchContext.minimumScore() != null) {
                collector = new MinimumScoreCollector(collector, searchContext.minimumScore());
            }

            final boolean timeoutSet = searchContext.timeoutInMillis() != SearchService.NO_TIMEOUT.millis();
            if (timeoutSet) {
                // TODO: change to use our own counter that uses the scheduler in ThreadPool
                // throws TimeLimitingCollector.TimeExceededException when timeout has reached
                collector = Lucene.wrapTimeLimitingCollector(collector, searchContext.timeEstimateCounter(), searchContext.timeoutInMillis());
            }

            try {
                searcher.search(query, collector);
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

            searchContext.queryResult().topDocs(topDocsCallable.call());
        } catch (Throwable e) {
            throw new QueryPhaseExecutionException(searchContext, "Failed to execute main query", e);
        }
        if (rescore) { // only if we do a regular search
            rescorePhase.execute(searchContext);
        }
        suggestPhase.execute(searchContext);
        aggregationPhase.execute(searchContext);
    }
}
