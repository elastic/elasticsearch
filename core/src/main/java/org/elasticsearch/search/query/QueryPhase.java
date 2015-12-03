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

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.queries.MinDocQuery;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MultiCollector;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TermQuery;
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
import org.elasticsearch.search.profile.*;
import org.elasticsearch.search.rescore.RescorePhase;
import org.elasticsearch.search.rescore.RescoreSearchContext;
import org.elasticsearch.search.sort.SortParseElement;
import org.elasticsearch.search.sort.TrackScoresParseElement;
import org.elasticsearch.search.suggest.SuggestPhase;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import static java.util.Collections.unmodifiableMap;

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
        Map<String, SearchParseElement> parseElements = new HashMap<>();
        parseElements.put("from", new FromParseElement());
        parseElements.put("size", new SizeParseElement());
        parseElements.put("indices_boost", new IndicesBoostParseElement());
        parseElements.put("indicesBoost", new IndicesBoostParseElement());
        parseElements.put("query", new QueryParseElement());
        parseElements.put("post_filter", new PostFilterParseElement());
        parseElements.put("postFilter", new PostFilterParseElement());
        parseElements.put("sort", new SortParseElement());
        parseElements.put("trackScores", new TrackScoresParseElement());
        parseElements.put("track_scores", new TrackScoresParseElement());
        parseElements.put("min_score", new MinScoreParseElement());
        parseElements.put("minScore", new MinScoreParseElement());
        parseElements.put("timeout", new TimeoutParseElement());
        parseElements.put("terminate_after", new TerminateAfterParseElement());
        parseElements.putAll(aggregationPhase.parseElements());
        parseElements.putAll(suggestPhase.parseElements());
        parseElements.putAll(rescorePhase.parseElements());
        return unmodifiableMap(parseElements);
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

        boolean rescore = execute(searchContext, searchContext.searcher());

        if (rescore) { // only if we do a regular search
            rescorePhase.execute(searchContext);
        }
        suggestPhase.execute(searchContext);
        aggregationPhase.execute(searchContext);

        if (searchContext.getProfilers() != null) {
            List<ProfileShardResult> shardResults = Profiler.buildShardResults(searchContext.getProfilers().getProfilers());
            searchContext.queryResult().profileResults(shardResults);
        }
    }

    private static boolean returnsDocsInOrder(Query query, Sort sort) {
        if (sort == null || Sort.RELEVANCE.equals(sort)) {
            // sort by score
            // queries that return constant scores will return docs in index
            // order since Lucene tie-breaks on the doc id
            return query.getClass() == ConstantScoreQuery.class
                    || query.getClass() == MatchAllDocsQuery.class;
        } else {
            return Sort.INDEXORDER.equals(sort);
        }
    }

    /**
     * In a package-private method so that it can be tested without having to
     * wire everything (mapperService, etc.)
     * @return whether the rescoring phase should be executed
     */
    static boolean execute(SearchContext searchContext, final IndexSearcher searcher) throws QueryPhaseExecutionException {
        QuerySearchResult queryResult = searchContext.queryResult();
        queryResult.searchTimedOut(false);

        final boolean doProfile = searchContext.getProfilers() != null;
        final SearchType searchType = searchContext.searchType();
        boolean rescore = false;
        try {
            queryResult.from(searchContext.from());
            queryResult.size(searchContext.size());

            Query query = searchContext.query();

            final int totalNumDocs = searcher.getIndexReader().numDocs();
            int numDocs = Math.min(searchContext.from() + searchContext.size(), totalNumDocs);

            ProfileCollectorBuilder collectorBuilder = new ProfileCollectorBuilder(doProfile);

            assert query == searcher.rewrite(query); // already rewritten

            if (searchContext.size() == 0) { // no matter what the value of from is
                collectorBuilder.addTotalHitCountCollector();
            } else {
                // Perhaps have a dedicated scroll phase?
                final ScrollContext scrollContext = searchContext.scrollContext();
                assert (scrollContext != null) == (searchContext.request().scroll() != null);
                final TopDocsCollector<?> topDocsCollector;
                ScoreDoc lastEmittedDoc;
                if (searchContext.request().scroll() != null) {
                    numDocs = Math.min(searchContext.size(), totalNumDocs);
                    lastEmittedDoc = scrollContext.lastEmittedDoc;

                    if (returnsDocsInOrder(query, searchContext.sort())) {
                        if (scrollContext.totalHits == -1) {
                            // first round
                            assert scrollContext.lastEmittedDoc == null;
                            // there is not much that we can optimize here since we want to collect all
                            // documents in order to get the total number of hits
                        } else {
                            // now this gets interesting: since we sort in index-order, we can directly
                            // skip to the desired doc and stop collecting after ${size} matches
                            if (scrollContext.lastEmittedDoc != null) {
                                BooleanQuery bq = new BooleanQuery.Builder()
                                    .add(query, BooleanClause.Occur.MUST)
                                    .add(new MinDocQuery(lastEmittedDoc.doc + 1), BooleanClause.Occur.FILTER)
                                    .build();
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
                    collectorBuilder.addTopFieldCollector(searchContext.sort(), numDocs,
                            (FieldDoc) lastEmittedDoc, true, searchContext.trackScores(),
                            searchContext.trackScores(), scrollContext, searchType);
                } else {
                    rescore = !searchContext.rescore().isEmpty();
                    for (RescoreSearchContext rescoreContext : searchContext.rescore()) {
                        numDocs = Math.max(rescoreContext.window(), numDocs);
                    }
                    collectorBuilder.addTopScoreDocCollector(numDocs, lastEmittedDoc, scrollContext, searchType);
                }
            }

            final boolean terminateAfterSet = searchContext.terminateAfter() != SearchContext.DEFAULT_TERMINATE_AFTER;
            if (terminateAfterSet) {
                // throws Lucene.EarlyTerminationException when given count is reached
                collectorBuilder.addEarlyTerminatingCollector(searchContext.terminateAfter());
            }

            if (searchContext.parsedPostFilter() != null) {
                // this will only get applied to the actual search collector and not
                // to any scoped collectors, also, it will only be applied to the main collector
                // since that is where the filter should only work
                final Weight filterWeight = searcher.createNormalizedWeight(searchContext.parsedPostFilter().query(), false);
                collectorBuilder.addFilteredCollector(filterWeight);
            }

            // plug in additional collectors, like aggregations
            final List<Collector> subCollectors = new ArrayList<>();
            subCollectors.addAll(searchContext.queryCollectors().values());
            collectorBuilder.addMultiCollector(subCollectors);

            // apply the minimum score after multi collector so we filter aggs as well
            if (searchContext.minimumScore() != null) {
                collectorBuilder.addMinimumScoreCollector(searchContext.minimumScore());
            }

            if (collectorBuilder.getCollectorClass() == TotalHitCountCollector.class) {
                // Optimize counts in simple cases to return in constant time
                // instead of using a collector
                while (true) {
                    // remove wrappers that don't matter for counts
                    // this is necessary so that we don't only optimize match_all
                    // queries but also match_all queries that are nested in
                    // a constant_score query
                    if (query instanceof ConstantScoreQuery) {
                        query = ((ConstantScoreQuery) query).getQuery();
                    } else {
                        break;
                    }
                }

                if (query.getClass() == MatchAllDocsQuery.class) {
                    collectorBuilder.disableCollection();
                    collectorBuilder.setTopDocsCallable(() -> {
                        int count = searcher.getIndexReader().numDocs();
                        return new TopDocs(count, Lucene.EMPTY_SCORE_DOCS, 0);
                    });
                } else if (query.getClass() == TermQuery.class && searcher.getIndexReader().hasDeletions() == false) {
                    final Term term = ((TermQuery) query).getTerm();
                    collectorBuilder.disableCollection();
                    collectorBuilder.setTopDocsCallable(() -> {
                        int count = 0;
                        for (LeafReaderContext context : searcher.getIndexReader().leaves()) {
                            count += context.reader().docFreq(term);
                        }
                        return new TopDocs(count, Lucene.EMPTY_SCORE_DOCS, 0);
                    });
                }
            }

            final boolean timeoutSet = searchContext.timeoutInMillis() != SearchService.NO_TIMEOUT.millis();
            if (timeoutSet) {
                // TODO: change to use our own counter that uses the scheduler in ThreadPool
                // throws TimeLimitingCollector.TimeExceededException when timeout has reached
                collectorBuilder.addTimeLimitingCollector(searchContext.timeEstimateCounter(), searchContext.timeoutInMillis());
            }

            try {
                if (collectorBuilder.isCollectionEnabled()) {
                    Collector collector = collectorBuilder.buildCollector();
                    if (doProfile) {
                        searchContext.getProfilers().getCurrent().setCollector((InternalProfileCollector) collector);
                    }
                    searcher.search(query, collector);
                }
            } catch (TimeLimitingCollector.TimeExceededException e) {
                assert timeoutSet : "TimeExceededException thrown even though timeout wasn't set";
                queryResult.searchTimedOut(true);
            } catch (Lucene.EarlyTerminationException e) {
                assert terminateAfterSet : "EarlyTerminationException thrown even though terminateAfter wasn't set";
                queryResult.terminatedEarly(true);
            } finally {
                searchContext.clearReleasables(SearchContext.Lifetime.COLLECTION);
            }
            if (terminateAfterSet && queryResult.terminatedEarly() == null) {
                queryResult.terminatedEarly(false);
            }

            Callable<TopDocs> topDocsCallable = collectorBuilder.buildTopDocsCallable();
            queryResult.topDocs(topDocsCallable.call());

            if (doProfile) {
                List<ProfileShardResult> shardResults = Profiler.buildShardResults(searchContext.getProfilers().getProfilers());
                searchContext.queryResult().profileResults(shardResults);
            }

            return rescore;

        } catch (Throwable e) {
            throw new QueryPhaseExecutionException(searchContext, "Failed to execute main query", e);
        }
    }
}
