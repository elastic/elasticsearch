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
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.lucene.MinimumScoreCollector;
import org.elasticsearch.common.lucene.search.FilteredCollector;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.SearchPhase;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.aggregations.AggregationPhase;
import org.elasticsearch.search.internal.ScrollContext;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.profile.ProfileShardResult;
import org.elasticsearch.search.profile.SearchProfileShardResults;
import org.elasticsearch.search.profile.query.CollectorResult;
import org.elasticsearch.search.profile.query.InternalProfileCollector;
import org.elasticsearch.search.rescore.RescorePhase;
import org.elasticsearch.search.rescore.RescoreSearchContext;
import org.elasticsearch.search.sort.SortAndFormats;
import org.elasticsearch.search.suggest.SuggestPhase;

import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;

/**
 *
 */
public class QueryPhase implements SearchPhase {

    private final AggregationPhase aggregationPhase;
    private final SuggestPhase suggestPhase;
    private RescorePhase rescorePhase;

    public QueryPhase(Settings settings) {
        this.aggregationPhase = new AggregationPhase();
        this.suggestPhase = new SuggestPhase(settings);
        this.rescorePhase = new RescorePhase(settings);
    }

    @Override
    public void preProcess(SearchContext context) {
        context.preProcess();
    }

    @Override
    public void execute(SearchContext searchContext) throws QueryPhaseExecutionException {
        if (searchContext.hasOnlySuggest()) {
            suggestPhase.execute(searchContext);
            // TODO: fix this once we can fetch docs for suggestions
            searchContext.queryResult().topDocs(
                    new TopDocs(0, Lucene.EMPTY_SCORE_DOCS, 0),
                    new DocValueFormat[0]);
            return;
        }
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
            ProfileShardResult shardResults = SearchProfileShardResults
                    .buildShardResults(searchContext.getProfilers());
            searchContext.queryResult().profileResults(shardResults);
        }
    }

    private static boolean returnsDocsInOrder(Query query, SortAndFormats sf) {
        if (sf == null || Sort.RELEVANCE.equals(sf.sort)) {
            // sort by score
            // queries that return constant scores will return docs in index
            // order since Lucene tie-breaks on the doc id
            return query.getClass() == ConstantScoreQuery.class
                    || query.getClass() == MatchAllDocsQuery.class;
        } else {
            return Sort.INDEXORDER.equals(sf.sort);
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

            Collector collector;
            Callable<TopDocs> topDocsCallable;
            DocValueFormat[] sortValueFormats = new DocValueFormat[0];

            assert query == searcher.rewrite(query); // already rewritten

            if (searchContext.size() == 0) { // no matter what the value of from is
                final TotalHitCountCollector totalHitCountCollector = new TotalHitCountCollector();
                collector = totalHitCountCollector;
                if (searchContext.getProfilers() != null) {
                    collector = new InternalProfileCollector(collector, CollectorResult.REASON_SEARCH_COUNT, Collections.emptyList());
                }
                topDocsCallable = new Callable<TopDocs>() {
                    @Override
                    public TopDocs call() throws Exception {
                        return new TopDocs(totalHitCountCollector.getTotalHits(), Lucene.EMPTY_SCORE_DOCS, 0);
                    }
                };
            } else {
                // Perhaps have a dedicated scroll phase?
                final ScrollContext scrollContext = searchContext.scrollContext();
                assert (scrollContext != null) == (searchContext.request().scroll() != null);
                final TopDocsCollector<?> topDocsCollector;
                ScoreDoc after = null;
                if (searchContext.request().scroll() != null) {
                    numDocs = Math.min(searchContext.size(), totalNumDocs);
                    after = scrollContext.lastEmittedDoc;

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
                                    .add(new MinDocQuery(after.doc + 1), BooleanClause.Occur.FILTER)
                                    .build();
                                query = bq;
                            }
                            searchContext.terminateAfter(numDocs);
                        }
                    }
                } else {
                    after = searchContext.searchAfter();
                }
                if (totalNumDocs == 0) {
                    // top collectors don't like a size of 0
                    numDocs = 1;
                }
                assert numDocs > 0;
                if (searchContext.sort() != null) {
                    SortAndFormats sf = searchContext.sort();
                    topDocsCollector = TopFieldCollector.create(sf.sort, numDocs,
                            (FieldDoc) after, true, searchContext.trackScores(), searchContext.trackScores());
                    sortValueFormats = sf.formats;
                } else {
                    rescore = !searchContext.rescore().isEmpty();
                    for (RescoreSearchContext rescoreContext : searchContext.rescore()) {
                        numDocs = Math.max(rescoreContext.window(), numDocs);
                    }
                    topDocsCollector = TopScoreDocCollector.create(numDocs, after);
                }
                collector = topDocsCollector;
                if (doProfile) {
                    collector = new InternalProfileCollector(collector, CollectorResult.REASON_SEARCH_TOP_HITS, Collections.emptyList());
                }
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
                                break;
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
                final Collector child = collector;
                // throws Lucene.EarlyTerminationException when given count is reached
                collector = Lucene.wrapCountBasedEarlyTerminatingCollector(collector, searchContext.terminateAfter());
                if (doProfile) {
                    collector = new InternalProfileCollector(collector, CollectorResult.REASON_SEARCH_TERMINATE_AFTER_COUNT,
                            Collections.singletonList((InternalProfileCollector) child));
                }
            }

            if (searchContext.parsedPostFilter() != null) {
                final Collector child = collector;
                // this will only get applied to the actual search collector and not
                // to any scoped collectors, also, it will only be applied to the main collector
                // since that is where the filter should only work
                final Weight filterWeight = searcher.createNormalizedWeight(searchContext.parsedPostFilter().query(), false);
                collector = new FilteredCollector(collector, filterWeight);
                if (doProfile) {
                    collector = new InternalProfileCollector(collector, CollectorResult.REASON_SEARCH_POST_FILTER,
                            Collections.singletonList((InternalProfileCollector) child));
                }
            }

            // plug in additional collectors, like aggregations
            final List<Collector> subCollectors = new ArrayList<>();
            subCollectors.add(collector);
            subCollectors.addAll(searchContext.queryCollectors().values());
            collector = MultiCollector.wrap(subCollectors);
            if (doProfile && collector instanceof InternalProfileCollector == false) {
                // When there is a single collector to wrap, MultiCollector returns it
                // directly, so only wrap in the case that there are several sub collectors
                final List<InternalProfileCollector> children = new AbstractList<InternalProfileCollector>() {
                    @Override
                    public InternalProfileCollector get(int index) {
                        return (InternalProfileCollector) subCollectors.get(index);
                    }
                    @Override
                    public int size() {
                        return subCollectors.size();
                    }
                };
                collector = new InternalProfileCollector(collector, CollectorResult.REASON_SEARCH_MULTI, children);
            }

            // apply the minimum score after multi collector so we filter aggs as well
            if (searchContext.minimumScore() != null) {
                final Collector child = collector;
                collector = new MinimumScoreCollector(collector, searchContext.minimumScore());
                if (doProfile) {
                    collector = new InternalProfileCollector(collector, CollectorResult.REASON_SEARCH_MIN_SCORE,
                            Collections.singletonList((InternalProfileCollector) child));
                }
            }

            if (collector.getClass() == TotalHitCountCollector.class) {
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
                    collector = null;
                    topDocsCallable = new Callable<TopDocs>() {
                        @Override
                        public TopDocs call() throws Exception {
                            int count = searcher.getIndexReader().numDocs();
                            return new TopDocs(count, Lucene.EMPTY_SCORE_DOCS, 0);
                        }
                    };
                } else if (query.getClass() == TermQuery.class && searcher.getIndexReader().hasDeletions() == false) {
                    final Term term = ((TermQuery) query).getTerm();
                    collector = null;
                    topDocsCallable = new Callable<TopDocs>() {
                        @Override
                        public TopDocs call() throws Exception {
                            int count = 0;
                            for (LeafReaderContext context : searcher.getIndexReader().leaves()) {
                                count += context.reader().docFreq(term);
                            }
                            return new TopDocs(count, Lucene.EMPTY_SCORE_DOCS, 0);
                        }
                    };
                }
            }

            final boolean timeoutSet = searchContext.timeoutInMillis() != SearchService.NO_TIMEOUT.millis();
            if (timeoutSet && collector != null) { // collector might be null if no collection is actually needed
                final Collector child = collector;
                // TODO: change to use our own counter that uses the scheduler in ThreadPool
                // throws TimeLimitingCollector.TimeExceededException when timeout has reached
                collector = Lucene.wrapTimeLimitingCollector(collector, searchContext.timeEstimateCounter(), searchContext.timeoutInMillis());
                if (doProfile) {
                    collector = new InternalProfileCollector(collector, CollectorResult.REASON_SEARCH_TIMEOUT,
                            Collections.singletonList((InternalProfileCollector) child));
                }
            }

            try {
                if (collector != null) {
                    if (doProfile) {
                        searchContext.getProfilers().getCurrentQueryProfiler().setCollector((InternalProfileCollector) collector);
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

            queryResult.topDocs(topDocsCallable.call(), sortValueFormats);

            if (searchContext.getProfilers() != null) {
                ProfileShardResult shardResults = SearchProfileShardResults
                        .buildShardResults(searchContext.getProfilers());
                searchContext.queryResult().profileResults(shardResults);
            }

            return rescore;

        } catch (Throwable e) {
            throw new QueryPhaseExecutionException(searchContext, "Failed to execute main query", e);
        }
    }
}
