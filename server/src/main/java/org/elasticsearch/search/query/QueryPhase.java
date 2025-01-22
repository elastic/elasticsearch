/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.query;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.Weight;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.lucene.search.TopDocsAndMaxScore;
import org.elasticsearch.common.util.concurrent.EsThreadPoolExecutor;
import org.elasticsearch.common.util.concurrent.TaskExecutionTimeTrackingEsThreadPoolExecutor;
import org.elasticsearch.lucene.queries.SearchAfterSortedDocQuery;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.SearchContextSourcePrinter;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.aggregations.AggregationPhase;
import org.elasticsearch.search.internal.ContextIndexSearcher;
import org.elasticsearch.search.internal.ScrollContext;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.rank.RankSearchContext;
import org.elasticsearch.search.rank.context.QueryPhaseRankShardContext;
import org.elasticsearch.search.rescore.RescorePhase;
import org.elasticsearch.search.sort.SortAndFormats;
import org.elasticsearch.search.suggest.SuggestPhase;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;

import static org.elasticsearch.search.internal.SearchContext.TRACK_TOTAL_HITS_DISABLED;

/**
 * Query phase of a search request, used to run the query and get back from each shard information about the matching documents
 * (document ids and score or sort criteria) so that matches can be reduced on the coordinating node
 */
public class QueryPhase {
    private static final Logger LOGGER = LogManager.getLogger(QueryPhase.class);

    private QueryPhase() {}

    public static void execute(SearchContext searchContext) throws QueryPhaseExecutionException {
        if (searchContext.queryPhaseRankShardContext() == null) {
            if (searchContext.request().source() != null && searchContext.request().source().rankBuilder() != null) {
                // if we have a RankBuilder provided, we want to fetch all rankWindowSize results
                // and rerank the documents as per the RankBuilder's instructions.
                // Pagination will take place later once they're all (re)ranked.
                searchContext.size(searchContext.request().source().rankBuilder().rankWindowSize());
                searchContext.from(0);
            }
            executeQuery(searchContext);
        } else {
            executeRank(searchContext);
        }
    }

    static void executeRank(SearchContext searchContext) throws QueryPhaseExecutionException {
        QueryPhaseRankShardContext queryPhaseRankShardContext = searchContext.queryPhaseRankShardContext();
        QuerySearchResult querySearchResult = searchContext.queryResult();

        // run the combined boolean query total hits or aggregations
        // otherwise mark top docs as empty
        if (searchContext.trackTotalHitsUpTo() != TRACK_TOTAL_HITS_DISABLED || searchContext.aggregations() != null) {
            searchContext.size(0);
            QueryPhase.executeQuery(searchContext);
        } else {
            searchContext.queryResult().topDocs(new TopDocsAndMaxScore(Lucene.EMPTY_TOP_DOCS, Float.NaN), new DocValueFormat[0]);
        }

        List<TopDocs> rrfRankResults = new ArrayList<>();
        boolean searchTimedOut = querySearchResult.searchTimedOut();
        long serviceTimeEWMA = querySearchResult.serviceTimeEWMA();
        int nodeQueueSize = querySearchResult.nodeQueueSize();
        try {
            // run each of the rank queries
            for (Query rankQuery : queryPhaseRankShardContext.queries()) {
                // if a search timeout occurs, exit with partial results
                if (searchTimedOut) {
                    break;
                }
                try (
                    RankSearchContext rankSearchContext = new RankSearchContext(
                        searchContext,
                        rankQuery,
                        queryPhaseRankShardContext.rankWindowSize()
                    )
                ) {
                    QueryPhase.addCollectorsAndSearch(rankSearchContext);
                    QuerySearchResult rrfQuerySearchResult = rankSearchContext.queryResult();
                    rrfRankResults.add(rrfQuerySearchResult.topDocs().topDocs);
                    serviceTimeEWMA += rrfQuerySearchResult.serviceTimeEWMA();
                    nodeQueueSize = Math.max(nodeQueueSize, rrfQuerySearchResult.nodeQueueSize());
                    searchTimedOut = rrfQuerySearchResult.searchTimedOut();
                }
            }

            querySearchResult.setRankShardResult(queryPhaseRankShardContext.combineQueryPhaseResults(rrfRankResults));

            // record values relevant to all queries
            querySearchResult.searchTimedOut(searchTimedOut);
            querySearchResult.serviceTimeEWMA(serviceTimeEWMA);
            querySearchResult.nodeQueueSize(nodeQueueSize);
        } catch (Exception e) {
            throw new QueryPhaseExecutionException(searchContext.shardTarget(), "Failed to execute rank query", e);
        }
    }

    static void executeQuery(SearchContext searchContext) throws QueryPhaseExecutionException {
        if (searchContext.hasOnlySuggest()) {
            SuggestPhase.execute(searchContext);
            searchContext.queryResult().topDocs(new TopDocsAndMaxScore(Lucene.EMPTY_TOP_DOCS, Float.NaN), new DocValueFormat[0]);
            return;
        }

        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("{}", new SearchContextSourcePrinter(searchContext));
        }

        // Pre-process aggregations as late as possible. In the case of a DFS_Q_T_F
        // request, preProcess is called on the DFS phase, this is why we pre-process them
        // here to make sure it happens during the QUERY phase
        AggregationPhase.preProcess(searchContext);

        addCollectorsAndSearch(searchContext);

        RescorePhase.execute(searchContext);
        SuggestPhase.execute(searchContext);

        if (searchContext.getProfilers() != null) {
            searchContext.queryResult().profileResults(searchContext.getProfilers().buildQueryPhaseResults());
        }
    }

    /**
     * In a package-private method so that it can be tested without having to
     * wire everything (mapperService, etc.)
     */
    static void addCollectorsAndSearch(SearchContext searchContext) throws QueryPhaseExecutionException {
        final ContextIndexSearcher searcher = searchContext.searcher();
        final IndexReader reader = searcher.getIndexReader();
        QuerySearchResult queryResult = searchContext.queryResult();
        queryResult.searchTimedOut(false);
        try {
            queryResult.from(searchContext.from());
            queryResult.size(searchContext.size());
            Query query = searchContext.rewrittenQuery();
            assert query == searcher.rewrite(query); // already rewritten

            final ScrollContext scrollContext = searchContext.scrollContext();
            if (scrollContext != null) {
                if (scrollContext.totalHits == null) {
                    // first round
                    assert scrollContext.lastEmittedDoc == null;
                    // there is not much that we can optimize here since we want to collect all
                    // documents in order to get the total number of hits

                } else {
                    final ScoreDoc after = scrollContext.lastEmittedDoc;
                    if (canEarlyTerminate(reader, searchContext.sort())) {
                        // now this gets interesting: since the search sort is a prefix of the index sort, we can directly
                        // skip to the desired doc
                        if (after != null) {
                            query = new BooleanQuery.Builder().add(query, BooleanClause.Occur.MUST)
                                .add(new SearchAfterSortedDocQuery(searchContext.sort().sort, (FieldDoc) after), BooleanClause.Occur.FILTER)
                                .build();
                        }
                    }
                }
            }

            final boolean hasFilterCollector = searchContext.parsedPostFilter() != null || searchContext.minimumScore() != null;

            Weight postFilterWeight = null;
            if (searchContext.parsedPostFilter() != null) {
                postFilterWeight = searcher.createWeight(
                    searcher.rewrite(searchContext.parsedPostFilter().query()),
                    ScoreMode.COMPLETE_NO_SCORES,
                    1f
                );
            }

            CollectorManager<Collector, QueryPhaseResult> collectorManager = QueryPhaseCollectorManager.createQueryPhaseCollectorManager(
                postFilterWeight,
                searchContext.aggregations() == null ? null : searchContext.aggregations().getAggsCollectorManager(),
                searchContext,
                hasFilterCollector
            );

            final Runnable timeoutRunnable = getTimeoutCheck(searchContext);
            if (timeoutRunnable != null) {
                searcher.addQueryCancellation(timeoutRunnable);
            }

            QueryPhaseResult queryPhaseResult = searcher.search(query, collectorManager);
            if (searchContext.getProfilers() != null) {
                searchContext.getProfilers().getCurrentQueryProfiler().setCollectorResult(queryPhaseResult.collectorResult());
            }
            queryResult.topDocs(queryPhaseResult.topDocsAndMaxScore(), queryPhaseResult.sortValueFormats());
            if (searcher.timeExceeded()) {
                assert timeoutRunnable != null : "TimeExceededException thrown even though timeout wasn't set";
                SearchTimeoutException.handleTimeout(
                    searchContext.request().allowPartialSearchResults(),
                    searchContext.shardTarget(),
                    searchContext.queryResult()
                );
            }
            if (searchContext.terminateAfter() != SearchContext.DEFAULT_TERMINATE_AFTER) {
                queryResult.terminatedEarly(queryPhaseResult.terminatedAfter());
            }
            ExecutorService executor = searchContext.indexShard().getThreadPool().executor(ThreadPool.Names.SEARCH);
            assert executor instanceof TaskExecutionTimeTrackingEsThreadPoolExecutor
                || (executor instanceof EsThreadPoolExecutor == false /* in case thread pool is mocked out in tests */)
                : "SEARCH threadpool should have an executor that exposes EWMA metrics, but is of type " + executor.getClass();
            if (executor instanceof TaskExecutionTimeTrackingEsThreadPoolExecutor rExecutor) {
                queryResult.nodeQueueSize(rExecutor.getCurrentQueueSize());
                queryResult.serviceTimeEWMA((long) rExecutor.getTaskExecutionEWMA());
            }
        } catch (Exception e) {
            throw new QueryPhaseExecutionException(searchContext.shardTarget(), "Failed to execute main query", e);
        }
    }

    /**
     * Returns whether collection within the provided <code>reader</code> can be early-terminated if it sorts
     * with <code>sortAndFormats</code>.
     **/
    private static boolean canEarlyTerminate(IndexReader reader, SortAndFormats sortAndFormats) {
        if (sortAndFormats == null || sortAndFormats.sort == null) {
            return false;
        }
        final Sort sort = sortAndFormats.sort;
        for (LeafReaderContext ctx : reader.leaves()) {
            Sort indexSort = ctx.reader().getMetaData().sort();
            if (indexSort == null || Lucene.canEarlyTerminate(sort, indexSort) == false) {
                return false;
            }
        }
        return true;
    }

    public static Runnable getTimeoutCheck(SearchContext searchContext) {
        boolean timeoutSet = searchContext.scrollContext() == null
            && searchContext.timeout() != null
            && searchContext.timeout().equals(SearchService.NO_TIMEOUT) == false;

        if (timeoutSet) {
            final long startTime = searchContext.getRelativeTimeInMillis();
            final long timeout = searchContext.timeout().millis();
            final long maxTime = startTime + timeout;
            return () -> {
                final long time = searchContext.getRelativeTimeInMillis();
                if (time > maxTime) {
                    searchContext.searcher().throwTimeExceededException();
                }
            };
        } else {
            return null;
        }
    }
}
