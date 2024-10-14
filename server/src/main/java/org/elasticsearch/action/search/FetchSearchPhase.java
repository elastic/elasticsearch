/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.action.search;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.search.ScoreDoc;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.search.SearchPhaseResult;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.dfs.AggregatedDfs;
import org.elasticsearch.search.fetch.FetchSearchResult;
import org.elasticsearch.search.fetch.ShardFetchSearchRequest;
import org.elasticsearch.search.internal.ShardSearchContextId;
import org.elasticsearch.search.rank.RankDoc;
import org.elasticsearch.search.rank.RankDocShardInfo;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

/**
 * This search phase merges the query results from the previous phase together and calculates the topN hits for this search.
 * Then it reaches out to all relevant shards to fetch the topN hits.
 */
final class FetchSearchPhase extends SearchPhase {
    private final ArraySearchPhaseResults<FetchSearchResult> fetchResults;
    private final AtomicArray<SearchPhaseResult> searchPhaseShardResults;
    private final BiFunction<SearchResponseSections, AtomicArray<SearchPhaseResult>, SearchPhase> nextPhaseFactory;
    private final SearchPhaseContext context;
    private final Logger logger;
    private final SearchProgressListener progressListener;
    private final AggregatedDfs aggregatedDfs;
    @Nullable
    private final SearchPhaseResults<SearchPhaseResult> resultConsumer;
    private final SearchPhaseController.ReducedQueryPhase reducedQueryPhase;

    FetchSearchPhase(
        SearchPhaseResults<SearchPhaseResult> resultConsumer,
        AggregatedDfs aggregatedDfs,
        SearchPhaseContext context,
        @Nullable SearchPhaseController.ReducedQueryPhase reducedQueryPhase
    ) {
        this(
            resultConsumer,
            aggregatedDfs,
            context,
            reducedQueryPhase,
            (response, queryPhaseResults) -> new ExpandSearchPhase(
                context,
                response.hits,
                () -> new FetchLookupFieldsPhase(context, response, queryPhaseResults)
            )
        );
    }

    FetchSearchPhase(
        SearchPhaseResults<SearchPhaseResult> resultConsumer,
        AggregatedDfs aggregatedDfs,
        SearchPhaseContext context,
        @Nullable SearchPhaseController.ReducedQueryPhase reducedQueryPhase,
        BiFunction<SearchResponseSections, AtomicArray<SearchPhaseResult>, SearchPhase> nextPhaseFactory
    ) {
        super("fetch");
        if (context.getNumShards() != resultConsumer.getNumShards()) {
            throw new IllegalStateException(
                "number of shards must match the length of the query results but doesn't:"
                    + context.getNumShards()
                    + "!="
                    + resultConsumer.getNumShards()
            );
        }
        this.fetchResults = new ArraySearchPhaseResults<>(resultConsumer.getNumShards());
        context.addReleasable(fetchResults);
        this.searchPhaseShardResults = resultConsumer.getAtomicArray();
        this.aggregatedDfs = aggregatedDfs;
        this.nextPhaseFactory = nextPhaseFactory;
        this.context = context;
        this.logger = context.getLogger();
        this.progressListener = context.getTask().getProgressListener();
        this.reducedQueryPhase = reducedQueryPhase;
        this.resultConsumer = reducedQueryPhase == null ? resultConsumer : null;
    }

    @Override
    public void run() {
        context.execute(new AbstractRunnable() {

            @Override
            protected void doRun() throws Exception {
                innerRun();
            }

            @Override
            public void onFailure(Exception e) {
                context.onPhaseFailure(FetchSearchPhase.this, "", e);
            }
        });
    }

    private void innerRun() throws Exception {
        assert this.reducedQueryPhase == null ^ this.resultConsumer == null;
        // depending on whether we executed the RankFeaturePhase we may or may not have the reduced query result computed already
        final var reducedQueryPhase = this.reducedQueryPhase == null ? resultConsumer.reduce() : this.reducedQueryPhase;
        final int numShards = context.getNumShards();
        // Usually when there is a single shard, we force the search type QUERY_THEN_FETCH. But when there's kNN, we might
        // still use DFS_QUERY_THEN_FETCH, which does not perform the "query and fetch" optimization during the query phase.
        final boolean queryAndFetchOptimization = searchPhaseShardResults.length() == 1
            && context.getRequest().hasKnnSearch() == false
            && reducedQueryPhase.queryPhaseRankCoordinatorContext() == null
            && (context.getRequest().source() == null || context.getRequest().source().rankBuilder() == null);
        if (queryAndFetchOptimization) {
            assert assertConsistentWithQueryAndFetchOptimization();
            // query AND fetch optimization
            moveToNextPhase(searchPhaseShardResults, reducedQueryPhase);
        } else {
            ScoreDoc[] scoreDocs = reducedQueryPhase.sortedTopDocs().scoreDocs();
            // no docs to fetch -- sidestep everything and return
            if (scoreDocs.length == 0) {
                // we have to release contexts here to free up resources
                searchPhaseShardResults.asList()
                    .forEach(searchPhaseShardResult -> releaseIrrelevantSearchContext(searchPhaseShardResult, context));
                moveToNextPhase(fetchResults.getAtomicArray(), reducedQueryPhase);
            } else {
                final boolean shouldExplainRank = shouldExplainRankScores(context.getRequest());
                final List<Map<Integer, RankDoc>> rankDocsPerShard = false == shouldExplainRank
                    ? null
                    : splitRankDocsPerShard(scoreDocs, numShards);
                final ScoreDoc[] lastEmittedDocPerShard = context.getRequest().scroll() != null
                    ? SearchPhaseController.getLastEmittedDocPerShard(reducedQueryPhase, numShards)
                    : null;
                final List<Integer>[] docIdsToLoad = SearchPhaseController.fillDocIdsToLoad(numShards, scoreDocs);
                final CountedCollector<FetchSearchResult> counter = new CountedCollector<>(
                    fetchResults,
                    docIdsToLoad.length, // we count down every shard in the result no matter if we got any results or not
                    () -> moveToNextPhase(fetchResults.getAtomicArray(), reducedQueryPhase),
                    context
                );
                for (int i = 0; i < docIdsToLoad.length; i++) {
                    List<Integer> entry = docIdsToLoad[i];
                    RankDocShardInfo rankDocs = rankDocsPerShard == null || rankDocsPerShard.get(i).isEmpty()
                        ? null
                        : new RankDocShardInfo(rankDocsPerShard.get(i));
                    SearchPhaseResult shardPhaseResult = searchPhaseShardResults.get(i);
                    if (entry == null) { // no results for this shard ID
                        if (shardPhaseResult != null) {
                            // if we got some hits from this shard we have to release the context there
                            // we do this as we go since it will free up resources and passing on the request on the
                            // transport layer is cheap.
                            releaseIrrelevantSearchContext(shardPhaseResult, context);
                            progressListener.notifyFetchResult(i);
                        }
                        // in any case we count down this result since we don't talk to this shard anymore
                        counter.countDown();
                    } else {
                        executeFetch(
                            shardPhaseResult,
                            counter,
                            entry,
                            rankDocs,
                            (lastEmittedDocPerShard != null) ? lastEmittedDocPerShard[i] : null
                        );
                    }
                }
            }
        }
    }

    private List<Map<Integer, RankDoc>> splitRankDocsPerShard(ScoreDoc[] scoreDocs, int numShards) {
        List<Map<Integer, RankDoc>> rankDocsPerShard = new ArrayList<>(numShards);
        for (int i = 0; i < numShards; i++) {
            rankDocsPerShard.add(new HashMap<>());
        }
        for (ScoreDoc scoreDoc : scoreDocs) {
            assert scoreDoc instanceof RankDoc : "ScoreDoc is not a RankDoc";
            assert scoreDoc.shardIndex >= 0 && scoreDoc.shardIndex <= numShards;
            RankDoc rankDoc = (RankDoc) scoreDoc;
            Map<Integer, RankDoc> shardScoreDocs = rankDocsPerShard.get(rankDoc.shardIndex);
            shardScoreDocs.put(rankDoc.doc, rankDoc);
        }
        return rankDocsPerShard;
    }

    private boolean assertConsistentWithQueryAndFetchOptimization() {
        var phaseResults = searchPhaseShardResults.asList();
        assert phaseResults.isEmpty() || phaseResults.get(0).fetchResult() != null
            : "phaseResults empty [" + phaseResults.isEmpty() + "], single result: " + phaseResults.get(0).fetchResult();
        return true;
    }

    private void executeFetch(
        SearchPhaseResult shardPhaseResult,
        final CountedCollector<FetchSearchResult> counter,
        final List<Integer> entry,
        final RankDocShardInfo rankDocs,
        ScoreDoc lastEmittedDocForShard
    ) {
        final SearchShardTarget shardTarget = shardPhaseResult.getSearchShardTarget();
        final int shardIndex = shardPhaseResult.getShardIndex();
        final ShardSearchContextId contextId = shardPhaseResult.queryResult() != null
            ? shardPhaseResult.queryResult().getContextId()
            : shardPhaseResult.rankFeatureResult().getContextId();
        context.getSearchTransport()
            .sendExecuteFetch(
                context.getConnection(shardTarget.getClusterAlias(), shardTarget.getNodeId()),
                new ShardFetchSearchRequest(
                    context.getOriginalIndices(shardPhaseResult.getShardIndex()),
                    contextId,
                    shardPhaseResult.getShardSearchRequest(),
                    entry,
                    rankDocs,
                    lastEmittedDocForShard,
                    shardPhaseResult.getRescoreDocIds(),
                    aggregatedDfs
                ),
                context.getTask(),
                new SearchActionListener<>(shardTarget, shardIndex) {
                    @Override
                    public void innerOnResponse(FetchSearchResult result) {
                        try {
                            progressListener.notifyFetchResult(shardIndex);
                            counter.onResult(result);
                        } catch (Exception e) {
                            context.onPhaseFailure(FetchSearchPhase.this, "", e);
                        }
                    }

                    @Override
                    public void onFailure(Exception e) {
                        try {
                            logger.debug(() -> "[" + contextId + "] Failed to execute fetch phase", e);
                            progressListener.notifyFetchFailure(shardIndex, shardTarget, e);
                            counter.onFailure(shardIndex, shardTarget, e);
                        } finally {
                            // the search context might not be cleared on the node where the fetch was executed for example
                            // because the action was rejected by the thread pool. in this case we need to send a dedicated
                            // request to clear the search context.
                            releaseIrrelevantSearchContext(shardPhaseResult, context);
                        }
                    }
                }
            );
    }

    private void moveToNextPhase(
        AtomicArray<? extends SearchPhaseResult> fetchResultsArr,
        SearchPhaseController.ReducedQueryPhase reducedQueryPhase
    ) {
        var resp = SearchPhaseController.merge(context.getRequest().scroll() != null, reducedQueryPhase, fetchResultsArr);
        context.addReleasable(resp::decRef);
        fetchResults.close();
        context.executeNextPhase(this, nextPhaseFactory.apply(resp, searchPhaseShardResults));
    }

    private boolean shouldExplainRankScores(SearchRequest request) {
        return request.source() != null
            && request.source().explain() != null
            && request.source().explain()
            && request.source().rankBuilder() != null;
    }

}
