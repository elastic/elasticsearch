/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.action.search;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.search.ScoreDoc;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ThreadedActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.search.SearchPhaseResult;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.dfs.AggregatedDfs;
import org.elasticsearch.search.internal.ShardSearchContextId;
import org.elasticsearch.search.rank.context.RankFeaturePhaseRankCoordinatorContext;
import org.elasticsearch.search.rank.feature.RankFeatureDoc;
import org.elasticsearch.search.rank.feature.RankFeatureResult;
import org.elasticsearch.search.rank.feature.RankFeatureShardRequest;

import java.util.List;

/**
 * This search phase is responsible for executing any re-ranking needed for the given search request, iff that is applicable.
 * It starts by retrieving {@code num_shards * rank_window_size} results from the query phase and reduces them to a global list of
 * the top {@code rank_window_size} results. It then reaches out to the shards to extract the needed feature data,
 * and finally passes all this information to the appropriate {@code RankFeatureRankCoordinatorContext} which is responsible for reranking
 * the results. If no rank query is specified, it proceeds directly to the next phase (FetchSearchPhase) by first reducing the results.
 */
public class RankFeaturePhase extends SearchPhase {

    private static final Logger logger = LogManager.getLogger(RankFeaturePhase.class);
    private final SearchPhaseContext context;
    final SearchPhaseResults<SearchPhaseResult> queryPhaseResults;
    final SearchPhaseResults<SearchPhaseResult> rankPhaseResults;
    private final AggregatedDfs aggregatedDfs;
    private final SearchProgressListener progressListener;
    private final Client client;

    RankFeaturePhase(
        SearchPhaseResults<SearchPhaseResult> queryPhaseResults,
        AggregatedDfs aggregatedDfs,
        SearchPhaseContext context,
        Client client
    ) {
        super("rank-feature");
        if (context.getNumShards() != queryPhaseResults.getNumShards()) {
            throw new IllegalStateException(
                "number of shards must match the length of the query results but doesn't:"
                    + context.getNumShards()
                    + "!="
                    + queryPhaseResults.getNumShards()
            );
        }
        this.context = context;
        this.queryPhaseResults = queryPhaseResults;
        this.aggregatedDfs = aggregatedDfs;
        this.rankPhaseResults = new ArraySearchPhaseResults<>(context.getNumShards());
        context.addReleasable(rankPhaseResults);
        this.progressListener = context.getTask().getProgressListener();
        this.client = client;
    }

    @Override
    public void run() {
        context.execute(new AbstractRunnable() {
            @Override
            protected void doRun() throws Exception {
                // we need to reduce the results at this point instead of fetch phase, so we fork this process similarly to how
                // was set up at FetchSearchPhase.

                // we do the heavy lifting in this inner run method where we reduce aggs etc
                innerRun();
            }

            @Override
            public void onFailure(Exception e) {
                context.onPhaseFailure(RankFeaturePhase.this, "", e);
            }
        });
    }

    void innerRun() throws Exception {
        // if the RankBuilder specifies a QueryPhaseCoordinatorContext, it will be called as part of the reduce call
        // to operate on the first `rank_window_size * num_shards` results and merge them appropriately.
        SearchPhaseController.ReducedQueryPhase reducedQueryPhase = queryPhaseResults.reduce();
        RankFeaturePhaseRankCoordinatorContext rankFeaturePhaseRankCoordinatorContext = coordinatorContext(context.getRequest().source());
        if (rankFeaturePhaseRankCoordinatorContext != null) {
            ScoreDoc[] queryScoreDocs = reducedQueryPhase.sortedTopDocs().scoreDocs(); // rank_window_size
            final List<Integer>[] docIdsToLoad = SearchPhaseController.fillDocIdsToLoad(context.getNumShards(), queryScoreDocs);
            final CountedCollector<SearchPhaseResult> rankRequestCounter = new CountedCollector<>(
                rankPhaseResults,
                context.getNumShards(),
                () -> onPhaseDone(rankFeaturePhaseRankCoordinatorContext, reducedQueryPhase),
                context
            );

            // we send out a request to each shard in order to fetch the needed feature info
            for (int i = 0; i < docIdsToLoad.length; i++) {
                List<Integer> entry = docIdsToLoad[i];
                SearchPhaseResult queryResult = queryPhaseResults.getAtomicArray().get(i);
                if (entry == null || entry.isEmpty()) {
                    if (queryResult != null) {
                        releaseIrrelevantSearchContext(queryResult, context);
                        progressListener.notifyRankFeatureResult(i);
                    }
                    rankRequestCounter.countDown();
                } else {
                    executeRankFeatureShardPhase(queryResult, rankRequestCounter, entry);
                }
            }
        } else {
            moveToNextPhase(queryPhaseResults, reducedQueryPhase);
        }
    }

    private RankFeaturePhaseRankCoordinatorContext coordinatorContext(SearchSourceBuilder source) {
        return source == null || source.rankBuilder() == null
            ? null
            : context.getRequest()
                .source()
                .rankBuilder()
                .buildRankFeaturePhaseCoordinatorContext(
                    context.getRequest().source().size(),
                    context.getRequest().source().from(),
                    client
                );
    }

    private void executeRankFeatureShardPhase(
        SearchPhaseResult queryResult,
        final CountedCollector<SearchPhaseResult> rankRequestCounter,
        final List<Integer> entry
    ) {
        final SearchShardTarget shardTarget = queryResult.queryResult().getSearchShardTarget();
        final ShardSearchContextId contextId = queryResult.queryResult().getContextId();
        final int shardIndex = queryResult.getShardIndex();
        context.getSearchTransport()
            .sendExecuteRankFeature(
                context.getConnection(shardTarget.getClusterAlias(), shardTarget.getNodeId()),
                new RankFeatureShardRequest(
                    context.getOriginalIndices(queryResult.getShardIndex()),
                    queryResult.getContextId(),
                    queryResult.getShardSearchRequest(),
                    entry
                ),
                context.getTask(),
                new SearchActionListener<>(shardTarget, shardIndex) {
                    @Override
                    protected void innerOnResponse(RankFeatureResult response) {
                        try {
                            progressListener.notifyRankFeatureResult(shardIndex);
                            rankRequestCounter.onResult(response);
                        } catch (Exception e) {
                            context.onPhaseFailure(RankFeaturePhase.this, "", e);
                        }
                    }

                    @Override
                    public void onFailure(Exception e) {
                        try {
                            logger.debug(() -> "[" + contextId + "] Failed to execute rank phase", e);
                            progressListener.notifyRankFeatureFailure(shardIndex, shardTarget, e);
                            rankRequestCounter.onFailure(shardIndex, shardTarget, e);
                        } finally {
                            releaseIrrelevantSearchContext(queryResult, context);
                        }
                    }
                }
            );
    }

    private void onPhaseDone(
        RankFeaturePhaseRankCoordinatorContext rankFeaturePhaseRankCoordinatorContext,
        SearchPhaseController.ReducedQueryPhase reducedQueryPhase
    ) {
        assert rankFeaturePhaseRankCoordinatorContext != null;
        ThreadedActionListener<RankFeatureDoc[]> rankResultListener = new ThreadedActionListener<>(context, new ActionListener<>() {
            @Override
            public void onResponse(RankFeatureDoc[] docsWithUpdatedScores) {
                RankFeatureDoc[] topResults = rankFeaturePhaseRankCoordinatorContext.rankAndPaginate(docsWithUpdatedScores);
                SearchPhaseController.ReducedQueryPhase reducedRankFeaturePhase = newReducedQueryPhaseResults(
                    reducedQueryPhase,
                    topResults
                );
                moveToNextPhase(rankPhaseResults, reducedRankFeaturePhase);
            }

            @Override
            public void onFailure(Exception e) {
                context.onPhaseFailure(RankFeaturePhase.this, "Computing updated ranks for results failed", e);
            }
        });
        rankFeaturePhaseRankCoordinatorContext.computeRankScoresForGlobalResults(
            rankPhaseResults.getAtomicArray().asList().stream().map(SearchPhaseResult::rankFeatureResult).toList(),
            rankResultListener
        );
    }

    private SearchPhaseController.ReducedQueryPhase newReducedQueryPhaseResults(
        SearchPhaseController.ReducedQueryPhase reducedQueryPhase,
        ScoreDoc[] scoreDocs
    ) {

        return new SearchPhaseController.ReducedQueryPhase(
            reducedQueryPhase.totalHits(),
            reducedQueryPhase.fetchHits(),
            maxScore(scoreDocs),
            reducedQueryPhase.timedOut(),
            reducedQueryPhase.terminatedEarly(),
            reducedQueryPhase.suggest(),
            reducedQueryPhase.aggregations(),
            reducedQueryPhase.profileBuilder(),
            new SearchPhaseController.SortedTopDocs(scoreDocs, false, null, null, null, 0),
            reducedQueryPhase.sortValueFormats(),
            reducedQueryPhase.queryPhaseRankCoordinatorContext(),
            reducedQueryPhase.numReducePhases(),
            reducedQueryPhase.size(),
            reducedQueryPhase.from(),
            reducedQueryPhase.isEmptyResult()
        );
    }

    private float maxScore(ScoreDoc[] scoreDocs) {
        float maxScore = Float.NaN;
        for (ScoreDoc scoreDoc : scoreDocs) {
            if (Float.isNaN(maxScore) || scoreDoc.score > maxScore) {
                maxScore = scoreDoc.score;
            }
        }
        return maxScore;
    }

    void moveToNextPhase(SearchPhaseResults<SearchPhaseResult> phaseResults, SearchPhaseController.ReducedQueryPhase reducedQueryPhase) {
        context.executeNextPhase(this, new FetchSearchPhase(phaseResults, aggregatedDfs, context, reducedQueryPhase));
    }
}
