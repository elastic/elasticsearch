/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.action.search;

import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.search.SearchPhaseResult;
import org.elasticsearch.search.dfs.AggregatedDfs;

/**
 * This search phase is responsible for executing any re-ranking needed for the given search request, iff that is applicable.
 * It starts by retrieving {code num_shards * window_size} results from the query phase and reduces them to a global list of
 * the top {@code window_size} results. It then reaches out to the shards to extract the needed feature data,
 * and finally passes all this information to the appropriate {@code RankFeatureRankCoordinatorContext} which is responsible for reranking
 * the results. If no rank query is specified, it proceeds directly to the next phase (FetchSearchPhase) by first reducing the results.
 */
public final class RankFeaturePhase extends SearchPhase {

    private final SearchPhaseContext context;
    private final SearchPhaseResults<SearchPhaseResult> queryPhaseResults;
    private final SearchPhaseResults<SearchPhaseResult> rankPhaseResults;

    private final AggregatedDfs aggregatedDfs;

    RankFeaturePhase(SearchPhaseResults<SearchPhaseResult> queryPhaseResults, AggregatedDfs aggregatedDfs, SearchPhaseContext context) {
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

    private void innerRun() throws Exception {
        // other than running reduce, this is currently close to a no-op
        SearchPhaseController.ReducedQueryPhase reducedQueryPhase = queryPhaseResults.reduce();
        moveToNextPhase(queryPhaseResults, reducedQueryPhase);
    }

    private void moveToNextPhase(
        SearchPhaseResults<SearchPhaseResult> phaseResults,
        SearchPhaseController.ReducedQueryPhase reducedQueryPhase
    ) {
        context.executeNextPhase(this, new FetchSearchPhase(phaseResults, aggregatedDfs, context, reducedQueryPhase));
    }
}
