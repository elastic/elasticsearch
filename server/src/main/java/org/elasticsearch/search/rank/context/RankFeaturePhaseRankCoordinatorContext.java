/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.rank.context;

import org.apache.lucene.search.ScoreDoc;
import org.elasticsearch.search.rank.feature.RankFeatureResult;

import java.util.List;
import java.util.function.Consumer;

import static org.elasticsearch.search.SearchService.DEFAULT_FROM;
import static org.elasticsearch.search.SearchService.DEFAULT_SIZE;

/*
 * {@code RankFeaturePhaseRankCoordinatorContext} is a base class that runs on the coordinating node and is responsible for retrieving
 * {@code window_size} total results from all shards, rank them, and then produce a final paginated response of [from, from+size] results.
 */
public abstract class RankFeaturePhaseRankCoordinatorContext {

    protected final int size;
    protected final int from;
    protected final int rankWindowSize;

    public RankFeaturePhaseRankCoordinatorContext(int size, int from, int rankWindowSize) {
        this.size = size < 0 ? DEFAULT_SIZE : size;
        this.from = from < 0 ? DEFAULT_FROM : from;
        this.rankWindowSize = rankWindowSize;
    }

    /**
     * This is used to re-rank the results once we have retrieved all {@code RankFeatureResult} shard responses
     * during the {@code RankFeaturePhase} phase. This method computes a reranked list of global top `rank_window_size` results,
     * based on the feature data extracted by each of the shards.
     * To support non-blocking async operations, once we generate a final array of {@link ScoreDoc} results, we pass them
     * to the {@param onFinish} consumer to proceed with the next steps, as defined by the caller.
     *
     * @param phaseResultsPerShard a list of the appropriate phase results from each shard
     * @param onFinish a consumer to be called once the global ranking is complete
     */
    public abstract void rankGlobalResults(List<RankFeatureResult> phaseResultsPerShard, Consumer<ScoreDoc[]> onFinish);
}
