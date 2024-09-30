/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.rank.rerank;

import org.apache.lucene.search.ScoreDoc;
import org.elasticsearch.action.search.SearchPhaseController;
import org.elasticsearch.search.query.QuerySearchResult;
import org.elasticsearch.search.rank.context.QueryPhaseRankCoordinatorContext;
import org.elasticsearch.search.rank.feature.RankFeatureDoc;
import org.elasticsearch.search.rank.feature.RankFeatureShardPhase;
import org.elasticsearch.search.rank.feature.RankFeatureShardResult;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * The {@link RerankingQueryPhaseRankCoordinatorContext} provides the main functionality for sorting the initial query phase results
 * based on their score, and trim them down to a global `rank_window_size`-sized list. These results could later be sent to each
 * of the shards to execute the {@link RankFeatureShardPhase} shard phase, and then they will be merged and ranked again
 * as part of the {@link org.elasticsearch.search.rank.context.RankFeaturePhaseRankCoordinatorContext}.
 */
public class RerankingQueryPhaseRankCoordinatorContext extends QueryPhaseRankCoordinatorContext {

    public RerankingQueryPhaseRankCoordinatorContext(int windowSize) {
        super(windowSize);
    }

    @Override
    public ScoreDoc[] rankQueryPhaseResults(List<QuerySearchResult> querySearchResults, SearchPhaseController.TopDocsStats topDocStats) {
        List<RankFeatureDoc> rankDocs = new ArrayList<>();
        for (int i = 0; i < querySearchResults.size(); i++) {
            QuerySearchResult querySearchResult = querySearchResults.get(i);
            RankFeatureShardResult shardResult = (RankFeatureShardResult) querySearchResult.getRankShardResult();
            if (shardResult == null) {
                continue;
            }
            for (RankFeatureDoc frd : shardResult.rankFeatureDocs) {
                frd.shardIndex = i;
                rankDocs.add(frd);
            }
        }
        // no support for sort field at the moment
        rankDocs.sort(Comparator.comparing((RankFeatureDoc doc) -> doc.score).reversed());
        RankFeatureDoc[] topResults = rankDocs.stream().limit(rankWindowSize).toArray(RankFeatureDoc[]::new);

        assert topDocStats.fetchHits == 0;
        topDocStats.fetchHits = topResults.length;

        return topResults;
    }
}
