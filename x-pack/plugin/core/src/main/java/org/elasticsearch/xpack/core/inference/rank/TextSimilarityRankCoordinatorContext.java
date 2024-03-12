/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.inference.rank;

import org.apache.lucene.search.ScoreDoc;
import org.elasticsearch.search.query.QuerySearchResult;
import org.elasticsearch.search.rank.RankCoordinatorContext;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.elasticsearch.action.search.SearchPhaseController.SortedTopDocs;
import static org.elasticsearch.action.search.SearchPhaseController.TopDocsStats;

public class TextSimilarityRankCoordinatorContext extends RankCoordinatorContext {

    public TextSimilarityRankCoordinatorContext(int size, int from, int windowSize) {
        super(size, from, windowSize);
    }
//
//    public TextSimilarityRankCoordinatorContext(RankContext rankContext) {
//        super(rankContext);
//    }

    @Override
    public SortedTopDocs rank(List<QuerySearchResult> querySearchResults, TopDocsStats topDocsStats) {
        // QSR: one per shard, has shard index + metadata
        // fetch phase organizes docs per shard
        // topResults: combined top K from all shards
        List<TextSimilarityRankDoc> topResults = new ArrayList<>();

        for (QuerySearchResult querySearchResult : querySearchResults) {
            assert querySearchResult.getRankShardResult() instanceof TextSimilarityRankShardResult;
            Arrays.stream(((TextSimilarityRankShardResult) querySearchResult.getRankShardResult()).rankDocs).forEach(rankDoc -> {
                rankDoc.shardIndex = querySearchResult.getShardIndex();
                //rankDoc.rank = 1;
                topResults.add(rankDoc);
            });
        }

        rank(topResults);

        topDocsStats.fetchHits = topResults.size();
        return new SortedTopDocs(topResults.toArray(new ScoreDoc[0]), false, null, null, null, 0);
    }

    public void rank(List<TextSimilarityRankDoc> docs) {
        //this.rankContext.getClient().execute()

//        getClient().execute(
//            DeleteDataStreamAction.INSTANCE,
//            deleteReq,
//            listener.delegateFailureAndWrap((l, response) -> l.onResponse(null))
//        );


        //rankContext.getClient().execute()

        docs.forEach(doc -> doc.score = (float) Math.random());
        docs.sort((o1, o2) -> (int) Math.signum(o2.score - o1.score));
    }

}
