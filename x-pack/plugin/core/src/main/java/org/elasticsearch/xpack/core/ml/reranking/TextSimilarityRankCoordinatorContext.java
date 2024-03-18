/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.reranking;

import org.apache.lucene.search.ScoreDoc;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.search.query.QuerySearchResult;
import org.elasticsearch.search.rank.RankContext;
import org.elasticsearch.search.rank.RankCoordinatorContext;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.action.search.SearchPhaseController.SortedTopDocs;
import static org.elasticsearch.action.search.SearchPhaseController.TopDocsStats;

public class TextSimilarityRankCoordinatorContext extends RankCoordinatorContext {

    public TextSimilarityRankCoordinatorContext(int size, int from, int windowSize) {
        super(size, from, windowSize);
    }

    public TextSimilarityRankCoordinatorContext(RankContext rankContext) {
        super(rankContext);
    }

    @Override
    public SortedTopDocs rank(List<QuerySearchResult> querySearchResults, TopDocsStats topDocsStats) {
        // DFS: collect global term stats from shards, for 100% accuracy

        // Top K trimming happens here (end of query phase)
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
        // TODO: get inference entity ID from reranker config; get text from document sources
        var req = new InferenceAction.Request(TaskType.SPARSE_EMBEDDING, "my-elser-model",
            List.of("these are not the droids you're looking for", "move along"), Map.of(), InputType.SEARCH);

        this.rankContext.getClient().execute(InferenceAction.INSTANCE, req, new ActionListener<>() {
            @Override
            public void onResponse(InferenceAction.Response response) {
                System.out.println(response);
            }

            @Override
            public void onFailure(Exception e) {
                System.out.println(e);
            }
        });

        docs.forEach(doc -> doc.score = (float) Math.random());
        docs.sort((o1, o2) -> (int) Math.signum(o2.score - o1.score));
    }

}
