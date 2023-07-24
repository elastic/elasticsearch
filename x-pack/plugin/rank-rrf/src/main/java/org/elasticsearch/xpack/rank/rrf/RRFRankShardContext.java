/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.rank.rrf;

import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.search.rank.RankShardContext;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Executes queries and generates results on the shard for RRF.
 */
public class RRFRankShardContext extends RankShardContext {

    protected final int rankConstant;

    public RRFRankShardContext(List<Query> queries, int from, int windowSize, int rankConstant) {
        super(queries, from, windowSize);
        this.rankConstant = rankConstant;
    }

    @Override
    public RRFRankShardResult combine(List<TopDocs> rankResults) {
        // combine the disjointed sets of TopDocs into a single set or RRFRankDocs
        // each RRFRankDoc will have both the position and score for each query where
        // it was within the result set for that query
        // if a doc isn't part of a result set its position will be NO_RANK [0] and
        // its score is [0f]
        int queries = rankResults.size();
        Map<Integer, RRFRankDoc> docsToRankResults = Maps.newMapWithExpectedSize(windowSize);
        int index = 0;
        for (TopDocs rrfRankResult : rankResults) {
            int rank = 1;
            for (ScoreDoc scoreDoc : rrfRankResult.scoreDocs) {
                final int findex = index;
                final int frank = rank;
                docsToRankResults.compute(scoreDoc.doc, (key, value) -> {
                    if (value == null) {
                        value = new RRFRankDoc(scoreDoc.doc, scoreDoc.shardIndex, queries);
                    }

                    // calculate the current rrf score for this document
                    // later used to sort and covert to a rank
                    value.score += 1.0f / (rankConstant + frank);

                    // record the position for each query
                    // for explain and debugging
                    value.positions[findex] = frank - 1;

                    // record the score for each query
                    // used to later re-rank on the coordinator
                    value.scores[findex] = scoreDoc.score;

                    return value;
                });
                ++rank;
            }
            ++index;
        }

        // sort the results based on rrf score, tiebreaker based on smaller doc id
        RRFRankDoc[] sortedResults = docsToRankResults.values().toArray(RRFRankDoc[]::new);
        Arrays.sort(sortedResults, (RRFRankDoc rrf1, RRFRankDoc rrf2) -> {
            if (rrf1.score != rrf2.score) {
                return rrf1.score < rrf2.score ? 1 : -1;
            }
            return rrf1.doc < rrf2.doc ? -1 : 1;
        });
        // trim the results to window size
        RRFRankDoc[] topResults = new RRFRankDoc[Math.min(windowSize + from, sortedResults.length)];
        for (int rank = 0; rank < topResults.length; ++rank) {
            topResults[rank] = sortedResults[rank];
            topResults[rank].rank = rank + 1;
            topResults[rank].score = Float.NaN;
        }
        return new RRFRankShardResult(rankResults.size(), topResults);
    }
}
