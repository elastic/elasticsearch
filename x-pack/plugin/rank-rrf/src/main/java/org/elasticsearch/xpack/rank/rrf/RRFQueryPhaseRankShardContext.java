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
import org.elasticsearch.search.rank.context.QueryPhaseRankShardContext;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.search.rank.RankDoc.NO_RANK;

/**
 * Executes queries and generates results on the shard for RRF.
 */
public class RRFQueryPhaseRankShardContext extends QueryPhaseRankShardContext {

    private final int rankConstant;

    public RRFQueryPhaseRankShardContext(List<Query> queries, int rankWindowSize, int rankConstant) {
        super(queries, rankWindowSize);
        this.rankConstant = rankConstant;
    }

    @Override
    public RRFRankShardResult combineQueryPhaseResults(List<TopDocs> rankResults) {
        // combine the disjointed sets of TopDocs into a single set or RRFRankDocs
        // each RRFRankDoc will have both the position and score for each query where
        // it was within the result set for that query
        // if a doc isn't part of a result set its position will be NO_RANK [0] and
        // its score is [0f]
        int queries = rankResults.size();
        Map<Integer, RRFRankDoc> docsToRankResults = Maps.newMapWithExpectedSize(rankWindowSize);
        int index = 0;
        for (TopDocs rrfRankResult : rankResults) {
            int rank = 1;
            for (ScoreDoc scoreDoc : rrfRankResult.scoreDocs) {
                final int findex = index;
                final int frank = rank;
                docsToRankResults.compute(scoreDoc.doc, (key, value) -> {
                    if (value == null) {
                        value = new RRFRankDoc(scoreDoc.doc, scoreDoc.shardIndex, queries, rankConstant);
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
            assert rrf1.positions.length == rrf2.positions.length;
            for (int qi = 0; qi < rrf1.positions.length; ++qi) {
                if (rrf1.positions[qi] != NO_RANK && rrf2.positions[qi] != NO_RANK) {
                    if (rrf1.scores[qi] != rrf2.scores[qi]) {
                        return rrf1.scores[qi] < rrf2.scores[qi] ? 1 : -1;
                    }
                } else if (rrf1.positions[qi] != NO_RANK) {
                    return -1;
                } else if (rrf2.positions[qi] != NO_RANK) {
                    return 1;
                }
            }
            return rrf1.doc < rrf2.doc ? -1 : 1;
        });
        // trim the results if needed, otherwise each shard will always return `rank_window_size` results.
        // pagination and all else will happen on the coordinator when combining the shard responses
        RRFRankDoc[] topResults = new RRFRankDoc[Math.min(rankWindowSize, sortedResults.length)];
        for (int rank = 0; rank < topResults.length; ++rank) {
            topResults[rank] = sortedResults[rank];
            topResults[rank].rank = rank + 1;
            topResults[rank].score = Float.NaN;
        }
        return new RRFRankShardResult(rankResults.size(), topResults);
    }

    public int rankConstant() {
        return rankConstant;
    }
}
