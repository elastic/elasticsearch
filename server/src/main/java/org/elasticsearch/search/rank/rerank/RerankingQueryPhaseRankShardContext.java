/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.rank.rerank;

import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.elasticsearch.search.rank.RankShardResult;
import org.elasticsearch.search.rank.context.QueryPhaseRankShardContext;
import org.elasticsearch.search.rank.feature.RankFeatureDoc;
import org.elasticsearch.search.rank.feature.RankFeatureShardResult;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The {@link RerankingQueryPhaseRankShardContext} is responsible for combining the different shard-level query results, and
 * then pack them to a {@code RankFeatureShardResult} to return to the coordinator. If a document is found in more than one queries, we
 * only keep the max score for that document. This is to be treated with care, as different queries might have different score ranges that
 * could affect the final ranking.
 */
public class RerankingQueryPhaseRankShardContext extends QueryPhaseRankShardContext {

    public RerankingQueryPhaseRankShardContext(List<Query> queries, int windowSize) {
        super(queries, windowSize);
    }

    @Override
    public RankShardResult combineQueryPhaseResults(List<TopDocs> rankResults) {
        Map<Integer, RankFeatureDoc> rankDocs = new HashMap<>();
        rankResults.forEach(topDocs -> {
            for (ScoreDoc scoreDoc : topDocs.scoreDocs) {
                rankDocs.compute(scoreDoc.doc, (key, value) -> {
                    if (value == null) {
                        return new RankFeatureDoc(scoreDoc.doc, scoreDoc.score, scoreDoc.shardIndex);
                    } else {
                        value.score = Math.max(scoreDoc.score, rankDocs.get(scoreDoc.doc).score);
                        return value;
                    }
                });
            }
        });
        RankFeatureDoc[] sortedResults = rankDocs.values().toArray(RankFeatureDoc[]::new);
        Arrays.sort(sortedResults, (o1, o2) -> Float.compare(o2.score, o1.score));
        return new RankFeatureShardResult(sortedResults);
    }
}
