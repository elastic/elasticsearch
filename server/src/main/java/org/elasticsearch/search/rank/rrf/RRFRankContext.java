/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.rank.rrf;

import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.elasticsearch.action.search.SearchPhaseController;
import org.elasticsearch.action.search.SearchPhaseController.SortedTopDocs;
import org.elasticsearch.action.search.SearchPhaseController.TopDocsStats;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.query.QuerySearchResult;
import org.elasticsearch.search.rank.RankContext;
import org.elasticsearch.search.rank.RankShardResult;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RRFRankContext extends RankContext {

    private final int windowSize;
    private final int rankConstant;

    public RRFRankContext(List<QueryBuilder> queryBuilders, int size, int from, int windowSize, int rankConstant) {
        super(queryBuilders, size, from);
        this.windowSize = windowSize;
        this.rankConstant = rankConstant;
    }

    @Override
    public SortedTopDocs rank(List<QuerySearchResult> querySearchResults, TopDocsStats topDocsStats) {
        List<List<TopDocs>> unmergedTopDocs = new ArrayList<>();
        int shardCount = -1;

        for (QuerySearchResult querySearchResult : querySearchResults) {
            if (querySearchResult.searchTimedOut()) {
                topDocsStats.timedOut = true;
                continue;
            }
            if (querySearchResult.terminatedEarly() != null && querySearchResult.terminatedEarly()) {
                topDocsStats.terminatedEarly = true;
            }

            RankShardResult rankShardResult = querySearchResult.getRankShardResult();
            assert rankShardResult instanceof RRFRankShardResult;
            RRFRankShardResult rrfRankShardResult = (RRFRankShardResult) rankShardResult;
            List<TopDocs> shardTopDocs = rrfRankShardResult.getTopDocs();

            if (shardCount == -1) {
                shardCount = shardTopDocs.size();
                for (int index = 0; index < shardCount; ++index) {
                    unmergedTopDocs.add(new ArrayList<>());
                }
            }
            assert shardCount == shardTopDocs.size();

            for (int index = 0; index < shardCount; ++index) {
                TopDocs std = shardTopDocs.get(index);
                SearchPhaseController.setShardIndex(std, querySearchResult.getShardIndex());
                unmergedTopDocs.get(index).add(std);
            }
        }

        List<SortedTopDocs> mergedTopDocs = new ArrayList<>();
        for (List<TopDocs> utd : unmergedTopDocs) {
            mergedTopDocs.add(SearchPhaseController.sortDocs(false, utd, 0, windowSize, List.of()));
        }

        if (mergedTopDocs.isEmpty()) {
            return SortedTopDocs.EMPTY;
        }

        Map<String, RRFRankResult> docsToRankResults = new HashMap<>();
        int index = 0;
        for (SortedTopDocs mtd : mergedTopDocs) {
            int rank = 0;
            for (ScoreDoc scoreDoc : mtd.scoreDocs()) {
                ++rank;
                final int findex = index;
                final int frank = rank;
                docsToRankResults.compute(scoreDoc.doc + ":" + scoreDoc.shardIndex, (key, value) -> {
                    if (value == null) {
                        value = new RRFRankResult(scoreDoc.doc, scoreDoc.shardIndex, mergedTopDocs.size());
                    }

                    value.score += 1.0f / (rankConstant + frank);
                    value.positions[findex] = frank;
                    value.scores[findex] = scoreDoc.score;

                    return value;
                });
            }
            ++index;
        }

        RRFRankResult[] allRankResults = docsToRankResults.values().toArray(RRFRankResult[]::new);
        Arrays.sort(allRankResults, Comparator.comparingDouble(rr -> -rr.score));
        RRFRankResult[] topRankResults = new RRFRankResult[Math.min(size + from, allRankResults.length)];
        for (int rank = 0; rank < topRankResults.length; ++rank) {
            topRankResults[rank] = allRankResults[rank];
            topRankResults[rank].rank = rank + 1;
        }
        assert topDocsStats.fetchHits == 0;
        topDocsStats.fetchHits = topRankResults.length;

        SortedTopDocs copy = mergedTopDocs.get(1);
        return new SortedTopDocs(
            topRankResults,
            copy.isSortedByField(),
            copy.sortFields(),
            copy.collapseField(),
            copy.collapseValues(),
            copy.numberOfCompletionsSuggestions()
        );
    }

    public void decorateSearchHit(ScoreDoc scoreDoc, SearchHit searchHit) {
        assert scoreDoc instanceof RRFRankResult;
        RRFRankResult rankResult = (RRFRankResult) scoreDoc;
        searchHit.setRank(rankResult.rank);
        searchHit.setRankResult(rankResult);
    }
}
