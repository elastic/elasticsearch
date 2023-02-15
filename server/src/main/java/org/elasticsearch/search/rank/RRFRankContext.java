/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.rank;

import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.elasticsearch.action.search.SearchPhaseController;
import org.elasticsearch.action.search.SearchPhaseController.SortedTopDocs;
import org.elasticsearch.action.search.SearchPhaseController.TopDocsStats;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.query.QueryPhase;
import org.elasticsearch.search.query.QueryPhaseExecutionException;
import org.elasticsearch.search.query.QuerySearchResult;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RRFRankContext implements RankContext {

    private final int windowSize;
    private final int rankConstant;

    private RRFRankQuery rrfRankQuery;

    private int size;

    public RRFRankContext(int windowSize, int rankConstant) {
        this.windowSize = windowSize;
        this.rankConstant = rankConstant;
    }

    @Override
    public void setQuery(Query query) {
        if (query instanceof RRFRankQuery rrfRankQuery) {
            this.rrfRankQuery = rrfRankQuery;
        }
    }

    @Override
    public void executeQuery(SearchContext searchContext) {
        assert rrfRankQuery != null;

        try {
            RRFRankSearchContext rrfRankSearchContext = new RRFRankSearchContext(searchContext);
            QueryPhase.executeInternal(rrfRankSearchContext);

            List<TopDocs> rrfRankResults = new ArrayList<>();
            rrfRankSearchContext.windowSize(windowSize);
            for (Query query : rrfRankQuery.getQueries()) {
                rrfRankSearchContext.rrfRankQuery(query);
                QueryPhase.executeInternal(rrfRankSearchContext);
                rrfRankResults.add(rrfRankSearchContext.queryResult().topDocs().topDocs);
            }
            RankShardResult rankShardResult = new RRFRankShardResult(rrfRankResults);
            searchContext.queryResult().setRankShardResult(rankShardResult);
        } catch (QueryPhaseExecutionException e) {
            throw e;
        } catch (Exception e) {
            throw new QueryPhaseExecutionException(searchContext.shardTarget(), "Failed to execute main query", e);
        }
    }

    @Override
    public void setSize(int size) {
        this.size = size == -1 ? 10 : size;
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
        long fetchHits = -1;
        for (List<TopDocs> utd : unmergedTopDocs) {
            fetchHits = Math.min(fetchHits == -1 ? Long.MAX_VALUE : fetchHits, utd.stream().mapToInt(td -> td.scoreDocs.length).sum());
            mergedTopDocs.add(SearchPhaseController.sortDocs(false, utd, 0, windowSize, List.of()));
        }
        assert topDocsStats.fetchHits == 0;
        if (fetchHits != -1) {
            topDocsStats.fetchHits = fetchHits;
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
        RRFRankResult[] topRankResults = new RRFRankResult[Math.min(size, allRankResults.length)];
        for (int rank = 0; rank < topRankResults.length; ++rank) {
            topRankResults[rank] = allRankResults[rank];
            topRankResults[rank].rank = rank + 1;
        }

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
