/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.rank.rrf;

import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.lucene.search.TopDocsAndMaxScore;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.query.QueryPhase;
import org.elasticsearch.search.query.QueryPhaseExecutionException;
import org.elasticsearch.search.query.QuerySearchResult;
import org.elasticsearch.search.rank.RankShardContext;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.search.internal.SearchContext.TRACK_TOTAL_HITS_DISABLED;

/**
 * Executes queries and generates results on the shard for RRF.
 */
public class RRFRankShardContext extends RankShardContext {

    protected final int windowSize;
    protected final int rankConstant;

    public RRFRankShardContext(List<Query> queries, int size, int from, int windowSize, int rankConstant) {
        super(queries, size, from);
        this.windowSize = windowSize;
        this.rankConstant = rankConstant;
    }

    @Override
    public void executeQueries(SearchContext searchContext) {
        try {
            QuerySearchResult querySearchResult = searchContext.queryResult();
            RRFRankSearchContext rrfRankSearchContext = new RRFRankSearchContext(searchContext);

            // run the combined boolean query for suggest, total hits, or aggregations
            // otherwise mark top docs as empty
            if (searchContext.suggest() != null
                || searchContext.trackTotalHitsUpTo() != TRACK_TOTAL_HITS_DISABLED
                || searchContext.aggregations() != null) {
                QueryPhase.executeInternal(rrfRankSearchContext);
            } else {
                searchContext.queryResult()
                    .topDocs(
                        new TopDocsAndMaxScore(
                            new TopDocs(new TotalHits(0, TotalHits.Relation.EQUAL_TO), Lucene.EMPTY_SCORE_DOCS),
                            Float.NaN
                        ),
                        new DocValueFormat[0]
                    );
            }

            List<TopDocs> rrfRankResults = new ArrayList<>();
            rrfRankSearchContext.windowSize(windowSize);
            boolean searchTimedOut = querySearchResult.searchTimedOut();
            long serviceTimeEWMA = querySearchResult.serviceTimeEWMA();
            int nodeQueueSize = querySearchResult.nodeQueueSize();

            // run each of the rrf queries
            for (Query query : queries) {
                // if a search timeout occurs, exit with partial results
                if (searchTimedOut) {
                    break;
                }
                rrfRankSearchContext.rrfRankQuery(query);
                QueryPhase.executeInternal(rrfRankSearchContext);
                QuerySearchResult rrfQuerySearchResult = rrfRankSearchContext.queryResult();
                rrfRankResults.add(rrfQuerySearchResult.topDocs().topDocs);
                serviceTimeEWMA += rrfQuerySearchResult.serviceTimeEWMA();
                nodeQueueSize = Math.max(nodeQueueSize, rrfQuerySearchResult.nodeQueueSize());
                searchTimedOut = rrfQuerySearchResult.searchTimedOut();
            }

            sort(rrfRankResults, querySearchResult);

            // record values relevant to all queries
            querySearchResult.searchTimedOut(searchTimedOut);
            querySearchResult.serviceTimeEWMA(serviceTimeEWMA);
            querySearchResult.nodeQueueSize(nodeQueueSize);
        } catch (QueryPhaseExecutionException e) {
            throw e;
        } catch (Exception e) {
            throw new QueryPhaseExecutionException(searchContext.shardTarget(), "Failed to execute main query", e);
        }
    }

    protected void sort(List<TopDocs> rrfRankResults, QuerySearchResult querySearchResult) {
        // combine the disjointed sets of TopDocs into a single set or RRFRankDocs
        // each RRFRankDoc will have both the position and score for each query where
        // it was within the result set for that query
        // if a doc isn't part of a result set its position will be NO_RANK [0] and
        // its score is [0f]
        int queries = rrfRankResults.size();
        Map<Integer, RRFRankDoc> docsToRankResults = Maps.newMapWithExpectedSize(windowSize);
        int index = 0;
        for (TopDocs rrfRankResult : rrfRankResults) {
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
            return rrf1.doc < rrf2.doc ? 1 : -1;
        });
        // trim the results to window size
        RRFRankDoc[] topResults = new RRFRankDoc[Math.min(windowSize + from, sortedResults.length)];
        for (int rank = 0; rank < topResults.length; ++rank) {
            topResults[rank] = sortedResults[rank];
            topResults[rank].rank = rank + 1;
            topResults[rank].score = Float.NaN;
        }
        querySearchResult.setRankShardResult(new RRFRankShardResult(rrfRankResults.size(), topResults));
    }
}
