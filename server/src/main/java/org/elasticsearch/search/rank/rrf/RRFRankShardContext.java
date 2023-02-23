/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.rank.rrf;

import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.lucene.search.TopDocsAndMaxScore;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.query.QueryPhase;
import org.elasticsearch.search.query.QueryPhaseExecutionException;
import org.elasticsearch.search.query.QuerySearchResult;
import org.elasticsearch.search.rank.RankShardContext;
import org.elasticsearch.search.rank.RankShardResult;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.search.internal.SearchContext.TRACK_TOTAL_HITS_DISABLED;

public class RRFRankShardContext extends RankShardContext {

    protected final int windowSize;

    public RRFRankShardContext(List<Query> queries, int size, int from, int windowSize) {
        super(queries, size, from);
        this.windowSize = windowSize;
    }

    @Override
    public void executeQueries(SearchContext searchContext) {
        try {
            QuerySearchResult querySearchResult = searchContext.queryResult();
            RRFRankSearchContext rrfRankSearchContext = new RRFRankSearchContext(searchContext);

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

            for (Query query : queries) {
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

            RankShardResult rankShardResult = new RRFRankShardResult(rrfRankResults);
            querySearchResult.setRankShardResult(rankShardResult);
            querySearchResult.searchTimedOut(searchTimedOut);
            querySearchResult.serviceTimeEWMA(serviceTimeEWMA);
            querySearchResult.nodeQueueSize(nodeQueueSize);
        } catch (QueryPhaseExecutionException e) {
            throw e;
        } catch (Exception e) {
            throw new QueryPhaseExecutionException(searchContext.shardTarget(), "Failed to execute main query", e);
        }
    }
}
