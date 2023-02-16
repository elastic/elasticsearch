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
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.query.QueryPhase;
import org.elasticsearch.search.query.QueryPhaseExecutionException;
import org.elasticsearch.search.rank.RankShardContext;
import org.elasticsearch.search.rank.RankShardResult;

import java.util.ArrayList;
import java.util.List;

public class RRFRankShardContext extends RankShardContext {

    protected final int windowSize;

    public RRFRankShardContext(List<Query> queries, int size, int from, int windowSize) {
        super(queries, size, from);
        this.windowSize = windowSize;
    }

    @Override
    public void executeQueries(SearchContext searchContext) {
        try {
            RRFRankSearchContext rrfRankSearchContext = new RRFRankSearchContext(searchContext);
            QueryPhase.executeInternal(rrfRankSearchContext);

            List<TopDocs> rrfRankResults = new ArrayList<>();
            rrfRankSearchContext.windowSize(windowSize);
            for (Query query : queries) {
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
}
