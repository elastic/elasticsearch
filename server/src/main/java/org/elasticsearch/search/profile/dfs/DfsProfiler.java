/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.profile.dfs;

import org.elasticsearch.search.profile.AbstractProfileBreakdown;
import org.elasticsearch.search.profile.ProfileResult;
import org.elasticsearch.search.profile.SearchProfileDfsPhaseResult;
import org.elasticsearch.search.profile.Timer;
import org.elasticsearch.search.profile.query.QueryProfileShardResult;
import org.elasticsearch.search.profile.query.QueryProfiler;

import java.util.ArrayList;
import java.util.List;

/**
 * This class collects profiling information for the dfs phase and
 * generates a {@link ProfileResult} for the results of the timing
 * information for statistics collection. An additional {@link QueryProfiler}
 * is used to profile a knn vector query if one is executed.
 */
public class DfsProfiler extends AbstractProfileBreakdown<DfsTimingType> {

    private long startTime;
    private long totalTime;

    private final List<QueryProfiler> knnQueryProfilers = new ArrayList<>();

    public DfsProfiler() {
        super(DfsTimingType.class);
    }

    public void start() {
        startTime = System.nanoTime();
    }

    public void stop() {
        totalTime = System.nanoTime() - startTime;
    }

    public Timer startTimer(DfsTimingType dfsTimingType) {
        Timer newTimer = getNewTimer(dfsTimingType);
        newTimer.start();
        return newTimer;
    }

    public QueryProfiler addQueryProfiler() {
        QueryProfiler queryProfiler = new QueryProfiler();
        knnQueryProfilers.add(queryProfiler);
        return queryProfiler;
    }

    public SearchProfileDfsPhaseResult buildDfsPhaseResults() {
        ProfileResult dfsProfileResult = new ProfileResult(
            "statistics",
            "collect term statistics",
            toBreakdownMap(),
            toDebugMap(),
            totalTime,
            List.of()
        );
        if (knnQueryProfilers.size() > 0) {
            final List<QueryProfileShardResult> queryProfileShardResult = new ArrayList<>(knnQueryProfilers.size());
            for (QueryProfiler queryProfiler : knnQueryProfilers) {
                queryProfileShardResult.add(
                    new QueryProfileShardResult(
                        queryProfiler.getTree(),
                        queryProfiler.getRewriteTime(),
                        queryProfiler.getCollectorResult(),
                        queryProfiler.getVectorOpsCount()
                    )
                );
            }
            return new SearchProfileDfsPhaseResult(dfsProfileResult, queryProfileShardResult);
        }
        return new SearchProfileDfsPhaseResult(dfsProfileResult, null);
    }
}
