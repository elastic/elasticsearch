/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.profile.dfs;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.search.profile.AbstractProfileBreakdown;
import org.elasticsearch.search.profile.ProfileResult;
import org.elasticsearch.search.profile.ProfileSelection;
import org.elasticsearch.search.profile.SearchProfileDfsPhaseResult;
import org.elasticsearch.search.profile.Timer;
import org.elasticsearch.search.profile.query.QueryProfileShardResult;
import org.elasticsearch.search.profile.query.QueryProfiler;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

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
    @Nullable
    private final ProfileSelection codecReadSelection;

    /**
     * @param codecReadSelection the codec read selection of the kNN query nodes, or null when codec reads are not
     *                           profiled for this shard
     */
    public DfsProfiler(@Nullable ProfileSelection codecReadSelection) {
        super(DfsTimingType.class);
        this.codecReadSelection = codecReadSelection;
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
        QueryProfiler queryProfiler = new QueryProfiler(codecReadSelection);
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
            List.of(),
            Map.of(),
            Map.of(),
            Map.of()
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
