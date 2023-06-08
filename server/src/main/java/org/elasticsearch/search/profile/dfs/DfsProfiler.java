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
import org.elasticsearch.search.profile.query.ProfileCollectorManager;
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
    private boolean collectorManagerSet = false;

    public DfsProfiler() {
        super(DfsTimingType.class);
    }

    public void start() {
        startTime = System.nanoTime();
    }

    public void stop() {
        totalTime = System.nanoTime() - startTime;
    }

    public void startTimer(DfsTimingType dfsTimingType) {
        getTimer(dfsTimingType).start();
    }

    public void stopTimer(DfsTimingType dfsTimingType) {
        getTimer(dfsTimingType).stop();
    }

    public QueryProfiler addQueryProfiler(ProfileCollectorManager collectorManager) {
        QueryProfiler queryProfiler = new QueryProfiler();
        queryProfiler.setCollectorManager(collectorManager::getCollectorTree);
        knnQueryProfilers.add(queryProfiler);
        collectorManagerSet = true;
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
        final List<QueryProfileShardResult> queryProfileShardResult;
        if (collectorManagerSet) {
            queryProfileShardResult = new ArrayList<>(knnQueryProfilers.size());
            for (QueryProfiler queryProfiler : knnQueryProfilers) {
                queryProfileShardResult.add(
                    new QueryProfileShardResult(queryProfiler.getTree(), queryProfiler.getRewriteTime(), queryProfiler.getCollectorResult())
                );
            }
        } else {
            queryProfileShardResult = null;
        }
        return new SearchProfileDfsPhaseResult(dfsProfileResult, queryProfileShardResult);
    }
}
