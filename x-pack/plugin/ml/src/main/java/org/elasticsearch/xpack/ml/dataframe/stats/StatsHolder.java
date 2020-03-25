/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.dataframe.stats;

import org.elasticsearch.xpack.core.ml.dataframe.stats.AnalysisStats;
import org.elasticsearch.xpack.core.ml.dataframe.stats.MemoryUsage;

import java.util.concurrent.atomic.AtomicReference;

/**
 * Holds data frame analytics stats in memory so that they may be retrieved
 * from the get stats api for started jobs efficiently.
 */
public class StatsHolder {

    private final ProgressTracker progressTracker;
    private final AtomicReference<MemoryUsage> memoryUsageHolder;
    private final AtomicReference<AnalysisStats> analysisStatsHolder;
    private final DataCountsTracker dataCountsTracker;

    public StatsHolder() {
        progressTracker = new ProgressTracker();
        memoryUsageHolder = new AtomicReference<>();
        analysisStatsHolder = new AtomicReference<>();
        dataCountsTracker = new DataCountsTracker();
    }

    public ProgressTracker getProgressTracker() {
        return progressTracker;
    }

    public void setMemoryUsage(MemoryUsage memoryUsage) {
        memoryUsageHolder.set(memoryUsage);
    }

    public MemoryUsage getMemoryUsage() {
        return memoryUsageHolder.get();
    }

    public void setAnalysisStats(AnalysisStats analysisStats) {
        analysisStatsHolder.set(analysisStats);
    }

    public AnalysisStats getAnalysisStats() {
        return analysisStatsHolder.get();
    }

    public DataCountsTracker getDataCountsTracker() {
        return dataCountsTracker;
    }
}
