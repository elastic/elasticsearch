/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.dataframe.stats;

import org.elasticsearch.xpack.core.ml.dataframe.stats.AnalysisStats;
import org.elasticsearch.xpack.core.ml.dataframe.stats.common.MemoryUsage;
import org.elasticsearch.xpack.core.ml.utils.PhaseProgress;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Holds data frame analytics stats in memory so that they may be retrieved
 * from the get stats api for started jobs efficiently.
 */
public class StatsHolder {

    private volatile ProgressTracker progressTracker;
    private final AtomicReference<MemoryUsage> memoryUsageHolder;
    private final AtomicReference<AnalysisStats> analysisStatsHolder;
    private final DataCountsTracker dataCountsTracker;

    public StatsHolder(List<PhaseProgress> progressOnStart) {
        progressTracker = new ProgressTracker(progressOnStart);
        memoryUsageHolder = new AtomicReference<>();
        analysisStatsHolder = new AtomicReference<>();
        dataCountsTracker = new DataCountsTracker();
    }

    public void resetProgressTrackerPreservingReindexingProgress(List<String> analysisPhases, boolean hasInferencePhase) {
        int reindexingProgressPercent = progressTracker.getReindexingProgressPercent();
        progressTracker = ProgressTracker.fromZeroes(analysisPhases, hasInferencePhase);
        progressTracker.updateReindexingProgress(reindexingProgressPercent);
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
