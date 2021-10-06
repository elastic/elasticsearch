/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.dataframe.stats;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.core.ml.dataframe.stats.AnalysisStats;
import org.elasticsearch.xpack.core.ml.dataframe.stats.common.DataCounts;
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

    public StatsHolder(List<PhaseProgress> progress, @Nullable MemoryUsage memoryUsage, @Nullable AnalysisStats analysisStats,
                       DataCounts dataCounts) {
        progressTracker = new ProgressTracker(progress);
        memoryUsageHolder = new AtomicReference<>(memoryUsage);
        analysisStatsHolder = new AtomicReference<>(analysisStats);
        dataCountsTracker = new DataCountsTracker(dataCounts);
    }

    /**
     * Updates the progress tracker with potentially new in-between phases
     * that were introduced in a later version while making sure progress indicators
     * are correct.
     * @param analysisPhases the full set of phases of the analysis in current version
     * @param hasInferencePhase whether the analysis supports inference
     */
    public void adjustProgressTracker(List<String> analysisPhases, boolean hasInferencePhase) {
        int reindexingProgressPercent = progressTracker.getReindexingProgressPercent();
        boolean areAllPhasesBeforeInferenceComplete = progressTracker.areAllPhasesExceptInferenceComplete();
        progressTracker = ProgressTracker.fromZeroes(analysisPhases, hasInferencePhase);

        // If reindexing progress was more than 0 and less than 100 (ie not complete) we reset it to 1
        // as we will have to do reindexing from scratch and at the same time we want
        // to differentiate from a job that has never started before.
        if (reindexingProgressPercent > 0 && reindexingProgressPercent < 100) {
            progressTracker.updateReindexingProgress(1);
        } else {
            progressTracker.updateReindexingProgress(reindexingProgressPercent);
        }

        if (hasInferencePhase && areAllPhasesBeforeInferenceComplete) {
            progressTracker.resetForInference();
        }
    }

    public void resetProgressTracker(List<String> analysisPhases, boolean hasInferencePhase) {
        progressTracker = ProgressTracker.fromZeroes(analysisPhases, hasInferencePhase);
        progressTracker.updateReindexingProgress(1);
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
