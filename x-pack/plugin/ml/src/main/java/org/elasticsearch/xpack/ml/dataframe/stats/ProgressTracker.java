/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.dataframe.stats;

import org.elasticsearch.xpack.core.ml.utils.PhaseProgress;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Tracks progress of a data frame analytics job.
 * It includes phases "reindexing", "loading_data" and "writing_results"
 * and allows for custom phases between "loading_data" and "writing_results".
 */
public class ProgressTracker {

    public static final String REINDEXING = "reindexing";
    public static final String LOADING_DATA = "loading_data";
    public static final String WRITING_RESULTS = "writing_results";
    public static final String INFERENCE = "inference";

    private final String[] phasesInOrder;
    private final Map<String, Integer> progressPercentPerPhase;

    public static ProgressTracker fromZeroes(List<String> analysisProgressPhases, boolean hasInferencePhase) {
        List<PhaseProgress> phases = new ArrayList<>(3 + analysisProgressPhases.size() + (hasInferencePhase ? 1 : 0));
        phases.add(new PhaseProgress(REINDEXING, 0));
        phases.add(new PhaseProgress(LOADING_DATA, 0));
        analysisProgressPhases.forEach(analysisPhase -> phases.add(new PhaseProgress(analysisPhase, 0)));
        phases.add(new PhaseProgress(WRITING_RESULTS, 0));
        if (hasInferencePhase) {
            phases.add(new PhaseProgress(INFERENCE, 0));
        }
        return new ProgressTracker(phases);
    }

    public ProgressTracker(List<PhaseProgress> phaseProgresses) {
        phasesInOrder = new String[phaseProgresses.size()];
        progressPercentPerPhase = new ConcurrentHashMap<>();

        for (int i = 0; i < phaseProgresses.size(); i++) {
            PhaseProgress phaseProgress = phaseProgresses.get(i);
            phasesInOrder[i] = phaseProgress.getPhase();
            progressPercentPerPhase.put(phaseProgress.getPhase(), phaseProgress.getProgressPercent());
        }

        assert progressPercentPerPhase.containsKey(REINDEXING);
        assert progressPercentPerPhase.containsKey(LOADING_DATA);
        assert progressPercentPerPhase.containsKey(WRITING_RESULTS);
        // If there is inference it should be the last phase otherwise there
        // are assumptions that do not hold.
        assert progressPercentPerPhase.containsKey(INFERENCE) == false || INFERENCE.equals(phasesInOrder[phasesInOrder.length - 1]);
    }

    public void updateReindexingProgress(int progressPercent) {
        updatePhase(REINDEXING, progressPercent);
    }

    public int getReindexingProgressPercent() {
        return progressPercentPerPhase.get(REINDEXING);
    }

    public void updateLoadingDataProgress(int progressPercent) {
        updatePhase(LOADING_DATA, progressPercent);
    }

    public int getLoadingDataProgressPercent() {
        return progressPercentPerPhase.get(LOADING_DATA);
    }

    public void updateWritingResultsProgress(int progressPercent) {
        updatePhase(WRITING_RESULTS, progressPercent);
    }

    public int getWritingResultsProgressPercent() {
        return progressPercentPerPhase.get(WRITING_RESULTS);
    }

    public void updateInferenceProgress(int progressPercent) {
        updatePhase(INFERENCE, progressPercent);
    }

    public int getInferenceProgressPercent() {
        return progressPercentPerPhase.getOrDefault(INFERENCE, 0);
    }

    public void updatePhase(PhaseProgress phase) {
        updatePhase(phase.getPhase(), phase.getProgressPercent());
    }

    private void updatePhase(String phase, int progress) {
        progressPercentPerPhase.computeIfPresent(phase, (k, v) -> Math.max(v, progress));
    }

    /**
     * Resets progress to reflect all phases are complete except for inference
     * which is set to zero.
     */
    public void resetForInference() {
        for (Map.Entry<String, Integer> phaseProgress : progressPercentPerPhase.entrySet()) {
            if (phaseProgress.getKey().equals(INFERENCE)) {
                progressPercentPerPhase.put(phaseProgress.getKey(), 0);
            } else {
                progressPercentPerPhase.put(phaseProgress.getKey(), 100);
            }
        }
    }

    /**
     * Returns whether all phases before inference are complete
     */
    public boolean areAllPhasesExceptInferenceComplete() {
        for (Map.Entry<String, Integer> phaseProgress : progressPercentPerPhase.entrySet()) {
            if (phaseProgress.getKey().equals(INFERENCE) == false && phaseProgress.getValue() < 100) {
                return false;
            }
        }
        return true;
    }

    public List<PhaseProgress> report() {
        return Arrays.stream(phasesInOrder)
            .map(phase -> new PhaseProgress(phase, progressPercentPerPhase.get(phase)))
            .collect(Collectors.toUnmodifiableList());
    }
}
