/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.dataframe.stats;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.utils.PhaseProgress;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class ProgressTrackerTests extends ESTestCase {

    public void testCtor() {
        List<PhaseProgress> phases = Collections.unmodifiableList(
            Arrays.asList(
                new PhaseProgress("reindexing", 10),
                new PhaseProgress("loading_data", 20),
                new PhaseProgress("a", 30),
                new PhaseProgress("b", 40),
                new PhaseProgress("writing_results", 50)
            )
        );

        ProgressTracker progressTracker = new ProgressTracker(phases);

        assertThat(progressTracker.report(), equalTo(phases));
    }

    public void testFromZeroes() {
        ProgressTracker progressTracker = ProgressTracker.fromZeroes(Arrays.asList("a", "b", "c"), false);

        List<PhaseProgress> phases = progressTracker.report();

        assertThat(phases.size(), equalTo(6));
        assertThat(phases.stream().map(PhaseProgress::getPhase).collect(Collectors.toList()),
            contains("reindexing", "loading_data", "a", "b", "c", "writing_results"));
        assertThat(phases.stream().map(PhaseProgress::getProgressPercent).allMatch(p -> p == 0), is(true));
    }

    public void testFromZeroes_GivenAnalysisWithoutInference() {
        ProgressTracker progressTracker = ProgressTracker.fromZeroes(Arrays.asList("a", "b"), false);

        List<PhaseProgress> phaseProgresses = progressTracker.report();

        assertThat(phaseProgresses.size(), equalTo(5));
        assertThat(phaseProgresses.stream().map(PhaseProgress::getPhase).collect(Collectors.toList()),
            contains("reindexing", "loading_data", "a", "b", "writing_results"));
    }

    public void testFromZeroes_GivenAnalysisWithInference() {
        ProgressTracker progressTracker = ProgressTracker.fromZeroes(Arrays.asList("a", "b"), true);

        List<PhaseProgress> phaseProgresses = progressTracker.report();

        assertThat(phaseProgresses.size(), equalTo(6));
        assertThat(phaseProgresses.stream().map(PhaseProgress::getPhase).collect(Collectors.toList()),
            contains("reindexing", "loading_data", "a", "b", "writing_results", "inference"));
    }

    public void testUpdates() {
        ProgressTracker progressTracker = ProgressTracker.fromZeroes(Collections.singletonList("foo"), false);

        progressTracker.updateReindexingProgress(1);
        progressTracker.updateLoadingDataProgress(2);
        progressTracker.updatePhase(new PhaseProgress("foo", 3));
        progressTracker.updateWritingResultsProgress(4);

        assertThat(progressTracker.getReindexingProgressPercent(), equalTo(1));
        assertThat(progressTracker.getWritingResultsProgressPercent(), equalTo(4));

        List<PhaseProgress> phases = progressTracker.report();

        assertThat(phases.size(), equalTo(4));
        assertThat(phases.stream().map(PhaseProgress::getPhase).collect(Collectors.toList()),
            contains("reindexing", "loading_data", "foo", "writing_results"));
        assertThat(phases.get(0).getProgressPercent(), equalTo(1));
        assertThat(phases.get(1).getProgressPercent(), equalTo(2));
        assertThat(phases.get(2).getProgressPercent(), equalTo(3));
        assertThat(phases.get(3).getProgressPercent(), equalTo(4));
    }

    public void testUpdatePhase_GivenUnknownPhase() {
        ProgressTracker progressTracker = ProgressTracker.fromZeroes(Collections.singletonList("foo"), false);

        progressTracker.updatePhase(new PhaseProgress("unknown", 42));
        List<PhaseProgress> phases = progressTracker.report();

        assertThat(phases.size(), equalTo(4));
        assertThat(phases.stream().map(PhaseProgress::getPhase).collect(Collectors.toList()),
            contains("reindexing", "loading_data", "foo", "writing_results"));
    }

    public void testUpdateReindexingProgress_GivenLowerValueThanCurrentProgress() {
        ProgressTracker progressTracker = ProgressTracker.fromZeroes(Collections.singletonList("foo"), false);

        progressTracker.updateReindexingProgress(10);

        progressTracker.updateReindexingProgress(11);
        assertThat(progressTracker.getReindexingProgressPercent(), equalTo(11));

        progressTracker.updateReindexingProgress(10);
        assertThat(progressTracker.getReindexingProgressPercent(), equalTo(11));
    }

    public void testUpdateLoadingDataProgress_GivenLowerValueThanCurrentProgress() {
        ProgressTracker progressTracker = ProgressTracker.fromZeroes(Collections.singletonList("foo"), false);

        progressTracker.updateLoadingDataProgress(20);

        progressTracker.updateLoadingDataProgress(21);
        assertThat(progressTracker.getLoadingDataProgressPercent(), equalTo(21));

        progressTracker.updateLoadingDataProgress(20);
        assertThat(progressTracker.getLoadingDataProgressPercent(), equalTo(21));
    }

    public void testUpdateWritingResultsProgress_GivenLowerValueThanCurrentProgress() {
        ProgressTracker progressTracker = ProgressTracker.fromZeroes(Collections.singletonList("foo"), false);

        progressTracker.updateWritingResultsProgress(30);

        progressTracker.updateWritingResultsProgress(31);
        assertThat(progressTracker.getWritingResultsProgressPercent(), equalTo(31));

        progressTracker.updateWritingResultsProgress(30);
        assertThat(progressTracker.getWritingResultsProgressPercent(), equalTo(31));
    }

    public void testUpdatePhase_GivenLowerValueThanCurrentProgress() {
        ProgressTracker progressTracker = ProgressTracker.fromZeroes(Collections.singletonList("foo"), false);

        progressTracker.updatePhase(new PhaseProgress("foo", 40));

        progressTracker.updatePhase(new PhaseProgress("foo", 41));
        assertThat(getProgressForPhase(progressTracker, "foo"), equalTo(41));

        progressTracker.updatePhase(new PhaseProgress("foo", 40));
        assertThat(getProgressForPhase(progressTracker, "foo"), equalTo(41));
    }

    public void testResetForInference_GivenInference() {
        ProgressTracker progressTracker = ProgressTracker.fromZeroes(Arrays.asList("a", "b"), true);
        progressTracker.updateReindexingProgress(10);
        progressTracker.updateLoadingDataProgress(20);
        progressTracker.updatePhase(new PhaseProgress("a", 30));
        progressTracker.updatePhase(new PhaseProgress("b", 40));
        progressTracker.updateWritingResultsProgress(50);
        progressTracker.updateInferenceProgress(60);

        progressTracker.resetForInference();

        List<PhaseProgress> progress = progressTracker.report();
        assertThat(progress, contains(
            new PhaseProgress(ProgressTracker.REINDEXING, 100),
            new PhaseProgress(ProgressTracker.LOADING_DATA, 100),
            new PhaseProgress("a", 100),
            new PhaseProgress("b", 100),
            new PhaseProgress(ProgressTracker.WRITING_RESULTS, 100),
            new PhaseProgress(ProgressTracker.INFERENCE, 0)
        ));
    }

    public void testResetForInference_GivenNoInference() {
        ProgressTracker progressTracker = ProgressTracker.fromZeroes(Arrays.asList("a", "b"), false);
        progressTracker.updateReindexingProgress(10);
        progressTracker.updateLoadingDataProgress(20);
        progressTracker.updatePhase(new PhaseProgress("a", 30));
        progressTracker.updatePhase(new PhaseProgress("b", 40));
        progressTracker.updateWritingResultsProgress(50);

        progressTracker.resetForInference();

        List<PhaseProgress> progress = progressTracker.report();
        assertThat(progress, contains(
            new PhaseProgress(ProgressTracker.REINDEXING, 100),
            new PhaseProgress(ProgressTracker.LOADING_DATA, 100),
            new PhaseProgress("a", 100),
            new PhaseProgress("b", 100),
            new PhaseProgress(ProgressTracker.WRITING_RESULTS, 100)
        ));
    }

    public void testAreAllPhasesExceptInferenceComplete_GivenComplete() {
        ProgressTracker progressTracker = ProgressTracker.fromZeroes(Collections.singletonList("a"), true);
        progressTracker.updateReindexingProgress(100);
        progressTracker.updateLoadingDataProgress(100);
        progressTracker.updatePhase(new PhaseProgress("a", 100));
        progressTracker.updateWritingResultsProgress(100);
        progressTracker.updateInferenceProgress(50);

        assertThat(progressTracker.areAllPhasesExceptInferenceComplete(), is(true));
    }

    public void testAreAllPhasesExceptInferenceComplete_GivenNotComplete() {
        Map<String, Integer> phasePerProgress = new LinkedHashMap<>();
        phasePerProgress.put(ProgressTracker.REINDEXING, 100);
        phasePerProgress.put(ProgressTracker.LOADING_DATA, 100);
        phasePerProgress.put("a", 100);
        phasePerProgress.put(ProgressTracker.WRITING_RESULTS, 100);
        String nonCompletePhase = randomFrom(phasePerProgress.keySet());
        phasePerProgress.put(ProgressTracker.INFERENCE, 50);
        phasePerProgress.put(nonCompletePhase, randomIntBetween(0, 99));

        ProgressTracker progressTracker = new ProgressTracker(phasePerProgress.entrySet().stream()
            .map(entry -> new PhaseProgress(entry.getKey(), entry.getValue())).collect(Collectors.toList()));

        assertThat(progressTracker.areAllPhasesExceptInferenceComplete(), is(false));
    }

    private static int getProgressForPhase(ProgressTracker progressTracker, String phase) {
        return progressTracker.report().stream().filter(p -> p.getPhase().equals(phase)).findFirst().get().getProgressPercent();
    }
}
