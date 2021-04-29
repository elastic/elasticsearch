/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.dataframe.stats;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.dataframe.stats.common.DataCounts;
import org.elasticsearch.xpack.core.ml.utils.PhaseProgress;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;

public class StatsHolderTests extends ESTestCase {

    public void testAdjustProgressTracker_GivenZeroProgress() {
        List<PhaseProgress> phases = Collections.unmodifiableList(
            Arrays.asList(
                new PhaseProgress("reindexing", 0),
                new PhaseProgress("loading_data", 0),
                new PhaseProgress("a", 0),
                new PhaseProgress("b", 0),
                new PhaseProgress("writing_results", 0)
            )
        );
        StatsHolder statsHolder = newStatsHolder(phases);

        statsHolder.adjustProgressTracker(Arrays.asList("a", "b"), false);

        List<PhaseProgress> phaseProgresses = statsHolder.getProgressTracker().report();

        assertThat(phaseProgresses.size(), equalTo(5));
        assertThat(phaseProgresses.stream().map(PhaseProgress::getPhase).collect(Collectors.toList()),
            contains("reindexing", "loading_data", "a", "b", "writing_results"));
        assertThat(phaseProgresses.get(0).getProgressPercent(), equalTo(0));
        assertThat(phaseProgresses.get(1).getProgressPercent(), equalTo(0));
        assertThat(phaseProgresses.get(2).getProgressPercent(), equalTo(0));
        assertThat(phaseProgresses.get(3).getProgressPercent(), equalTo(0));
        assertThat(phaseProgresses.get(4).getProgressPercent(), equalTo(0));
    }

    public void testAdjustProgressTracker_GivenSameAnalysisPhases() {
        List<PhaseProgress> phases = Collections.unmodifiableList(
            Arrays.asList(
                new PhaseProgress("reindexing", 100),
                new PhaseProgress("loading_data", 20),
                new PhaseProgress("a", 30),
                new PhaseProgress("b", 40),
                new PhaseProgress("writing_results", 50)
            )
        );
        StatsHolder statsHolder = newStatsHolder(phases);

        statsHolder.adjustProgressTracker(Arrays.asList("a", "b"), false);

        List<PhaseProgress> phaseProgresses = statsHolder.getProgressTracker().report();

        assertThat(phaseProgresses.size(), equalTo(5));
        assertThat(phaseProgresses.stream().map(PhaseProgress::getPhase).collect(Collectors.toList()),
            contains("reindexing", "loading_data", "a", "b", "writing_results"));
        assertThat(phaseProgresses.get(0).getProgressPercent(), equalTo(100));
        assertThat(phaseProgresses.get(1).getProgressPercent(), equalTo(0));
        assertThat(phaseProgresses.get(2).getProgressPercent(), equalTo(0));
        assertThat(phaseProgresses.get(3).getProgressPercent(), equalTo(0));
        assertThat(phaseProgresses.get(4).getProgressPercent(), equalTo(0));
    }

    public void testAdjustProgressTracker_GivenDifferentAnalysisPhases() {
        List<PhaseProgress> phases = Collections.unmodifiableList(
            Arrays.asList(
                new PhaseProgress("reindexing", 100),
                new PhaseProgress("loading_data", 20),
                new PhaseProgress("a", 30),
                new PhaseProgress("b", 40),
                new PhaseProgress("writing_results", 50)
            )
        );
        StatsHolder statsHolder = newStatsHolder(phases);

        statsHolder.adjustProgressTracker(Arrays.asList("c", "d"), false);

        List<PhaseProgress> phaseProgresses = statsHolder.getProgressTracker().report();

        assertThat(phaseProgresses.size(), equalTo(5));
        assertThat(phaseProgresses.stream().map(PhaseProgress::getPhase).collect(Collectors.toList()),
            contains("reindexing", "loading_data", "c", "d", "writing_results"));
        assertThat(phaseProgresses.get(0).getProgressPercent(), equalTo(100));
        assertThat(phaseProgresses.get(1).getProgressPercent(), equalTo(0));
        assertThat(phaseProgresses.get(2).getProgressPercent(), equalTo(0));
        assertThat(phaseProgresses.get(3).getProgressPercent(), equalTo(0));
        assertThat(phaseProgresses.get(4).getProgressPercent(), equalTo(0));
    }

    public void testAdjustProgressTracker_GivenReindexingProgressIncomplete() {
        List<PhaseProgress> phases = Collections.unmodifiableList(
            Arrays.asList(
                new PhaseProgress("reindexing", 42),
                new PhaseProgress("loading_data", 20),
                new PhaseProgress("a", 30),
                new PhaseProgress("b", 40),
                new PhaseProgress("writing_results", 50)
            )
        );
        StatsHolder statsHolder = newStatsHolder(phases);

        statsHolder.adjustProgressTracker(Arrays.asList("a", "b"), false);

        List<PhaseProgress> phaseProgresses = statsHolder.getProgressTracker().report();

        assertThat(phaseProgresses.size(), equalTo(5));
        assertThat(phaseProgresses.stream().map(PhaseProgress::getPhase).collect(Collectors.toList()),
            contains("reindexing", "loading_data", "a", "b", "writing_results"));
        assertThat(phaseProgresses.get(0).getProgressPercent(), equalTo(1));
        assertThat(phaseProgresses.get(1).getProgressPercent(), equalTo(0));
        assertThat(phaseProgresses.get(2).getProgressPercent(), equalTo(0));
        assertThat(phaseProgresses.get(3).getProgressPercent(), equalTo(0));
        assertThat(phaseProgresses.get(4).getProgressPercent(), equalTo(0));
    }

    public void testAdjustProgressTracker_GivenAllPhasesCompleteExceptInference() {
        List<PhaseProgress> phases = Collections.unmodifiableList(
            Arrays.asList(
                new PhaseProgress("reindexing", 100),
                new PhaseProgress("loading_data", 100),
                new PhaseProgress("a", 100),
                new PhaseProgress("writing_results", 100),
                new PhaseProgress("inference", 20)
            )
        );
        StatsHolder statsHolder = newStatsHolder(phases);

        statsHolder.adjustProgressTracker(Arrays.asList("a", "b"), true);

        List<PhaseProgress> phaseProgresses = statsHolder.getProgressTracker().report();

        assertThat(phaseProgresses, contains(
            new PhaseProgress("reindexing", 100),
            new PhaseProgress("loading_data", 100),
            new PhaseProgress("a", 100),
            new PhaseProgress("b", 100),
            new PhaseProgress("writing_results", 100),
            new PhaseProgress("inference", 0)
        ));
    }

    public void testResetProgressTracker() {
        List<PhaseProgress> phases = Collections.unmodifiableList(
            Arrays.asList(
                new PhaseProgress("reindexing", 100),
                new PhaseProgress("loading_data", 20),
                new PhaseProgress("a", 30),
                new PhaseProgress("b", 40),
                new PhaseProgress("writing_results", 50)
            )
        );
        StatsHolder statsHolder = newStatsHolder(phases);

        statsHolder.resetProgressTracker(Arrays.asList("a", "b"), false);

        List<PhaseProgress> phaseProgresses = statsHolder.getProgressTracker().report();

        assertThat(phaseProgresses.size(), equalTo(5));
        assertThat(phaseProgresses.stream().map(PhaseProgress::getPhase).collect(Collectors.toList()),
            contains("reindexing", "loading_data", "a", "b", "writing_results"));
        assertThat(phaseProgresses.get(0).getProgressPercent(), equalTo(1));
        assertThat(phaseProgresses.get(1).getProgressPercent(), equalTo(0));
        assertThat(phaseProgresses.get(2).getProgressPercent(), equalTo(0));
        assertThat(phaseProgresses.get(3).getProgressPercent(), equalTo(0));
        assertThat(phaseProgresses.get(4).getProgressPercent(), equalTo(0));
    }

    private static StatsHolder newStatsHolder(List<PhaseProgress> progress) {
        return new StatsHolder(progress, null, null, new DataCounts("test_job"));
    }
}
