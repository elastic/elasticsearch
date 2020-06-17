/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ml.dataframe.stats;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.utils.PhaseProgress;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;

public class StatsHolderTests extends ESTestCase {

    public void testResetProgressTrackerPreservingReindexingProgress_GivenSameAnalysisPhases() {
        List<PhaseProgress> phases = Collections.unmodifiableList(
            Arrays.asList(
                new org.elasticsearch.xpack.core.ml.utils.PhaseProgress("reindexing", 10),
                new org.elasticsearch.xpack.core.ml.utils.PhaseProgress("loading_data", 20),
                new org.elasticsearch.xpack.core.ml.utils.PhaseProgress("a", 30),
                new org.elasticsearch.xpack.core.ml.utils.PhaseProgress("b", 40),
                new PhaseProgress("writing_results", 50)
            )
        );
        StatsHolder statsHolder = new StatsHolder(phases);

        statsHolder.resetProgressTrackerPreservingReindexingProgress(Arrays.asList("a", "b"));

        List<PhaseProgress> phaseProgresses = statsHolder.getProgressTracker().report();

        assertThat(phaseProgresses.size(), equalTo(5));
        assertThat(phaseProgresses.stream().map(PhaseProgress::getPhase).collect(Collectors.toList()),
            contains("reindexing", "loading_data", "a", "b", "writing_results"));
        assertThat(phaseProgresses.get(0).getProgressPercent(), equalTo(10));
        assertThat(phaseProgresses.get(1).getProgressPercent(), equalTo(0));
        assertThat(phaseProgresses.get(2).getProgressPercent(), equalTo(0));
        assertThat(phaseProgresses.get(3).getProgressPercent(), equalTo(0));
        assertThat(phaseProgresses.get(4).getProgressPercent(), equalTo(0));
    }

    public void testResetProgressTrackerPreservingReindexingProgress_GivenDifferentAnalysisPhases() {
        List<PhaseProgress> phases = Collections.unmodifiableList(
            Arrays.asList(
                new org.elasticsearch.xpack.core.ml.utils.PhaseProgress("reindexing", 10),
                new org.elasticsearch.xpack.core.ml.utils.PhaseProgress("loading_data", 20),
                new org.elasticsearch.xpack.core.ml.utils.PhaseProgress("a", 30),
                new org.elasticsearch.xpack.core.ml.utils.PhaseProgress("b", 40),
                new PhaseProgress("writing_results", 50)
            )
        );
        StatsHolder statsHolder = new StatsHolder(phases);

        statsHolder.resetProgressTrackerPreservingReindexingProgress(Arrays.asList("c", "d"));

        List<PhaseProgress> phaseProgresses = statsHolder.getProgressTracker().report();

        assertThat(phaseProgresses.size(), equalTo(5));
        assertThat(phaseProgresses.stream().map(PhaseProgress::getPhase).collect(Collectors.toList()),
            contains("reindexing", "loading_data", "c", "d", "writing_results"));
        assertThat(phaseProgresses.get(0).getProgressPercent(), equalTo(10));
        assertThat(phaseProgresses.get(1).getProgressPercent(), equalTo(0));
        assertThat(phaseProgresses.get(2).getProgressPercent(), equalTo(0));
        assertThat(phaseProgresses.get(3).getProgressPercent(), equalTo(0));
        assertThat(phaseProgresses.get(4).getProgressPercent(), equalTo(0));
    }
}
