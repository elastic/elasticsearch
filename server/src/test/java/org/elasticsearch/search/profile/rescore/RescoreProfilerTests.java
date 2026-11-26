/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.profile.rescore;

import org.elasticsearch.search.profile.AbstractProfileBreakdown;
import org.elasticsearch.search.profile.ProfileResult;
import org.elasticsearch.search.profile.Timer;
import org.elasticsearch.test.ESTestCase;

import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class RescoreProfilerTests extends ESTestCase {

    public void testBuildResult() {
        int windowSize = randomIntBetween(1, 100);
        RescoreProfiler profiler = new RescoreProfiler("query", "f:a", windowSize);
        Timer timer = profiler.startRescoreTimer();
        timer.stop();

        ProfileResult result = profiler.buildResult();

        assertThat(result.getQueryName(), equalTo("query"));
        assertThat(result.getLuceneDescription(), equalTo("f:a"));
        assertThat(result.getTime(), greaterThanOrEqualTo(0L));
        assertThat(result.getTimeBreakdown(), equalTo(Map.of()));
        assertThat(result.getDebugInfo(), equalTo(Map.of("window_size", windowSize)));
        assertThat(result.getProfiledChildren(), empty());
    }

    /**
     * A rescorer can report its own steps in the breakdown, timed while the rescorer itself is timed, and add to the debug section.
     */
    public void testRescorerBreakdownAndDebugInfo() {
        RescoreProfiler profiler = new RescoreProfiler("learning_to_rank", "LTR model [my-model]", 10);
        StepsBreakdown breakdown = new StepsBreakdown();
        profiler.setBreakdown(breakdown);
        profiler.addDebugInfo("model_cached", true);
        Timer rescoreTimer = profiler.startRescoreTimer();
        Timer stepTimer = breakdown.getNewTimer(Step.STEP);
        int steps = randomIntBetween(1, 5);
        for (int i = 0; i < steps; i++) {
            stepTimer.start();
            stepTimer.stop();
        }
        rescoreTimer.stop();

        ProfileResult result = profiler.buildResult();

        assertThat(result.getTimeBreakdown().keySet(), equalTo(Set.of("step", "step_count")));
        assertThat(result.getTimeBreakdown().get("step_count"), equalTo((long) steps));
        assertThat(result.getTimeBreakdown().get("step"), lessThanOrEqualTo(result.getTime()));
        assertThat(result.getDebugInfo(), equalTo(Map.of("window_size", 10, "model_cached", true)));
    }

    public void testBuildResultTimedOut() {
        int windowSize = randomIntBetween(1, 100);
        RescoreProfiler profiler = new RescoreProfiler("query", "f:a", windowSize);
        Timer timer = profiler.startRescoreTimer();
        profiler.setTimedOut();
        timer.stop();

        ProfileResult result = profiler.buildResult();

        assertThat(result.getDebugInfo(), equalTo(Map.of("window_size", windowSize, "timed_out", true)));
    }

    private enum Step {
        STEP;

        @Override
        public String toString() {
            return name().toLowerCase(Locale.ROOT);
        }
    }

    private static class StepsBreakdown extends AbstractProfileBreakdown<Step> {
        StepsBreakdown() {
            super(Step.class);
        }
    }
}
