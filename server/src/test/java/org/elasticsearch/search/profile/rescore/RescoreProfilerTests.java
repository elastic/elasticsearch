/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.profile.rescore;

import org.elasticsearch.search.profile.ProfileResult;
import org.elasticsearch.search.profile.Timer;
import org.elasticsearch.test.ESTestCase;

import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.not;

public class RescoreProfilerTests extends ESTestCase {

    public void testBuildResult() {
        int windowSize = randomIntBetween(1, 100);
        int docsBefore = randomIntBetween(0, 100);
        int docsAfter = randomIntBetween(0, docsBefore);
        RescoreProfiler profiler = new RescoreProfiler("query", windowSize);
        profiler.setDocsBeforeRescore(docsBefore);
        profiler.setDocsAfterRescore(docsAfter);
        Timer timer = profiler.startRescoreTimer();
        timer.stop();

        ProfileResult result = profiler.buildResult();

        assertThat(result.getQueryName(), equalTo("query"));
        assertThat(result.getLuceneDescription(), equalTo("rescore top documents"));
        assertThat(result.getTime(), greaterThanOrEqualTo(0L));
        assertThat(result.getTimeBreakdown().get("rescore_count"), equalTo(1L));
        assertThat(result.getDebugInfo().keySet(), equalTo(Set.of("window_size", "docs_before_rescore", "docs_after_rescore")));
        assertThat(result.getDebugInfo().get("window_size"), equalTo(windowSize));
        assertThat(result.getDebugInfo().get("docs_before_rescore"), equalTo(docsBefore));
        assertThat(result.getDebugInfo().get("docs_after_rescore"), equalTo(docsAfter));
        assertThat(result.getProfiledChildren(), empty());
    }

    public void testRescorerDebugInfo() {
        RescoreProfiler profiler = new RescoreProfiler("learning_to_rank", 10);
        profiler.addDebugInfo("inference_time_in_nanos", 42L);
        profiler.addDebugInfo("feature_time_in_nanos", Map.of("feature", 7L));

        ProfileResult result = profiler.buildResult();

        assertThat(result.getDebugInfo().get("inference_time_in_nanos"), equalTo(42L));
        assertThat(result.getDebugInfo().get("feature_time_in_nanos"), equalTo(Map.of("feature", 7L)));
        assertThat(result.getDebugInfo().get("window_size"), equalTo(10));
    }

    public void testBuildResultTimedOut() {
        int docsBefore = randomIntBetween(0, 100);
        RescoreProfiler profiler = new RescoreProfiler("query", randomIntBetween(1, 100));
        profiler.setDocsBeforeRescore(docsBefore);
        Timer timer = profiler.startRescoreTimer();
        profiler.setTimedOut();
        timer.stop();

        ProfileResult result = profiler.buildResult();

        assertThat(result.getDebugInfo().get("docs_before_rescore"), equalTo(docsBefore));
        assertThat(result.getDebugInfo().get("timed_out"), equalTo(true));
        assertThat(result.getDebugInfo(), not(hasKey("docs_after_rescore")));
    }
}
