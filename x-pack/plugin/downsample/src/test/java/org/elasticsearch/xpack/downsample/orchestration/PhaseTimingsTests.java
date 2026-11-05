/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.downsample.orchestration;

import org.elasticsearch.test.ESTestCase;

import java.util.Map;

import static org.elasticsearch.xpack.downsample.orchestration.OrchestrationTestUtils.newIndexTask;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.startsWith;

public class PhaseTimingsTests extends ESTestCase {

    public void testTimesEveryPhaseFromTheRequest() {
        IndexTaskDoc requested = newIndexTask("target");
        long t0 = requested.createdAt();
        PhaseTimings timings = new PhaseTimings(requested, t0 + 5);
        timings.transition(IndexTaskPhase.RUN_SHARDS, t0 + 100);
        // a retry within a phase does not end it
        timings.transition(IndexTaskPhase.RUN_SHARDS, t0 + 500);
        timings.transition(IndexTaskPhase.FINALIZE_SETTINGS, t0 + 1_100);
        timings.transition(IndexTaskPhase.DONE, t0 + 1_150);

        assertThat(
            timings.durations(),
            equalTo(Map.of(IndexTaskPhase.CREATE_TARGET, 100L, IndexTaskPhase.RUN_SHARDS, 1_000L, IndexTaskPhase.FINALIZE_SETTINGS, 50L))
        );
        assertThat(timings.summary(), equalTo("CREATE_TARGET [100ms], RUN_SHARDS [1s], FINALIZE_SETTINGS [50ms]"));
    }

    public void testOperationFirstSeenAfterAFailover() {
        IndexTaskDoc running = newIndexTask("target").targetCreated(randomUUID());
        long now = running.createdAt() + 10_000;
        PhaseTimings timings = new PhaseTimings(running, now);
        timings.transition(IndexTaskPhase.FINALIZE_SETTINGS, now + 300);

        assertThat(timings.durations(), equalTo(Map.of(IndexTaskPhase.RUN_SHARDS, 300L)));
        assertThat(timings.summary(), startsWith("earlier phases not seen by this master, RUN_SHARDS [300ms]"));
    }
}
