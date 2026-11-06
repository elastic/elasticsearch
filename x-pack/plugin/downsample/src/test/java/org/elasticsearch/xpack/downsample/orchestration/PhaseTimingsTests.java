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
        timings.transition(IndexTaskPhase.DOWNSAMPLING_SHARDS, t0 + 100);
        // a retry within a phase does not end it
        timings.transition(IndexTaskPhase.DOWNSAMPLING_SHARDS, t0 + 500);
        timings.transition(IndexTaskPhase.FINALIZE_SETTINGS, t0 + 1_100);
        timings.transition(IndexTaskPhase.DONE, t0 + 1_150);

        assertThat(
            timings.durations(),
            equalTo(
                Map.of(
                    IndexTaskPhase.CREATE_TARGET,
                    100L,
                    IndexTaskPhase.DOWNSAMPLING_SHARDS,
                    1_000L,
                    IndexTaskPhase.FINALIZE_SETTINGS,
                    50L
                )
            )
        );
        assertThat(timings.summary(), equalTo("CREATE_TARGET [100ms], DOWNSAMPLING_SHARDS [1s], FINALIZE_SETTINGS [50ms]"));
    }

    public void testClockOfAnotherNodeAheadOfThisOne() {
        IndexTaskDoc requested = newIndexTask("target");
        // the operation was requested on a node whose clock is ahead of this master's
        PhaseTimings timings = new PhaseTimings(requested, requested.createdAt() - 50);
        timings.transition(IndexTaskPhase.DOWNSAMPLING_SHARDS, requested.createdAt() - 10);
        assertThat(timings.durations(), equalTo(Map.of(IndexTaskPhase.CREATE_TARGET, 0L)));
        assertThat(timings.summary(), equalTo("CREATE_TARGET [0s]"));
    }

    public void testOperationFirstSeenAfterAFailover() {
        IndexTaskDoc running = newIndexTask("target").targetCreated(randomUUID());
        long now = running.createdAt() + 10_000;
        PhaseTimings timings = new PhaseTimings(running, now);
        timings.transition(IndexTaskPhase.FINALIZE_SETTINGS, now + 300);

        assertThat(timings.durations(), equalTo(Map.of(IndexTaskPhase.DOWNSAMPLING_SHARDS, 300L)));
        assertThat(timings.summary(), startsWith("earlier phases not seen by this master, DOWNSAMPLING_SHARDS [300ms]"));
    }
}
