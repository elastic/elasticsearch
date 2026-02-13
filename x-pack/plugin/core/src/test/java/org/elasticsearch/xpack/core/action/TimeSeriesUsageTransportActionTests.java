/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.action;

import org.elasticsearch.core.TimeValue;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.datastreams.TimeSeriesFeatureSetUsage;
import org.elasticsearch.xpack.core.ilm.DownsampleAction;
import org.elasticsearch.xpack.core.ilm.ForceMergeAction;
import org.elasticsearch.xpack.core.ilm.LifecycleAction;
import org.elasticsearch.xpack.core.ilm.LifecyclePolicy;
import org.elasticsearch.xpack.core.ilm.Phase;
import org.elasticsearch.xpack.core.ilm.RolloverAction;
import org.elasticsearch.xpack.core.ilm.SearchableSnapshotAction;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.equalTo;

public class TimeSeriesUsageTransportActionTests extends ESTestCase {

    public void testForceMergeCountsSinglePhase() {
        var policies = Stream.of(
            // Explicitly enabled
            randomPolicy(Map.of(randomFrom("hot", "warm", "cold"), new PhaseConfig(true, true, false, false, null))),
            // ... but not needed
            randomPolicy(Map.of(randomFrom("hot", "cold", "frozen"), new PhaseConfig(true, true, false, true, false))),
            // Explicitly disabled but not needed
            randomPolicy(Map.of(randomFrom("hot", "warm"), new PhaseConfig(true, false, true, false, false))),
            // Unspecified and needed
            randomPolicy(Map.of(randomFrom("hot", "warm"), new PhaseConfig(true, null, false, true, false))),
            // Unspecified and not needed
            randomPolicy(Map.of(randomFrom("hot", "warm"), new PhaseConfig(true, null, false, true, true))),
            randomPolicy(Map.of(randomFrom("hot", "warm"), new PhaseConfig(true, null, true, true, false))),
            randomPolicy(Map.of(randomFrom("hot", "warm"), new PhaseConfig(true, null, false, true, true))),
            // No downsampling
            randomPolicy(Map.of(randomFrom("hot", "warm"), new PhaseConfig(false, null, false, true, true)))
        );
        var tracker = new TimeSeriesUsageTransportAction.IlmDownsamplingStatsTracker();
        policies.forEach(tracker::trackPolicy);
        TimeSeriesFeatureSetUsage.IlmPolicyStats ilmPolicyStats = tracker.calculateIlmPolicyStats();
        assertThat(ilmPolicyStats.forceMergeExplicitlyEnabledCounter(), equalTo(2L));
        assertThat(ilmPolicyStats.forceMergeExplicitlyDisabledCounter(), equalTo(1L));
        assertThat(ilmPolicyStats.forceMergeDefaultCounter(), equalTo(4L));
        assertThat(ilmPolicyStats.downsampledForceMergeNeededCounter(), equalTo(1L));
    }

    public void testForceMergeCountsOverMultiplePhases() {
        var tracker = new TimeSeriesUsageTransportAction.IlmDownsamplingStatsTracker();
        tracker.trackPolicy(
            randomPolicy(
                Map.of("warm", new PhaseConfig(true, null, false, false, false), "cold", new PhaseConfig(true, null, false, true, false))
            )
        );
        TimeSeriesFeatureSetUsage.IlmPolicyStats ilmPolicyStats = tracker.calculateIlmPolicyStats();
        assertThat(ilmPolicyStats.forceMergeExplicitlyEnabledCounter(), equalTo(0L));
        assertThat(ilmPolicyStats.forceMergeExplicitlyDisabledCounter(), equalTo(0L));
        assertThat(ilmPolicyStats.forceMergeDefaultCounter(), equalTo(2L));
        assertThat(ilmPolicyStats.downsampledForceMergeNeededCounter(), equalTo(1L));

        // Because the force merge is happening due to another downsample action, we still count one.
        tracker = new TimeSeriesUsageTransportAction.IlmDownsamplingStatsTracker();
        tracker.trackPolicy(
            randomPolicy(
                Map.of("warm", new PhaseConfig(true, null, false, false, false), "cold", new PhaseConfig(true, true, false, true, false))
            )
        );
        ilmPolicyStats = tracker.calculateIlmPolicyStats();
        assertThat(ilmPolicyStats.forceMergeExplicitlyEnabledCounter(), equalTo(1L));
        assertThat(ilmPolicyStats.forceMergeExplicitlyDisabledCounter(), equalTo(0L));
        assertThat(ilmPolicyStats.forceMergeDefaultCounter(), equalTo(1L));
        assertThat(ilmPolicyStats.downsampledForceMergeNeededCounter(), equalTo(1L));
    }

    private LifecyclePolicy randomPolicy(Map<String, PhaseConfig> phases) {
        Map<String, Phase> phasesMap = new HashMap<>();
        for (var entry : phases.entrySet()) {
            Map<String, LifecycleAction> actions = new HashMap<>();
            PhaseConfig phaseConfig = entry.getValue();
            if (entry.getKey().equals("hot")) {
                actions.put(
                    RolloverAction.NAME,
                    new RolloverAction(null, null, TimeValue.ONE_HOUR, null, null, null, null, null, null, null)
                );
            }
            if (phaseConfig.hasDownsampling) {
                actions.put(
                    DownsampleAction.NAME,
                    new DownsampleAction(new DateHistogramInterval("1m"), null, phaseConfig.hasDownsamplingForceMerge, null)
                );
            }
            if (phaseConfig.hasForceMerge) {
                actions.put(ForceMergeAction.NAME, new ForceMergeAction(1, null));
            }
            if (phaseConfig.hasSearchableSnapshot) {
                actions.put(
                    SearchableSnapshotAction.NAME,
                    new SearchableSnapshotAction("my-repo", phaseConfig.searchableSnapshotForceMerge)
                );
            }
            phasesMap.put(entry.getKey(), new Phase(entry.getKey(), null, actions));
        }
        return new LifecyclePolicy(randomAlphaOfLength(10), phasesMap);
    }

    private record PhaseConfig(
        boolean hasDownsampling,
        Boolean hasDownsamplingForceMerge,
        Boolean hasForceMerge,
        Boolean hasSearchableSnapshot,
        Boolean searchableSnapshotForceMerge
    ) {}
}
