/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ilm;

import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.DataStreamLifecycle;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ilm.DeleteAction;
import org.elasticsearch.xpack.core.ilm.DownsampleAction;
import org.elasticsearch.xpack.core.ilm.ForceMergeAction;
import org.elasticsearch.xpack.core.ilm.IndexLifecycleMetadata;
import org.elasticsearch.xpack.core.ilm.LifecycleAction;
import org.elasticsearch.xpack.core.ilm.LifecyclePolicy;
import org.elasticsearch.xpack.core.ilm.LifecyclePolicyMetadata;
import org.elasticsearch.xpack.core.ilm.LifecycleSettings;
import org.elasticsearch.xpack.core.ilm.OperationMode;
import org.elasticsearch.xpack.core.ilm.Phase;
import org.elasticsearch.xpack.core.ilm.ReadOnlyAction;
import org.elasticsearch.xpack.core.ilm.RolloverAction;
import org.elasticsearch.xpack.core.ilm.SearchableSnapshotAction;
import org.elasticsearch.xpack.core.ilm.ShrinkAction;
import org.elasticsearch.xpack.core.ilm.TimeseriesLifecycleType;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.core.ilm.DeleteAction.WITH_SNAPSHOT_DELETE;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class TimeSeriesEligibleWriteWindowLocatorWithIlmTests extends ESTestCase {

    private final TimeSeriesEligibleWriteWindowLocatorWithIlm locator = new TimeSeriesEligibleWriteWindowLocatorWithIlm();
    private static final Map<String, LifecycleAction> readOnlyActions = Map.of(
        ReadOnlyAction.NAME,
        new ReadOnlyAction(),
        DownsampleAction.NAME,
        new DownsampleAction(new DateHistogramInterval("1m"), TimeValue.ONE_HOUR, true, null),
        ShrinkAction.NAME,
        new ShrinkAction(1, null, true),
        ForceMergeAction.NAME,
        new ForceMergeAction(1, null),
        SearchableSnapshotAction.NAME,
        new SearchableSnapshotAction("my-repo"),
        DeleteAction.NAME,
        WITH_SNAPSHOT_DELETE
    );

    public void testNoIlmPolicy() {
        String name = "metrics-test";
        ProjectMetadata project = ProjectMetadata.builder(randomProjectIdOrDefault()).build();
        DataStream dataStream = dataStream(name, null);
        assertThat(locator.getEffectiveIlmPolicy(dataStream, project), nullValue());

        // template matches the stream name but carries no index.lifecycle.name setting
        projectWithTemplate(name + "*", Settings.EMPTY);
        assertThat(locator.getEffectiveIlmPolicy(dataStream, project), nullValue());
    }

    public void testGetEffectiveIlmPolicy() {
        String name = "metrics-test";
        ProjectMetadata project = projectWithTemplate(
            name + "*",
            Settings.builder().put(LifecycleSettings.LIFECYCLE_NAME, "my-policy").build()
        );
        // ILM policy set on template, no DLM lifecycle on the stream
        {
            DataStream dataStream = dataStream(name, null);
            assertThat(locator.getEffectiveIlmPolicy(dataStream, project), equalTo("my-policy"));
        }

        // ILM policy + DLM lifecycle; prefer_ilm defaults to true so ILM wins
        {
            DataStream dataStream = dataStream(name, DataStreamLifecycle.DEFAULT_DATA_LIFECYCLE);
            assertThat(locator.getEffectiveIlmPolicy(dataStream, project), equalTo("my-policy"));
        }
    }

    public void testGetEffectiveIlmPolicyBothConfiguredPreferDlm() {
        String name = "metrics-test";
        ProjectMetadata project = projectWithTemplate(
            name + "*",
            Settings.builder().put(LifecycleSettings.LIFECYCLE_NAME, "my-policy").put(IndexSettings.PREFER_ILM, false).build()
        );
        DataStream dataStream = dataStream(name, DataStreamLifecycle.DEFAULT_DATA_LIFECYCLE);
        assertThat(locator.getEffectiveIlmPolicy(dataStream, project), nullValue());
    }

    public void testNoEligibleWriteWindowStart() {
        // project has no IndexLifecycleMetadata at all
        {
            ProjectMetadata project = ProjectMetadata.builder(randomProjectIdOrDefault()).build();
            assertThat(locator.getEligibleWriteWindowFromPolicy("my-policy", project), equalTo(-1L));
        }

        // No ILM policy with the provided name
        {
            ProjectMetadata project = projectWithIlmMetadata(Map.of());
            assertThat(locator.getEligibleWriteWindowFromPolicy("missing-policy", project), equalTo(-1L));
        }

        // policy exists but none of its actions are read-only actions
        {
            Phase hot = new Phase("hot", TimeValue.timeValueDays(1), Map.of());
            LifecyclePolicy policy = new LifecyclePolicy("my-policy", Map.of("hot", hot));
            ProjectMetadata project = projectWithIlmMetadata(
                Map.of("my-policy", new LifecyclePolicyMetadata(policy, Map.of(), 1L, randomNonNegativeLong()))
            );
            assertThat(locator.getEligibleWriteWindowFromPolicy("my-policy", project), equalTo(-1L));
        }
    }

    public void testEligibleWriteWindowStartReadOnlyActionInAPhase() {
        TimeValue age = TimeValue.timeValueDays(randomIntBetween(1, 10));
        Phase phase = randomReadOnlyPhase(age);
        LifecyclePolicy policy = new LifecyclePolicy("my-policy", Map.of(phase.getName(), phase));
        ProjectMetadata project = projectWithIlmMetadata(
            Map.of("my-policy", new LifecyclePolicyMetadata(policy, Map.of(), 1L, randomNonNegativeLong()))
        );
        assertThat(locator.getEligibleWriteWindowFromPolicy("my-policy", project), equalTo(age.millis()));
    }

    public void testEligibleWriteWindowStartReadOnlyActionInSecondPhase() {
        TimeValue warmAge = TimeValue.timeValueDays(randomIntBetween(1, 10));
        Phase hot = new Phase(
            "hot",
            TimeValue.ZERO,
            Map.of(RolloverAction.NAME, new RolloverAction(null, null, null, 1L, null, null, null, null, null, null))
        );
        Phase warm = new Phase("warm", warmAge, Map.of(ReadOnlyAction.NAME, new ReadOnlyAction()));
        LifecyclePolicy policy = new LifecyclePolicy("my-policy", Map.of("hot", hot, "warm", warm));
        ProjectMetadata project = projectWithIlmMetadata(
            Map.of("my-policy", new LifecyclePolicyMetadata(policy, Map.of(), 1L, randomNonNegativeLong()))
        );
        assertEquals(warmAge.millis(), locator.getEligibleWriteWindowFromPolicy("my-policy", project));
    }

    public void testEligibleWriteWindowStartEarliestPhaseWins() {
        // both hot and warm define read-only actions; hot comes first in ORDERED_VALID_PHASES
        TimeValue hotAge = TimeValue.timeValueDays(1);
        TimeValue warmAge = TimeValue.timeValueDays(7);
        Phase hot = new Phase("hot", hotAge, Map.of(ForceMergeAction.NAME, new ForceMergeAction(1, null)));
        Phase warm = new Phase("warm", warmAge, Map.of(ReadOnlyAction.NAME, new ReadOnlyAction()));
        LifecyclePolicy policy = new LifecyclePolicy("my-policy", Map.of("hot", hot, "warm", warm));
        ProjectMetadata project = projectWithIlmMetadata(
            Map.of("my-policy", new LifecyclePolicyMetadata(policy, Map.of(), 1L, randomNonNegativeLong()))
        );
        assertEquals(hotAge.millis(), locator.getEligibleWriteWindowFromPolicy("my-policy", project));
    }

    /**
     * This method produces a SINGLE valid read-only phase. If this method is used to create
     * different phases, it doesn't mean they can be combined to a valid policy since it contains
     * actions that could be seen as "final". For example, a searchable snapshot in hot, means
     * that you cannot force merge in warm.
     */
    private static Phase randomReadOnlyPhase(TimeValue age) {
        String phaseName = randomFrom(TimeseriesLifecycleType.ORDERED_VALID_PHASES);
        List<String> validPhaseActions = switch (phaseName) {
            case "hot" -> TimeseriesLifecycleType.ORDERED_VALID_HOT_ACTIONS;
            case "warm" -> TimeseriesLifecycleType.ORDERED_VALID_WARM_ACTIONS;
            case "cold" -> TimeseriesLifecycleType.ORDERED_VALID_COLD_ACTIONS;
            case "frozen" -> TimeseriesLifecycleType.ORDERED_VALID_FROZEN_ACTIONS;
            case "delete" -> TimeseriesLifecycleType.ORDERED_VALID_DELETE_ACTIONS;
            default -> throw new AssertionError("unexpected phase name: " + phaseName);
        };
        List<String> readOnlyActionsInPhase = validPhaseActions.stream()
            .filter(TimeseriesLifecycleType.READ_ONLY_ACTIONS::contains)
            .toList();
        Map<String, LifecycleAction> phaseConfig = randomNonEmptySubsetOf(readOnlyActionsInPhase).stream()
            .collect(Collectors.toMap(Function.identity(), readOnlyActions::get));
        return new Phase(phaseName, age, phaseConfig);
    }

    private static DataStream dataStream(String name, DataStreamLifecycle lifecycle) {
        Index index = new Index(".ds-" + name + "-000001", randomAlphaOfLength(10));
        return DataStream.builder(name, List.of(index)).setLifecycle(lifecycle).setIndexMode(IndexMode.TIME_SERIES).build();
    }

    private static ProjectMetadata projectWithTemplate(String indexPattern, Settings templateSettings) {
        return ProjectMetadata.builder(randomProjectIdOrDefault())
            .put(
                "my-template",
                ComposableIndexTemplate.builder()
                    .indexPatterns(List.of(indexPattern))
                    .template(Template.builder().settings(templateSettings).build())
                    .dataStreamTemplate(new ComposableIndexTemplate.DataStreamTemplate())
                    .build()
            )
            .build();
    }

    private static ProjectMetadata projectWithIlmMetadata(Map<String, LifecyclePolicyMetadata> policies) {
        return ProjectMetadata.builder(randomProjectIdOrDefault())
            .putCustom(IndexLifecycleMetadata.TYPE, new IndexLifecycleMetadata(policies, OperationMode.RUNNING))
            .build();
    }
}
