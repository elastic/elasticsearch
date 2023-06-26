/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ilm;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.Version;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xpack.core.ilm.IndexLifecycleMetadata;
import org.elasticsearch.xpack.core.ilm.LifecycleExecutionState;
import org.elasticsearch.xpack.core.ilm.LifecyclePolicy;
import org.elasticsearch.xpack.core.ilm.LifecyclePolicyMetadata;
import org.elasticsearch.xpack.core.ilm.LifecyclePolicyTests;
import org.elasticsearch.xpack.core.ilm.LifecycleSettings;
import org.elasticsearch.xpack.core.ilm.OperationMode;
import org.elasticsearch.xpack.core.ilm.Step;
import org.elasticsearch.xpack.core.ilm.Step.StepKey;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.xpack.core.ilm.LifecycleExecutionState.ILM_CUSTOM_METADATA_KEY;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MoveToNextStepUpdateTaskTests extends ESTestCase {

    private static final NamedXContentRegistry REGISTRY;

    String policy;
    ClusterState clusterState;
    Index index;
    LifecyclePolicy lifecyclePolicy;

    static {
        try (IndexLifecycle indexLifecycle = new IndexLifecycle(Settings.EMPTY)) {
            List<NamedXContentRegistry.Entry> entries = new ArrayList<>(indexLifecycle.getNamedXContent());
            REGISTRY = new NamedXContentRegistry(entries);
        }
    }

    @Before
    public void setupClusterState() {
        policy = randomAlphaOfLength(10);
        IndexMetadata indexMetadata = IndexMetadata.builder(randomAlphaOfLength(5))
            .settings(settings(Version.CURRENT).put(LifecycleSettings.LIFECYCLE_NAME, policy))
            .numberOfShards(randomIntBetween(1, 5))
            .numberOfReplicas(randomIntBetween(0, 5))
            .build();
        index = indexMetadata.getIndex();
        lifecyclePolicy = LifecyclePolicyTests.randomTestLifecyclePolicy(policy);
        IndexLifecycleMetadata ilmMeta = new IndexLifecycleMetadata(
            Collections.singletonMap(
                policy,
                new LifecyclePolicyMetadata(lifecyclePolicy, Collections.emptyMap(), randomNonNegativeLong(), randomNonNegativeLong())
            ),
            OperationMode.RUNNING
        );
        Metadata metadata = Metadata.builder()
            .persistentSettings(settings(Version.CURRENT).build())
            .put(IndexMetadata.builder(indexMetadata))
            .putCustom(IndexLifecycleMetadata.TYPE, ilmMeta)
            .build();
        clusterState = ClusterState.builder(ClusterName.DEFAULT).metadata(metadata).build();
    }

    public void testExecuteSuccessfullyMoved() throws Exception {
        long now = randomNonNegativeLong();
        List<Step> steps = lifecyclePolicy.toSteps(null, null);
        StepKey currentStepKey = steps.get(0).getKey();
        StepKey nextStepKey = steps.get(0).getNextStepKey();

        setStateToKey(currentStepKey, now);

        AtomicBoolean changed = new AtomicBoolean(false);
        Client client = mock(Client.class);
        when(client.settings()).thenReturn(Settings.EMPTY);
        AlwaysExistingStepRegistry stepRegistry = new AlwaysExistingStepRegistry(client);
        stepRegistry.update(
            new IndexLifecycleMetadata(
                org.elasticsearch.core.Map.of(policy, new LifecyclePolicyMetadata(lifecyclePolicy, Collections.emptyMap(), 2L, 2L)),
                OperationMode.RUNNING
            )
        );
        MoveToNextStepUpdateTask task = new MoveToNextStepUpdateTask(
            index,
            policy,
            currentStepKey,
            nextStepKey,
            () -> now,
            stepRegistry,
            state -> changed.set(true)
        );
        ClusterState newState = task.execute(clusterState);
        LifecycleExecutionState lifecycleState = LifecycleExecutionState.fromIndexMetadata(newState.getMetadata().index(index));
        StepKey actualKey = LifecycleExecutionState.getCurrentStepKey(lifecycleState);
        assertThat(actualKey, equalTo(nextStepKey));
        assertThat(lifecycleState.getPhaseTime(), equalTo(now));
        assertThat(lifecycleState.getActionTime(), equalTo(now));
        assertThat(lifecycleState.getStepTime(), equalTo(now));
        task.clusterStateProcessed("source", clusterState, newState);
        assertTrue(changed.get());
    }

    public void testExecuteDifferentCurrentStep() throws Exception {
        StepKey currentStepKey = new StepKey("current-phase", "current-action", "current-name");
        StepKey notCurrentStepKey = new StepKey("not-current", "not-current", "not-current");
        long now = randomNonNegativeLong();
        setStateToKey(notCurrentStepKey, now);
        MoveToNextStepUpdateTask task = new MoveToNextStepUpdateTask(
            index,
            policy,
            currentStepKey,
            null,
            () -> now,
            new AlwaysExistingStepRegistry(),
            null
        );
        ClusterState newState = task.execute(clusterState);
        assertSame(newState, clusterState);
    }

    public void testExecuteDifferentPolicy() throws Exception {
        StepKey currentStepKey = new StepKey("current-phase", "current-action", "current-name");
        long now = randomNonNegativeLong();
        setStateToKey(currentStepKey, now);
        setStatePolicy("not-" + policy);
        MoveToNextStepUpdateTask task = new MoveToNextStepUpdateTask(
            index,
            policy,
            currentStepKey,
            null,
            () -> now,
            new AlwaysExistingStepRegistry(),
            null
        );
        ClusterState newState = task.execute(clusterState);
        assertSame(newState, clusterState);
    }

    public void testExecuteSuccessfulMoveWithInvalidNextStep() throws Exception {
        long now = randomNonNegativeLong();
        List<Step> steps = lifecyclePolicy.toSteps(null, null);
        StepKey currentStepKey = steps.get(0).getKey();
        StepKey invalidNextStep = new StepKey("next-invalid", "next-invalid", "next-invalid");

        setStateToKey(currentStepKey, now);

        SetOnce<Boolean> changed = new SetOnce<>();
        Client client = mock(Client.class);
        when(client.settings()).thenReturn(Settings.EMPTY);
        AlwaysExistingStepRegistry stepRegistry = new AlwaysExistingStepRegistry(client);
        stepRegistry.update(
            new IndexLifecycleMetadata(
                org.elasticsearch.core.Map.of(policy, new LifecyclePolicyMetadata(lifecyclePolicy, Collections.emptyMap(), 2L, 2L)),
                OperationMode.RUNNING
            )
        );
        MoveToNextStepUpdateTask task = new MoveToNextStepUpdateTask(
            index,
            policy,
            currentStepKey,
            invalidNextStep,
            () -> now,
            stepRegistry,
            s -> changed.set(true)
        );
        ClusterState newState = task.execute(clusterState);
        LifecycleExecutionState lifecycleState = LifecycleExecutionState.fromIndexMetadata(newState.getMetadata().index(index));
        StepKey actualKey = LifecycleExecutionState.getCurrentStepKey(lifecycleState);
        assertThat(actualKey, equalTo(invalidNextStep));
        assertThat(lifecycleState.getPhaseTime(), equalTo(now));
        assertThat(lifecycleState.getActionTime(), equalTo(now));
        assertThat(lifecycleState.getStepTime(), equalTo(now));
        task.clusterStateProcessed("source", clusterState, newState);
        assertTrue(changed.get());
    }

    public void testOnFailure() {
        StepKey currentStepKey = new StepKey("current-phase", "current-action", "current-name");
        StepKey nextStepKey = new StepKey("next-phase", "next-action", "next-name");
        long now = randomNonNegativeLong();

        setStateToKey(currentStepKey, now);

        MoveToNextStepUpdateTask task = new MoveToNextStepUpdateTask(
            index,
            policy,
            currentStepKey,
            nextStepKey,
            () -> now,
            new AlwaysExistingStepRegistry(),
            state -> {}
        );
        Exception expectedException = new RuntimeException();
        task.onFailure(randomAlphaOfLength(10), expectedException);
    }

    /**
     * Fake policy steps registry that will always pass validation that the step exists
     */
    private static class AlwaysExistingStepRegistry extends PolicyStepsRegistry {

        AlwaysExistingStepRegistry() {
            this(null);
        }

        AlwaysExistingStepRegistry(Client client) {
            super(REGISTRY, client, null);
        }

        @Override
        public boolean stepExists(String policy, StepKey stepKey) {
            return true;
        }
    }

    private void setStatePolicy(String policyValue) {
        clusterState = ClusterState.builder(clusterState)
            .metadata(
                Metadata.builder(clusterState.metadata())
                    .updateSettings(Settings.builder().put(LifecycleSettings.LIFECYCLE_NAME, policyValue).build(), index.getName())
            )
            .build();

    }

    private void setStateToKey(StepKey stepKey, long now) {
        LifecycleExecutionState.Builder lifecycleState = LifecycleExecutionState.builder(
            LifecycleExecutionState.fromIndexMetadata(clusterState.metadata().index(index))
        );
        lifecycleState.setPhase(stepKey.getPhase());
        lifecycleState.setPhaseTime(now);
        lifecycleState.setAction(stepKey.getAction());
        lifecycleState.setActionTime(now);
        lifecycleState.setStep(stepKey.getName());
        lifecycleState.setStepTime(now);

        lifecycleState.setPhaseDefinition(
            String.format(
                Locale.ROOT,
                ""
                    + " {\n"
                    + "              \"policy\" : \"%s\",\n"
                    + "              \"phase_definition\" : {\n"
                    + "                \"min_age\" : \"20m\",\n"
                    + "                \"actions\" : {\n"
                    + "                }\n"
                    + "              },\n"
                    + "              \"version\" : 1,\n"
                    + "              \"modified_date_in_millis\" : 1578521007076\n"
                    + "            }"
                    + "",
                policy
            )
        );
        clusterState = ClusterState.builder(clusterState)
            .metadata(
                Metadata.builder(clusterState.getMetadata())
                    .put(
                        IndexMetadata.builder(clusterState.getMetadata().index(index))
                            .putCustom(ILM_CUSTOM_METADATA_KEY, lifecycleState.build().asMap())
                    )
            )
            .build();
    }
}
