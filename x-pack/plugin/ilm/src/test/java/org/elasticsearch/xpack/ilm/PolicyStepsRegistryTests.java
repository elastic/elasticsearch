/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ilm;

import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.LifecycleExecutionState;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xpack.core.ilm.ErrorStep;
import org.elasticsearch.xpack.core.ilm.IndexLifecycleMetadata;
import org.elasticsearch.xpack.core.ilm.InitializePolicyContextStep;
import org.elasticsearch.xpack.core.ilm.LifecycleAction;
import org.elasticsearch.xpack.core.ilm.LifecyclePolicy;
import org.elasticsearch.xpack.core.ilm.LifecyclePolicyMetadata;
import org.elasticsearch.xpack.core.ilm.LifecyclePolicyTests;
import org.elasticsearch.xpack.core.ilm.LifecycleSettings;
import org.elasticsearch.xpack.core.ilm.MigrateAction;
import org.elasticsearch.xpack.core.ilm.MockStep;
import org.elasticsearch.xpack.core.ilm.OperationMode;
import org.elasticsearch.xpack.core.ilm.Phase;
import org.elasticsearch.xpack.core.ilm.PhaseExecutionInfo;
import org.elasticsearch.xpack.core.ilm.ShrinkAction;
import org.elasticsearch.xpack.core.ilm.ShrinkStep;
import org.elasticsearch.xpack.core.ilm.Step;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.cluster.metadata.LifecycleExecutionState.ILM_CUSTOM_METADATA_KEY;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.Mockito.mock;

public class PolicyStepsRegistryTests extends ESTestCase {
    private static final Step.StepKey MOCK_STEP_KEY = new Step.StepKey("mock", "mock", "mock");
    private static final NamedXContentRegistry REGISTRY = new NamedXContentRegistry(new IndexLifecycle(Settings.EMPTY).getNamedXContent());

    private IndexMetadata emptyMetadata(Index index) {
        return IndexMetadata.builder(index.getName())
            .settings(settings(IndexVersion.current()))
            .numberOfShards(randomIntBetween(1, 5))
            .numberOfReplicas(randomIntBetween(0, 5))
            .build();
    }

    public void testGetFirstStep() {
        String policyName = randomAlphaOfLengthBetween(2, 10);
        Step expectedFirstStep = new MockStep(MOCK_STEP_KEY, null);
        Map<String, Step> firstStepMap = Map.of(policyName, expectedFirstStep);
        PolicyStepsRegistry registry = new PolicyStepsRegistry(null, firstStepMap, null, NamedXContentRegistry.EMPTY, null, null);
        Step actualFirstStep = registry.getFirstStep(policyName);
        assertThat(actualFirstStep, sameInstance(expectedFirstStep));
    }

    public void testGetFirstStepUnknownPolicy() {
        String policyName = randomAlphaOfLengthBetween(2, 10);
        Step expectedFirstStep = new MockStep(MOCK_STEP_KEY, null);
        Map<String, Step> firstStepMap = Map.of(policyName, expectedFirstStep);
        PolicyStepsRegistry registry = new PolicyStepsRegistry(null, firstStepMap, null, NamedXContentRegistry.EMPTY, null, null);
        Step actualFirstStep = registry.getFirstStep(policyName + "unknown");
        assertNull(actualFirstStep);
    }

    public void testGetStep() {
        Client client = mock(Client.class);
        Mockito.when(client.settings()).thenReturn(Settings.EMPTY);
        LifecyclePolicy policy = LifecyclePolicyTests.randomTimeseriesLifecyclePolicyWithAllPhases("policy");
        LifecyclePolicyMetadata policyMetadata = new LifecyclePolicyMetadata(policy, Map.of(), 1, randomNonNegativeLong());
        String phaseName = randomFrom(policy.getPhases().keySet());
        Phase phase = policy.getPhases().get(phaseName);
        PhaseExecutionInfo pei = new PhaseExecutionInfo(policy.getName(), phase, 1, randomNonNegativeLong());
        String phaseJson = Strings.toString(pei);
        LifecycleAction action = randomValueOtherThan(MigrateAction.DISABLED, () -> randomFrom(phase.getActions().values()));
        Step step = randomFrom(action.toSteps(client, phaseName, MOCK_STEP_KEY, null));
        LifecycleExecutionState.Builder lifecycleState = LifecycleExecutionState.builder();
        lifecycleState.setPhaseDefinition(phaseJson);
        IndexMetadata indexMetadata = IndexMetadata.builder("test")
            .settings(indexSettings(IndexVersion.current(), 1, 0).put(LifecycleSettings.LIFECYCLE_NAME, "policy"))
            .putCustom(ILM_CUSTOM_METADATA_KEY, lifecycleState.build().asMap())
            .build();
        SortedMap<String, LifecyclePolicyMetadata> metas = new TreeMap<>();
        metas.put("policy", policyMetadata);
        PolicyStepsRegistry registry = new PolicyStepsRegistry(metas, null, null, REGISTRY, client, null);
        Step actualStep = registry.getStep(indexMetadata, step.getKey());
        assertThat(actualStep.getKey(), equalTo(step.getKey()));
    }

    public void testGetStepErrorStep() {
        Step.StepKey errorStepKey = new Step.StepKey(randomAlphaOfLengthBetween(1, 10), randomAlphaOfLengthBetween(1, 10), ErrorStep.NAME);
        Step expectedStep = new ErrorStep(errorStepKey);
        Index index = new Index("test", "uuid");
        Map<Index, List<Step>> indexSteps = Map.of(index, List.of(expectedStep));
        PolicyStepsRegistry registry = new PolicyStepsRegistry(null, null, null, NamedXContentRegistry.EMPTY, null, null);
        Step actualStep = registry.getStep(emptyMetadata(index), errorStepKey);
        assertThat(actualStep, equalTo(expectedStep));
    }

    public void testGetStepUnknownPolicy() {
        PolicyStepsRegistry registry = new PolicyStepsRegistry(null, null, null, NamedXContentRegistry.EMPTY, null, null);
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> registry.getStep(emptyMetadata(new Index("test", "uuid")), MOCK_STEP_KEY)
        );
        assertThat(
            e.getMessage(),
            containsString(
                "failed to retrieve step {\"phase\":\"mock\",\"action\":\"mock\",\"name\":\"mock\"}" + " as index [test] has no policy"
            )
        );
    }

    public void testGetStepForIndexWithNoPhaseGetsInitializationStep() {
        Client client = mock(Client.class);
        Mockito.when(client.settings()).thenReturn(Settings.EMPTY);
        LifecyclePolicy policy = LifecyclePolicyTests.randomTimeseriesLifecyclePolicy("policy");
        LifecyclePolicyMetadata policyMetadata = new LifecyclePolicyMetadata(policy, Map.of(), 1, randomNonNegativeLong());
        IndexMetadata indexMetadata = IndexMetadata.builder("test")
            .settings(indexSettings(IndexVersion.current(), 1, 0).put(LifecycleSettings.LIFECYCLE_NAME, "policy").build())
            .build();
        SortedMap<String, LifecyclePolicyMetadata> metas = new TreeMap<>();
        metas.put("policy", policyMetadata);
        PolicyStepsRegistry registry = new PolicyStepsRegistry(metas, null, null, REGISTRY, client, null);
        Step step = registry.getStep(indexMetadata, InitializePolicyContextStep.KEY);
        assertNotNull(step);
    }

    public void testGetStepUnknownStepKey() {
        Client client = mock(Client.class);
        Mockito.when(client.settings()).thenReturn(Settings.EMPTY);
        LifecyclePolicy policy = LifecyclePolicyTests.randomTimeseriesLifecyclePolicyWithAllPhases("policy");
        LifecyclePolicyMetadata policyMetadata = new LifecyclePolicyMetadata(policy, Map.of(), 1, randomNonNegativeLong());
        String phaseName = randomFrom(policy.getPhases().keySet());
        Phase phase = policy.getPhases().get(phaseName);
        PhaseExecutionInfo pei = new PhaseExecutionInfo(policy.getName(), phase, 1, randomNonNegativeLong());
        String phaseJson = Strings.toString(pei);
        LifecycleAction action = randomValueOtherThan(MigrateAction.DISABLED, () -> randomFrom(phase.getActions().values()));
        Step step = randomFrom(action.toSteps(client, phaseName, MOCK_STEP_KEY, null));
        LifecycleExecutionState.Builder lifecycleState = LifecycleExecutionState.builder();
        lifecycleState.setPhaseDefinition(phaseJson);
        IndexMetadata indexMetadata = IndexMetadata.builder("test")
            .settings(indexSettings(IndexVersion.current(), 1, 0).put(LifecycleSettings.LIFECYCLE_NAME, "policy").build())
            .putCustom(ILM_CUSTOM_METADATA_KEY, lifecycleState.build().asMap())
            .build();
        SortedMap<String, LifecyclePolicyMetadata> metas = new TreeMap<>();
        metas.put("policy", policyMetadata);
        PolicyStepsRegistry registry = new PolicyStepsRegistry(metas, null, null, REGISTRY, client, null);
        Step.StepKey badStepKey = new Step.StepKey(step.getKey().phase(), step.getKey().action(), step.getKey().name() + "-bad");
        assertNull(registry.getStep(indexMetadata, badStepKey));
        // repeat the test to make sure that nulls don't poison the registry's cache
        assertNull(registry.getStep(indexMetadata, badStepKey));
    }

    public void testUpdateFromNothingToSomethingToNothing() throws Exception {
        Index index = new Index("test", "uuid");
        Client client = mock(Client.class);
        Mockito.when(client.settings()).thenReturn(Settings.EMPTY);
        String policyName = randomAlphaOfLength(5);
        LifecyclePolicy newPolicy = LifecyclePolicyTests.randomTestLifecyclePolicy(policyName);
        logger.info("--> policy: {}", newPolicy);
        List<Step> policySteps = newPolicy.toSteps(client, null);
        Map<String, String> headers = new HashMap<>();
        if (randomBoolean()) {
            headers.put(randomAlphaOfLength(10), randomAlphaOfLength(10));
            headers.put(randomAlphaOfLength(10), randomAlphaOfLength(10));
        }
        Map<String, LifecyclePolicyMetadata> policyMap = Map.of(
            newPolicy.getName(),
            new LifecyclePolicyMetadata(newPolicy, headers, randomNonNegativeLong(), randomNonNegativeLong())
        );
        IndexLifecycleMetadata lifecycleMetadata = new IndexLifecycleMetadata(policyMap, OperationMode.RUNNING);
        LifecycleExecutionState.Builder lifecycleState = LifecycleExecutionState.builder();
        lifecycleState.setPhase("new");
        ProjectMetadata currentProject = ProjectMetadata.builder(randomProjectIdOrDefault())
            .putCustom(IndexLifecycleMetadata.TYPE, lifecycleMetadata)
            .put(
                IndexMetadata.builder("test")
                    .settings(
                        indexSettings(1, 0).put("index.uuid", "uuid")
                            .put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current())
                            .put(LifecycleSettings.LIFECYCLE_NAME, policyName)
                    )
                    .putCustom(ILM_CUSTOM_METADATA_KEY, lifecycleState.build().asMap())
            )
            .build();

        // start with empty registry
        PolicyStepsRegistry registry = new PolicyStepsRegistry(NamedXContentRegistry.EMPTY, client, null);

        // add new policy
        registry.update(currentProject.custom(IndexLifecycleMetadata.TYPE));

        assertThat(registry.getFirstStep(newPolicy.getName()), equalTo(policySteps.get(0)));
        assertThat(registry.getLifecyclePolicyMap().size(), equalTo(1));
        assertNotNull(registry.getLifecyclePolicyMap().get(newPolicy.getName()));
        assertThat(registry.getLifecyclePolicyMap().get(newPolicy.getName()).getHeaders(), equalTo(headers));
        assertThat(registry.getFirstStepMap().size(), equalTo(1));
        assertThat(registry.getStepMap().size(), equalTo(1));
        Map<Step.StepKey, Step> registeredStepsForPolicy = registry.getStepMap().get(newPolicy.getName());
        assertThat(registeredStepsForPolicy.size(), equalTo(policySteps.size()));
        for (Step step : policySteps) {
            LifecycleExecutionState.Builder newIndexState = LifecycleExecutionState.builder();
            newIndexState.setPhase(step.getKey().phase());
            currentProject = ProjectMetadata.builder(currentProject)
                .put(
                    IndexMetadata.builder(currentProject.index("test"))
                        .settings(Settings.builder().put(currentProject.index("test").getSettings()))
                        .putCustom(ILM_CUSTOM_METADATA_KEY, newIndexState.build().asMap())
                )
                // .nodes(DiscoveryNodes.builder().localNodeId(nodeId).masterNodeId(nodeId).add(masterNode).build())
                .build();
            registry.update(currentProject.custom(IndexLifecycleMetadata.TYPE));
            assertThat(registeredStepsForPolicy.get(step.getKey()), equalTo(step));
            assertThat(registry.getStep(currentProject.index(index), step.getKey()), equalTo(step));
        }

        Map<String, LifecyclePolicyMetadata> registryPolicyMap = registry.getLifecyclePolicyMap();
        Map<String, Step> registryFirstStepMap = registry.getFirstStepMap();
        Map<String, Map<Step.StepKey, Step>> registryStepMap = registry.getStepMap();
        registry.update(currentProject.custom(IndexLifecycleMetadata.TYPE));
        assertThat(registry.getLifecyclePolicyMap(), equalTo(registryPolicyMap));
        assertThat(registry.getFirstStepMap(), equalTo(registryFirstStepMap));
        assertThat(registry.getStepMap(), equalTo(registryStepMap));

        // remove policy
        lifecycleMetadata = new IndexLifecycleMetadata(Map.of(), OperationMode.RUNNING);
        currentProject = ProjectMetadata.builder(currentProject).putCustom(IndexLifecycleMetadata.TYPE, lifecycleMetadata).build();
        registry.update(currentProject.custom(IndexLifecycleMetadata.TYPE));
        assertTrue(registry.getLifecyclePolicyMap().isEmpty());
        assertTrue(registry.getFirstStepMap().isEmpty());
        assertTrue(registry.getStepMap().isEmpty());
    }

    public void testUpdateChangedPolicy() {
        Client client = mock(Client.class);
        Mockito.when(client.settings()).thenReturn(Settings.EMPTY);
        String policyName = randomAlphaOfLengthBetween(5, 10);
        LifecyclePolicy newPolicy = LifecyclePolicyTests.randomTestLifecyclePolicy(policyName);
        Map<String, String> headers = new HashMap<>();
        if (randomBoolean()) {
            headers.put(randomAlphaOfLength(10), randomAlphaOfLength(10));
            headers.put(randomAlphaOfLength(10), randomAlphaOfLength(10));
        }
        Map<String, LifecyclePolicyMetadata> policyMap = Map.of(
            newPolicy.getName(),
            new LifecyclePolicyMetadata(newPolicy, headers, randomNonNegativeLong(), randomNonNegativeLong())
        );
        IndexLifecycleMetadata lifecycleMetadata = new IndexLifecycleMetadata(policyMap, OperationMode.RUNNING);
        ProjectMetadata currentProject = ProjectMetadata.builder(randomProjectIdOrDefault())
            .putCustom(IndexLifecycleMetadata.TYPE, lifecycleMetadata)
            .build();
        PolicyStepsRegistry registry = new PolicyStepsRegistry(NamedXContentRegistry.EMPTY, client, null);
        // add new policy
        registry.update(currentProject.custom(IndexLifecycleMetadata.TYPE));

        // swap out policy
        newPolicy = LifecyclePolicyTests.randomTestLifecyclePolicy(policyName);
        lifecycleMetadata = new IndexLifecycleMetadata(
            Map.of(policyName, new LifecyclePolicyMetadata(newPolicy, Map.of(), randomNonNegativeLong(), randomNonNegativeLong())),
            OperationMode.RUNNING
        );
        currentProject = ProjectMetadata.builder(currentProject).putCustom(IndexLifecycleMetadata.TYPE, lifecycleMetadata).build();
        registry.update(currentProject.custom(IndexLifecycleMetadata.TYPE));
        // TODO(talevy): assert changes... right now we do not support updates to policies. will require internal cleanup
    }

    public void testUpdatePolicyButNoPhaseChangeIndexStepsDontChange() throws Exception {
        Index index = new Index("test", "uuid");
        Client client = mock(Client.class);
        Mockito.when(client.settings()).thenReturn(Settings.EMPTY);
        String policyName = randomAlphaOfLength(5);
        Map<String, LifecycleAction> actions = new HashMap<>();
        actions.put("shrink", new ShrinkAction(1, null, false));
        Map<String, Phase> phases = new HashMap<>();
        Phase warmPhase = new Phase("warm", TimeValue.ZERO, actions);
        PhaseExecutionInfo pei = new PhaseExecutionInfo(policyName, warmPhase, 1, randomNonNegativeLong());
        String phaseJson = Strings.toString(pei);
        phases.put("warm", new Phase("warm", TimeValue.ZERO, actions));
        LifecyclePolicy newPolicy = new LifecyclePolicy(policyName, phases);
        // Modify the policy
        actions = new HashMap<>();
        actions.put("shrink", new ShrinkAction(2, null, false));
        phases = new HashMap<>();
        phases.put("warm", new Phase("warm", TimeValue.ZERO, actions));
        LifecyclePolicy updatedPolicy = new LifecyclePolicy(policyName, phases);
        logger.info("--> policy: {}", newPolicy);
        logger.info("--> updated policy: {}", updatedPolicy);
        List<Step> policySteps = newPolicy.toSteps(client, null);
        Map<String, String> headers = new HashMap<>();
        if (randomBoolean()) {
            headers.put(randomAlphaOfLength(10), randomAlphaOfLength(10));
            headers.put(randomAlphaOfLength(10), randomAlphaOfLength(10));
        }
        Map<String, LifecyclePolicyMetadata> policyMap = Map.of(
            newPolicy.getName(),
            new LifecyclePolicyMetadata(newPolicy, headers, randomNonNegativeLong(), randomNonNegativeLong())
        );
        IndexLifecycleMetadata lifecycleMetadata = new IndexLifecycleMetadata(policyMap, OperationMode.RUNNING);
        LifecycleExecutionState.Builder lifecycleState = LifecycleExecutionState.builder();
        lifecycleState.setPhase("warm");
        lifecycleState.setPhaseDefinition(phaseJson);
        ProjectMetadata currentProject = ProjectMetadata.builder(randomProjectIdOrDefault())
            .putCustom(IndexLifecycleMetadata.TYPE, lifecycleMetadata)
            .put(
                IndexMetadata.builder("test")
                    .settings(
                        indexSettings(1, 0).put("index.uuid", "uuid")
                            .put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current())
                            .put(LifecycleSettings.LIFECYCLE_NAME, policyName)
                    )
                    .putCustom(ILM_CUSTOM_METADATA_KEY, lifecycleState.build().asMap())
            )
            .build();

        // start with empty registry
        PolicyStepsRegistry registry = new PolicyStepsRegistry(REGISTRY, client, null);

        // add new policy
        registry.update(currentProject.custom(IndexLifecycleMetadata.TYPE));

        Map<Step.StepKey, Step> registeredStepsForPolicy = registry.getStepMap().get(newPolicy.getName());
        Step shrinkStep = registeredStepsForPolicy.entrySet()
            .stream()
            .filter(e -> e.getKey().phase().equals("warm") && e.getKey().name().equals("shrink"))
            .findFirst()
            .get()
            .getValue();
        Step gotStep = registry.getStep(currentProject.index(index), shrinkStep.getKey());
        assertThat(((ShrinkStep) shrinkStep).getNumberOfShards(), equalTo(1));
        assertThat(((ShrinkStep) gotStep).getNumberOfShards(), equalTo(1));

        // Update the policy with the new policy, but keep the phase the same
        policyMap = Map.of(
            updatedPolicy.getName(),
            new LifecyclePolicyMetadata(updatedPolicy, headers, randomNonNegativeLong(), randomNonNegativeLong())
        );
        lifecycleMetadata = new IndexLifecycleMetadata(policyMap, OperationMode.RUNNING);
        currentProject = ProjectMetadata.builder(currentProject).putCustom(IndexLifecycleMetadata.TYPE, lifecycleMetadata).build();

        // Update the policies
        registry.update(currentProject.custom(IndexLifecycleMetadata.TYPE));

        registeredStepsForPolicy = registry.getStepMap().get(newPolicy.getName());
        shrinkStep = registeredStepsForPolicy.entrySet()
            .stream()
            .filter(e -> e.getKey().phase().equals("warm") && e.getKey().name().equals("shrink"))
            .findFirst()
            .get()
            .getValue();
        gotStep = registry.getStep(currentProject.index(index), shrinkStep.getKey());
        assertThat(((ShrinkStep) shrinkStep).getNumberOfShards(), equalTo(2));
        assertThat(((ShrinkStep) gotStep).getNumberOfShards(), equalTo(1));
    }

    public void testGetStepMultithreaded() throws Exception {
        Client client = mock(Client.class);
        Mockito.when(client.settings()).thenReturn(Settings.EMPTY);

        LifecyclePolicy policy = LifecyclePolicyTests.randomTimeseriesLifecyclePolicyWithAllPhases("policy");
        String phaseName = randomFrom(policy.getPhases().keySet());
        Phase phase = policy.getPhases().get(phaseName);

        LifecycleExecutionState lifecycleState = LifecycleExecutionState.builder()
            .setPhaseDefinition(Strings.toString(new PhaseExecutionInfo(policy.getName(), phase, 1, randomNonNegativeLong())))
            .build();
        IndexMetadata indexMetadata = IndexMetadata.builder("test")
            .settings(indexSettings(IndexVersion.current(), 1, 0).put(LifecycleSettings.LIFECYCLE_NAME, "policy").build())
            .putCustom(ILM_CUSTOM_METADATA_KEY, lifecycleState.asMap())
            .build();

        SortedMap<String, LifecyclePolicyMetadata> metas = new TreeMap<>();
        metas.put("policy", new LifecyclePolicyMetadata(policy, Map.of(), 1, randomNonNegativeLong()));
        IndexLifecycleMetadata meta = new IndexLifecycleMetadata(metas, OperationMode.RUNNING);

        PolicyStepsRegistry registry = new PolicyStepsRegistry(REGISTRY, client, null);
        registry.update(meta);

        // test a variety of getStep calls with random actions and steps
        for (int i = 0; i < scaledRandomIntBetween(100, 1000); i++) {
            LifecycleAction action = randomValueOtherThan(MigrateAction.DISABLED, () -> randomFrom(phase.getActions().values()));
            Step step = randomFrom(action.toSteps(client, phaseName, MOCK_STEP_KEY, null));
            // if the step's key is different from the previous iteration of the loop, then the cache will be updated, and we'll
            // get a non-cached response. if the step's key happens to be the same as the previous iteration of the loop, then
            // we'll get a cached response. so this loop randomly tests both cached and non-cached responses.
            Step actualStep = registry.getStep(indexMetadata, step.getKey());
            assertThat(actualStep.getKey(), equalTo(step.getKey()));
        }

        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicBoolean done = new AtomicBoolean(false);

        // now, in another thread, update the registry repeatedly as fast as possible.
        // updating the registry has the side effect of clearing the cache.
        Thread t = new Thread(() -> {
            latch.countDown(); // signal that we're starting
            while (done.get() == false) {
                registry.update(meta);
            }
        });
        t.start();

        try {
            latch.await(); // wait until the other thread started

            // and, while the cache is being repeatedly cleared,
            // test a variety of getStep calls with random actions and steps
            for (int i = 0; i < scaledRandomIntBetween(100, 1000); i++) {
                LifecycleAction action = randomValueOtherThan(MigrateAction.DISABLED, () -> randomFrom(phase.getActions().values()));
                Step step = randomFrom(action.toSteps(client, phaseName, MOCK_STEP_KEY, null));
                Step actualStep = registry.getStep(indexMetadata, step.getKey());
                assertThat(actualStep.getKey(), equalTo(step.getKey()));
            }
        } finally {
            // tell the other thread we're finished and wait for it to die
            done.set(true);
            t.join(1000);
        }
    }
}
