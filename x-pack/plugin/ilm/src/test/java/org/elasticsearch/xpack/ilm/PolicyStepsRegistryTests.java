/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ilm;

import org.elasticsearch.Version;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.NodeRoles;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.ilm.ErrorStep;
import org.elasticsearch.xpack.core.ilm.IndexLifecycleMetadata;
import org.elasticsearch.xpack.core.ilm.InitializePolicyContextStep;
import org.elasticsearch.xpack.core.ilm.LifecycleAction;
import org.elasticsearch.xpack.core.ilm.LifecycleExecutionState;
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

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.xpack.core.ilm.LifecycleExecutionState.ILM_CUSTOM_METADATA_KEY;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.Mockito.mock;

public class PolicyStepsRegistryTests extends ESTestCase {
    private static final Step.StepKey MOCK_STEP_KEY = new Step.StepKey("mock", "mock", "mock");
    private static final NamedXContentRegistry REGISTRY = new NamedXContentRegistry(new IndexLifecycle(Settings.EMPTY).getNamedXContent());

    private IndexMetadata emptyMetadata(Index index) {
        return IndexMetadata.builder(index.getName())
            .settings(settings(Version.CURRENT))
            .numberOfShards(randomIntBetween(1, 5))
            .numberOfReplicas(randomIntBetween(0, 5))
            .build();
    }

    public void testGetFirstStep() {
        String policyName = randomAlphaOfLengthBetween(2, 10);
        Step expectedFirstStep = new MockStep(MOCK_STEP_KEY, null);
        Map<String, Step> firstStepMap = Collections.singletonMap(policyName, expectedFirstStep);
        PolicyStepsRegistry registry = new PolicyStepsRegistry(null, firstStepMap, null, NamedXContentRegistry.EMPTY, null, null);
        Step actualFirstStep = registry.getFirstStep(policyName);
        assertThat(actualFirstStep, sameInstance(expectedFirstStep));
    }

    public void testGetFirstStepUnknownPolicy() {
        String policyName = randomAlphaOfLengthBetween(2, 10);
        Step expectedFirstStep = new MockStep(MOCK_STEP_KEY, null);
        Map<String, Step> firstStepMap = Collections.singletonMap(policyName, expectedFirstStep);
        PolicyStepsRegistry registry = new PolicyStepsRegistry(null, firstStepMap, null, NamedXContentRegistry.EMPTY, null, null);
        Step actualFirstStep = registry.getFirstStep(policyName + "unknown");
        assertNull(actualFirstStep);
    }

    public void testGetStep() {
        Client client = mock(Client.class);
        Mockito.when(client.settings()).thenReturn(Settings.EMPTY);
        LifecyclePolicy policy = LifecyclePolicyTests.randomTimeseriesLifecyclePolicyWithAllPhases("policy");
        LifecyclePolicyMetadata policyMetadata = new LifecyclePolicyMetadata(policy, Collections.emptyMap(), 1, randomNonNegativeLong());
        String phaseName = randomFrom(policy.getPhases().keySet());
        Phase phase = policy.getPhases().get(phaseName);
        PhaseExecutionInfo pei = new PhaseExecutionInfo(policy.getName(), phase, 1, randomNonNegativeLong());
        String phaseJson = Strings.toString(pei);
        LifecycleAction action = randomValueOtherThan(new MigrateAction(false), () -> randomFrom(phase.getActions().values()));
        Step step = randomFrom(action.toSteps(client, phaseName, MOCK_STEP_KEY, null));
        LifecycleExecutionState.Builder lifecycleState = LifecycleExecutionState.builder();
        lifecycleState.setPhaseDefinition(phaseJson);
        IndexMetadata indexMetadata = IndexMetadata.builder("test")
            .settings(
                Settings.builder()
                    .put("index.number_of_shards", 1)
                    .put("index.number_of_replicas", 0)
                    .put("index.version.created", Version.CURRENT)
                    .put(LifecycleSettings.LIFECYCLE_NAME, "policy")
                    .build()
            )
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
        Map<Index, List<Step>> indexSteps = Collections.singletonMap(index, Collections.singletonList(expectedStep));
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
        LifecyclePolicyMetadata policyMetadata = new LifecyclePolicyMetadata(policy, Collections.emptyMap(), 1, randomNonNegativeLong());
        IndexMetadata indexMetadata = IndexMetadata.builder("test")
            .settings(
                Settings.builder()
                    .put("index.number_of_shards", 1)
                    .put("index.number_of_replicas", 0)
                    .put("index.version.created", Version.CURRENT)
                    .put(LifecycleSettings.LIFECYCLE_NAME, "policy")
                    .build()
            )
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
        LifecyclePolicyMetadata policyMetadata = new LifecyclePolicyMetadata(policy, Collections.emptyMap(), 1, randomNonNegativeLong());
        String phaseName = randomFrom(policy.getPhases().keySet());
        Phase phase = policy.getPhases().get(phaseName);
        PhaseExecutionInfo pei = new PhaseExecutionInfo(policy.getName(), phase, 1, randomNonNegativeLong());
        String phaseJson = Strings.toString(pei);
        LifecycleAction action = randomValueOtherThan(new MigrateAction(false), () -> randomFrom(phase.getActions().values()));
        Step step = randomFrom(action.toSteps(client, phaseName, MOCK_STEP_KEY, null));
        LifecycleExecutionState.Builder lifecycleState = LifecycleExecutionState.builder();
        lifecycleState.setPhaseDefinition(phaseJson);
        IndexMetadata indexMetadata = IndexMetadata.builder("test")
            .settings(
                Settings.builder()
                    .put("index.number_of_shards", 1)
                    .put("index.number_of_replicas", 0)
                    .put("index.version.created", Version.CURRENT)
                    .put(LifecycleSettings.LIFECYCLE_NAME, "policy")
                    .build()
            )
            .putCustom(ILM_CUSTOM_METADATA_KEY, lifecycleState.build().asMap())
            .build();
        SortedMap<String, LifecyclePolicyMetadata> metas = new TreeMap<>();
        metas.put("policy", policyMetadata);
        PolicyStepsRegistry registry = new PolicyStepsRegistry(metas, null, null, REGISTRY, client, null);
        Step.StepKey badStepKey = new Step.StepKey(step.getKey().getPhase(), step.getKey().getAction(), step.getKey().getName() + "-bad");
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
        Map<String, LifecyclePolicyMetadata> policyMap = Collections.singletonMap(
            newPolicy.getName(),
            new LifecyclePolicyMetadata(newPolicy, headers, randomNonNegativeLong(), randomNonNegativeLong())
        );
        IndexLifecycleMetadata lifecycleMetadata = new IndexLifecycleMetadata(policyMap, OperationMode.RUNNING);
        LifecycleExecutionState.Builder lifecycleState = LifecycleExecutionState.builder();
        lifecycleState.setPhase("new");
        Metadata metadata = Metadata.builder()
            .persistentSettings(settings(Version.CURRENT).build())
            .putCustom(IndexLifecycleMetadata.TYPE, lifecycleMetadata)
            .put(
                IndexMetadata.builder("test")
                    .settings(
                        Settings.builder()
                            .put("index.uuid", "uuid")
                            .put("index.number_of_shards", 1)
                            .put("index.number_of_replicas", 0)
                            .put("index.version.created", Version.CURRENT.id)
                            .put(LifecycleSettings.LIFECYCLE_NAME, policyName)
                    )
                    .putCustom(ILM_CUSTOM_METADATA_KEY, lifecycleState.build().asMap())
            )
            .build();
        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            builder.startObject();
            metadata.toXContent(builder, ToXContent.EMPTY_PARAMS);
            builder.endObject();
            logger.info("--> metadata: {}", Strings.toString(builder));
        }
        String nodeId = randomAlphaOfLength(10);
        DiscoveryNode masterNode = DiscoveryNode.createLocal(
            NodeRoles.masterNode(settings(Version.CURRENT).build()),
            new TransportAddress(TransportAddress.META_ADDRESS, 9300),
            nodeId
        );
        ClusterState currentState = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(metadata)
            .nodes(DiscoveryNodes.builder().localNodeId(nodeId).masterNodeId(nodeId).add(masterNode).build())
            .build();

        // start with empty registry
        PolicyStepsRegistry registry = new PolicyStepsRegistry(NamedXContentRegistry.EMPTY, client, null);

        // add new policy
        registry.update(currentState.metadata().custom(IndexLifecycleMetadata.TYPE));

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
            newIndexState.setPhase(step.getKey().getPhase());
            currentState = ClusterState.builder(currentState)
                .metadata(
                    Metadata.builder(currentState.metadata())
                        .put(
                            IndexMetadata.builder(currentState.metadata().index("test"))
                                .settings(Settings.builder().put(currentState.metadata().index("test").getSettings()))
                                .putCustom(ILM_CUSTOM_METADATA_KEY, newIndexState.build().asMap())
                        )
                )
                .nodes(DiscoveryNodes.builder().localNodeId(nodeId).masterNodeId(nodeId).add(masterNode).build())
                .build();
            registry.update(currentState.metadata().custom(IndexLifecycleMetadata.TYPE));
            assertThat(registeredStepsForPolicy.get(step.getKey()), equalTo(step));
            assertThat(registry.getStep(metadata.index(index), step.getKey()), equalTo(step));
        }

        Map<String, LifecyclePolicyMetadata> registryPolicyMap = registry.getLifecyclePolicyMap();
        Map<String, Step> registryFirstStepMap = registry.getFirstStepMap();
        Map<String, Map<Step.StepKey, Step>> registryStepMap = registry.getStepMap();
        registry.update(currentState.metadata().custom(IndexLifecycleMetadata.TYPE));
        assertThat(registry.getLifecyclePolicyMap(), equalTo(registryPolicyMap));
        assertThat(registry.getFirstStepMap(), equalTo(registryFirstStepMap));
        assertThat(registry.getStepMap(), equalTo(registryStepMap));

        // remove policy
        lifecycleMetadata = new IndexLifecycleMetadata(Collections.emptyMap(), OperationMode.RUNNING);
        currentState = ClusterState.builder(currentState)
            .metadata(Metadata.builder(metadata).putCustom(IndexLifecycleMetadata.TYPE, lifecycleMetadata))
            .build();
        registry.update(currentState.metadata().custom(IndexLifecycleMetadata.TYPE));
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
        Map<String, LifecyclePolicyMetadata> policyMap = Collections.singletonMap(
            newPolicy.getName(),
            new LifecyclePolicyMetadata(newPolicy, headers, randomNonNegativeLong(), randomNonNegativeLong())
        );
        IndexLifecycleMetadata lifecycleMetadata = new IndexLifecycleMetadata(policyMap, OperationMode.RUNNING);
        Metadata metadata = Metadata.builder()
            .persistentSettings(settings(Version.CURRENT).build())
            .putCustom(IndexLifecycleMetadata.TYPE, lifecycleMetadata)
            .build();
        String nodeId = randomAlphaOfLength(10);
        DiscoveryNode masterNode = DiscoveryNode.createLocal(
            NodeRoles.masterNode(settings(Version.CURRENT).build()),
            new TransportAddress(TransportAddress.META_ADDRESS, 9300),
            nodeId
        );
        ClusterState currentState = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(metadata)
            .nodes(DiscoveryNodes.builder().localNodeId(nodeId).masterNodeId(nodeId).add(masterNode).build())
            .build();
        PolicyStepsRegistry registry = new PolicyStepsRegistry(NamedXContentRegistry.EMPTY, client, null);
        // add new policy
        registry.update(currentState.metadata().custom(IndexLifecycleMetadata.TYPE));

        // swap out policy
        newPolicy = LifecyclePolicyTests.randomTestLifecyclePolicy(policyName);
        lifecycleMetadata = new IndexLifecycleMetadata(
            Collections.singletonMap(
                policyName,
                new LifecyclePolicyMetadata(newPolicy, Collections.emptyMap(), randomNonNegativeLong(), randomNonNegativeLong())
            ),
            OperationMode.RUNNING
        );
        currentState = ClusterState.builder(currentState)
            .metadata(Metadata.builder(metadata).putCustom(IndexLifecycleMetadata.TYPE, lifecycleMetadata))
            .build();
        registry.update(currentState.metadata().custom(IndexLifecycleMetadata.TYPE));
        // TODO(talevy): assert changes... right now we do not support updates to policies. will require internal cleanup
    }

    public void testUpdatePolicyButNoPhaseChangeIndexStepsDontChange() throws Exception {
        Index index = new Index("test", "uuid");
        Client client = mock(Client.class);
        Mockito.when(client.settings()).thenReturn(Settings.EMPTY);
        String policyName = randomAlphaOfLength(5);
        Map<String, LifecycleAction> actions = new HashMap<>();
        actions.put("shrink", new ShrinkAction(1, null));
        Map<String, Phase> phases = new HashMap<>();
        Phase warmPhase = new Phase("warm", TimeValue.ZERO, actions);
        PhaseExecutionInfo pei = new PhaseExecutionInfo(policyName, warmPhase, 1, randomNonNegativeLong());
        String phaseJson = Strings.toString(pei);
        phases.put("warm", new Phase("warm", TimeValue.ZERO, actions));
        LifecyclePolicy newPolicy = new LifecyclePolicy(policyName, phases);
        // Modify the policy
        actions = new HashMap<>();
        actions.put("shrink", new ShrinkAction(2, null));
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
        Map<String, LifecyclePolicyMetadata> policyMap = Collections.singletonMap(
            newPolicy.getName(),
            new LifecyclePolicyMetadata(newPolicy, headers, randomNonNegativeLong(), randomNonNegativeLong())
        );
        IndexLifecycleMetadata lifecycleMetadata = new IndexLifecycleMetadata(policyMap, OperationMode.RUNNING);
        LifecycleExecutionState.Builder lifecycleState = LifecycleExecutionState.builder();
        lifecycleState.setPhase("warm");
        lifecycleState.setPhaseDefinition(phaseJson);
        Metadata metadata = Metadata.builder()
            .persistentSettings(settings(Version.CURRENT).build())
            .putCustom(IndexLifecycleMetadata.TYPE, lifecycleMetadata)
            .put(
                IndexMetadata.builder("test")
                    .settings(
                        Settings.builder()
                            .put("index.uuid", "uuid")
                            .put("index.number_of_shards", 1)
                            .put("index.number_of_replicas", 0)
                            .put("index.version.created", Version.CURRENT.id)
                            .put(LifecycleSettings.LIFECYCLE_NAME, policyName)
                    )
                    .putCustom(ILM_CUSTOM_METADATA_KEY, lifecycleState.build().asMap())
            )
            .build();
        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            builder.startObject();
            metadata.toXContent(builder, ToXContent.EMPTY_PARAMS);
            builder.endObject();
            logger.info("--> metadata: {}", Strings.toString(builder));
        }
        String nodeId = randomAlphaOfLength(10);
        DiscoveryNode masterNode = DiscoveryNode.createLocal(
            NodeRoles.masterNode(settings(Version.CURRENT).build()),
            new TransportAddress(TransportAddress.META_ADDRESS, 9300),
            nodeId
        );
        ClusterState currentState = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(metadata)
            .nodes(DiscoveryNodes.builder().localNodeId(nodeId).masterNodeId(nodeId).add(masterNode).build())
            .build();

        // start with empty registry
        PolicyStepsRegistry registry = new PolicyStepsRegistry(REGISTRY, client, null);

        // add new policy
        registry.update(currentState.metadata().custom(IndexLifecycleMetadata.TYPE));

        Map<Step.StepKey, Step> registeredStepsForPolicy = registry.getStepMap().get(newPolicy.getName());
        Step shrinkStep = registeredStepsForPolicy.entrySet()
            .stream()
            .filter(e -> e.getKey().getPhase().equals("warm") && e.getKey().getName().equals("shrink"))
            .findFirst()
            .get()
            .getValue();
        Step gotStep = registry.getStep(metadata.index(index), shrinkStep.getKey());
        assertThat(((ShrinkStep) shrinkStep).getNumberOfShards(), equalTo(1));
        assertThat(((ShrinkStep) gotStep).getNumberOfShards(), equalTo(1));

        // Update the policy with the new policy, but keep the phase the same
        policyMap = Collections.singletonMap(
            updatedPolicy.getName(),
            new LifecyclePolicyMetadata(updatedPolicy, headers, randomNonNegativeLong(), randomNonNegativeLong())
        );
        lifecycleMetadata = new IndexLifecycleMetadata(policyMap, OperationMode.RUNNING);
        metadata = Metadata.builder(metadata).putCustom(IndexLifecycleMetadata.TYPE, lifecycleMetadata).build();
        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            builder.startObject();
            metadata.toXContent(builder, ToXContent.EMPTY_PARAMS);
            builder.endObject();
            logger.info("--> metadata: {}", Strings.toString(builder));
        }
        currentState = ClusterState.builder(ClusterName.DEFAULT).metadata(metadata).build();

        // Update the policies
        registry.update(currentState.metadata().custom(IndexLifecycleMetadata.TYPE));

        registeredStepsForPolicy = registry.getStepMap().get(newPolicy.getName());
        shrinkStep = registeredStepsForPolicy.entrySet()
            .stream()
            .filter(e -> e.getKey().getPhase().equals("warm") && e.getKey().getName().equals("shrink"))
            .findFirst()
            .get()
            .getValue();
        gotStep = registry.getStep(metadata.index(index), shrinkStep.getKey());
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
            .settings(
                Settings.builder()
                    .put("index.number_of_shards", 1)
                    .put("index.number_of_replicas", 0)
                    .put("index.version.created", Version.CURRENT)
                    .put(LifecycleSettings.LIFECYCLE_NAME, "policy")
                    .build()
            )
            .putCustom(ILM_CUSTOM_METADATA_KEY, lifecycleState.asMap())
            .build();

        SortedMap<String, LifecyclePolicyMetadata> metas = new TreeMap<>();
        metas.put("policy", new LifecyclePolicyMetadata(policy, Collections.emptyMap(), 1, randomNonNegativeLong()));
        IndexLifecycleMetadata meta = new IndexLifecycleMetadata(metas, OperationMode.RUNNING);

        PolicyStepsRegistry registry = new PolicyStepsRegistry(REGISTRY, client, null);
        registry.update(meta);

        // test a variety of getStep calls with random actions and steps
        for (int i = 0; i < scaledRandomIntBetween(100, 1000); i++) {
            LifecycleAction action = randomValueOtherThan(new MigrateAction(false), () -> randomFrom(phase.getActions().values()));
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
                LifecycleAction action = randomValueOtherThan(new MigrateAction(false), () -> randomFrom(phase.getActions().values()));
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
