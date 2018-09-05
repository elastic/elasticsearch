/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.indexlifecycle;

import org.elasticsearch.Version;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.Index;
import org.elasticsearch.node.Node;
import org.elasticsearch.xpack.core.indexlifecycle.OperationMode;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.indexlifecycle.ErrorStep;
import org.elasticsearch.xpack.core.indexlifecycle.IndexLifecycleMetadata;
import org.elasticsearch.xpack.core.indexlifecycle.LifecycleAction;
import org.elasticsearch.xpack.core.indexlifecycle.LifecyclePolicy;
import org.elasticsearch.xpack.core.indexlifecycle.LifecyclePolicyMetadata;
import org.elasticsearch.xpack.core.indexlifecycle.LifecyclePolicyTests;
import org.elasticsearch.xpack.core.indexlifecycle.LifecycleSettings;
import org.elasticsearch.xpack.core.indexlifecycle.MockStep;
import org.elasticsearch.xpack.core.indexlifecycle.Phase;
import org.elasticsearch.xpack.core.indexlifecycle.ShrinkAction;
import org.elasticsearch.xpack.core.indexlifecycle.ShrinkStep;
import org.elasticsearch.xpack.core.indexlifecycle.Step;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.sameInstance;

public class PolicyStepsRegistryTests extends ESTestCase {
    private static final Step.StepKey MOCK_STEP_KEY = new Step.StepKey("mock", "mock", "mock");

    public void testGetFirstStep() {
        String policyName = randomAlphaOfLengthBetween(2, 10);
        Step expectedFirstStep = new MockStep(MOCK_STEP_KEY, null);
        Map<String, Step> firstStepMap = Collections.singletonMap(policyName, expectedFirstStep);
        PolicyStepsRegistry registry = new PolicyStepsRegistry(null, firstStepMap, null, null, NamedXContentRegistry.EMPTY);
        Step actualFirstStep = registry.getFirstStep(policyName);
        assertThat(actualFirstStep, sameInstance(expectedFirstStep));
    }

    public void testGetFirstStepUnknownPolicy() {
        String policyName = randomAlphaOfLengthBetween(2, 10);
        Step expectedFirstStep = new MockStep(MOCK_STEP_KEY, null);
        Map<String, Step> firstStepMap = Collections.singletonMap(policyName, expectedFirstStep);
        PolicyStepsRegistry registry = new PolicyStepsRegistry(null, firstStepMap, null, null, NamedXContentRegistry.EMPTY);
        Step actualFirstStep = registry.getFirstStep(policyName + "unknown");
        assertNull(actualFirstStep);
    }

    public void testGetStep() {
        Step expectedStep = new MockStep(MOCK_STEP_KEY, null);
        Index index = new Index("test", "uuid");
        Map<Index, List<Step>> indexSteps = Collections.singletonMap(index, Collections.singletonList(expectedStep));
        PolicyStepsRegistry registry = new PolicyStepsRegistry(null, null, null, indexSteps, NamedXContentRegistry.EMPTY);
        Step actualStep = registry.getStep(index, MOCK_STEP_KEY);
        assertThat(actualStep, sameInstance(expectedStep));
    }

    public void testGetStepErrorStep() {
        Step.StepKey errorStepKey = new Step.StepKey(randomAlphaOfLengthBetween(1, 10), randomAlphaOfLengthBetween(1, 10), ErrorStep.NAME);
        Step expectedStep = new ErrorStep(errorStepKey);
        Index index = new Index("test", "uuid");
        Map<Index, List<Step>> indexSteps = Collections.singletonMap(index, Collections.singletonList(expectedStep));
        PolicyStepsRegistry registry = new PolicyStepsRegistry(null, null, null, indexSteps, NamedXContentRegistry.EMPTY);
        Step actualStep = registry.getStep(index, errorStepKey);
        assertThat(actualStep, equalTo(expectedStep));
    }

    public void testGetStepUnknownPolicy() {
        PolicyStepsRegistry registry = new PolicyStepsRegistry(null, null, null, Collections.emptyMap(), NamedXContentRegistry.EMPTY);
        assertNull(registry.getStep(new Index("test", "uuid"), MOCK_STEP_KEY));
    }

    public void testGetStepUnknownStepKey() {
        Step expectedStep = new MockStep(MOCK_STEP_KEY, null);
        Index index = new Index("test", "uuid");
        Map<Index, List<Step>> indexSteps = Collections.singletonMap(index, Collections.singletonList(expectedStep));
        PolicyStepsRegistry registry = new PolicyStepsRegistry(null, null, null, indexSteps, NamedXContentRegistry.EMPTY);
        Step.StepKey unknownStepKey = new Step.StepKey(MOCK_STEP_KEY.getPhase(),
            MOCK_STEP_KEY.getAction(),MOCK_STEP_KEY.getName() + "not");
        assertNull(registry.getStep(index, unknownStepKey));
    }

    public void testUpdateFromNothingToSomethingToNothing() throws Exception {
        Index index = new Index("test", "uuid");
        Client client = Mockito.mock(Client.class);
        Mockito.when(client.settings()).thenReturn(Settings.EMPTY);
        String policyName = randomAlphaOfLength(5);
        LifecyclePolicy newPolicy = LifecyclePolicyTests.randomTestLifecyclePolicy(policyName);
        logger.info("--> policy: {}", newPolicy);
        List<Step> policySteps = newPolicy.toSteps(client);
        Map<String, String> headers = new HashMap<>();
        if (randomBoolean()) {
            headers.put(randomAlphaOfLength(10), randomAlphaOfLength(10));
            headers.put(randomAlphaOfLength(10), randomAlphaOfLength(10));
        }
        Map<String, LifecyclePolicyMetadata> policyMap = Collections.singletonMap(newPolicy.getName(),
                new LifecyclePolicyMetadata(newPolicy, headers));
        IndexLifecycleMetadata lifecycleMetadata = new IndexLifecycleMetadata(policyMap, OperationMode.RUNNING);
        MetaData metaData = MetaData.builder()
            .persistentSettings(settings(Version.CURRENT).build())
            .putCustom(IndexLifecycleMetadata.TYPE, lifecycleMetadata)
            .put(IndexMetaData.builder("test")
                .settings(Settings.builder()
                    .put("index.uuid", "uuid")
                    .put("index.number_of_shards", 1)
                    .put("index.number_of_replicas", 0)
                    .put("index.version.created", Version.CURRENT.id)
                    .put(LifecycleSettings.LIFECYCLE_NAME, policyName)
                    .put(LifecycleSettings.LIFECYCLE_PHASE, "new")))
            .build();
        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            builder.startObject();
            metaData.toXContent(builder, ToXContent.EMPTY_PARAMS);
            builder.endObject();
            logger.info("--> metadata: {}", Strings.toString(builder));
        }
        String nodeId = randomAlphaOfLength(10);
        DiscoveryNode masterNode = DiscoveryNode.createLocal(settings(Version.CURRENT)
                .put(Node.NODE_MASTER_SETTING.getKey(), true).build(),
            new TransportAddress(TransportAddress.META_ADDRESS, 9300), nodeId);
        ClusterState currentState = ClusterState.builder(ClusterName.DEFAULT)
            .metaData(metaData)
            .nodes(DiscoveryNodes.builder().localNodeId(nodeId).masterNodeId(nodeId).add(masterNode).build())
            .build();

        // start with empty registry
        PolicyStepsRegistry registry = new PolicyStepsRegistry(NamedXContentRegistry.EMPTY);

        // add new policy
        registry.update(currentState, client);

        assertThat(registry.getFirstStep(newPolicy.getName()), equalTo(policySteps.get(0)));
        assertThat(registry.getLifecyclePolicyMap().size(), equalTo(1));
        assertNotNull(registry.getLifecyclePolicyMap().get(newPolicy.getName()));
        assertThat(registry.getLifecyclePolicyMap().get(newPolicy.getName()).getHeaders(), equalTo(headers));
        assertThat(registry.getFirstStepMap().size(), equalTo(1));
        assertThat(registry.getStepMap().size(), equalTo(1));
        Map<Step.StepKey, Step> registeredStepsForPolicy = registry.getStepMap().get(newPolicy.getName());
        assertThat(registeredStepsForPolicy.size(), equalTo(policySteps.size()));
        for (Step step : policySteps) {
            currentState = ClusterState.builder(currentState)
                .metaData(MetaData.builder(currentState.metaData())
                    .put(IndexMetaData.builder(currentState.metaData().index("test"))
                        .settings(Settings.builder().put(currentState.metaData().index("test").getSettings())
                            .put(LifecycleSettings.LIFECYCLE_PHASE, step.getKey().getPhase()))))
                .nodes(DiscoveryNodes.builder().localNodeId(nodeId).masterNodeId(nodeId).add(masterNode).build())
                .build();
            registry.update(currentState, client);
            assertThat(registeredStepsForPolicy.get(step.getKey()), equalTo(step));
            assertThat(registry.getStep(index, step.getKey()), equalTo(step));
        }

        Map<String, LifecyclePolicyMetadata> registryPolicyMap = registry.getLifecyclePolicyMap();
        Map<String, Step> registryFirstStepMap = registry.getFirstStepMap();
        Map<String, Map<Step.StepKey, Step>> registryStepMap = registry.getStepMap();
        registry.update(currentState, client);
        assertThat(registry.getLifecyclePolicyMap(), equalTo(registryPolicyMap));
        assertThat(registry.getFirstStepMap(), equalTo(registryFirstStepMap));
        assertThat(registry.getStepMap(), equalTo(registryStepMap));

        // remove policy
        lifecycleMetadata = new IndexLifecycleMetadata(Collections.emptyMap(), OperationMode.RUNNING);
        currentState = ClusterState.builder(currentState)
            .metaData(
                MetaData.builder(metaData)
                    .putCustom(IndexLifecycleMetadata.TYPE, lifecycleMetadata)).build();
        registry.update(currentState, client);
        assertTrue(registry.getLifecyclePolicyMap().isEmpty());
        assertTrue(registry.getFirstStepMap().isEmpty());
        assertTrue(registry.getStepMap().isEmpty());
    }

    public void testUpdateChangedPolicy() throws Exception {
        Client client = Mockito.mock(Client.class);
        Mockito.when(client.settings()).thenReturn(Settings.EMPTY);
        String policyName = randomAlphaOfLengthBetween(5, 10);
        LifecyclePolicy newPolicy = LifecyclePolicyTests.randomTestLifecyclePolicy(policyName);
        Map<String, String> headers = new HashMap<>();
        if (randomBoolean()) {
            headers.put(randomAlphaOfLength(10), randomAlphaOfLength(10));
            headers.put(randomAlphaOfLength(10), randomAlphaOfLength(10));
        }
        Map<String, LifecyclePolicyMetadata> policyMap = Collections.singletonMap(newPolicy.getName(),
                new LifecyclePolicyMetadata(newPolicy, headers));
        IndexLifecycleMetadata lifecycleMetadata = new IndexLifecycleMetadata(policyMap, OperationMode.RUNNING);
        MetaData metaData = MetaData.builder()
            .persistentSettings(settings(Version.CURRENT).build())
            .putCustom(IndexLifecycleMetadata.TYPE, lifecycleMetadata)
            .build();
        String nodeId = randomAlphaOfLength(10);
        DiscoveryNode masterNode = DiscoveryNode.createLocal(settings(Version.CURRENT)
                .put(Node.NODE_MASTER_SETTING.getKey(), true).build(),
            new TransportAddress(TransportAddress.META_ADDRESS, 9300), nodeId);
        ClusterState currentState = ClusterState.builder(ClusterName.DEFAULT)
            .metaData(metaData)
            .nodes(DiscoveryNodes.builder().localNodeId(nodeId).masterNodeId(nodeId).add(masterNode).build())
            .build();
        PolicyStepsRegistry registry = new PolicyStepsRegistry(NamedXContentRegistry.EMPTY);
        // add new policy
        registry.update(currentState, client);

        // swap out policy
        newPolicy = LifecyclePolicyTests.randomTestLifecyclePolicy(policyName);
        lifecycleMetadata = new IndexLifecycleMetadata(Collections.singletonMap(policyName,
                                                new LifecyclePolicyMetadata(newPolicy, Collections.emptyMap())), OperationMode.RUNNING);
        currentState = ClusterState.builder(currentState)
            .metaData(MetaData.builder(metaData).putCustom(IndexLifecycleMetadata.TYPE, lifecycleMetadata)).build();
        registry.update(currentState, client);
        // TODO(talevy): assert changes... right now we do not support updates to policies. will require internal cleanup
    }

    public void testUpdatePolicyButNoPhaseChangeIndexStepsDontChange() throws Exception {
        Index index = new Index("test", "uuid");
        Client client = Mockito.mock(Client.class);
        Mockito.when(client.settings()).thenReturn(Settings.EMPTY);
        String policyName = randomAlphaOfLength(5);
        Map<String, LifecycleAction> actions = new HashMap<>();
        actions.put("shrink", new ShrinkAction(1));
        Map<String, Phase> phases = new HashMap<>();
        phases.put("warm", new Phase("warm", TimeValue.ZERO, actions));
        LifecyclePolicy newPolicy = new LifecyclePolicy(policyName, phases);
        // Modify the policy
        actions = new HashMap<>();
        actions.put("shrink", new ShrinkAction(2));
        phases = new HashMap<>();
        phases.put("warm", new Phase("warm", TimeValue.ZERO, actions));
        LifecyclePolicy updatedPolicy = new LifecyclePolicy(policyName, phases);
        logger.info("--> policy: {}", newPolicy);
        logger.info("--> updated policy: {}", updatedPolicy);
        List<Step> policySteps = newPolicy.toSteps(client);
        Map<String, String> headers = new HashMap<>();
        if (randomBoolean()) {
            headers.put(randomAlphaOfLength(10), randomAlphaOfLength(10));
            headers.put(randomAlphaOfLength(10), randomAlphaOfLength(10));
        }
        Map<String, LifecyclePolicyMetadata> policyMap = Collections.singletonMap(newPolicy.getName(),
            new LifecyclePolicyMetadata(newPolicy, headers));
        IndexLifecycleMetadata lifecycleMetadata = new IndexLifecycleMetadata(policyMap, OperationMode.RUNNING);
        MetaData metaData = MetaData.builder()
            .persistentSettings(settings(Version.CURRENT).build())
            .putCustom(IndexLifecycleMetadata.TYPE, lifecycleMetadata)
            .put(IndexMetaData.builder("test")
                .settings(Settings.builder()
                    .put("index.uuid", "uuid")
                    .put("index.number_of_shards", 1)
                    .put("index.number_of_replicas", 0)
                    .put("index.version.created", Version.CURRENT.id)
                    .put(LifecycleSettings.LIFECYCLE_NAME, policyName)
                    .put(LifecycleSettings.LIFECYCLE_PHASE, "warm")))
            .build();
        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            builder.startObject();
            metaData.toXContent(builder, ToXContent.EMPTY_PARAMS);
            builder.endObject();
            logger.info("--> metadata: {}", Strings.toString(builder));
        }
        String nodeId = randomAlphaOfLength(10);
        DiscoveryNode masterNode = DiscoveryNode.createLocal(settings(Version.CURRENT)
                .put(Node.NODE_MASTER_SETTING.getKey(), true).build(),
            new TransportAddress(TransportAddress.META_ADDRESS, 9300), nodeId);
        ClusterState currentState = ClusterState.builder(ClusterName.DEFAULT)
            .metaData(metaData)
            .nodes(DiscoveryNodes.builder().localNodeId(nodeId).masterNodeId(nodeId).add(masterNode).build())
            .build();

        // start with empty registry
        PolicyStepsRegistry registry = new PolicyStepsRegistry(NamedXContentRegistry.EMPTY);

        // add new policy
        registry.update(currentState, client);

        Map<Step.StepKey, Step> registeredStepsForPolicy = registry.getStepMap().get(newPolicy.getName());
        Step shrinkStep = registeredStepsForPolicy.entrySet().stream()
            .filter(e -> e.getKey().getPhase().equals("warm") && e.getKey().getName().equals("shrink"))
            .findFirst().get().getValue();
        Step gotStep = registry.getStep(index, shrinkStep.getKey());
        assertThat(((ShrinkStep) shrinkStep).getNumberOfShards(), equalTo(1));
        assertThat(((ShrinkStep) gotStep).getNumberOfShards(), equalTo(1));

        // Update the policy with the new policy, but keep the phase the same
        policyMap = Collections.singletonMap(updatedPolicy.getName(), new LifecyclePolicyMetadata(updatedPolicy, headers));
        lifecycleMetadata = new IndexLifecycleMetadata(policyMap, OperationMode.RUNNING);
        metaData = MetaData.builder(metaData)
            .putCustom(IndexLifecycleMetadata.TYPE, lifecycleMetadata)
            .build();
        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            builder.startObject();
            metaData.toXContent(builder, ToXContent.EMPTY_PARAMS);
            builder.endObject();
            logger.info("--> metadata: {}", Strings.toString(builder));
        }
        currentState = ClusterState.builder(ClusterName.DEFAULT).metaData(metaData).build();

        // Update the policies
        registry.update(currentState, client);

        registeredStepsForPolicy = registry.getStepMap().get(newPolicy.getName());
        shrinkStep = registeredStepsForPolicy.entrySet().stream()
            .filter(e -> e.getKey().getPhase().equals("warm") && e.getKey().getName().equals("shrink"))
            .findFirst().get().getValue();
        gotStep = registry.getStep(index, shrinkStep.getKey());
        assertThat(((ShrinkStep) shrinkStep).getNumberOfShards(), equalTo(2));
        assertThat(((ShrinkStep) gotStep).getNumberOfShards(), equalTo(1));
    }
}
