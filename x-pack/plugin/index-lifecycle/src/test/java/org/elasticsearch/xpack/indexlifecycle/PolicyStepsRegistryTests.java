/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.indexlifecycle;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.node.Node;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.indexlifecycle.LifecyclePolicy;
import org.elasticsearch.xpack.core.indexlifecycle.LifecyclePolicyTests;
import org.elasticsearch.xpack.core.indexlifecycle.MockStep;
import org.elasticsearch.xpack.core.indexlifecycle.Step;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class PolicyStepsRegistryTests extends ESTestCase {
    private static final Step.StepKey MOCK_STEP_KEY = new Step.StepKey("mock", "mock", "mock");

    public void testGetFirstStep() {
        String policyName = randomAlphaOfLengthBetween(2, 10);
        Step expectedFirstStep = new MockStep(MOCK_STEP_KEY, null);
        Map<String, Step> firstStepMap = Collections.singletonMap(policyName, expectedFirstStep);
        PolicyStepsRegistry registry = new PolicyStepsRegistry(null, firstStepMap, null);
        Step actualFirstStep = registry.getFirstStep(policyName);
        assertThat(actualFirstStep, equalTo(expectedFirstStep));
    }

    public void testGetFirstStepUnknownPolicy() {
        String policyName = randomAlphaOfLengthBetween(2, 10);
        Step expectedFirstStep = new MockStep(MOCK_STEP_KEY, null);
        Map<String, Step> firstStepMap = Collections.singletonMap(policyName, expectedFirstStep);
        PolicyStepsRegistry registry = new PolicyStepsRegistry(null, firstStepMap, null);
        Step actualFirstStep = registry.getFirstStep(policyName + "unknown");
        assertNull(actualFirstStep);
    }

    public void testGetStep() {
        String policyName = randomAlphaOfLengthBetween(2, 10);
        Step expectedStep = new MockStep(MOCK_STEP_KEY, null);
        Map<String, Map<Step.StepKey, Step>> stepMap =
            Collections.singletonMap(policyName, Collections.singletonMap(MOCK_STEP_KEY, expectedStep));
        PolicyStepsRegistry registry = new PolicyStepsRegistry(null, null, stepMap);
        Step actualStep = registry.getStep(policyName, MOCK_STEP_KEY);
        assertThat(actualStep, equalTo(expectedStep));
    }

    public void testGetStepUnknownPolicy() {
        String policyName = randomAlphaOfLengthBetween(2, 10);
        PolicyStepsRegistry registry = new PolicyStepsRegistry(null, null, Collections.emptyMap());
        IllegalStateException exception = expectThrows(IllegalStateException.class, () -> registry.getStep(policyName, MOCK_STEP_KEY));
        assertThat(exception.getMessage(), equalTo("policy [" + policyName +"] does not exist"));
    }

    public void testGetStepUnknownStepKey() {
        String policyName = randomAlphaOfLengthBetween(2, 10);
        Step expectedStep = new MockStep(MOCK_STEP_KEY, null);
        Map<String, Map<Step.StepKey, Step>> stepMap =
            Collections.singletonMap(policyName, Collections.singletonMap(MOCK_STEP_KEY, expectedStep));
        PolicyStepsRegistry registry = new PolicyStepsRegistry(null, null, stepMap);
        Step.StepKey unknownStepKey = new Step.StepKey(MOCK_STEP_KEY.getPhase(),
            MOCK_STEP_KEY.getAction(),MOCK_STEP_KEY.getName() + "not");
        IllegalStateException exception = expectThrows(IllegalStateException.class, () -> registry.getStep(policyName, unknownStepKey));
        assertThat(exception.getMessage(), equalTo("step [" + unknownStepKey +"] does not exist"));
    }

    public void testUpdateFromNothingToSomethingToNothing() {
        LifecyclePolicy newPolicy = LifecyclePolicyTests.randomLifecyclePolicy(randomAlphaOfLength(5));
        List<Step> policySteps = newPolicy.toSteps(null, () -> 0L);
        Map<String, LifecyclePolicy> policyMap = Collections.singletonMap(newPolicy.getName(), newPolicy);
        MetaData metaData = MetaData.builder()
            .persistentSettings(settings(Version.CURRENT).build())
            .putCustom(IndexLifecycleMetadata.TYPE, new IndexLifecycleMetadata(policyMap))
            .build();
        String nodeId = randomAlphaOfLength(10);
        DiscoveryNode masterNode = DiscoveryNode.createLocal(settings(Version.CURRENT)
                .put(Node.NODE_MASTER_SETTING.getKey(), true).build(),
            new TransportAddress(TransportAddress.META_ADDRESS, 9300), nodeId);
        ClusterState currentState = ClusterState.builder(ClusterName.DEFAULT)
            .metaData(metaData)
            .nodes(DiscoveryNodes.builder().localNodeId(nodeId).masterNodeId(nodeId).add(masterNode).build())
            .build();

        // start with empty registry
        PolicyStepsRegistry registry = new PolicyStepsRegistry();

        // add new policy
        registry.update(currentState, null, () -> 0L);

        assertThat(registry.getFirstStep(newPolicy.getName()), equalTo(policySteps.get(0)));
        assertThat(registry.getLifecyclePolicyMap().size(), equalTo(1));
        assertThat(registry.getFirstStepMap().size(), equalTo(1));
        assertThat(registry.getStepMap().size(), equalTo(1));
        Map<Step.StepKey, Step> registeredStepsForPolicy = registry.getStepMap().get(newPolicy.getName());
        assertThat(registeredStepsForPolicy.size(), equalTo(policySteps.size()));
        for (Step step : policySteps) {
            assertThat(registeredStepsForPolicy.get(step.getKey()), equalTo(step));
            assertThat(registry.getStep(newPolicy.getName(), step.getKey()), equalTo(step));
        }

        Map<String, LifecyclePolicy> registryPolicyMap = registry.getLifecyclePolicyMap();
        Map<String, Step> registryFirstStepMap = registry.getFirstStepMap();
        Map<String, Map<Step.StepKey, Step>> registryStepMap = registry.getStepMap();
        registry.update(currentState, null, () -> 0L);
        assertThat(registry.getLifecyclePolicyMap(), equalTo(registryPolicyMap));
        assertThat(registry.getFirstStepMap(), equalTo(registryFirstStepMap));
        assertThat(registry.getStepMap(), equalTo(registryStepMap));

        // remove policy
        currentState = ClusterState.builder(currentState)
            .metaData(
                MetaData.builder(metaData)
                    .putCustom(IndexLifecycleMetadata.TYPE, new IndexLifecycleMetadata(Collections.emptyMap()))).build();
        registry.update(currentState, null, () -> 0L);
        assertTrue(registry.getLifecyclePolicyMap().isEmpty());
        assertTrue(registry.getFirstStepMap().isEmpty());
        assertTrue(registry.getStepMap().isEmpty());
    }

    public void testUpdateChangedPolicy() {
        String policyName = randomAlphaOfLengthBetween(5, 10);
        LifecyclePolicy newPolicy = LifecyclePolicyTests.randomLifecyclePolicy(policyName);
        Map<String, LifecyclePolicy> policyMap = Collections.singletonMap(newPolicy.getName(), newPolicy);
        MetaData metaData = MetaData.builder()
            .persistentSettings(settings(Version.CURRENT).build())
            .putCustom(IndexLifecycleMetadata.TYPE, new IndexLifecycleMetadata(policyMap))
            .build();
        String nodeId = randomAlphaOfLength(10);
        DiscoveryNode masterNode = DiscoveryNode.createLocal(settings(Version.CURRENT)
                .put(Node.NODE_MASTER_SETTING.getKey(), true).build(),
            new TransportAddress(TransportAddress.META_ADDRESS, 9300), nodeId);
        ClusterState currentState = ClusterState.builder(ClusterName.DEFAULT)
            .metaData(metaData)
            .nodes(DiscoveryNodes.builder().localNodeId(nodeId).masterNodeId(nodeId).add(masterNode).build())
            .build();
        PolicyStepsRegistry registry = new PolicyStepsRegistry();
        // add new policy
        registry.update(currentState, null, () -> 0L);

        // swap out policy
        newPolicy = LifecyclePolicyTests.randomLifecyclePolicy(policyName);
        currentState = ClusterState.builder(currentState)
            .metaData(
                MetaData.builder(metaData)
                    .putCustom(IndexLifecycleMetadata.TYPE,
                        new IndexLifecycleMetadata(Collections.singletonMap(policyName, newPolicy)))).build();
        registry.update(currentState, null, () -> 0L);
        // TODO(talevy): assert changes... right now we do not support updates to policies. will require internal cleanup
    }
}
