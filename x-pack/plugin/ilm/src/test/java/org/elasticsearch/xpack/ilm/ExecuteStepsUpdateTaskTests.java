/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ilm;

import org.elasticsearch.Version;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.LifecycleExecutionState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.NodeRoles;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xpack.core.ilm.ErrorStep;
import org.elasticsearch.xpack.core.ilm.IndexLifecycleMetadata;
import org.elasticsearch.xpack.core.ilm.LifecyclePolicy;
import org.elasticsearch.xpack.core.ilm.LifecyclePolicyMetadata;
import org.elasticsearch.xpack.core.ilm.LifecycleSettings;
import org.elasticsearch.xpack.core.ilm.MockAction;
import org.elasticsearch.xpack.core.ilm.MockStep;
import org.elasticsearch.xpack.core.ilm.OperationMode;
import org.elasticsearch.xpack.core.ilm.Phase;
import org.elasticsearch.xpack.core.ilm.Step;
import org.elasticsearch.xpack.core.ilm.Step.StepKey;
import org.elasticsearch.xpack.core.ilm.TerminalPolicyStep;
import org.elasticsearch.xpack.ilm.IndexLifecycleRunnerTests.MockClusterStateActionStep;
import org.elasticsearch.xpack.ilm.IndexLifecycleRunnerTests.MockClusterStateWaitStep;
import org.junit.Before;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.cluster.metadata.LifecycleExecutionState.ILM_CUSTOM_METADATA_KEY;
import static org.elasticsearch.xpack.ilm.LifecyclePolicyTestsUtils.newTestLifecyclePolicy;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

public class ExecuteStepsUpdateTaskTests extends ESTestCase {

    private static final StepKey firstStepKey = new StepKey("first_phase", "action_1", "step_1");
    private static final StepKey secondStepKey = new StepKey("first_phase", "action_1", "step_2");
    private static final StepKey thirdStepKey = new StepKey("first_phase", "action_1", "step_3");
    private static final StepKey invalidStepKey = new StepKey("invalid", "invalid", "invalid");
    private ClusterState clusterState;
    private PolicyStepsRegistry policyStepsRegistry;
    private String mixedPolicyName;
    private String allClusterPolicyName;
    private String invalidPolicyName;
    private Index index;
    private IndexMetadata indexMetadata;
    private MockClusterStateActionStep firstStep;
    private MockClusterStateWaitStep secondStep;
    private MockClusterStateWaitStep allClusterSecondStep;
    private MockStep thirdStep;
    private Client client;
    private IndexLifecycleMetadata lifecycleMetadata;
    private String indexName;

    @Before
    public void prepareState() throws IOException {
        client = Mockito.mock(Client.class);
        Mockito.when(client.settings()).thenReturn(Settings.EMPTY);
        firstStep = new MockClusterStateActionStep(firstStepKey, secondStepKey);
        secondStep = new MockClusterStateWaitStep(secondStepKey, thirdStepKey);
        secondStep.setWillComplete(true);
        allClusterSecondStep = new MockClusterStateWaitStep(secondStepKey, TerminalPolicyStep.KEY);
        allClusterSecondStep.setWillComplete(true);
        thirdStep = new MockStep(thirdStepKey, null);
        mixedPolicyName = randomAlphaOfLengthBetween(5, 10);
        allClusterPolicyName = randomAlphaOfLengthBetween(1, 4);
        invalidPolicyName = randomAlphaOfLength(11);
        Phase mixedPhase = new Phase(
            "first_phase",
            TimeValue.ZERO,
            Collections.singletonMap(MockAction.NAME, new MockAction(Arrays.asList(firstStep, secondStep, thirdStep)))
        );
        Phase allClusterPhase = new Phase(
            "first_phase",
            TimeValue.ZERO,
            Collections.singletonMap(MockAction.NAME, new MockAction(Arrays.asList(firstStep, allClusterSecondStep)))
        );
        Phase invalidPhase = new Phase(
            "invalid_phase",
            TimeValue.ZERO,
            Collections.singletonMap(
                MockAction.NAME,
                new MockAction(Arrays.asList(new MockClusterStateActionStep(firstStepKey, invalidStepKey)))
            )
        );
        LifecyclePolicy mixedPolicy = newTestLifecyclePolicy(mixedPolicyName, Collections.singletonMap(mixedPhase.getName(), mixedPhase));
        LifecyclePolicy allClusterPolicy = newTestLifecyclePolicy(
            allClusterPolicyName,
            Collections.singletonMap(allClusterPhase.getName(), allClusterPhase)
        );
        LifecyclePolicy invalidPolicy = newTestLifecyclePolicy(
            invalidPolicyName,
            Collections.singletonMap(invalidPhase.getName(), invalidPhase)
        );
        Map<String, LifecyclePolicyMetadata> policyMap = new HashMap<>();
        policyMap.put(
            mixedPolicyName,
            new LifecyclePolicyMetadata(mixedPolicy, Collections.emptyMap(), randomNonNegativeLong(), randomNonNegativeLong())
        );
        policyMap.put(
            allClusterPolicyName,
            new LifecyclePolicyMetadata(allClusterPolicy, Collections.emptyMap(), randomNonNegativeLong(), randomNonNegativeLong())
        );
        policyMap.put(
            invalidPolicyName,
            new LifecyclePolicyMetadata(invalidPolicy, Collections.emptyMap(), randomNonNegativeLong(), randomNonNegativeLong())
        );
        policyStepsRegistry = new PolicyStepsRegistry(NamedXContentRegistry.EMPTY, client, null);

        indexName = randomAlphaOfLength(5);
        lifecycleMetadata = new IndexLifecycleMetadata(policyMap, OperationMode.RUNNING);
        setupIndexPolicy(mixedPolicyName);
    }

    private void setupIndexPolicy(String policyName) {
        // Reset the index to use the "allClusterPolicyName"
        LifecycleExecutionState.Builder lifecycleState = LifecycleExecutionState.builder();
        lifecycleState.setPhase("new");
        lifecycleState.setAction("init");
        lifecycleState.setStep("init");
        indexMetadata = IndexMetadata.builder(indexName)
            .settings(settings(Version.CURRENT).put(LifecycleSettings.LIFECYCLE_NAME, policyName))
            .putCustom(ILM_CUSTOM_METADATA_KEY, lifecycleState.build().asMap())
            .numberOfShards(randomIntBetween(1, 5))
            .numberOfReplicas(randomIntBetween(0, 5))
            .build();
        index = indexMetadata.getIndex();
        Metadata metadata = Metadata.builder()
            .persistentSettings(settings(Version.CURRENT).build())
            .putCustom(IndexLifecycleMetadata.TYPE, lifecycleMetadata)
            .put(IndexMetadata.builder(indexMetadata))
            .build();
        String nodeId = randomAlphaOfLength(10);
        DiscoveryNode masterNode = DiscoveryNode.createLocal(
            NodeRoles.masterNode(settings(Version.CURRENT).build()),
            new TransportAddress(TransportAddress.META_ADDRESS, 9300),
            nodeId
        );
        clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(metadata)
            .nodes(DiscoveryNodes.builder().localNodeId(nodeId).masterNodeId(nodeId).add(masterNode).build())
            .build();
        policyStepsRegistry.update(clusterState.metadata().custom(IndexLifecycleMetadata.TYPE));
    }

    public void testNeverExecuteNonClusterStateStep() throws Exception {
        setStateToKey(thirdStepKey);
        Step startStep = policyStepsRegistry.getStep(indexMetadata, thirdStepKey);
        long now = randomNonNegativeLong();
        ExecuteStepsUpdateTask task = new ExecuteStepsUpdateTask(mixedPolicyName, index, startStep, policyStepsRegistry, null, () -> now);
        assertThat(task.execute(clusterState), sameInstance(clusterState));
    }

    public void testSuccessThenFailureUnsetNextKey() throws Exception {
        secondStep.setWillComplete(false);
        setStateToKey(firstStepKey);
        Step startStep = policyStepsRegistry.getStep(indexMetadata, firstStepKey);
        long now = randomNonNegativeLong();
        ExecuteStepsUpdateTask task = new ExecuteStepsUpdateTask(mixedPolicyName, index, startStep, policyStepsRegistry, null, () -> now);
        ClusterState newState = task.execute(clusterState);
        LifecycleExecutionState lifecycleState = newState.getMetadata().index(index).getLifecycleExecutionState();
        StepKey currentStepKey = Step.getCurrentStepKey(lifecycleState);
        assertThat(currentStepKey, equalTo(secondStepKey));
        assertThat(firstStep.getExecuteCount(), equalTo(1L));
        assertThat(secondStep.getExecuteCount(), equalTo(1L));
        assertThat(task.getNextStepKey(), nullValue());
        assertThat(lifecycleState.phaseTime(), nullValue());
        assertThat(lifecycleState.actionTime(), nullValue());
        assertThat(lifecycleState.stepInfo(), nullValue());
    }

    public void testExecuteUntilFirstNonClusterStateStep() throws Exception {
        setStateToKey(secondStepKey);
        Step startStep = policyStepsRegistry.getStep(indexMetadata, secondStepKey);
        long now = randomNonNegativeLong();
        ExecuteStepsUpdateTask task = new ExecuteStepsUpdateTask(mixedPolicyName, index, startStep, policyStepsRegistry, null, () -> now);
        ClusterState newState = task.execute(clusterState);
        LifecycleExecutionState lifecycleState = newState.getMetadata().index(index).getLifecycleExecutionState();
        StepKey currentStepKey = Step.getCurrentStepKey(lifecycleState);
        assertThat(currentStepKey, equalTo(thirdStepKey));
        assertThat(firstStep.getExecuteCount(), equalTo(0L));
        assertThat(secondStep.getExecuteCount(), equalTo(1L));
        assertThat(lifecycleState.phaseTime(), nullValue());
        assertThat(lifecycleState.actionTime(), nullValue());
        assertThat(lifecycleState.stepInfo(), nullValue());
    }

    public void testExecuteInvalidStartStep() throws Exception {
        // Unset the index's phase/action/step to simulate starting from scratch
        LifecycleExecutionState.Builder lifecycleState = LifecycleExecutionState.builder(
            clusterState.getMetadata().index(index).getLifecycleExecutionState()
        );
        lifecycleState.setPhase(null);
        lifecycleState.setAction(null);
        lifecycleState.setStep(null);
        clusterState = ClusterState.builder(clusterState)
            .metadata(
                Metadata.builder(clusterState.getMetadata())
                    .put(
                        IndexMetadata.builder(clusterState.getMetadata().index(index))
                            .putCustom(ILM_CUSTOM_METADATA_KEY, lifecycleState.build().asMap())
                    )
            )
            .build();

        policyStepsRegistry.update(clusterState.metadata().custom(IndexLifecycleMetadata.TYPE));

        Step invalidStep = new MockClusterStateActionStep(firstStepKey, secondStepKey);
        long now = randomNonNegativeLong();
        ExecuteStepsUpdateTask task = new ExecuteStepsUpdateTask(
            invalidPolicyName,
            index,
            invalidStep,
            policyStepsRegistry,
            null,
            () -> now
        );
        ClusterState newState = task.execute(clusterState);
        assertSame(newState, clusterState);
    }

    public void testExecuteIncompleteWaitStepNoInfo() throws Exception {
        secondStep.setWillComplete(false);
        setStateToKey(secondStepKey);
        Step startStep = policyStepsRegistry.getStep(indexMetadata, secondStepKey);
        long now = randomNonNegativeLong();
        ExecuteStepsUpdateTask task = new ExecuteStepsUpdateTask(mixedPolicyName, index, startStep, policyStepsRegistry, null, () -> now);
        ClusterState newState = task.execute(clusterState);
        LifecycleExecutionState lifecycleState = newState.getMetadata().index(index).getLifecycleExecutionState();
        StepKey currentStepKey = Step.getCurrentStepKey(lifecycleState);
        assertThat(currentStepKey, equalTo(secondStepKey));
        assertThat(firstStep.getExecuteCount(), equalTo(0L));
        assertThat(secondStep.getExecuteCount(), equalTo(1L));
        assertThat(lifecycleState.phaseTime(), nullValue());
        assertThat(lifecycleState.actionTime(), nullValue());
        assertThat(lifecycleState.stepInfo(), nullValue());
    }

    public void testExecuteIncompleteWaitStepWithInfo() throws Exception {
        secondStep.setWillComplete(false);
        RandomStepInfo stepInfo = new RandomStepInfo(() -> randomAlphaOfLength(10));
        secondStep.expectedInfo(stepInfo);
        setStateToKey(secondStepKey);
        Step startStep = policyStepsRegistry.getStep(indexMetadata, secondStepKey);
        long now = randomNonNegativeLong();
        ExecuteStepsUpdateTask task = new ExecuteStepsUpdateTask(mixedPolicyName, index, startStep, policyStepsRegistry, null, () -> now);
        ClusterState newState = task.execute(clusterState);
        LifecycleExecutionState lifecycleState = newState.getMetadata().index(index).getLifecycleExecutionState();
        StepKey currentStepKey = Step.getCurrentStepKey(lifecycleState);
        assertThat(currentStepKey, equalTo(secondStepKey));
        assertThat(firstStep.getExecuteCount(), equalTo(0L));
        assertThat(secondStep.getExecuteCount(), equalTo(1L));
        assertThat(lifecycleState.phaseTime(), nullValue());
        assertThat(lifecycleState.actionTime(), nullValue());
        assertThat(lifecycleState.stepInfo(), equalTo(stepInfo.toString()));
    }

    public void testOnFailure() throws IOException {
        setStateToKey(secondStepKey);
        Step startStep = policyStepsRegistry.getStep(indexMetadata, secondStepKey);
        long now = randomNonNegativeLong();
        ExecuteStepsUpdateTask task = new ExecuteStepsUpdateTask(mixedPolicyName, index, startStep, policyStepsRegistry, null, () -> now);
        Exception expectedException = new RuntimeException();
        task.onFailure(expectedException);
    }

    public void testClusterActionStepThrowsException() throws Exception {
        RuntimeException thrownException = new RuntimeException("error");
        firstStep.setException(thrownException);
        setStateToKey(firstStepKey);
        Step startStep = policyStepsRegistry.getStep(indexMetadata, firstStepKey);
        long now = randomNonNegativeLong();
        ExecuteStepsUpdateTask task = new ExecuteStepsUpdateTask(mixedPolicyName, index, startStep, policyStepsRegistry, null, () -> now);
        ClusterState newState = task.execute(clusterState);
        LifecycleExecutionState lifecycleState = newState.getMetadata().index(index).getLifecycleExecutionState();
        StepKey currentStepKey = Step.getCurrentStepKey(lifecycleState);
        assertThat(currentStepKey, equalTo(new StepKey(firstStepKey.getPhase(), firstStepKey.getAction(), ErrorStep.NAME)));
        assertThat(firstStep.getExecuteCount(), equalTo(1L));
        assertThat(secondStep.getExecuteCount(), equalTo(0L));
        assertNull(task.getNextStepKey());
        assertThat(lifecycleState.phaseTime(), nullValue());
        assertThat(lifecycleState.actionTime(), nullValue());
        assertThat(lifecycleState.stepInfo(), containsString("""
            {"type":"runtime_exception","reason":"error\""""));
    }

    public void testClusterWaitStepThrowsException() throws Exception {
        RuntimeException thrownException = new RuntimeException("error");
        secondStep.setException(thrownException);
        setStateToKey(firstStepKey);
        Step startStep = policyStepsRegistry.getStep(indexMetadata, firstStepKey);
        long now = randomNonNegativeLong();
        ExecuteStepsUpdateTask task = new ExecuteStepsUpdateTask(mixedPolicyName, index, startStep, policyStepsRegistry, null, () -> now);
        ClusterState newState = task.execute(clusterState);
        LifecycleExecutionState lifecycleState = newState.getMetadata().index(index).getLifecycleExecutionState();
        StepKey currentStepKey = Step.getCurrentStepKey(lifecycleState);
        assertThat(currentStepKey, equalTo(new StepKey(firstStepKey.getPhase(), firstStepKey.getAction(), ErrorStep.NAME)));
        assertThat(firstStep.getExecuteCount(), equalTo(1L));
        assertThat(secondStep.getExecuteCount(), equalTo(1L));
        assertThat(lifecycleState.phaseTime(), nullValue());
        assertThat(lifecycleState.actionTime(), nullValue());
        assertThat(lifecycleState.stepInfo(), containsString("""
            {"type":"runtime_exception","reason":"error\""""));
    }

    private void setStateToKey(StepKey stepKey) throws IOException {
        LifecycleExecutionState.Builder lifecycleState = LifecycleExecutionState.builder(
            clusterState.getMetadata().index(index).getLifecycleExecutionState()
        );
        lifecycleState.setPhase(stepKey.getPhase());
        lifecycleState.setAction(stepKey.getAction());
        lifecycleState.setStep(stepKey.getName());
        clusterState = ClusterState.builder(clusterState)
            .metadata(
                Metadata.builder(clusterState.getMetadata())
                    .put(
                        IndexMetadata.builder(clusterState.getMetadata().index(index))
                            .putCustom(ILM_CUSTOM_METADATA_KEY, lifecycleState.build().asMap())
                    )
            )
            .build();
        policyStepsRegistry.update(clusterState.metadata().custom(IndexLifecycleMetadata.TYPE));
    }
}
