/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.indexlifecycle;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.node.Node;
import org.elasticsearch.protocol.xpack.indexlifecycle.OperationMode;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.indexlifecycle.IndexLifecycleMetadata;
import org.elasticsearch.xpack.core.indexlifecycle.LifecyclePolicy;
import org.elasticsearch.xpack.core.indexlifecycle.LifecyclePolicyMetadata;
import org.elasticsearch.xpack.core.indexlifecycle.LifecycleSettings;
import org.elasticsearch.xpack.core.indexlifecycle.MockAction;
import org.elasticsearch.xpack.core.indexlifecycle.MockStep;
import org.elasticsearch.xpack.core.indexlifecycle.Phase;
import org.elasticsearch.xpack.core.indexlifecycle.Step;
import org.elasticsearch.xpack.core.indexlifecycle.Step.StepKey;
import org.elasticsearch.xpack.core.indexlifecycle.TerminalPolicyStep;
import org.elasticsearch.xpack.core.indexlifecycle.TestLifecycleType;
import org.elasticsearch.xpack.indexlifecycle.IndexLifecycleRunnerTests.MockClusterStateActionStep;
import org.elasticsearch.xpack.indexlifecycle.IndexLifecycleRunnerTests.MockClusterStateWaitStep;
import org.junit.Before;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.sameInstance;

public class ExecuteStepsUpdateTaskTests extends ESTestCase {

    private static final StepKey firstStepKey = new StepKey("phase_1", "action_1", "step_1");
    private static final StepKey secondStepKey = new StepKey("phase_1", "action_1", "step_2");
    private static final StepKey thirdStepKey = new StepKey("phase_1", "action_1", "step_3");
    private static final StepKey invalidStepKey = new StepKey("invalid", "invalid", "invalid");
    private ClusterState clusterState;
    private PolicyStepsRegistry policyStepsRegistry;
    private String mixedPolicyName;
    private String allClusterPolicyName;
    private String invalidPolicyName;
    private Index index;
    private MockClusterStateActionStep firstStep;
    private MockClusterStateWaitStep secondStep;
    private MockClusterStateWaitStep allClusterSecondStep;
    private MockStep thirdStep;
    private Client client;

    @Before
    public void prepareState() {
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
        Phase mixedPhase = new Phase("first_phase", TimeValue.ZERO, Collections.singletonMap(MockAction.NAME,
            new MockAction(Arrays.asList(firstStep, secondStep, thirdStep))));
        Phase allClusterPhase = new Phase("first_phase", TimeValue.ZERO, Collections.singletonMap(MockAction.NAME,
            new MockAction(Arrays.asList(firstStep, allClusterSecondStep))));
        Phase invalidPhase = new Phase("invalid_phase", TimeValue.ZERO, Collections.singletonMap(MockAction.NAME,
            new MockAction(Arrays.asList(new MockClusterStateActionStep(firstStepKey, invalidStepKey)))));
        LifecyclePolicy mixedPolicy = new LifecyclePolicy(TestLifecycleType.INSTANCE, mixedPolicyName,
            Collections.singletonMap(mixedPhase.getName(), mixedPhase));
        LifecyclePolicy allClusterPolicy = new LifecyclePolicy(TestLifecycleType.INSTANCE, allClusterPolicyName,
            Collections.singletonMap(allClusterPhase.getName(), allClusterPhase));
        LifecyclePolicy invalidPolicy = new LifecyclePolicy(TestLifecycleType.INSTANCE, invalidPolicyName,
            Collections.singletonMap(invalidPhase.getName(), invalidPhase));
        Map<String, LifecyclePolicyMetadata> policyMap = new HashMap<>();
        policyMap.put(mixedPolicyName, new LifecyclePolicyMetadata(mixedPolicy, Collections.emptyMap()));
        policyMap.put(allClusterPolicyName, new LifecyclePolicyMetadata(allClusterPolicy, Collections.emptyMap()));
        policyMap.put(invalidPolicyName, new LifecyclePolicyMetadata(invalidPolicy, Collections.emptyMap()));
        policyStepsRegistry = new PolicyStepsRegistry();

        IndexMetaData indexMetadata = IndexMetaData.builder(randomAlphaOfLength(5))
            .settings(settings(Version.CURRENT)
                .put(LifecycleSettings.LIFECYCLE_NAME, mixedPolicyName)
            .put(LifecycleSettings.LIFECYCLE_PHASE, "pre-phase")
                .put(LifecycleSettings.LIFECYCLE_ACTION, "pre-action")
                .put(LifecycleSettings.LIFECYCLE_STEP, "init"))
            .numberOfShards(randomIntBetween(1, 5)).numberOfReplicas(randomIntBetween(0, 5)).build();
        index = indexMetadata.getIndex();
        IndexLifecycleMetadata lifecycleMetadata = new IndexLifecycleMetadata(policyMap, OperationMode.RUNNING);
        MetaData metaData = MetaData.builder()
            .persistentSettings(settings(Version.CURRENT).build())
            .putCustom(IndexLifecycleMetadata.TYPE, lifecycleMetadata)
            .put(IndexMetaData.builder(indexMetadata))
            .build();
        String nodeId = randomAlphaOfLength(10);
        DiscoveryNode masterNode = DiscoveryNode.createLocal(settings(Version.CURRENT)
                .put(Node.NODE_MASTER_SETTING.getKey(), true).build(),
            new TransportAddress(TransportAddress.META_ADDRESS, 9300), nodeId);
        clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .metaData(metaData)
            .nodes(DiscoveryNodes.builder().localNodeId(nodeId).masterNodeId(nodeId).add(masterNode).build())
            .build();
        policyStepsRegistry.update(lifecycleMetadata, client, () -> 0L);
    }

    public void testExecuteAllUntilEndOfPolicy() throws IOException {
        Step startStep = policyStepsRegistry.getFirstStep(allClusterPolicyName);
        long now = randomNonNegativeLong();
        ExecuteStepsUpdateTask task = new ExecuteStepsUpdateTask(allClusterPolicyName, index, startStep, policyStepsRegistry, () -> now);
        ClusterState newState = task.execute(clusterState);
        assertThat(firstStep.getExecuteCount(), equalTo(1L));
        assertThat(allClusterSecondStep.getExecuteCount(), equalTo(1L));
        StepKey currentStepKey = IndexLifecycleRunner.getCurrentStepKey(newState.metaData().index(index).getSettings());
        assertThat(currentStepKey, equalTo(TerminalPolicyStep.KEY));
        assertThat(LifecycleSettings.LIFECYCLE_PHASE_TIME_SETTING.get(newState.metaData().index(index).getSettings()), equalTo(now));
        assertThat(LifecycleSettings.LIFECYCLE_ACTION_TIME_SETTING.get(newState.metaData().index(index).getSettings()), equalTo(now));
        assertThat(LifecycleSettings.LIFECYCLE_STEP_INFO_SETTING.get(newState.metaData().index(index).getSettings()), equalTo(""));
    }

    public void testExecuteMoveToNextActionStep() throws IOException {
        secondStep.setWillComplete(false);
        Step startStep = policyStepsRegistry.getFirstStep(mixedPolicyName);
        long now = randomNonNegativeLong();
        ExecuteStepsUpdateTask task = new ExecuteStepsUpdateTask(mixedPolicyName, index, startStep, policyStepsRegistry, () -> now);
        ClusterState newState = task.execute(clusterState);
        assertThat(firstStep.getExecuteCount(), equalTo(1L));
        assertThat(secondStep.getExecuteCount(), equalTo(1L));
        StepKey currentStepKey = IndexLifecycleRunner.getCurrentStepKey(newState.metaData().index(index).getSettings());
        assertThat(currentStepKey, equalTo(secondStepKey));
        assertThat(LifecycleSettings.LIFECYCLE_PHASE_TIME_SETTING.get(newState.metaData().index(index).getSettings()), equalTo(now));
        assertThat(LifecycleSettings.LIFECYCLE_ACTION_TIME_SETTING.get(newState.metaData().index(index).getSettings()), equalTo(now));
        assertThat(LifecycleSettings.LIFECYCLE_STEP_INFO_SETTING.get(newState.metaData().index(index).getSettings()), equalTo(""));
    }

    public void testNeverExecuteNonClusterStateStep() throws IOException {
        setStateToKey(thirdStepKey);
        Step startStep = policyStepsRegistry.getStep(mixedPolicyName, thirdStepKey);
        long now = randomNonNegativeLong();
        ExecuteStepsUpdateTask task = new ExecuteStepsUpdateTask(mixedPolicyName, index, startStep, policyStepsRegistry, () -> now);
        assertThat(task.execute(clusterState), sameInstance(clusterState));
    }

    public void testExecuteUntilFirstNonClusterStateStep() throws IOException {
        setStateToKey(secondStepKey);
        Step startStep = policyStepsRegistry.getStep(mixedPolicyName, secondStepKey);
        long now = randomNonNegativeLong();
        ExecuteStepsUpdateTask task = new ExecuteStepsUpdateTask(mixedPolicyName, index, startStep, policyStepsRegistry, () -> now);
        ClusterState newState = task.execute(clusterState);
        StepKey currentStepKey = IndexLifecycleRunner.getCurrentStepKey(newState.metaData().index(index).getSettings());
        assertThat(currentStepKey, equalTo(thirdStepKey));
        assertThat(firstStep.getExecuteCount(), equalTo(0L));
        assertThat(secondStep.getExecuteCount(), equalTo(1L));
        assertThat(LifecycleSettings.LIFECYCLE_PHASE_TIME_SETTING.get(newState.metaData().index(index).getSettings()), equalTo(-1L));
        assertThat(LifecycleSettings.LIFECYCLE_ACTION_TIME_SETTING.get(newState.metaData().index(index).getSettings()), equalTo(-1L));
        assertThat(LifecycleSettings.LIFECYCLE_STEP_INFO_SETTING.get(newState.metaData().index(index).getSettings()), equalTo(""));
    }

    public void testExecuteInvalidStartStep() throws IOException {
        setStateToKey(firstStepKey);
        Step startStep = policyStepsRegistry.getStep(mixedPolicyName, firstStepKey);
        long now = randomNonNegativeLong();
        ExecuteStepsUpdateTask task = new ExecuteStepsUpdateTask(invalidPolicyName, index, startStep, policyStepsRegistry, () -> now);
        ClusterState newState = task.execute(clusterState);
        assertSame(newState, clusterState);

    }

    public void testExecuteUntilNullStep() throws IOException {
        setStateToKey(firstStepKey);
        Step startStep = policyStepsRegistry.getStep(invalidPolicyName, firstStepKey);
        long now = randomNonNegativeLong();
        ExecuteStepsUpdateTask task = new ExecuteStepsUpdateTask(invalidPolicyName, index, startStep, policyStepsRegistry, () -> now);
        ClusterState newState = task.execute(clusterState);
        StepKey currentStepKey = IndexLifecycleRunner.getCurrentStepKey(newState.metaData().index(index).getSettings());
        assertThat(currentStepKey, equalTo(invalidStepKey));
    }

    public void testExecuteIncompleteWaitStepNoInfo() throws IOException {
        secondStep.setWillComplete(false);
        setStateToKey(secondStepKey);
        Step startStep = policyStepsRegistry.getStep(mixedPolicyName, secondStepKey);
        long now = randomNonNegativeLong();
        ExecuteStepsUpdateTask task = new ExecuteStepsUpdateTask(mixedPolicyName, index, startStep, policyStepsRegistry, () -> now);
        ClusterState newState = task.execute(clusterState);
        StepKey currentStepKey = IndexLifecycleRunner.getCurrentStepKey(newState.metaData().index(index).getSettings());
        assertThat(currentStepKey, equalTo(secondStepKey));
        assertThat(firstStep.getExecuteCount(), equalTo(0L));
        assertThat(secondStep.getExecuteCount(), equalTo(1L));
        assertThat(LifecycleSettings.LIFECYCLE_PHASE_TIME_SETTING.get(newState.metaData().index(index).getSettings()), equalTo(-1L));
        assertThat(LifecycleSettings.LIFECYCLE_ACTION_TIME_SETTING.get(newState.metaData().index(index).getSettings()), equalTo(-1L));
        assertThat(LifecycleSettings.LIFECYCLE_STEP_INFO_SETTING.get(newState.metaData().index(index).getSettings()), equalTo(""));
    }

    public void testExecuteIncompleteWaitStepWithInfo() throws IOException {
        secondStep.setWillComplete(false);
        RandomStepInfo stepInfo = new RandomStepInfo(() -> randomAlphaOfLength(10));
        secondStep.expectedInfo(stepInfo);
        setStateToKey(secondStepKey);
        Step startStep = policyStepsRegistry.getStep(mixedPolicyName, secondStepKey);
        long now = randomNonNegativeLong();
        ExecuteStepsUpdateTask task = new ExecuteStepsUpdateTask(mixedPolicyName, index, startStep, policyStepsRegistry, () -> now);
        ClusterState newState = task.execute(clusterState);
        StepKey currentStepKey = IndexLifecycleRunner.getCurrentStepKey(newState.metaData().index(index).getSettings());
        assertThat(currentStepKey, equalTo(secondStepKey));
        assertThat(firstStep.getExecuteCount(), equalTo(0L));
        assertThat(secondStep.getExecuteCount(), equalTo(1L));
        assertThat(LifecycleSettings.LIFECYCLE_PHASE_TIME_SETTING.get(newState.metaData().index(index).getSettings()), equalTo(-1L));
        assertThat(LifecycleSettings.LIFECYCLE_ACTION_TIME_SETTING.get(newState.metaData().index(index).getSettings()), equalTo(-1L));
        assertThat(LifecycleSettings.LIFECYCLE_STEP_INFO_SETTING.get(newState.metaData().index(index).getSettings()),
                equalTo(stepInfo.toString()));
    }

    public void testOnFailure() {
        setStateToKey(secondStepKey);
        Step startStep = policyStepsRegistry.getStep(mixedPolicyName, secondStepKey);
        long now = randomNonNegativeLong();
        ExecuteStepsUpdateTask task = new ExecuteStepsUpdateTask(mixedPolicyName, index, startStep, policyStepsRegistry, () -> now);
        Exception expectedException = new RuntimeException();
        ElasticsearchException exception = expectThrows(ElasticsearchException.class,
                () -> task.onFailure(randomAlphaOfLength(10), expectedException));
        assertEquals("policy [" + mixedPolicyName + "] for index [" + index.getName() + "] failed on step [" + startStep.getKey() + "].",
                exception.getMessage());
        assertSame(expectedException, exception.getCause());
    }

    private void setStateToKey(StepKey stepKey) {
        clusterState = ClusterState.builder(clusterState)
            .metaData(MetaData.builder(clusterState.metaData())
                .updateSettings(Settings.builder()
                    .put(LifecycleSettings.LIFECYCLE_PHASE, stepKey.getPhase())
                    .put(LifecycleSettings.LIFECYCLE_ACTION, stepKey.getAction())
                    .put(LifecycleSettings.LIFECYCLE_STEP, stepKey.getName()).build(), index.getName())).build();
    }
}
