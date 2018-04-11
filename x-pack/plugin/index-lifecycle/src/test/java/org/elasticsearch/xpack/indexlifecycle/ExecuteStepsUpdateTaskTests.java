/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.indexlifecycle;

import org.elasticsearch.Version;
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
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.indexlifecycle.LifecyclePolicy;
import org.elasticsearch.xpack.core.indexlifecycle.LifecycleSettings;
import org.elasticsearch.xpack.core.indexlifecycle.MockAction;
import org.elasticsearch.xpack.core.indexlifecycle.MockStep;
import org.elasticsearch.xpack.core.indexlifecycle.Phase;
import org.elasticsearch.xpack.core.indexlifecycle.Step;
import org.elasticsearch.xpack.core.indexlifecycle.Step.StepKey;
import org.elasticsearch.xpack.core.indexlifecycle.TerminalPolicyStep;
import org.elasticsearch.xpack.core.indexlifecycle.TestLifecycleType;
import static org.elasticsearch.xpack.indexlifecycle.IndexLifecycleRunnerTests.MockInitializePolicyContextStep;
import static org.elasticsearch.xpack.indexlifecycle.IndexLifecycleRunnerTests.MockClusterStateWaitStep;
import static org.hamcrest.Matchers.equalTo;

import org.junit.Before;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class ExecuteStepsUpdateTaskTests extends ESTestCase {

    private static final StepKey firstStepKey = new StepKey("phase_1", "action_1", "step_1");
    private static final StepKey secondStepKey = new StepKey("phase_1", "action_1", "step_2");
    private static final StepKey thirdStepKey = new StepKey("phase_1", "action_1", "step_3");
    private ClusterState clusterState;
    private PolicyStepsRegistry policyStepsRegistry;
    private String mixedPolicyName;
    private String allClusterPolicyName;
    private Index index;
    private MockInitializePolicyContextStep firstStep;
    private MockClusterStateWaitStep secondStep;
    private MockClusterStateWaitStep allClusterSecondStep;
    private MockStep thirdStep;

    @Before
    public void prepareState() {
        firstStep = new MockInitializePolicyContextStep(firstStepKey, secondStepKey);
        secondStep = new MockClusterStateWaitStep(secondStepKey, thirdStepKey);
        secondStep.setWillComplete(true);
        allClusterSecondStep = new MockClusterStateWaitStep(secondStepKey, TerminalPolicyStep.KEY);
        allClusterSecondStep.setWillComplete(true);
        thirdStep = new MockStep(thirdStepKey, null);
        mixedPolicyName = randomAlphaOfLengthBetween(5, 10);
        allClusterPolicyName = randomAlphaOfLengthBetween(1, 4);
        Phase mixedPhase = new Phase("first_phase", TimeValue.ZERO, Collections.singletonMap(MockAction.NAME,
            new MockAction(Arrays.asList(firstStep, secondStep, thirdStep))));
        Phase allClusterPhase = new Phase("first_phase", TimeValue.ZERO, Collections.singletonMap(MockAction.NAME,
            new MockAction(Arrays.asList(firstStep, allClusterSecondStep))));
        LifecyclePolicy mixedPolicy = new LifecyclePolicy(TestLifecycleType.INSTANCE, mixedPolicyName,
            Collections.singletonMap(mixedPhase.getName(), mixedPhase));
        LifecyclePolicy allClusterPolicy = new LifecyclePolicy(TestLifecycleType.INSTANCE, allClusterPolicyName,
            Collections.singletonMap(allClusterPhase.getName(), allClusterPhase));
        Map<String, LifecyclePolicy> policyMap = new HashMap<>();
        policyMap.put(mixedPolicyName, mixedPolicy);
        policyMap.put(allClusterPolicyName, allClusterPolicy);
        policyStepsRegistry = new PolicyStepsRegistry();

        IndexMetaData indexMetadata = IndexMetaData.builder(randomAlphaOfLength(5))
            .settings(settings(Version.CURRENT)
                .put(LifecycleSettings.LIFECYCLE_NAME, mixedPolicyName)
            .put(LifecycleSettings.LIFECYCLE_PHASE, "pre-phase")
                .put(LifecycleSettings.LIFECYCLE_ACTION, "pre-action")
                .put(LifecycleSettings.LIFECYCLE_STEP, "init"))
            .numberOfShards(randomIntBetween(1, 5)).numberOfReplicas(randomIntBetween(0, 5)).build();
        index = indexMetadata.getIndex();

        MetaData metaData = MetaData.builder()
            .persistentSettings(settings(Version.CURRENT).build())
            .putCustom(IndexLifecycleMetadata.TYPE, new IndexLifecycleMetadata(policyMap))
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
        policyStepsRegistry.update(clusterState, null, () -> 0L);
    }

    public void testExecuteAllUntilEndOfPolicy() {
        Step startStep = policyStepsRegistry.getFirstStep(allClusterPolicyName);
        ExecuteStepsUpdateTask task = new ExecuteStepsUpdateTask(allClusterPolicyName, index, startStep, policyStepsRegistry);
        ClusterState newState = task.execute(clusterState);
        assertThat(firstStep.getExecuteCount(), equalTo(1L));
        assertThat(allClusterSecondStep.getExecuteCount(), equalTo(1L));
        StepKey currentStepKey = IndexLifecycleRunner.getCurrentStepKey(newState.metaData().index(index).getSettings());
        assertThat(currentStepKey, equalTo(TerminalPolicyStep.KEY));
    }

    public void testExecuteMoveToNextActionStep() {
        secondStep.setWillComplete(false);
        Step startStep = policyStepsRegistry.getFirstStep(mixedPolicyName);
        ExecuteStepsUpdateTask task = new ExecuteStepsUpdateTask(mixedPolicyName, index, startStep, policyStepsRegistry);
        ClusterState newState = task.execute(clusterState);
        assertThat(firstStep.getExecuteCount(), equalTo(1L));
        assertThat(secondStep.getExecuteCount(), equalTo(1L));
        StepKey currentStepKey = IndexLifecycleRunner.getCurrentStepKey(newState.metaData().index(index).getSettings());
        assertThat(currentStepKey, equalTo(secondStepKey));
    }

    public void testNeverExecuteNonClusterStateStep() {
        setStateToKey(thirdStepKey);
        Step startStep = policyStepsRegistry.getStep(mixedPolicyName, thirdStepKey);
        ExecuteStepsUpdateTask task = new ExecuteStepsUpdateTask(mixedPolicyName, index, startStep, policyStepsRegistry);
        assertThat(task.execute(clusterState), equalTo(clusterState));
    }

    public void testExecuteUntilFirstNonClusterStateStep() {
        setStateToKey(secondStepKey);
        Step startStep = policyStepsRegistry.getStep(mixedPolicyName, secondStepKey);
        ExecuteStepsUpdateTask task = new ExecuteStepsUpdateTask(mixedPolicyName, index, startStep, policyStepsRegistry);
        ClusterState newState = task.execute(clusterState);
        StepKey currentStepKey = IndexLifecycleRunner.getCurrentStepKey(newState.metaData().index(index).getSettings());
        assertThat(currentStepKey, equalTo(thirdStepKey));
        assertThat(firstStep.getExecuteCount(), equalTo(0L));
        assertThat(secondStep.getExecuteCount(), equalTo(1L));
    }

    public void testExecuteIncompleteWaitStep() {
        secondStep.setWillComplete(false);
        setStateToKey(secondStepKey);
        Step startStep = policyStepsRegistry.getStep(mixedPolicyName, secondStepKey);
        ExecuteStepsUpdateTask task = new ExecuteStepsUpdateTask(mixedPolicyName, index, startStep, policyStepsRegistry);
        ClusterState newState = task.execute(clusterState);
        StepKey currentStepKey = IndexLifecycleRunner.getCurrentStepKey(newState.metaData().index(index).getSettings());
        assertThat(currentStepKey, equalTo(secondStepKey));
        assertThat(firstStep.getExecuteCount(), equalTo(0L));
        assertThat(secondStep.getExecuteCount(), equalTo(1L));
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
