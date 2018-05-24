/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 *
 */
package org.elasticsearch.xpack.indexlifecycle.action;

import org.elasticsearch.Version;
import org.elasticsearch.action.support.ActionFilter;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.indexlifecycle.ErrorStep;
import org.elasticsearch.xpack.core.indexlifecycle.IndexLifecycleMetadata;
import org.elasticsearch.xpack.core.indexlifecycle.LifecyclePolicy;
import org.elasticsearch.xpack.core.indexlifecycle.LifecyclePolicyMetadata;
import org.elasticsearch.xpack.core.indexlifecycle.LifecyclePolicyTests;
import org.elasticsearch.xpack.core.indexlifecycle.LifecycleSettings;
import org.elasticsearch.xpack.core.indexlifecycle.Step;
import org.elasticsearch.xpack.indexlifecycle.IndexLifecycleRunner;
import org.elasticsearch.xpack.indexlifecycle.IndexLifecycleRunnerTests;
import org.elasticsearch.xpack.indexlifecycle.IndexLifecycleService;
import org.junit.After;
import org.junit.Before;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.node.Node.NODE_MASTER_SETTING;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ReRunActionTests extends ESTestCase {

    TransportReRunAction action;
    ClusterState initialClusterState;
    String[] indices;
    LifecyclePolicy policy;
    IndexLifecycleService lifecycleService;

    @Before
    public void setup() {
        String nodeId = randomAlphaOfLength(5);
        DiscoveryNode masterNode = DiscoveryNode.createLocal(settings(Version.CURRENT)
                .put(NODE_MASTER_SETTING.getKey(), true).build(),
            new TransportAddress(TransportAddress.META_ADDRESS, 9300), nodeId);
        ClusterService clusterService = mock(ClusterService.class);
        doNothing().when(clusterService).addListener(any());
        TransportService transportService = mock(TransportService.class);
        when(transportService.getTaskManager()).thenReturn(null);
        doNothing().when(transportService).registerRequestHandler(anyString(), anyString(), any(), any());
        ActionFilters actionFilters = mock(ActionFilters.class);
        when(actionFilters.filters()).thenReturn(new ActionFilter[] {});

        String index = randomAlphaOfLength(5);
        policy = LifecyclePolicyTests.randomLifecyclePolicy(randomAlphaOfLength(5));
        IndexMetaData indexMetaData = IndexMetaData.builder(index)
            .settings(settings(Version.CURRENT)
                .put(LifecycleSettings.LIFECYCLE_NAME_SETTING.getKey(), policy.getName())
            )
            .numberOfShards(randomIntBetween(1, 5)).numberOfReplicas(randomIntBetween(0, 5)).build();
        MetaData metaData = MetaData.builder()
            .putCustom(IndexLifecycleMetadata.TYPE, new IndexLifecycleMetadata(Collections.singletonMap(policy.getName(),
                new LifecyclePolicyMetadata(policy, Collections.emptyMap()))))
            .persistentSettings(settings(Version.CURRENT).build())
            .put(indexMetaData, false)
            .build();
        initialClusterState = ClusterState.builder(ClusterName.DEFAULT).metaData(metaData)
            .nodes(DiscoveryNodes.builder().localNodeId(nodeId).masterNodeId(nodeId).add(masterNode).build())
            .build();
        Client client = mock(Client.class);
        when(client.settings()).thenReturn(Settings.EMPTY);
        when(client.threadPool()).thenReturn(null);
        lifecycleService = new IndexLifecycleService(Settings.EMPTY, client, clusterService,
            Clock.fixed(Instant.EPOCH, ZoneId.of(randomFrom(ZoneId.getAvailableZoneIds()))), null, () -> 0L);
        lifecycleService.clusterChanged(new ClusterChangedEvent("_source", initialClusterState, ClusterState.EMPTY_STATE));
        indices = new String[] { index };
        action = new TransportReRunAction(Settings.EMPTY, transportService, clusterService,
            null, actionFilters, null, lifecycleService);
    }

    @After
    public void close() {
        lifecycleService.close();
    }

    public void testInnerExecute() {
        List<Step> policySteps = policy.toSteps(null, () -> 0L);
        IndexMetaData initialIdxMeta = initialClusterState.metaData().index(indices[0]);
        IndexMetaData idxMeta = IndexMetaData.builder(initialIdxMeta)
            .settings(Settings.builder().put(initialIdxMeta.getSettings())
                .put(LifecycleSettings.LIFECYCLE_PHASE, policySteps.get(0).getKey().getPhase())
                .put(LifecycleSettings.LIFECYCLE_ACTION, policySteps.get(0).getKey().getAction())
                .put(LifecycleSettings.LIFECYCLE_FAILED_STEP, policySteps.get(0).getKey().getName())
                .put(LifecycleSettings.LIFECYCLE_STEP, ErrorStep.NAME)
                .build()).build();
        ClusterState clusterState = ClusterState.builder(initialClusterState)
            .metaData(MetaData.builder(initialClusterState.metaData()).put(idxMeta, false)).build();
        Step.StepKey currentStepKey = IndexLifecycleRunner.getCurrentStepKey(idxMeta.getSettings());

        ClusterState nextClusterState = action.innerExecute(clusterState, indices);
        Step.StepKey nextStepKey = IndexLifecycleRunner.getCurrentStepKey(
            nextClusterState.metaData().index(idxMeta.getIndex()).getSettings());

        IndexLifecycleRunnerTests.assertClusterStateOnNextStep(clusterState, idxMeta.getIndex(), currentStepKey, nextStepKey,
            nextClusterState, 0L);
    }

    public void testInnerExecuteIndexNotFound() {
        String invalidIndex = indices[0] + "other";
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class,
            () -> action.innerExecute(initialClusterState, new String[] { invalidIndex }));
        assertThat(exception.getMessage(), equalTo("index [" + invalidIndex + "] does not exist"));
    }

    public void testInnerExecuteInvalidPolicySetting() {
        IndexMetaData initialIdxMeta = initialClusterState.metaData().index(indices[0]);
        IndexMetaData idxMeta = IndexMetaData.builder(initialIdxMeta)
            .settings(Settings.builder().put(initialIdxMeta.getSettings())
                .put(LifecycleSettings.LIFECYCLE_NAME, (String) null)
                .build()).build();
        ClusterState clusterState = ClusterState.builder(initialClusterState)
            .metaData(MetaData.builder(initialClusterState.metaData()).put(idxMeta, false)).build();
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class,
            () -> action.innerExecute(clusterState, indices));
        assertThat(exception.getMessage(), equalTo("cannot re-run an action for an index [" + indices[0]
            + "] that has not encountered an error when running a Lifecycle Policy"));
    }

    public void testInnerExecuteNotOnError() {
        List<Step> policySteps = policy.toSteps(null, () -> 0L);
        IndexMetaData initialIdxMeta = initialClusterState.metaData().index(indices[0]);
        IndexMetaData idxMeta = IndexMetaData.builder(initialIdxMeta)
            .settings(Settings.builder().put(initialIdxMeta.getSettings())
                .put(LifecycleSettings.LIFECYCLE_PHASE, policySteps.get(0).getKey().getPhase())
                .put(LifecycleSettings.LIFECYCLE_ACTION, policySteps.get(0).getKey().getAction())
                .put(LifecycleSettings.LIFECYCLE_STEP, policySteps.get(0).getKey().getName())
                .build()).build();
        ClusterState clusterState = ClusterState.builder(initialClusterState)
            .metaData(MetaData.builder(initialClusterState.metaData()).put(idxMeta, false)).build();
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class,
            () -> action.innerExecute(clusterState, indices));
        assertThat(exception.getMessage(), equalTo("cannot re-run an action for an index [" + indices[0]
            + "] that has not encountered an error when running a Lifecycle Policy"));
    }
}
