/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.settings.secure;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSecrets;
import org.elasticsearch.common.settings.SecureClusterStateSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.plugins.ReloadablePlugin;
import org.elasticsearch.test.ESTestCase;
import org.mockito.ArgumentCaptor;

import java.util.Set;

import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ClusterStateSecretsListenerTests extends ESTestCase {

    public void testRemovesLegacyClusterCustomWhenMasterAndClusterRecovered() throws Exception {
        ClusterService clusterService = mock(ClusterService.class);
        Environment environment = mock(Environment.class);
        when(environment.settings()).thenReturn(Settings.EMPTY);

        ClusterStateSecretsListener listener = new ClusterStateSecretsListener(clusterService, environment);
        verify(clusterService).addListener(listener);

        ReloadablePlugin reloadablePlugin = mock(ReloadablePlugin.class);
        listener.setReloadCallback(reloadablePlugin);

        DiscoveryNodes nodes = discoveryNodesMasterLocal();
        ClusterState previous = ClusterState.builder(ClusterName.DEFAULT).nodes(nodes).build();
        ClusterState current = ClusterState.builder(previous)
            .putCustom(ClusterStateSecretsMetadata.TYPE, ClusterStateSecretsMetadata.createSuccessful(1L))
            .incrementVersion()
            .build();

        ArgumentCaptor<ClusterStateUpdateTask> taskCaptor = ArgumentCaptor.forClass(ClusterStateUpdateTask.class);
        listener.clusterChanged(new ClusterChangedEvent("test", current, previous));

        verify(clusterService).submitUnbatchedStateUpdateTask(eq("removeClusterStateSecretsMetadata"), taskCaptor.capture());
        ClusterStateUpdateTask task = taskCaptor.getValue();
        ClusterState updated = task.execute(current);
        assertNull(updated.custom(ClusterStateSecretsMetadata.TYPE));

        clearInvocations(clusterService);
        listener.clusterChanged(new ClusterChangedEvent("test", current, previous));
        verify(clusterService, never()).submitUnbatchedStateUpdateTask(anyString(), any(ClusterStateUpdateTask.class));
    }

    public void testSubmitRemovalAgainAfterFailure() {
        ClusterService clusterService = mock(ClusterService.class);
        Environment environment = mock(Environment.class);
        when(environment.settings()).thenReturn(Settings.EMPTY);

        ClusterStateSecretsListener listener = new ClusterStateSecretsListener(clusterService, environment);
        ReloadablePlugin reloadablePlugin = mock(ReloadablePlugin.class);
        listener.setReloadCallback(reloadablePlugin);

        DiscoveryNodes nodes = discoveryNodesMasterLocal();
        ClusterState previous = ClusterState.builder(ClusterName.DEFAULT).nodes(nodes).build();
        ClusterState current = ClusterState.builder(previous)
            .putCustom(ClusterStateSecretsMetadata.TYPE, ClusterStateSecretsMetadata.createSuccessful(1L))
            .incrementVersion()
            .build();

        ArgumentCaptor<ClusterStateUpdateTask> taskCaptor = ArgumentCaptor.forClass(ClusterStateUpdateTask.class);
        listener.clusterChanged(new ClusterChangedEvent("test", current, previous));
        verify(clusterService).submitUnbatchedStateUpdateTask(eq("removeClusterStateSecretsMetadata"), taskCaptor.capture());
        taskCaptor.getValue().onFailure(new ElasticsearchException("simulated"));

        clearInvocations(clusterService);
        listener.clusterChanged(new ClusterChangedEvent("test", current, previous));
        verify(clusterService).submitUnbatchedStateUpdateTask(eq("removeClusterStateSecretsMetadata"), taskCaptor.capture());
    }

    public void testDoesNotSubmitWhenClusterNotRecovered() {
        ClusterService clusterService = mock(ClusterService.class);
        Environment environment = mock(Environment.class);
        when(environment.settings()).thenReturn(Settings.EMPTY);

        ClusterStateSecretsListener listener = new ClusterStateSecretsListener(clusterService, environment);
        ReloadablePlugin reloadablePlugin = mock(ReloadablePlugin.class);
        listener.setReloadCallback(reloadablePlugin);

        DiscoveryNodes nodes = discoveryNodesMasterLocal();
        ClusterBlocks blocks = ClusterBlocks.builder().addGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK).build();
        ClusterState previous = ClusterState.builder(ClusterName.DEFAULT).nodes(nodes).blocks(blocks).build();
        ClusterState current = ClusterState.builder(previous)
            .putCustom(ClusterStateSecretsMetadata.TYPE, ClusterStateSecretsMetadata.createSuccessful(1L))
            .incrementVersion()
            .build();

        listener.clusterChanged(new ClusterChangedEvent("test", current, previous));

        verify(clusterService, never()).submitUnbatchedStateUpdateTask(anyString(), any(ClusterStateUpdateTask.class));
    }

    public void testDoesNotSubmitWhenLocalNodeIsNotMaster() {
        ClusterService clusterService = mock(ClusterService.class);
        Environment environment = mock(Environment.class);
        when(environment.settings()).thenReturn(Settings.EMPTY);

        ClusterStateSecretsListener listener = new ClusterStateSecretsListener(clusterService, environment);
        ReloadablePlugin reloadablePlugin = mock(ReloadablePlugin.class);
        listener.setReloadCallback(reloadablePlugin);

        DiscoveryNode localDataNode = DiscoveryNodeUtils.builder("local-data").roles(Set.of(DiscoveryNodeRole.DATA_ROLE)).build();
        DiscoveryNode remoteMaster = DiscoveryNodeUtils.builder("remote-master").roles(Set.of(DiscoveryNodeRole.MASTER_ROLE)).build();
        DiscoveryNodes nodes = DiscoveryNodes.builder()
            .add(localDataNode)
            .add(remoteMaster)
            .localNodeId(localDataNode.getId())
            .masterNodeId(remoteMaster.getId())
            .build();

        ClusterState previous = ClusterState.builder(ClusterName.DEFAULT).nodes(nodes).build();
        ClusterState current = ClusterState.builder(previous)
            .putCustom(ClusterStateSecretsMetadata.TYPE, ClusterStateSecretsMetadata.createSuccessful(1L))
            .incrementVersion()
            .build();

        listener.clusterChanged(new ClusterChangedEvent("test", current, previous));

        verify(clusterService, never()).submitUnbatchedStateUpdateTask(anyString(), any(ClusterStateUpdateTask.class));
    }

    public void testDoesNotSubmitWhenNoLegacyMetadata() {
        ClusterService clusterService = mock(ClusterService.class);
        Environment environment = mock(Environment.class);
        when(environment.settings()).thenReturn(Settings.EMPTY);

        ClusterStateSecretsListener listener = new ClusterStateSecretsListener(clusterService, environment);
        ReloadablePlugin reloadablePlugin = mock(ReloadablePlugin.class);
        listener.setReloadCallback(reloadablePlugin);

        DiscoveryNodes nodes = discoveryNodesMasterLocal();
        ClusterState current = ClusterState.builder(ClusterName.DEFAULT).nodes(nodes).build();

        listener.clusterChanged(new ClusterChangedEvent("test", current, ClusterState.EMPTY_STATE));

        verify(clusterService, never()).submitUnbatchedStateUpdateTask(anyString(), any(ClusterStateUpdateTask.class));
    }

    public void testRegistersClusterStateListenerWithClusterService() {
        ClusterService clusterService = mock(ClusterService.class);
        Environment environment = mock(Environment.class);

        ClusterStateSecretsListener listener = new ClusterStateSecretsListener(clusterService, environment);

        ArgumentCaptor<ClusterStateListener> listenerCaptor = ArgumentCaptor.forClass(ClusterStateListener.class);
        verify(clusterService).addListener(listenerCaptor.capture());
        assertThat(listenerCaptor.getValue(), sameInstance(listener));
    }

    public void testReloadsWhenClusterSecretsVersionIncreases() throws Exception {
        ClusterService clusterService = mock(ClusterService.class);
        Environment environment = mock(Environment.class);
        when(environment.settings()).thenReturn(Settings.EMPTY);

        ClusterStateSecretsListener listener = new ClusterStateSecretsListener(clusterService, environment);
        ReloadablePlugin reloadablePlugin = mock(ReloadablePlugin.class);
        listener.setReloadCallback(reloadablePlugin);

        DiscoveryNodes nodes = discoveryNodesMasterLocal();
        ClusterSecrets firstSecrets = new ClusterSecrets(1L, SecureClusterStateSettings.EMPTY);
        ClusterSecrets secondSecrets = new ClusterSecrets(2L, SecureClusterStateSettings.EMPTY);

        ClusterState previous = ClusterState.builder(ClusterName.DEFAULT).nodes(nodes).putCustom(ClusterSecrets.TYPE, firstSecrets).build();
        ClusterState current = ClusterState.builder(previous).putCustom(ClusterSecrets.TYPE, secondSecrets).incrementVersion().build();

        listener.clusterChanged(new ClusterChangedEvent("test", current, previous));

        verify(reloadablePlugin).reload(any(Settings.class));
    }

    private static DiscoveryNodes discoveryNodesMasterLocal() {
        DiscoveryNode localMaster = DiscoveryNodeUtils.builder("local-master").roles(Set.of(DiscoveryNodeRole.MASTER_ROLE)).build();
        return DiscoveryNodes.builder().add(localMaster).localNodeId(localMaster.getId()).masterNodeId(localMaster.getId()).build();
    }
}
