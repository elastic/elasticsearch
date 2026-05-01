/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.crypto;

import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.ClusterStateTaskListener;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.project.DefaultProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.MasterServiceTaskQueue;
import org.elasticsearch.features.FeatureService;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.crypto.PrimaryEncryptionKeyMetadata;
import org.mockito.ArgumentCaptor;

import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class PrimaryEncryptionKeyServiceTests extends ESTestCase {

    private static final ClusterName CLUSTER_NAME = new ClusterName("test");

    @SuppressWarnings("unchecked")
    private MasterServiceTaskQueue<ClusterStateTaskListener> mockTaskQueue() {
        return mock(MasterServiceTaskQueue.class);
    }

    private ClusterService mockClusterService(MasterServiceTaskQueue<ClusterStateTaskListener> taskQueue) {
        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.createTaskQueue(anyString(), any(), any())).thenReturn(taskQueue);
        return clusterService;
    }

    private ClusterStateListener captureListener(ClusterService clusterService) {
        ArgumentCaptor<ClusterStateListener> captor = ArgumentCaptor.forClass(ClusterStateListener.class);
        verify(clusterService).addListener(captor.capture());
        return captor.getValue();
    }

    private static PrimaryEncryptionKeyMetadata randomPekMetadata() {
        byte[] keyBytes = new byte[32];
        random().nextBytes(keyBytes);
        String keyId = PrimaryEncryptionKeyMetadata.generateKeyId();
        return new PrimaryEncryptionKeyMetadata(Map.of(keyId, keyBytes), keyId);
    }

    private static DiscoveryNodes masterNodes(boolean isLocalMaster) {
        DiscoveryNode localNode = DiscoveryNodeUtils.create("local");
        DiscoveryNode otherNode = DiscoveryNodeUtils.create("other");
        DiscoveryNodes.Builder builder = DiscoveryNodes.builder().add(localNode).add(otherNode).localNodeId("local");
        builder.masterNodeId(isLocalMaster ? "local" : "other");
        return builder.build();
    }

    private static ClusterState stateWithKey(PrimaryEncryptionKeyMetadata pek, DiscoveryNodes nodes) {
        ProjectMetadata project = ProjectMetadata.builder(Metadata.DEFAULT_PROJECT_ID)
            .putCustom(PrimaryEncryptionKeyMetadata.TYPE, pek)
            .build();
        return ClusterState.builder(CLUSTER_NAME).metadata(Metadata.builder().put(project).build()).nodes(nodes).build();
    }

    private static ClusterState stateWithoutKey(DiscoveryNodes nodes) {
        return ClusterState.builder(CLUSTER_NAME).nodes(nodes).build();
    }

    public void testGetActiveKeyReturnsNullInitially() {
        MasterServiceTaskQueue<ClusterStateTaskListener> taskQueue = mockTaskQueue();
        ClusterService clusterService = mockClusterService(taskQueue);
        PrimaryEncryptionKeyService service = PrimaryEncryptionKeyService.create(
            clusterService,
            DefaultProjectResolver.INSTANCE,
            mock(FeatureService.class)
        );
        assertNull(service.getActiveKey());
        assertNull(service.getKey("anything"));
    }

    public void testServiceRegistersAsClusterStateListener() {
        MasterServiceTaskQueue<ClusterStateTaskListener> taskQueue = mockTaskQueue();
        ClusterService clusterService = mockClusterService(taskQueue);
        PrimaryEncryptionKeyService.create(clusterService, DefaultProjectResolver.INSTANCE, mock(FeatureService.class));
        verify(clusterService).addListener(any());
    }

    public void testCacheUpdatedOnMetadataChange() {
        MasterServiceTaskQueue<ClusterStateTaskListener> taskQueue = mockTaskQueue();
        ClusterService clusterService = mockClusterService(taskQueue);
        PrimaryEncryptionKeyService service = PrimaryEncryptionKeyService.create(
            clusterService,
            DefaultProjectResolver.INSTANCE,
            mock(FeatureService.class)
        );
        ClusterStateListener listener = captureListener(clusterService);

        PrimaryEncryptionKeyMetadata pek = randomPekMetadata();
        DiscoveryNodes nodes = masterNodes(false);
        ClusterState prevState = stateWithoutKey(nodes);
        ClusterState newState = stateWithKey(pek, nodes);

        listener.clusterChanged(new ClusterChangedEvent("test", newState, prevState));

        AesGcmEncryptionService.ActiveKey activeKey = service.getActiveKey();
        assertNotNull(activeKey);
        assertEquals(pek.getActiveKeyId(), activeKey.keyId());
        assertEquals("AES", activeKey.key().getAlgorithm());
        assertNotNull(service.getKey(pek.getActiveKeyId()));
        assertNull(service.getKey("nonexistent"));
    }

    public void testGatewayBlockSkipsProcessing() {
        MasterServiceTaskQueue<ClusterStateTaskListener> taskQueue = mockTaskQueue();
        ClusterService clusterService = mockClusterService(taskQueue);
        PrimaryEncryptionKeyService service = PrimaryEncryptionKeyService.create(
            clusterService,
            DefaultProjectResolver.INSTANCE,
            mock(FeatureService.class)
        );
        ClusterStateListener listener = captureListener(clusterService);

        DiscoveryNodes nodes = masterNodes(true);
        ClusterState prevState = stateWithoutKey(nodes);
        ClusterState blockedState = ClusterState.builder(stateWithoutKey(nodes))
            .blocks(ClusterBlocks.builder().addGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK).build())
            .build();

        listener.clusterChanged(new ClusterChangedEvent("test", blockedState, prevState));

        assertNull(service.getActiveKey());
        verify(taskQueue, never()).submitTask(anyString(), any(), any());
    }

    public void testMasterSubmitsTaskWhenNoKeyExists() {
        MasterServiceTaskQueue<ClusterStateTaskListener> taskQueue = mockTaskQueue();
        ClusterService clusterService = mockClusterService(taskQueue);
        FeatureService featureService = mock(FeatureService.class);
        when(featureService.clusterHasFeature(any(), any())).thenReturn(true);

        PrimaryEncryptionKeyService.create(clusterService, DefaultProjectResolver.INSTANCE, featureService);
        ClusterStateListener listener = captureListener(clusterService);

        DiscoveryNodes nodes = masterNodes(true);
        ClusterState prevState = stateWithoutKey(nodes);
        ClusterState newState = stateWithoutKey(nodes);

        listener.clusterChanged(new ClusterChangedEvent("test", newState, prevState));

        verify(taskQueue).submitTask(anyString(), any(), any());
    }

    public void testNonMasterDoesNotSubmitTask() {
        MasterServiceTaskQueue<ClusterStateTaskListener> taskQueue = mockTaskQueue();
        ClusterService clusterService = mockClusterService(taskQueue);
        PrimaryEncryptionKeyService.create(clusterService, DefaultProjectResolver.INSTANCE, mock(FeatureService.class));
        ClusterStateListener listener = captureListener(clusterService);

        DiscoveryNodes nodes = masterNodes(false);
        ClusterState prevState = stateWithoutKey(nodes);
        ClusterState newState = stateWithoutKey(nodes);

        listener.clusterChanged(new ClusterChangedEvent("test", newState, prevState));

        verify(taskQueue, never()).submitTask(anyString(), any(), any());
    }

    public void testMasterDoesNotSubmitTaskWhenFeatureMissing() {
        MasterServiceTaskQueue<ClusterStateTaskListener> taskQueue = mockTaskQueue();
        ClusterService clusterService = mockClusterService(taskQueue);
        FeatureService featureService = mock(FeatureService.class);
        when(featureService.clusterHasFeature(any(), any())).thenReturn(false);

        PrimaryEncryptionKeyService.create(clusterService, DefaultProjectResolver.INSTANCE, featureService);
        ClusterStateListener listener = captureListener(clusterService);

        DiscoveryNodes nodes = masterNodes(true);
        ClusterState prevState = stateWithoutKey(nodes);
        ClusterState newState = stateWithoutKey(nodes);

        listener.clusterChanged(new ClusterChangedEvent("test", newState, prevState));

        verify(taskQueue, never()).submitTask(anyString(), any(), any());
    }

    public void testMasterDoesNotSubmitTaskWhenKeyAlreadyCached() {
        MasterServiceTaskQueue<ClusterStateTaskListener> taskQueue = mockTaskQueue();
        ClusterService clusterService = mockClusterService(taskQueue);
        FeatureService featureService = mock(FeatureService.class);
        when(featureService.clusterHasFeature(any(), any())).thenReturn(true);

        PrimaryEncryptionKeyService service = PrimaryEncryptionKeyService.create(
            clusterService,
            DefaultProjectResolver.INSTANCE,
            featureService
        );
        ClusterStateListener listener = captureListener(clusterService);

        PrimaryEncryptionKeyMetadata pek = randomPekMetadata();
        DiscoveryNodes nodes = masterNodes(true);

        // First event: key appears, populates cache
        ClusterState prevState = stateWithoutKey(nodes);
        ClusterState stateWithKey = stateWithKey(pek, nodes);
        listener.clusterChanged(new ClusterChangedEvent("test", stateWithKey, prevState));
        assertNotNull(service.getActiveKey());

        // Second event: no metadata change, master with cached key should not submit
        ClusterState nextState = ClusterState.builder(stateWithKey).build();
        listener.clusterChanged(new ClusterChangedEvent("test", nextState, stateWithKey));

        verify(taskQueue, never()).submitTask(anyString(), any(), any());
    }
}
