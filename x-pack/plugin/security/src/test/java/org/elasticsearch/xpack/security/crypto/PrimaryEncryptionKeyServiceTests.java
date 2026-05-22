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
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.project.DefaultProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.test.ESTestCase;
import org.mockito.ArgumentCaptor;

import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class PrimaryEncryptionKeyServiceTests extends ESTestCase {

    private static final ClusterName CLUSTER_NAME = new ClusterName("test");

    private static ClusterStateListener captureListener(ClusterService clusterService) {
        ArgumentCaptor<ClusterStateListener> captor = ArgumentCaptor.forClass(ClusterStateListener.class);
        verify(clusterService).addListener(captor.capture());
        return captor.getValue();
    }

    private static PrimaryEncryptionKeyMetadata randomPekMetadata() {
        byte[] keyBytes = new byte[32];
        random().nextBytes(keyBytes);
        String keyId = PrimaryEncryptionKeyMetadata.generateKeyId();
        return new PrimaryEncryptionKeyMetadata(Map.of(keyId, new PrimaryEncryptionKeyMetadata.KeyEntry(keyBytes, 0L)), keyId);
    }

    private static DiscoveryNodes nodes() {
        DiscoveryNode localNode = DiscoveryNodeUtils.create("local");
        DiscoveryNodes.Builder builder = DiscoveryNodes.builder().add(localNode).localNodeId("local").masterNodeId("local");
        return builder.build();
    }

    private static ClusterState stateWithKey(PrimaryEncryptionKeyMetadata pek) {
        ProjectMetadata project = ProjectMetadata.builder(Metadata.DEFAULT_PROJECT_ID)
            .putCustom(PrimaryEncryptionKeyMetadata.TYPE, pek)
            .build();
        return ClusterState.builder(CLUSTER_NAME).metadata(Metadata.builder().put(project).build()).nodes(nodes()).build();
    }

    private static ClusterState stateWithoutKey() {
        return ClusterState.builder(CLUSTER_NAME).nodes(nodes()).build();
    }

    public void testGetActiveKeyReturnsNullInitially() {
        ClusterService clusterService = mock(ClusterService.class);
        PrimaryEncryptionKeyService service = PrimaryEncryptionKeyService.create(clusterService, DefaultProjectResolver.INSTANCE);
        assertNull(service.getActiveKey());
        assertNull(service.getKey("anything"));
    }

    public void testServiceRegistersAsClusterStateListener() {
        ClusterService clusterService = mock(ClusterService.class);
        PrimaryEncryptionKeyService.create(clusterService, DefaultProjectResolver.INSTANCE);
        captureListener(clusterService);
    }

    public void testCacheUpdatedOnMetadataChange() {
        ClusterService clusterService = mock(ClusterService.class);
        PrimaryEncryptionKeyService service = PrimaryEncryptionKeyService.create(clusterService, DefaultProjectResolver.INSTANCE);
        ClusterStateListener listener = captureListener(clusterService);

        PrimaryEncryptionKeyMetadata pek = randomPekMetadata();
        listener.clusterChanged(new ClusterChangedEvent("test", stateWithKey(pek), stateWithoutKey()));

        AesGcmEncryptionService.ActiveKey activeKey = service.getActiveKey();
        assertNotNull(activeKey);
        assertEquals(pek.getActiveKeyId(), activeKey.keyId());
        assertEquals("AES", activeKey.key().getAlgorithm());
        assertNotNull(service.getKey(pek.getActiveKeyId()));
        assertNull(service.getKey("nonexistent"));
    }

    public void testGatewayBlockSkipsCacheUpdate() {
        ClusterService clusterService = mock(ClusterService.class);
        PrimaryEncryptionKeyService service = PrimaryEncryptionKeyService.create(clusterService, DefaultProjectResolver.INSTANCE);
        ClusterStateListener listener = captureListener(clusterService);

        ClusterState blockedState = ClusterState.builder(stateWithKey(randomPekMetadata()))
            .blocks(ClusterBlocks.builder().addGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK).build())
            .build();
        listener.clusterChanged(new ClusterChangedEvent("test", blockedState, stateWithoutKey()));

        assertNull(service.getActiveKey());
    }

    public void testCacheClearedWhenMetadataRemoved() {
        ClusterService clusterService = mock(ClusterService.class);
        PrimaryEncryptionKeyService service = PrimaryEncryptionKeyService.create(clusterService, DefaultProjectResolver.INSTANCE);
        ClusterStateListener listener = captureListener(clusterService);

        PrimaryEncryptionKeyMetadata pek = randomPekMetadata();
        ClusterState withKey = stateWithKey(pek);
        listener.clusterChanged(new ClusterChangedEvent("test", withKey, stateWithoutKey()));
        assertNotNull(service.getActiveKey());

        listener.clusterChanged(new ClusterChangedEvent("test", stateWithoutKey(), withKey));
        assertNull(service.getActiveKey());
    }
}
