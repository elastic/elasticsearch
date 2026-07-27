/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.encryption;

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
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.encryption.spi.EncryptionServiceState;
import org.mockito.ArgumentCaptor;

import java.util.Map;
import java.util.Set;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ProjectEncryptionKeyServiceTests extends ESTestCase {

    private static final ClusterName CLUSTER_NAME = new ClusterName("test");
    private static final String PASSWORD_ID = "v1";
    private static final ProjectEncryptionKeyMetadata.PekEncryption NO_OP_ENCRYPTION = TestPekEncryption.NO_OP;

    private static ClusterService mockClusterService() {
        ClusterService cs = mock(ClusterService.class);
        ClusterSettings clusterSettings = new ClusterSettings(
            Settings.EMPTY,
            Set.of(ProjectEncryptionKeyPasswordSettings.ENCRYPTION_REQUIRED)
        );
        when(cs.getClusterSettings()).thenReturn(clusterSettings);
        return cs;
    }

    private static ClusterStateListener captureListener(ClusterService clusterService) {
        ArgumentCaptor<ClusterStateListener> captor = ArgumentCaptor.forClass(ClusterStateListener.class);
        verify(clusterService).addListener(captor.capture());
        return captor.getValue();
    }

    private static ProjectEncryptionKeyMetadata plaintextPekMetadata(String passwordId) {
        byte[] plaintextKey = new byte[PasswordBasedEncryption.PEK_LENGTH_BYTES];
        random().nextBytes(plaintextKey);
        String keyId = ProjectEncryptionKeyMetadata.generateKeyId();
        return new ProjectEncryptionKeyMetadata(
            Map.of(keyId, new ProjectEncryptionKeyMetadata.KeyEntry(plaintextKey, 0L)),
            keyId,
            passwordId,
            Map.of(),
            NO_OP_ENCRYPTION
        );
    }

    private static DiscoveryNodes nodes() {
        DiscoveryNode localNode = DiscoveryNodeUtils.create("local");
        DiscoveryNodes.Builder builder = DiscoveryNodes.builder().add(localNode).localNodeId("local").masterNodeId("local");
        return builder.build();
    }

    private static ClusterState stateWithKey(ProjectEncryptionKeyMetadata pek) {
        ProjectMetadata project = ProjectMetadata.builder(Metadata.DEFAULT_PROJECT_ID)
            .putCustom(ProjectEncryptionKeyMetadata.TYPE, pek)
            .build();
        return ClusterState.builder(CLUSTER_NAME).metadata(Metadata.builder().put(project).build()).nodes(nodes()).build();
    }

    private static ClusterState stateWithoutKey() {
        return ClusterState.builder(CLUSTER_NAME).nodes(nodes()).build();
    }

    public void testGetActiveKeyReturnsNullInitially() {
        ClusterService clusterService = mockClusterService();
        ProjectEncryptionKeyService service = ProjectEncryptionKeyService.create(
            clusterService,
            DefaultProjectResolver.INSTANCE,
            () -> Settings.EMPTY
        );
        assertNull(service.getActiveKey());
        assertNull(service.getKey("anything"));
    }

    public void testCacheUpdatedOnMetadataChangeAndKeyImmediatelyAvailable() {
        ClusterService clusterService = mockClusterService();
        ProjectEncryptionKeyService service = ProjectEncryptionKeyService.create(
            clusterService,
            DefaultProjectResolver.INSTANCE,
            () -> Settings.EMPTY
        );
        ClusterStateListener listener = captureListener(clusterService);

        ProjectEncryptionKeyMetadata pek = plaintextPekMetadata(PASSWORD_ID);
        listener.clusterChanged(new ClusterChangedEvent("test", stateWithKey(pek), stateWithoutKey()));

        AesGcmEncryptionService.ActiveKey activeKey = service.getActiveKey();
        assertNotNull("active key should be available immediately from plaintext bytes", activeKey);
        assertEquals(pek.getActiveKeyId(), activeKey.keyId());
        assertEquals("AES", activeKey.key().getAlgorithm());
        assertNotNull(service.getKey(pek.getActiveKeyId()));
        assertNull(service.getKey("nonexistent"));
    }

    public void testGatewayBlockSkipsCacheUpdate() {
        ClusterService clusterService = mockClusterService();
        ProjectEncryptionKeyService service = ProjectEncryptionKeyService.create(
            clusterService,
            DefaultProjectResolver.INSTANCE,
            () -> Settings.EMPTY
        );
        ClusterStateListener listener = captureListener(clusterService);

        ClusterState blockedState = ClusterState.builder(stateWithKey(plaintextPekMetadata(PASSWORD_ID)))
            .blocks(ClusterBlocks.builder().addGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK).build())
            .build();
        listener.clusterChanged(new ClusterChangedEvent("test", blockedState, stateWithoutKey()));

        assertNull(service.getActiveKey());
    }

    public void testCacheClearedWhenMetadataRemoved() {
        ClusterService clusterService = mockClusterService();
        ProjectEncryptionKeyService service = ProjectEncryptionKeyService.create(
            clusterService,
            DefaultProjectResolver.INSTANCE,
            () -> Settings.EMPTY
        );
        ClusterStateListener listener = captureListener(clusterService);

        ProjectEncryptionKeyMetadata pek = plaintextPekMetadata(PASSWORD_ID);
        ClusterState withKey = stateWithKey(pek);
        listener.clusterChanged(new ClusterChangedEvent("test", withKey, stateWithoutKey()));
        assertNotNull(service.getActiveKey());

        listener.clusterChanged(new ClusterChangedEvent("test", stateWithoutKey(), withKey));
        assertNull(service.getActiveKey());
    }

    public void testStateDisabledWhenNoPekAndNoPassword() {
        ClusterService clusterService = mockClusterService();
        ProjectEncryptionKeyService service = ProjectEncryptionKeyService.create(
            clusterService,
            DefaultProjectResolver.INSTANCE,
            () -> Settings.EMPTY
        );
        assertEquals(EncryptionServiceState.DISABLED, service.state());
    }

    public void testStateReadyWhenNoPekButPasswordConfigured() {
        ClusterService clusterService = mockClusterService();
        MockSecureSettings secure = new MockSecureSettings();
        secure.setString(ProjectEncryptionKeyPasswordSettings.ACTIVE_PASSWORD_ID_KEY, PASSWORD_ID);
        secure.setString(ProjectEncryptionKeyPasswordSettings.PASSWORD_PREFIX + PASSWORD_ID, "some-password");
        Settings settings = Settings.builder().setSecureSettings(secure).build();
        ProjectEncryptionKeyService service = ProjectEncryptionKeyService.create(
            clusterService,
            DefaultProjectResolver.INSTANCE,
            () -> settings
        );
        // Password configured but no PEK installed yet — coordinator will install it shortly.
        assertEquals(EncryptionServiceState.READY, service.state());
    }

    public void testStateReadyWhenPekInstalled() {
        ClusterService clusterService = mockClusterService();
        ProjectEncryptionKeyService service = ProjectEncryptionKeyService.create(
            clusterService,
            DefaultProjectResolver.INSTANCE,
            () -> Settings.EMPTY
        );
        ClusterStateListener listener = captureListener(clusterService);

        ProjectEncryptionKeyMetadata pek = plaintextPekMetadata(PASSWORD_ID);
        listener.clusterChanged(new ClusterChangedEvent("test", stateWithKey(pek), stateWithoutKey()));

        // Keys are plaintext in memory; READY regardless of whether a password is configured.
        assertEquals(EncryptionServiceState.READY, service.state());
    }

    public void testSettingsChangeDoesNotAffectKeyCache() {
        ClusterService clusterService = mockClusterService();
        java.util.concurrent.atomic.AtomicReference<Settings> settingsRef = new java.util.concurrent.atomic.AtomicReference<>(
            Settings.EMPTY
        );
        ProjectEncryptionKeyService service = ProjectEncryptionKeyService.create(
            clusterService,
            DefaultProjectResolver.INSTANCE,
            settingsRef::get
        );
        ClusterStateListener listener = captureListener(clusterService);

        ProjectEncryptionKeyMetadata pek = plaintextPekMetadata(PASSWORD_ID);
        listener.clusterChanged(new ClusterChangedEvent("test", stateWithKey(pek), stateWithoutKey()));
        assertNotNull(service.getActiveKey());

        // Simulate a secure-settings reload — keys are plaintext, no password needed, cache is unaffected.
        settingsRef.set(Settings.EMPTY);
        assertNotNull("key cache must survive a password settings change since keys are plaintext", service.getActiveKey());
    }
}
