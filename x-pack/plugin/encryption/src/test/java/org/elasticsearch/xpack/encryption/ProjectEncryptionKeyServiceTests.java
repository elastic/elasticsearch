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
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.encryption.spi.EncryptedData;
import org.mockito.ArgumentCaptor;

import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class ProjectEncryptionKeyServiceTests extends ESTestCase {

    private static final ClusterName CLUSTER_NAME = new ClusterName("test");
    private static final String PASSWORD_ID = "v1";
    private static final String PASSWORD_VALUE = "test-password-fips";

    private static ClusterStateListener captureListener(ClusterService clusterService) {
        ArgumentCaptor<ClusterStateListener> captor = ArgumentCaptor.forClass(ClusterStateListener.class);
        verify(clusterService).addListener(captor.capture());
        return captor.getValue();
    }

    private static Settings settingsWithPassword(String passwordId, String password) {
        MockSecureSettings secure = new MockSecureSettings();
        secure.setString(ProjectEncryptionKeyPasswordSettings.ACTIVE_PASSWORD_ID_KEY, passwordId);
        secure.setString(ProjectEncryptionKeyPasswordSettings.PASSWORD_PREFIX + passwordId, password);
        return Settings.builder().setSecureSettings(secure).build();
    }

    /**
     * Builds a metadata instance whose entries are real PasswordBasedEncryption wraps of a freshly generated PEK under
     * {@code passwordId} / {@code password}, so the service's lazy-unwrap path has something it can actually decrypt.
     */
    private static ProjectEncryptionKeyMetadata wrappedPekMetadata(String passwordId, String password) {
        byte[] plaintextKey = new byte[PasswordBasedEncryption.PEK_LENGTH_BYTES];
        random().nextBytes(plaintextKey);
        EncryptedData wrapped = PasswordBasedEncryption.wrap(plaintextKey, passwordId, password.toCharArray());
        String keyId = ProjectEncryptionKeyMetadata.generateKeyId();
        return new ProjectEncryptionKeyMetadata(
            Map.of(keyId, new ProjectEncryptionKeyMetadata.KeyEntry(wrapped.payload(), 0L)),
            keyId,
            passwordId
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
        ClusterService clusterService = mock(ClusterService.class);
        ProjectEncryptionKeyService service = ProjectEncryptionKeyService.create(
            clusterService,
            DefaultProjectResolver.INSTANCE,
            Settings.EMPTY
        );
        assertNull(service.getActiveKey());
        assertNull(service.getKey("anything"));
    }

    public void testCacheUpdatedOnMetadataChangeAndLazyUnwrapSucceeds() {
        ClusterService clusterService = mock(ClusterService.class);
        ProjectEncryptionKeyService service = ProjectEncryptionKeyService.create(
            clusterService,
            DefaultProjectResolver.INSTANCE,
            settingsWithPassword(PASSWORD_ID, PASSWORD_VALUE)
        );
        ClusterStateListener listener = captureListener(clusterService);

        ProjectEncryptionKeyMetadata pek = wrappedPekMetadata(PASSWORD_ID, PASSWORD_VALUE);
        listener.clusterChanged(new ClusterChangedEvent("test", stateWithKey(pek), stateWithoutKey()));

        AesGcmEncryptionService.ActiveKey activeKey = service.getActiveKey();
        assertNotNull("active key should be lazily unwrapped under the configured password", activeKey);
        assertEquals(pek.getActiveKeyId(), activeKey.keyId());
        assertEquals("AES", activeKey.key().getAlgorithm());
        assertNotNull(service.getKey(pek.getActiveKeyId()));
        assertNull(service.getKey("nonexistent"));
    }

    public void testGetActiveKeyReturnsNullWhenPasswordMissing() {
        ClusterService clusterService = mock(ClusterService.class);
        ProjectEncryptionKeyService service = ProjectEncryptionKeyService.create(
            clusterService,
            DefaultProjectResolver.INSTANCE,
            Settings.EMPTY // no password configured
        );
        ClusterStateListener listener = captureListener(clusterService);

        ProjectEncryptionKeyMetadata pek = wrappedPekMetadata(PASSWORD_ID, PASSWORD_VALUE);
        listener.clusterChanged(new ClusterChangedEvent("test", stateWithKey(pek), stateWithoutKey()));

        assertNull("getActiveKey should return null when the matching password is not configured", service.getActiveKey());
    }

    public void testGetActiveKeyReturnsNullWhenWrongPassword() {
        ClusterService clusterService = mock(ClusterService.class);
        ProjectEncryptionKeyService service = ProjectEncryptionKeyService.create(
            clusterService,
            DefaultProjectResolver.INSTANCE,
            settingsWithPassword(PASSWORD_ID, "wrong-password")
        );
        ClusterStateListener listener = captureListener(clusterService);

        ProjectEncryptionKeyMetadata pek = wrappedPekMetadata(PASSWORD_ID, PASSWORD_VALUE);
        listener.clusterChanged(new ClusterChangedEvent("test", stateWithKey(pek), stateWithoutKey()));

        assertNull("unwrap with wrong password should not produce a usable key", service.getActiveKey());
    }

    public void testReloadInvalidatesDecryptedCache() {
        ClusterService clusterService = mock(ClusterService.class);
        ProjectEncryptionKeyService service = ProjectEncryptionKeyService.create(
            clusterService,
            DefaultProjectResolver.INSTANCE,
            settingsWithPassword(PASSWORD_ID, "wrong-password")
        );
        ClusterStateListener listener = captureListener(clusterService);

        ProjectEncryptionKeyMetadata pek = wrappedPekMetadata(PASSWORD_ID, PASSWORD_VALUE);
        listener.clusterChanged(new ClusterChangedEvent("test", stateWithKey(pek), stateWithoutKey()));

        assertNull("wrong password initially", service.getActiveKey());

        service.reload(settingsWithPassword(PASSWORD_ID, PASSWORD_VALUE));
        assertNotNull("after reload with correct password, unwrap should succeed", service.getActiveKey());
    }

    public void testGatewayBlockSkipsCacheUpdate() {
        ClusterService clusterService = mock(ClusterService.class);
        ProjectEncryptionKeyService service = ProjectEncryptionKeyService.create(
            clusterService,
            DefaultProjectResolver.INSTANCE,
            settingsWithPassword(PASSWORD_ID, PASSWORD_VALUE)
        );
        ClusterStateListener listener = captureListener(clusterService);

        ClusterState blockedState = ClusterState.builder(stateWithKey(wrappedPekMetadata(PASSWORD_ID, PASSWORD_VALUE)))
            .blocks(ClusterBlocks.builder().addGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK).build())
            .build();
        listener.clusterChanged(new ClusterChangedEvent("test", blockedState, stateWithoutKey()));

        assertNull(service.getActiveKey());
    }

    public void testCacheClearedWhenMetadataRemoved() {
        ClusterService clusterService = mock(ClusterService.class);
        ProjectEncryptionKeyService service = ProjectEncryptionKeyService.create(
            clusterService,
            DefaultProjectResolver.INSTANCE,
            settingsWithPassword(PASSWORD_ID, PASSWORD_VALUE)
        );
        ClusterStateListener listener = captureListener(clusterService);

        ProjectEncryptionKeyMetadata pek = wrappedPekMetadata(PASSWORD_ID, PASSWORD_VALUE);
        ClusterState withKey = stateWithKey(pek);
        listener.clusterChanged(new ClusterChangedEvent("test", withKey, stateWithoutKey()));
        assertNotNull(service.getActiveKey());

        listener.clusterChanged(new ClusterChangedEvent("test", stateWithoutKey(), withKey));
        assertNull(service.getActiveKey());
    }
}
