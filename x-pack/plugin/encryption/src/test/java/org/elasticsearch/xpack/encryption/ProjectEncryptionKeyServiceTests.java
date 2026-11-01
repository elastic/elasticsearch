/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.encryption;

import org.apache.logging.log4j.Level;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateApplier;
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
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.encryption.spi.EncryptionServiceState;
import org.mockito.ArgumentCaptor;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import javax.crypto.SecretKey;

import static org.hamcrest.Matchers.containsString;
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
        when(cs.state()).thenReturn(stateWithoutKey());
        return cs;
    }

    private static ClusterStateApplier captureApplier(ClusterService clusterService) {
        ArgumentCaptor<ClusterStateApplier> captor = ArgumentCaptor.forClass(ClusterStateApplier.class);
        verify(clusterService).addStateApplier(captor.capture());
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
        ClusterStateApplier applier = captureApplier(clusterService);

        ProjectEncryptionKeyMetadata pek = plaintextPekMetadata(PASSWORD_ID);
        applier.applyClusterState(new ClusterChangedEvent("test", stateWithKey(pek), stateWithoutKey()));

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
        ClusterStateApplier applier = captureApplier(clusterService);

        ClusterState blockedState = ClusterState.builder(stateWithKey(plaintextPekMetadata(PASSWORD_ID)))
            .blocks(ClusterBlocks.builder().addGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK).build())
            .build();
        applier.applyClusterState(new ClusterChangedEvent("test", blockedState, stateWithoutKey()));

        assertNull(service.getActiveKey());
    }

    public void testCacheClearedWhenMetadataRemoved() {
        ClusterService clusterService = mockClusterService();
        ProjectEncryptionKeyService service = ProjectEncryptionKeyService.create(
            clusterService,
            DefaultProjectResolver.INSTANCE,
            () -> Settings.EMPTY
        );
        ClusterStateApplier applier = captureApplier(clusterService);

        ProjectEncryptionKeyMetadata pek = plaintextPekMetadata(PASSWORD_ID);
        ClusterState withKey = stateWithKey(pek);
        applier.applyClusterState(new ClusterChangedEvent("test", withKey, stateWithoutKey()));
        assertNotNull(service.getActiveKey());

        applier.applyClusterState(new ClusterChangedEvent("test", stateWithoutKey(), withKey));
        assertNull(service.getActiveKey());
    }

    public void testKeyLoadFailureDegradesInsteadOfThrowing() {
        ClusterService clusterService = mockClusterService();
        ProjectEncryptionKeyService service = ProjectEncryptionKeyService.create(
            clusterService,
            DefaultProjectResolver.INSTANCE,
            () -> Settings.EMPTY
        );
        ClusterStateApplier applier = captureApplier(clusterService);

        String keyId = ProjectEncryptionKeyMetadata.generateKeyId();
        ProjectEncryptionKeyMetadata invalidPek = new ProjectEncryptionKeyMetadata(
            Map.of(keyId, new ProjectEncryptionKeyMetadata.KeyEntry(new byte[0], 0L)),
            keyId,
            PASSWORD_ID,
            Map.of(),
            NO_OP_ENCRYPTION
        );
        ClusterState invalidState = stateWithKey(invalidPek);
        MockLog.assertThatLogger(
            () -> applier.applyClusterState(new ClusterChangedEvent("test", invalidState, stateWithoutKey())),
            ProjectEncryptionKeyService.class,
            new MockLog.SeenEventExpectation(
                "load failure",
                ProjectEncryptionKeyService.class.getName(),
                Level.WARN,
                "project encryption key: failed to load key from cluster state*"
            )
        );
        when(clusterService.state()).thenReturn(invalidState);
        assertEquals(EncryptionServiceState.UNAVAILABLE_KEY_LOAD_FAILED, service.state());
        assertNull(service.getActiveKey());
        assertNull(service.getKey(keyId));

        ProjectEncryptionKeyMetadata pek = plaintextPekMetadata(PASSWORD_ID);
        applier.applyClusterState(new ClusterChangedEvent("test", stateWithKey(pek), invalidState));
        assertEquals(EncryptionServiceState.READY, service.state());
        assertNotNull(service.getActiveKey());
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
        ClusterStateApplier applier = captureApplier(clusterService);

        ProjectEncryptionKeyMetadata pek = plaintextPekMetadata(PASSWORD_ID);
        applier.applyClusterState(new ClusterChangedEvent("test", stateWithKey(pek), stateWithoutKey()));

        // Keys are plaintext in memory; READY regardless of whether a password is configured.
        assertEquals(EncryptionServiceState.READY, service.state());
    }

    public void testKeysAvailableBeforeStateIsVisible() {
        ThreadPool threadPool = new TestThreadPool(getTestName());
        ClusterSettings clusterSettings = new ClusterSettings(
            Settings.EMPTY,
            Sets.addToCopy(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS, ProjectEncryptionKeyPasswordSettings.ENCRYPTION_REQUIRED)
        );
        try (ClusterService clusterService = ClusterServiceUtils.createClusterService(threadPool, clusterSettings)) {
            AtomicReference<String> observedKeyId = new AtomicReference<>();
            AtomicReference<SecretKey> resolvedKey = new AtomicReference<>();
            AtomicReference<ProjectEncryptionKeyService> serviceRef = new AtomicReference<>();
            clusterService.addListener(event -> {
                ProjectEncryptionKeyMetadata visible = clusterService.state()
                    .metadata()
                    .getProject(Metadata.DEFAULT_PROJECT_ID)
                    .custom(ProjectEncryptionKeyMetadata.TYPE);
                if (visible != null) {
                    observedKeyId.set(visible.getActiveKeyId());
                    resolvedKey.set(serviceRef.get().getKey(visible.getActiveKeyId()));
                }
            });
            ProjectEncryptionKeyService service = ProjectEncryptionKeyService.create(
                clusterService,
                DefaultProjectResolver.INSTANCE,
                () -> Settings.EMPTY
            );
            serviceRef.set(service);

            ProjectEncryptionKeyMetadata pek = plaintextPekMetadata(PASSWORD_ID);
            ClusterState current = clusterService.state();
            ClusterServiceUtils.setState(
                clusterService,
                ClusterState.builder(current)
                    .metadata(
                        Metadata.builder(current.metadata())
                            .put(ProjectMetadata.builder(Metadata.DEFAULT_PROJECT_ID).putCustom(ProjectEncryptionKeyMetadata.TYPE, pek))
                    )
            );

            assertEquals(pek.getActiveKeyId(), observedKeyId.get());
            assertNotNull("key visible in cluster state before the key cache was updated", resolvedKey.get());
            assertEquals(service.getKey(pek.getActiveKeyId()), resolvedKey.get());
            service.close();
        } finally {
            terminate(threadPool);
        }
    }

    public void testCacheBehindVisibleStateTripsAssertion() {
        ClusterService clusterService = mockClusterService();
        ProjectEncryptionKeyService service = ProjectEncryptionKeyService.create(
            clusterService,
            DefaultProjectResolver.INSTANCE,
            () -> Settings.EMPTY
        );
        ProjectEncryptionKeyMetadata pek = plaintextPekMetadata(PASSWORD_ID);
        when(clusterService.state()).thenReturn(stateWithKey(pek));

        AssertionError e = expectThrows(AssertionError.class, () -> service.getKey(pek.getActiveKeyId()));
        assertThat(e.getMessage(), containsString("is in cluster state but not cached"));
        expectThrows(AssertionError.class, service::getActiveKey);
    }

    public void testKeyInstalledDuringMissDoesNotTripAssertion() {
        ClusterService clusterService = mockClusterService();
        ProjectEncryptionKeyService service = ProjectEncryptionKeyService.create(
            clusterService,
            DefaultProjectResolver.INSTANCE,
            () -> Settings.EMPTY
        );
        ClusterStateApplier applier = captureApplier(clusterService);
        ProjectEncryptionKeyMetadata pek = plaintextPekMetadata(PASSWORD_ID);
        ClusterState withKey = ClusterState.builder(stateWithKey(pek)).version(randomLongBetween(1, 100)).build();
        // the key is installed and published after the caller has read the (empty) cache but before the miss is checked
        when(clusterService.state()).thenAnswer(invocation -> {
            applier.applyClusterState(new ClusterChangedEvent("test", withKey, stateWithoutKey()));
            return withKey;
        });

        if (randomBoolean()) {
            assertNull(service.getActiveKey());
        } else {
            assertNull(service.getKey(pek.getActiveKeyId()));
        }
    }

    public void testWarnsOnceWhenKeyNotInClusterState() {
        ClusterService clusterService = mockClusterService();
        ProjectEncryptionKeyService service = ProjectEncryptionKeyService.create(
            clusterService,
            DefaultProjectResolver.INSTANCE,
            () -> Settings.EMPTY
        );
        ClusterStateApplier applier = captureApplier(clusterService);
        ProjectEncryptionKeyMetadata pek = plaintextPekMetadata(PASSWORD_ID);
        ClusterState withKey = stateWithKey(pek);
        applier.applyClusterState(new ClusterChangedEvent("test", withKey, stateWithoutKey()));
        when(clusterService.state()).thenReturn(withKey);

        String missingKeyId = ProjectEncryptionKeyMetadata.generateKeyId();
        MockLog.assertThatLogger(
            () -> assertNull(service.getKey(missingKeyId)),
            ProjectEncryptionKeyService.class,
            new MockLog.SeenEventExpectation(
                "missing key",
                ProjectEncryptionKeyService.class.getName(),
                Level.WARN,
                "project encryption key [" + missingKeyId + "] is not in cluster state*active key is [" + pek.getActiveKeyId() + "]*"
            )
        );
        MockLog.assertThatLogger(
            () -> assertNull(service.getKey(missingKeyId)),
            ProjectEncryptionKeyService.class,
            new MockLog.UnseenEventExpectation("no repeat", ProjectEncryptionKeyService.class.getName(), Level.WARN, "*")
        );
    }

    public void testNoWarningWhileNoKeyInstalled() {
        ClusterService clusterService = mockClusterService();
        ProjectEncryptionKeyService service = ProjectEncryptionKeyService.create(
            clusterService,
            DefaultProjectResolver.INSTANCE,
            () -> Settings.EMPTY
        );
        MockLog.assertThatLogger(() -> {
            assertNull(service.getActiveKey());
            assertNull(service.getKey(ProjectEncryptionKeyMetadata.generateKeyId()));
        },
            ProjectEncryptionKeyService.class,
            new MockLog.UnseenEventExpectation("no warning", ProjectEncryptionKeyService.class.getName(), Level.WARN, "*")
        );
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
        ClusterStateApplier applier = captureApplier(clusterService);

        ProjectEncryptionKeyMetadata pek = plaintextPekMetadata(PASSWORD_ID);
        applier.applyClusterState(new ClusterChangedEvent("test", stateWithKey(pek), stateWithoutKey()));
        assertNotNull(service.getActiveKey());

        // Simulate a secure-settings reload — keys are plaintext, no password needed, cache is unaffected.
        settingsRef.set(Settings.EMPTY);
        assertNotNull("key cache must survive a password settings change since keys are plaintext", service.getActiveKey());
    }
}
