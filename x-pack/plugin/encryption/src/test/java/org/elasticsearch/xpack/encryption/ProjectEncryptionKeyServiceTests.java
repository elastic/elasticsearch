/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.encryption;

import org.apache.logging.log4j.Level;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
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

import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import javax.crypto.SecretKey;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ProjectEncryptionKeyServiceTests extends ESTestCase {

    private static final ClusterName CLUSTER_NAME = new ClusterName("test");
    private static final String PASSWORD_ID = "v1";
    private static final ProjectEncryptionKeyMetadata.PekEncryption NO_OP_ENCRYPTION = TestPekEncryption.NO_OP;

    private final AtomicReference<ClusterState> visibleState = new AtomicReference<>(stateWithoutKey());

    /** Mocked so tests can swap the visible state directly; the real service is used in {@link #testKeyVisibleToClusterStateListener}. */
    private ClusterService mockClusterService() {
        ClusterService cs = mock(ClusterService.class);
        ClusterSettings clusterSettings = new ClusterSettings(
            Settings.EMPTY,
            Set.of(ProjectEncryptionKeyPasswordSettings.ENCRYPTION_REQUIRED)
        );
        when(cs.getClusterSettings()).thenReturn(clusterSettings);
        when(cs.state()).thenAnswer(invocation -> visibleState.get());
        return cs;
    }

    private ProjectEncryptionKeyService createService(Settings settings) {
        return ProjectEncryptionKeyService.create(mockClusterService(), DefaultProjectResolver.INSTANCE, () -> settings);
    }

    private static ProjectEncryptionKeyMetadata.KeyEntry randomKeyEntry(long generatedAt) {
        byte[] plaintextKey = new byte[PasswordBasedEncryption.PEK_LENGTH_BYTES];
        random().nextBytes(plaintextKey);
        return new ProjectEncryptionKeyMetadata.KeyEntry(plaintextKey, generatedAt);
    }

    private static ProjectEncryptionKeyMetadata plaintextPekMetadata(String passwordId) {
        String keyId = ProjectEncryptionKeyMetadata.generateKeyId();
        return new ProjectEncryptionKeyMetadata(Map.of(keyId, randomKeyEntry(0L)), keyId, passwordId, Map.of(), NO_OP_ENCRYPTION);
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

    public void testNoKeyWhenMetadataAbsent() {
        ProjectEncryptionKeyService service = createService(Settings.EMPTY);
        assertNull(service.getActiveKey());
        assertNull(service.getKey("anything"));
    }

    public void testKeysReadFromVisibleState() {
        ProjectEncryptionKeyService service = createService(Settings.EMPTY);
        ProjectEncryptionKeyMetadata pek = plaintextPekMetadata(PASSWORD_ID);
        visibleState.set(stateWithKey(pek));

        AesGcmEncryptionService.ActiveKey activeKey = service.getActiveKey();
        assertNotNull(activeKey);
        assertEquals(pek.getActiveKeyId(), activeKey.keyId());
        assertEquals("AES", activeKey.key().getAlgorithm());
        assertArrayEquals(pek.getKeys().get(pek.getActiveKeyId()).bytes(), activeKey.key().getEncoded());
        assertEquals(activeKey.key(), service.getKey(pek.getActiveKeyId()));
        assertNull(service.getKey("nonexistent"));
    }

    public void testRotationAndRetirementVisibleImmediately() {
        ProjectEncryptionKeyService service = createService(Settings.EMPTY);
        ProjectEncryptionKeyMetadata pek = plaintextPekMetadata(PASSWORD_ID);
        String oldKeyId = pek.getActiveKeyId();
        visibleState.set(stateWithKey(pek));
        assertEquals(oldKeyId, service.getActiveKey().keyId());

        String newKeyId = ProjectEncryptionKeyMetadata.generateKeyId();
        ProjectEncryptionKeyMetadata rotated = new ProjectEncryptionKeyMetadata(
            Map.of(oldKeyId, pek.getKeys().get(oldKeyId), newKeyId, randomKeyEntry(1L)),
            newKeyId,
            PASSWORD_ID,
            Map.of(),
            NO_OP_ENCRYPTION
        );
        visibleState.set(stateWithKey(rotated));
        assertEquals(newKeyId, service.getActiveKey().keyId());
        assertNotNull(service.getKey(oldKeyId));

        ProjectEncryptionKeyMetadata retired = new ProjectEncryptionKeyMetadata(
            Map.of(newKeyId, rotated.getKeys().get(newKeyId)),
            newKeyId,
            PASSWORD_ID,
            Map.of(),
            NO_OP_ENCRYPTION
        );
        visibleState.set(stateWithKey(retired));
        assertNull(service.getKey(oldKeyId));
        assertNotNull(service.getKey(newKeyId));
    }

    public void testKeysHiddenUntilStateRecovered() {
        ProjectEncryptionKeyService service = createService(Settings.EMPTY);
        ProjectEncryptionKeyMetadata pek = plaintextPekMetadata(PASSWORD_ID);
        visibleState.set(
            ClusterState.builder(stateWithKey(pek))
                .blocks(ClusterBlocks.builder().addGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK).build())
                .build()
        );

        assertNull(service.getActiveKey());
        assertNull(service.getKey(pek.getActiveKeyId()));
        assertEquals(EncryptionServiceState.DISABLED, service.state());
    }

    public void testKeysGoneWhenMetadataRemoved() {
        ProjectEncryptionKeyService service = createService(Settings.EMPTY);
        visibleState.set(stateWithKey(plaintextPekMetadata(PASSWORD_ID)));
        assertNotNull(service.getActiveKey());

        visibleState.set(stateWithoutKey());
        assertNull(service.getActiveKey());
    }

    public void testDegradedMetadataIsUnavailable() {
        ProjectEncryptionKeyService service = createService(Settings.EMPTY);
        String keyId = ProjectEncryptionKeyMetadata.generateKeyId();
        ProjectEncryptionKeyMetadata degraded = ProjectEncryptionKeyMetadata.degraded(
            keyId,
            PASSWORD_ID,
            NO_OP_ENCRYPTION,
            "wrong password",
            new ProjectEncryptionKeyMetadata.DegradedBlobHolder()
        );
        visibleState.set(stateWithKey(degraded));

        MockLog.assertThatLogger(() -> {
            assertEquals(EncryptionServiceState.UNAVAILABLE_DECRYPTION_FAILED, service.state());
            assertNull(service.getActiveKey());
            assertNull(service.getKey(keyId));
        },
            ProjectEncryptionKeyService.class,
            new MockLog.UnseenEventExpectation("no missing-key warning", ProjectEncryptionKeyService.class.getName(), Level.WARN, "*")
        );
    }

    public void testStateDisabledWhenNoPekAndNoPassword() {
        assertEquals(EncryptionServiceState.DISABLED, createService(Settings.EMPTY).state());
    }

    public void testStateReadyWhenNoPekButPasswordConfigured() {
        MockSecureSettings secure = new MockSecureSettings();
        secure.setString(ProjectEncryptionKeyPasswordSettings.ACTIVE_PASSWORD_ID_KEY, PASSWORD_ID);
        secure.setString(ProjectEncryptionKeyPasswordSettings.PASSWORD_PREFIX + PASSWORD_ID, "some-password");
        ProjectEncryptionKeyService service = createService(Settings.builder().setSecureSettings(secure).build());
        // Password configured but no PEK installed yet — coordinator will install it shortly.
        assertEquals(EncryptionServiceState.READY, service.state());
    }

    public void testStateReadyWhenPekInstalled() {
        ProjectEncryptionKeyService service = createService(Settings.EMPTY);
        visibleState.set(stateWithKey(plaintextPekMetadata(PASSWORD_ID)));
        // Keys are plaintext in memory; READY regardless of whether a password is configured.
        assertEquals(EncryptionServiceState.READY, service.state());
    }

    /**
     * A {@link org.elasticsearch.cluster.ClusterStateListener} that sees a newly installed key in {@link ClusterService#state()} must be
     * able to use it.
     */
    public void testKeyVisibleToClusterStateListener() {
        ThreadPool threadPool = new TestThreadPool(getTestName());
        ClusterSettings clusterSettings = new ClusterSettings(
            Settings.EMPTY,
            Sets.addToCopy(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS, ProjectEncryptionKeyPasswordSettings.ENCRYPTION_REQUIRED)
        );
        try (ClusterService clusterService = ClusterServiceUtils.createClusterService(threadPool, clusterSettings)) {
            AtomicReference<String> observedKeyId = new AtomicReference<>();
            AtomicReference<SecretKey> resolvedKey = new AtomicReference<>();
            ProjectEncryptionKeyService service = ProjectEncryptionKeyService.create(
                clusterService,
                DefaultProjectResolver.INSTANCE,
                () -> Settings.EMPTY
            );
            clusterService.addListener(event -> {
                ProjectEncryptionKeyMetadata visible = clusterService.state()
                    .metadata()
                    .getProject(Metadata.DEFAULT_PROJECT_ID)
                    .custom(ProjectEncryptionKeyMetadata.TYPE);
                if (visible != null) {
                    observedKeyId.set(visible.getActiveKeyId());
                    resolvedKey.set(service.getKey(visible.getActiveKeyId()));
                }
            });

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
            assertNotNull(resolvedKey.get());
            assertEquals(service.getKey(pek.getActiveKeyId()), resolvedKey.get());
        } finally {
            terminate(threadPool);
        }
    }

    public void testWarnsOnceWhenKeyNotInClusterState() {
        ProjectEncryptionKeyService service = createService(Settings.EMPTY);
        ProjectEncryptionKeyMetadata pek = plaintextPekMetadata(PASSWORD_ID);
        visibleState.set(stateWithKey(pek));

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
        ProjectEncryptionKeyService service = createService(Settings.EMPTY);
        MockLog.assertThatLogger(() -> {
            assertNull(service.getActiveKey());
            assertNull(service.getKey(ProjectEncryptionKeyMetadata.generateKeyId()));
        },
            ProjectEncryptionKeyService.class,
            new MockLog.UnseenEventExpectation("no warning", ProjectEncryptionKeyService.class.getName(), Level.WARN, "*")
        );
    }

    public void testSettingsChangeDoesNotAffectKeys() {
        MockSecureSettings secure = new MockSecureSettings();
        secure.setString(ProjectEncryptionKeyPasswordSettings.ACTIVE_PASSWORD_ID_KEY, PASSWORD_ID);
        secure.setString(ProjectEncryptionKeyPasswordSettings.PASSWORD_PREFIX + PASSWORD_ID, "some-password");
        AtomicReference<Settings> settingsRef = new AtomicReference<>(Settings.builder().setSecureSettings(secure).build());
        ProjectEncryptionKeyService service = ProjectEncryptionKeyService.create(
            mockClusterService(),
            DefaultProjectResolver.INSTANCE,
            settingsRef::get
        );
        visibleState.set(stateWithKey(plaintextPekMetadata(PASSWORD_ID)));
        assertNotNull(service.getActiveKey());

        settingsRef.set(Settings.EMPTY);
        assertNotNull("keys are plaintext in cluster state, so no password is needed to use them", service.getActiveKey());
    }
}
