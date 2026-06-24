/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.encryption;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.AbstractNamedDiffable;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.NamedDiff;
import org.elasticsearch.cluster.ProjectState;
import org.elasticsearch.cluster.SimpleBatchedExecutor;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.project.DefaultProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.MasterServiceTaskQueue;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.xcontent.ChunkedToXContentHelper;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.features.FeatureService;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xpack.encryption.ProjectEncryptionKeyMetadata.KeyEntry;
import org.elasticsearch.xpack.encryption.spi.EncryptedDataHandler;
import org.elasticsearch.xpack.encryption.spi.EncryptionService;

import java.io.IOException;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class KeyRotationCoordinatorTests extends ESTestCase {

    private static final ClusterName CLUSTER_NAME = new ClusterName("test");
    private static final String PASSWORD_ID = "v1";
    // BC FIPS rejects PBKDF2 passwords shorter than 14 chars (112 bits); use a longer literal.
    private static final String PASSWORD_VALUE = "test-password-fips";

    private static final ProjectEncryptionKeyMetadata.PekEncryption NO_OP_ENCRYPTION = TestPekEncryption.NO_OP;

    private static byte[] randomKey() {
        byte[] keyBytes = new byte[PasswordBasedEncryption.PEK_LENGTH_BYTES];
        random().nextBytes(keyBytes);
        return keyBytes;
    }

    private static KeyEntry entry(long generatedAt) {
        return new KeyEntry(randomKey(), generatedAt);
    }

    private static Settings settingsWithActivePassword() {
        MockSecureSettings secure = new MockSecureSettings();
        secure.setString(ProjectEncryptionKeyPasswordSettings.ACTIVE_PASSWORD_ID_KEY, KeyRotationCoordinatorTests.PASSWORD_ID);
        secure.setString(ProjectEncryptionKeyPasswordSettings.PASSWORD_PREFIX + KeyRotationCoordinatorTests.PASSWORD_ID, PASSWORD_VALUE);
        return Settings.builder().setSecureSettings(secure).build();
    }

    private static DiscoveryNodes nodes(boolean isLocalMaster) {
        DiscoveryNode local = DiscoveryNodeUtils.create("local");
        DiscoveryNode other = DiscoveryNodeUtils.create("other");
        DiscoveryNodes.Builder builder = DiscoveryNodes.builder().add(local).add(other).localNodeId("local");
        builder.masterNodeId(isLocalMaster ? "local" : "other");
        return builder.build();
    }

    private static ClusterState clusterStateWith(ProjectEncryptionKeyMetadata pek, boolean isLocalMaster) {
        return clusterStateWith(pek, null, isLocalMaster);
    }

    private static ClusterState clusterStateWith(ProjectEncryptionKeyMetadata pek, Metadata.ProjectCustom extra, boolean isLocalMaster) {
        ProjectMetadata.Builder project = ProjectMetadata.builder(Metadata.DEFAULT_PROJECT_ID);
        if (pek != null) {
            project.putCustom(ProjectEncryptionKeyMetadata.TYPE, pek);
        }
        if (extra != null) {
            project.putCustom(extra.getWriteableName(), extra);
        }
        return ClusterState.builder(CLUSTER_NAME)
            .metadata(Metadata.builder().put(project.build()).build())
            .nodes(nodes(isLocalMaster))
            .build();
    }

    private KeyRotationCoordinator coordinator;
    private MasterServiceTaskQueue<KeyRotationCoordinator.KeyRotationTask> taskQueue;
    private AtomicReference<Settings> settingsRef;

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private void setup(
        ClusterState state,
        long now,
        TimeValue rotationInterval,
        TimeValue checkInterval,
        FeatureService featureService,
        List<EncryptedDataHandler<?>> handlers,
        Settings extraSettings
    ) {
        taskQueue = mock(MasterServiceTaskQueue.class);
        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenReturn(state);
        MasterServiceTaskQueue rawQueue = taskQueue;
        when(clusterService.createTaskQueue(anyString(), any(), any())).thenReturn(rawQueue);
        ThreadPool threadPool = mock(ThreadPool.class);
        when(threadPool.absoluteTimeInMillis()).thenReturn(now);
        when(threadPool.generic()).thenReturn(EsExecutors.DIRECT_EXECUTOR_SERVICE);
        EncryptionService encryptionService = mock(EncryptionService.class);
        Settings combined = Settings.builder()
            .put(KeyRotationCoordinator.ROTATION_INTERVAL_SETTING.getKey(), rotationInterval)
            .put(KeyRotationCoordinator.CHECK_INTERVAL_SETTING.getKey(), checkInterval)
            .put(extraSettings)
            .build();
        settingsRef = new AtomicReference<>(combined);
        coordinator = KeyRotationCoordinator.create(
            clusterService,
            threadPool,
            DefaultProjectResolver.INSTANCE,
            featureService,
            encryptionService,
            handlers,
            settingsRef::get,
            NO_OP_ENCRYPTION
        );
    }

    private void setup(
        ClusterState state,
        long now,
        TimeValue rotationInterval,
        TimeValue checkInterval,
        FeatureService featureService,
        List<EncryptedDataHandler<?>> handlers
    ) {
        setup(state, now, rotationInterval, checkInterval, featureService, handlers, settingsWithActivePassword());
    }

    private void setup(ClusterState state, long now, TimeValue rotationInterval, TimeValue checkInterval, FeatureService featureService) {
        setup(state, now, rotationInterval, checkInterval, featureService, List.of());
    }

    private void setup(ClusterState state, long now, TimeValue rotationInterval, TimeValue checkInterval) {
        setup(state, now, rotationInterval, checkInterval, mock(FeatureService.class));
    }

    private void setup(ClusterState state, long now, TimeValue rotationInterval, List<EncryptedDataHandler<?>> handlers) {
        setup(state, now, rotationInterval, TimeValue.timeValueMinutes(1), mock(FeatureService.class), handlers);
    }

    private void setup(ClusterState state, long now, TimeValue rotationInterval) {
        setup(state, now, rotationInterval, TimeValue.timeValueMinutes(1));
    }

    public void testInstallSubmittedWhenMasterWithoutKeyAndFeatureAvailable() {
        FeatureService featureService = mock(FeatureService.class);
        when(featureService.clusterHasFeature(any(), any())).thenReturn(true);
        setup(clusterStateWith(null, true), 0L, TimeValue.timeValueDays(30), TimeValue.timeValueMinutes(1), featureService);

        ClusterState prev = clusterStateWith(null, true);
        ClusterState next = clusterStateWith(null, true);
        coordinator.onClusterStateChanged(new ClusterChangedEvent("test", next, prev));

        verify(taskQueue).submitTask(eq("install-project-encryption-key"), isA(KeyRotationCoordinator.InstallKeyTask.class), any());
    }

    public void testInstallNotSubmittedWhenActivePasswordMissing() {
        FeatureService featureService = mock(FeatureService.class);
        when(featureService.clusterHasFeature(any(), any())).thenReturn(true);
        // Empty extra-settings: no active password id configured.
        setup(
            clusterStateWith(null, true),
            0L,
            TimeValue.timeValueDays(30),
            TimeValue.timeValueMinutes(1),
            featureService,
            List.of(),
            Settings.EMPTY
        );

        coordinator.onClusterStateChanged(new ClusterChangedEvent("test", clusterStateWith(null, true), clusterStateWith(null, true)));

        verify(taskQueue, never()).submitTask(anyString(), any(), any());
    }

    public void testInstallNotSubmittedWhenNotMaster() {
        FeatureService featureService = mock(FeatureService.class);
        when(featureService.clusterHasFeature(any(), any())).thenReturn(true);
        setup(clusterStateWith(null, false), 0L, TimeValue.timeValueDays(30), TimeValue.timeValueMinutes(1), featureService);

        coordinator.onClusterStateChanged(new ClusterChangedEvent("test", clusterStateWith(null, false), clusterStateWith(null, false)));

        verify(taskQueue, never()).submitTask(anyString(), any(), any());
    }

    public void testInstallNotSubmittedWhenFeatureMissing() {
        FeatureService featureService = mock(FeatureService.class);
        when(featureService.clusterHasFeature(any(), any())).thenReturn(false);
        setup(clusterStateWith(null, true), 0L, TimeValue.timeValueDays(30), TimeValue.timeValueMinutes(1), featureService);

        coordinator.onClusterStateChanged(new ClusterChangedEvent("test", clusterStateWith(null, true), clusterStateWith(null, true)));

        verify(taskQueue, never()).submitTask(anyString(), any(), any());
    }

    public void testInstallNotSubmittedWhenKeyAlreadyExists() {
        FeatureService featureService = mock(FeatureService.class);
        when(featureService.clusterHasFeature(any(), any())).thenReturn(true);
        ProjectEncryptionKeyMetadata existing = new ProjectEncryptionKeyMetadata(
            Map.of("k1", entry(0L)),
            "k1",
            PASSWORD_ID,
            Map.of(),
            NO_OP_ENCRYPTION
        );
        setup(clusterStateWith(existing, true), 0L, TimeValue.timeValueDays(30), TimeValue.timeValueMinutes(1), featureService);

        coordinator.onClusterStateChanged(new ClusterChangedEvent("test", clusterStateWith(existing, true), clusterStateWith(null, true)));

        verify(taskQueue, never()).submitTask(anyString(), any(), any());
    }

    public void testPasswordRotationSubmittedWhenActivePasswordIdDiffers() {
        FeatureService featureService = mock(FeatureService.class);
        when(featureService.clusterHasFeature(any(), any())).thenReturn(true);
        MockSecureSettings secure = new MockSecureSettings();
        secure.setString(ProjectEncryptionKeyPasswordSettings.ACTIVE_PASSWORD_ID_KEY, "v2");
        secure.setString(ProjectEncryptionKeyPasswordSettings.PASSWORD_PREFIX + PASSWORD_ID, PASSWORD_VALUE);
        secure.setString(ProjectEncryptionKeyPasswordSettings.PASSWORD_PREFIX + "v2", "new-password-fips");

        ProjectEncryptionKeyMetadata existing = new ProjectEncryptionKeyMetadata(
            Map.of("k1", entry(0L)),
            "k1",
            PASSWORD_ID,
            Map.of(),
            NO_OP_ENCRYPTION
        );
        setup(
            clusterStateWith(existing, true),
            0L,
            TimeValue.timeValueDays(30),
            TimeValue.timeValueMinutes(1),
            featureService,
            List.of(),
            Settings.builder().setSecureSettings(secure).build()
        );

        coordinator.onClusterStateChanged(new ClusterChangedEvent("test", clusterStateWith(existing, true), clusterStateWith(null, true)));

        verify(taskQueue).submitTask(
            eq("update-project-encryption-key-password-id"),
            isA(KeyRotationCoordinator.UpdatePasswordIdTask.class),
            any()
        );
    }

    public void testReloadImmediatelyTriggersInstallWhenActivePasswordActivated() {
        FeatureService featureService = mock(FeatureService.class);
        when(featureService.clusterHasFeature(any(), any())).thenReturn(true);
        // Construct with empty settings: no active password is configured, so the listener path cannot install yet.
        setup(
            clusterStateWith(null, true),
            0L,
            TimeValue.timeValueDays(30),
            TimeValue.timeValueMinutes(1),
            featureService,
            List.of(),
            Settings.EMPTY
        );

        settingsRef.set(settingsWithActivePassword());
        coordinator.reload();

        verify(taskQueue).submitTask(eq("install-project-encryption-key"), isA(KeyRotationCoordinator.InstallKeyTask.class), any());
    }

    public void testReloadImmediatelyTriggersPasswordRotationWhenActivePasswordIdChanges() {
        FeatureService featureService = mock(FeatureService.class);
        when(featureService.clusterHasFeature(any(), any())).thenReturn(true);

        ProjectEncryptionKeyMetadata existing = new ProjectEncryptionKeyMetadata(
            Map.of("k1", entry(0L)),
            "k1",
            PASSWORD_ID,
            Map.of(),
            NO_OP_ENCRYPTION
        );
        setup(clusterStateWith(existing, true), 0L, TimeValue.timeValueDays(30), TimeValue.timeValueMinutes(1), featureService);

        // Reload with v2 active and both passwords available so the rewrap (unwrap-v1 then wrap-v2) can succeed.
        MockSecureSettings rotated = new MockSecureSettings();
        rotated.setString(ProjectEncryptionKeyPasswordSettings.ACTIVE_PASSWORD_ID_KEY, "v2");
        rotated.setString(ProjectEncryptionKeyPasswordSettings.PASSWORD_PREFIX + PASSWORD_ID, PASSWORD_VALUE);
        rotated.setString(ProjectEncryptionKeyPasswordSettings.PASSWORD_PREFIX + "v2", "new-password-fips");
        settingsRef.set(Settings.builder().setSecureSettings(rotated).build());
        coordinator.reload();

        verify(taskQueue).submitTask(
            eq("update-project-encryption-key-password-id"),
            isA(KeyRotationCoordinator.UpdatePasswordIdTask.class),
            any()
        );
    }

    public void testReloadTriggersRotationAfterOnClusterStateChangedFiredWithStaleSettings() {
        FeatureService featureService = mock(FeatureService.class);
        when(featureService.clusterHasFeature(any(), any())).thenReturn(true);

        ProjectEncryptionKeyMetadata existing = new ProjectEncryptionKeyMetadata(
            Map.of("k1", entry(0L)),
            "k1",
            PASSWORD_ID,
            Map.of(),
            NO_OP_ENCRYPTION
        );
        ClusterState state = clusterStateWith(existing, true);
        // Coordinator starts with v1 as active — matching the metadata, so no rotation yet.
        setup(state, 0L, TimeValue.timeValueDays(30), TimeValue.timeValueMinutes(1), featureService);

        // onClusterStateChanged fires first while the settings supplier still reflects the old active password (v1).
        coordinator.onClusterStateChanged(new ClusterChangedEvent("test", state, clusterStateWith(null, true)));
        verify(taskQueue, never()).submitTask(anyString(), any(), any());

        // reload() now delivers new settings (v2 active). It must immediately detect the mismatch and submit the rotation.
        MockSecureSettings rotated = new MockSecureSettings();
        rotated.setString(ProjectEncryptionKeyPasswordSettings.ACTIVE_PASSWORD_ID_KEY, "v2");
        rotated.setString(ProjectEncryptionKeyPasswordSettings.PASSWORD_PREFIX + PASSWORD_ID, PASSWORD_VALUE);
        rotated.setString(ProjectEncryptionKeyPasswordSettings.PASSWORD_PREFIX + "v2", "new-password-fips");
        settingsRef.set(Settings.builder().setSecureSettings(rotated).build());
        coordinator.reload();

        verify(taskQueue).submitTask(
            eq("update-project-encryption-key-password-id"),
            isA(KeyRotationCoordinator.UpdatePasswordIdTask.class),
            any()
        );
    }

    public void testTickIsNoopWhenRotationDisabled() {
        setup(clusterStateWith(null, true), 0L, TimeValue.ZERO);
        coordinator.tick();
        verify(taskQueue, never()).submitTask(anyString(), any(), any());
    }

    public void testTickIsNoopWhenNotMaster() {
        setup(clusterStateWith(null, false), 0L, TimeValue.timeValueDays(30));
        coordinator.tick();
        verify(taskQueue, never()).submitTask(anyString(), any(), any());
    }

    public void testTickIsNoopWhenNoMetadataAndFeatureUnavailable() {
        setup(clusterStateWith(null, true), 0L, TimeValue.timeValueDays(30));
        coordinator.tick();
        verify(taskQueue, never()).submitTask(anyString(), any(), any());
    }

    public void testTickIsNoopWhenNoMetadataAndActivePasswordMissing() {
        FeatureService featureService = mock(FeatureService.class);
        when(featureService.clusterHasFeature(any(), any())).thenReturn(true);
        // Empty extra-settings: feature is available but no active password is configured, so install must wait.
        setup(
            clusterStateWith(null, true),
            0L,
            TimeValue.timeValueDays(30),
            TimeValue.timeValueMinutes(1),
            featureService,
            List.of(),
            Settings.EMPTY
        );
        coordinator.tick();
        verify(taskQueue, never()).submitTask(anyString(), any(), any());
    }

    public void testTickBeginsRotationWhenActiveKeyIsOldEnough() {
        long generatedAt = 1_000_000_000L;
        long now = generatedAt + TimeValue.timeValueDays(30).millis() + 1;
        ProjectEncryptionKeyMetadata metadata = new ProjectEncryptionKeyMetadata(
            Map.of("k1", entry(generatedAt)),
            "k1",
            PASSWORD_ID,
            Map.of(),
            NO_OP_ENCRYPTION
        );
        setup(clusterStateWith(metadata, true), now, TimeValue.timeValueDays(30));

        coordinator.tick();

        verify(taskQueue).submitTask(
            eq("begin-project-encryption-key-rotation"),
            isA(KeyRotationCoordinator.BeginRotationTask.class),
            any()
        );
    }

    public void testTickDoesNotBeginRotationWhenOneIsAlreadyInFlight() {
        long generatedAt = 1_000_000_000L;
        long now = generatedAt + TimeValue.timeValueDays(30).millis() + 1;
        ProjectEncryptionKeyMetadata metadata = new ProjectEncryptionKeyMetadata(
            Map.of("k1", entry(generatedAt)),
            "k1",
            PASSWORD_ID,
            Map.of(),
            NO_OP_ENCRYPTION
        );
        setup(clusterStateWith(metadata, true), now, TimeValue.timeValueDays(30));

        coordinator.tick();
        coordinator.tick();

        verify(taskQueue).submitTask(
            eq("begin-project-encryption-key-rotation"),
            isA(KeyRotationCoordinator.BeginRotationTask.class),
            any()
        );
    }

    public void testTickDoesNotBeginRotationWhenActiveKeyIsYoung() {
        long generatedAt = 1_000_000_000L;
        long now = generatedAt + 1;
        ProjectEncryptionKeyMetadata metadata = new ProjectEncryptionKeyMetadata(
            Map.of("k1", entry(generatedAt)),
            "k1",
            PASSWORD_ID,
            Map.of(),
            NO_OP_ENCRYPTION
        );
        setup(clusterStateWith(metadata, true), now, TimeValue.timeValueDays(30));

        coordinator.tick();

        verify(taskQueue, never()).submitTask(anyString(), any(), any());
    }

    public void testTickShortCircuitsHandlerAlreadyOnActiveKey() {
        long now = 100L;
        ProjectEncryptionKeyMetadata metadata = new ProjectEncryptionKeyMetadata(
            Map.of("k1", entry(50L)),
            "k1",
            PASSWORD_ID,
            Map.of(TestCustom.TYPE, "k1"),
            NO_OP_ENCRYPTION
        );
        TestCustom seeded = TestCustom.encryptedUnder("k1");
        AtomicInteger calls = new AtomicInteger();
        setup(clusterStateWith(metadata, seeded, true), now, TimeValue.timeValueDays(30), List.of(captureHandler(calls, "k2")));

        coordinator.tick();

        assertEquals("handler must not be invoked when already on active key", 0, calls.get());
        verify(taskQueue, never()).submitTask(anyString(), any(), any());
    }

    public void testTickSubmitsReEncryptApplyTaskWhenHandlerLagsActiveKey() {
        long now = 100L;
        ProjectEncryptionKeyMetadata metadata = new ProjectEncryptionKeyMetadata(
            Map.of("k1", entry(50L)),
            "k1",
            PASSWORD_ID,
            Map.of(),
            NO_OP_ENCRYPTION
        );
        TestCustom seeded = TestCustom.encryptedUnder("old-key");
        AtomicInteger calls = new AtomicInteger();
        setup(clusterStateWith(metadata, seeded, true), now, TimeValue.timeValueDays(30), List.of(captureHandler(calls, "k1")));

        coordinator.tick();

        assertEquals(1, calls.get());
        verify(taskQueue).submitTask(eq("re-encrypt-" + TestCustom.TYPE), isA(KeyRotationCoordinator.ReEncryptApplyTask.class), any());
    }

    public void testHandlerReturningInputSkipsTaskSubmission() {
        long now = 100L;
        ProjectEncryptionKeyMetadata metadata = new ProjectEncryptionKeyMetadata(
            Map.of("k1", entry(50L)),
            "k1",
            PASSWORD_ID,
            Map.of(),
            NO_OP_ENCRYPTION
        );
        TestCustom seeded = TestCustom.encryptedUnder("k1");
        AtomicInteger calls = new AtomicInteger();
        EncryptedDataHandler<TestCustom> identityHandler = new EncryptedDataHandler<>() {
            @Override
            public String customName() {
                return TestCustom.TYPE;
            }

            @Override
            public TestCustom reEncrypt(TestCustom current, EncryptionService svc, String activeKeyId) {
                calls.incrementAndGet();
                return current;
            }
        };
        setup(clusterStateWith(metadata, seeded, true), now, TimeValue.timeValueDays(30), List.of(identityHandler));

        coordinator.tick();

        assertEquals(1, calls.get());
        verify(taskQueue, never()).submitTask(eq("re-encrypt-" + TestCustom.TYPE), any(), any());
    }

    public void testTickSubmitsRetireForKeysOlderThanGrace() {
        TimeValue checkInterval = TimeValue.timeValueMinutes(1);
        long now = 1_000_000L;
        long graceMillis = 10 * checkInterval.millis();
        long activeGeneratedAt = now - 15 * 60 * 1000L;
        long oldGeneratedAt = now - 30 * 60 * 1000L;
        ProjectEncryptionKeyMetadata metadata = new ProjectEncryptionKeyMetadata(
            Map.of("old", entry(oldGeneratedAt), "active", entry(activeGeneratedAt)),
            "active",
            PASSWORD_ID,
            Map.of(),
            NO_OP_ENCRYPTION
        );
        setup(clusterStateWith(metadata, true), now, TimeValue.timeValueDays(30), checkInterval);

        coordinator.tick();

        verify(taskQueue).submitTask(
            eq("retire-project-encryption-keys"),
            eq(new KeyRotationCoordinator.RetireKeysTask(now - graceMillis)),
            any()
        );
    }

    public void testTickDoesNotSubmitRetireWhenAllKeysWithinGrace() {
        TimeValue checkInterval = TimeValue.timeValueMinutes(1);
        long now = 1_000_000L;
        long activeGeneratedAt = now - 60 * 1000L;
        long oldGeneratedAt = now - 5 * 60 * 1000L;

        ProjectEncryptionKeyMetadata metadata = new ProjectEncryptionKeyMetadata(
            Map.of("old", entry(oldGeneratedAt), "active", entry(activeGeneratedAt)),
            "active",
            PASSWORD_ID,
            Map.of(),
            NO_OP_ENCRYPTION
        );
        setup(clusterStateWith(metadata, true), now, TimeValue.timeValueDays(30), checkInterval);

        coordinator.tick();

        verify(taskQueue, never()).submitTask(
            eq("retire-project-encryption-keys"),
            isA(KeyRotationCoordinator.RetireKeysTask.class),
            any()
        );
    }

    public void testRetireGateHonorsHandlerKeyIds() {
        TimeValue checkInterval = TimeValue.timeValueMinutes(1);
        long now = 1_000_000L;
        long activeGeneratedAt = now - 15 * 60 * 1000L;
        long oldGeneratedAt = now - 30 * 60 * 1000L;
        ProjectEncryptionKeyMetadata metadata = new ProjectEncryptionKeyMetadata(
            Map.of("old", entry(oldGeneratedAt), "active", entry(activeGeneratedAt)),
            "active",
            PASSWORD_ID,
            Map.of(TestCustom.TYPE, "old"),
            NO_OP_ENCRYPTION
        );
        setup(clusterStateWith(metadata, true), now, TimeValue.timeValueDays(30), checkInterval);

        coordinator.tick();

        verify(taskQueue, never()).submitTask(
            eq("retire-project-encryption-keys"),
            isA(KeyRotationCoordinator.RetireKeysTask.class),
            any()
        );
    }

    public void testReEncryptApplyTaskUpdatesCustomAndHandlerKeyIds() throws Exception {
        long now = 100L;
        ProjectEncryptionKeyMetadata metadata = new ProjectEncryptionKeyMetadata(
            Map.of("k1", entry(50L)),
            "k1",
            PASSWORD_ID,
            Map.of(),
            NO_OP_ENCRYPTION
        );
        TestCustom oldCustom = TestCustom.encryptedUnder("old-key");
        TestCustom newCustom = TestCustom.encryptedUnder("k1");
        ClusterState state = clusterStateWith(metadata, oldCustom, true);

        SimpleBatchedExecutor<KeyRotationCoordinator.KeyRotationTask, Void> executor = newExecutor(now);
        ActionListener<Void> listener = listenerCapturingResponses(new AtomicInteger());
        KeyRotationCoordinator.ReEncryptApplyTask task = new KeyRotationCoordinator.ReEncryptApplyTask(
            TestCustom.TYPE,
            oldCustom,
            newCustom,
            "k1",
            listener
        );

        Tuple<ClusterState, Void> result = executor.executeTask(task, state);

        ProjectState projectState = DefaultProjectResolver.INSTANCE.getProjectState(result.v1());
        assertSame("new custom installed", newCustom, projectState.metadata().custom(TestCustom.TYPE));
        ProjectEncryptionKeyMetadata updated = projectState.metadata().custom(ProjectEncryptionKeyMetadata.TYPE);
        assertEquals("handlerKeyIds entry recorded", "k1", updated.getHandlerKeyIds().get(TestCustom.TYPE));
    }

    public void testReEncryptApplyTaskDropsOnSliceConflict() throws Exception {
        long now = 100L;
        ProjectEncryptionKeyMetadata metadata = new ProjectEncryptionKeyMetadata(
            Map.of("k1", entry(50L)),
            "k1",
            PASSWORD_ID,
            Map.of(),
            NO_OP_ENCRYPTION
        );
        TestCustom expectedOld = TestCustom.encryptedUnder("old-key");
        TestCustom conflictingCurrent = TestCustom.encryptedUnder("intervening-write");
        TestCustom newCustom = TestCustom.encryptedUnder("k1");
        ClusterState state = clusterStateWith(metadata, conflictingCurrent, true);

        SimpleBatchedExecutor<KeyRotationCoordinator.KeyRotationTask, Void> executor = newExecutor(now);
        KeyRotationCoordinator.ReEncryptApplyTask task = new KeyRotationCoordinator.ReEncryptApplyTask(
            TestCustom.TYPE,
            expectedOld,
            newCustom,
            "k1",
            ActionListener.noop()
        );

        Tuple<ClusterState, Void> result = executor.executeTask(task, state);

        assertSame("cluster state unchanged on conflict", state, result.v1());
    }

    public void testReEncryptApplyTaskDropsOnActiveKeyDrift() throws Exception {
        long now = 100L;
        ProjectEncryptionKeyMetadata metadata = new ProjectEncryptionKeyMetadata(
            Map.of("k1", entry(50L), "k2", entry(60L)),
            "k2",
            PASSWORD_ID,
            Map.of(),
            NO_OP_ENCRYPTION
        );
        TestCustom oldCustom = TestCustom.encryptedUnder("old-key");
        TestCustom newCustomForK1 = TestCustom.encryptedUnder("k1");
        ClusterState state = clusterStateWith(metadata, oldCustom, true);

        SimpleBatchedExecutor<KeyRotationCoordinator.KeyRotationTask, Void> executor = newExecutor(now);
        KeyRotationCoordinator.ReEncryptApplyTask task = new KeyRotationCoordinator.ReEncryptApplyTask(
            TestCustom.TYPE,
            oldCustom,
            newCustomForK1,
            "k1", // computed against k1, but active is now k2
            ActionListener.noop()
        );

        Tuple<ClusterState, Void> result = executor.executeTask(task, state);

        assertSame("cluster state unchanged when activeKeyId drifted", state, result.v1());
    }

    private static SimpleBatchedExecutor<KeyRotationCoordinator.KeyRotationTask, Void> newExecutor(long now) {
        return new KeyRotationCoordinator.KeyRotationExecutor(DefaultProjectResolver.INSTANCE, NO_OP_ENCRYPTION);
    }

    private static ActionListener<Void> listenerCapturingResponses(AtomicInteger counter) {
        return ActionListener.wrap(unused -> counter.incrementAndGet(), e -> { throw new AssertionError(e); });
    }

    /**
     * Creates a handler that builds a new {@link TestCustom} pretending the data has been re-encrypted under {@code expectedKeyId}.
     */
    private static EncryptedDataHandler<TestCustom> captureHandler(AtomicInteger calls, String expectedKeyId) {
        return new EncryptedDataHandler<>() {
            @Override
            public String customName() {
                return TestCustom.TYPE;
            }

            @Override
            public TestCustom reEncrypt(TestCustom current, EncryptionService svc, String activeKeyId) {
                calls.incrementAndGet();
                return TestCustom.encryptedUnder(activeKeyId);
            }
        };
    }

    /**
     * Test-local {@link Metadata.ProjectCustom} carrying nothing but a synthetic "keyId" string. Used to seed cluster state for tests
     * that exercise the executor's CAS branches.
     */
    static final class TestCustom extends AbstractNamedDiffable<Metadata.ProjectCustom> implements Metadata.ProjectCustom {
        static final String TYPE = "test_kc_custom";

        private final String keyId;

        private TestCustom(String keyId) {
            this.keyId = Objects.requireNonNull(keyId);
        }

        static TestCustom encryptedUnder(String keyId) {
            return new TestCustom(keyId);
        }

        @Override
        public String getWriteableName() {
            return TYPE;
        }

        @Override
        public TransportVersion getMinimalSupportedVersion() {
            return TransportVersion.current();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(keyId);
        }

        public static NamedDiff<Metadata.ProjectCustom> readDiffFrom(StreamInput in) throws IOException {
            return readDiffFrom(Metadata.ProjectCustom.class, TYPE, in);
        }

        @Override
        public EnumSet<Metadata.XContentContext> context() {
            return EnumSet.of(Metadata.XContentContext.GATEWAY);
        }

        @Override
        public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params params) {
            return ChunkedToXContentHelper.chunk((builder, ignored) -> builder.field("key_id", keyId));
        }

        @Override
        public boolean equals(Object o) {
            return o instanceof TestCustom other && keyId.equals(other.keyId);
        }

        @Override
        public int hashCode() {
            return keyId.hashCode();
        }
    }
}
