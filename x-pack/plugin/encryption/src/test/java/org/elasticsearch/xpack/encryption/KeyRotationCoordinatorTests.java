/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.encryption;

import org.apache.logging.log4j.Level;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.internal.Client;
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
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.ChunkedToXContentHelper;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.features.FeatureService;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xpack.encryption.ProjectEncryptionKeyMetadata.KeyEntry;
import org.elasticsearch.xpack.encryption.spi.EncryptedData;
import org.elasticsearch.xpack.encryption.spi.EncryptedDataHandler;
import org.elasticsearch.xpack.encryption.spi.EncryptionService;
import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class KeyRotationCoordinatorTests extends ESTestCase {

    private static final ClusterName CLUSTER_NAME = new ClusterName("test");
    private static final String PASSWORD_ID = "v1";
    // BC FIPS rejects PBKDF2 passwords shorter than 14 chars (112 bits); use a longer literal.
    private static final String PASSWORD_VALUE = "test-password-fips";

    private static final ProjectEncryptionKeyMetadata.PekEncryption NO_OP_ENCRYPTION = TestPekEncryption.NO_OP;

    private ProjectEncryptionKeyMetadata.PekEncryption pekEncryption = NO_OP_ENCRYPTION;

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
        DiscoveryNode local = DiscoveryNodeUtils.builder("local").name("local").ephemeralId("local-ephemeral").build();
        DiscoveryNode other = DiscoveryNodeUtils.builder("other").name("other").ephemeralId("other-ephemeral").build();
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
    private ThreadPool threadPool;
    private ClusterService clusterService;
    private Predicate<DiscoveryNode> canPersist = node -> true;
    private Settings nodeSettings = Settings.EMPTY;

    /** Answers the readiness check from {@link #canPersist}, as the nodes would over transport. */
    @SuppressWarnings("unchecked")
    private Client stubClient() {
        Client client = mock(Client.class);
        doAnswer(invocation -> {
            ActionListener<Object> listener = invocation.getArgument(2);
            TransportNodesCanPersistKeyAction.Request request = invocation.getArgument(1);
            listener.onResponse(
                new TransportNodesCanPersistKeyAction.Response(
                    CLUSTER_NAME,
                    request.nodes().stream().map(n -> new TransportNodesCanPersistKeyAction.NodeResponse(n, canPersist.test(n))).toList(),
                    List.of()
                )
            );
            return null;
        }).when(client).execute(any(), any(), any());
        return client;
    }

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
        clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenReturn(state);
        when(clusterService.getSettings()).thenReturn(nodeSettings);
        MasterServiceTaskQueue rawQueue = taskQueue;
        when(clusterService.createTaskQueue(anyString(), any(), any())).thenReturn(rawQueue);
        threadPool = mock(ThreadPool.class);
        when(threadPool.absoluteTimeInMillis()).thenReturn(now);
        when(threadPool.generic()).thenReturn(EsExecutors.DIRECT_EXECUTOR_SERVICE);
        when(threadPool.getThreadContext()).thenReturn(new ThreadContext(Settings.EMPTY));
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
            pekEncryption,
            stubClient()
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

    /** Serializes {@code metadata} as for this node's disk, which records it as this node's copy of the key. */
    private static void writeToDisk(ProjectEncryptionKeyMetadata metadata) {
        metadata.toXContentChunked(new ToXContent.MapParams(Map.of(Metadata.CONTEXT_MODE_PARAM, Metadata.CONTEXT_MODE_GATEWAY)));
    }

    /** A reset removes the key on every node, so this node no longer reports a copy of it on disk. */
    public void testResetForgetsThisNodesCopyOfTheKey() throws IOException {
        ProjectEncryptionKeyMetadata.GatewayKeys keys = new ProjectEncryptionKeyMetadata.GatewayKeys();
        pekEncryption = TestPekEncryption.onNode(NO_OP_ENCRYPTION, keys);
        ProjectEncryptionKeyMetadata existing = new ProjectEncryptionKeyMetadata(
            Map.of("k1", entry(0L)),
            "k1",
            PASSWORD_ID,
            Map.of(),
            pekEncryption
        );
        writeToDisk(existing);
        assertTrue(keys.holdsKeys());
        setup(clusterStateWith(existing, false), 0L, TimeValue.timeValueDays(30));

        coordinator.onClusterStateChanged(
            new ClusterChangedEvent("test", clusterStateWith(null, false), clusterStateWith(existing, false))
        );

        assertFalse(keys.holdsKeys());
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

    /** The master installs the first key only once every node can persist it, and asks again until they can. */
    public void testInstallWaitsUntilEveryNodeCanPersistTheKey() {
        installWaitingForOther().run();

        assertEquals(
            Set.of("local-ephemeral", "other-ephemeral"),
            submitted("install-project-encryption-key", KeyRotationCoordinator.InstallKeyTask.class).readyNodeIds()
        );
        assertEquals(Set.of(), coordinator.nodesWithoutPassword());
    }

    /** On stateless with passwords from cluster secrets every node has the same password, so the master installs without asking. */
    public void testInstallWithoutAskingNodesWhenPasswordsComeFromClusterSecretsOnStateless() throws IOException {
        nodeSettings = Settings.builder().put(DiscoveryNode.STATELESS_ENABLED_SETTING_NAME, true).build();
        ClusterState state = PasswordPekEncryptionTests.withClusterSecrets(
            clusterStateWith(null, true),
            ProjectEncryptionKeyPasswordSettings.ACTIVE_PASSWORD_ID_KEY
        );
        setup(state, 0L, TimeValue.timeValueDays(30), TimeValue.timeValueMinutes(1), featureAvailable());
        canPersist = node -> false;

        coordinator.onClusterStateChanged(new ClusterChangedEvent("test", state, state));

        assertNull(submitted("install-project-encryption-key", KeyRotationCoordinator.InstallKeyTask.class).readyNodeIds());
    }

    /** Only the elected master reports the nodes it waits for, even if a check completed after it stepped down. */
    public void testNodesWithoutPasswordEmptyOnceNoLongerMaster() {
        installWaitingForOther();

        when(clusterService.state()).thenReturn(clusterStateWith(null, false));
        assertEquals(Set.of(), coordinator.nodesWithoutPassword());
    }

    /** With passwords from cluster secrets the master still checks that it can use its own, such as one too short for FIPS. */
    public void testNoInstallWhenMasterCannotUsePasswordFromClusterSecrets() throws IOException {
        nodeSettings = Settings.builder().put(DiscoveryNode.STATELESS_ENABLED_SETTING_NAME, true).build();
        pekEncryption = new ProjectEncryptionKeyMetadata.PekEncryption() {
            @Override
            public String activePasswordId() {
                return NO_OP_ENCRYPTION.activePasswordId();
            }

            @Override
            public WrappedKey wrap(byte[] plaintextPek) {
                return NO_OP_ENCRYPTION.wrap(plaintextPek);
            }

            @Override
            public byte[] unwrap(byte[] wrappedPek, String passwordId) {
                return NO_OP_ENCRYPTION.unwrap(wrappedPek, passwordId);
            }

            @Override
            public ProjectEncryptionKeyMetadata.GatewayKeys gatewayKeys() {
                return NO_OP_ENCRYPTION.gatewayKeys();
            }

            @Override
            public String wrapForGatewayFailure() {
                return "password too short";
            }
        };
        ClusterState state = PasswordPekEncryptionTests.withClusterSecrets(
            clusterStateWith(null, true),
            ProjectEncryptionKeyPasswordSettings.ACTIVE_PASSWORD_ID_KEY
        );
        setup(state, 0L, TimeValue.timeValueDays(30), TimeValue.timeValueMinutes(1), featureAvailable());

        MockLog.assertThatLogger(
            () -> coordinator.onClusterStateChanged(new ClusterChangedEvent("test", state, state)),
            KeyRotationCoordinator.class,
            new MockLog.SeenEventExpectation(
                "gives the reason",
                KeyRotationCoordinator.class.getName(),
                Level.WARN,
                "project encryption key not installed yet: the elected master cannot persist it (password too short)"
            )
        );
        verify(taskQueue, never()).submitTask(anyString(), any(), any());
        assertEquals(Set.of("local"), coordinator.nodesWithoutPassword());
    }

    /** A node that persists cluster state restarting, here with a password, ends the wait for the retry. */
    public void testInstallRetriedWhenANodeThatPersistsClusterStateRestarts() {
        Runnable scheduledRetry = installWaitingForOther();

        ClusterState state = clusterStateWith(null, true);
        DiscoveryNode otherRestarted = DiscoveryNodeUtils.builder("other").name("other").ephemeralId("other-restarted").build();
        ClusterState restarted = ClusterState.builder(state)
            .nodes(DiscoveryNodes.builder(state.nodes()).remove("other").add(otherRestarted))
            .build();
        when(clusterService.state()).thenReturn(restarted);
        coordinator.onClusterStateChanged(new ClusterChangedEvent("test", restarted, state));
        assertEquals(
            "readiness of the restarted node",
            Set.of("local-ephemeral", "other-restarted"),
            submitted("install-project-encryption-key", KeyRotationCoordinator.InstallKeyTask.class).readyNodeIds()
        );

        scheduledRetry.run();
        verify(taskQueue, times(1)).submitTask(anyString(), any(), any());
    }

    /** A secure settings reload on the master, as for a cluster-wide reload, ends the wait for the retry. */
    public void testInstallRetriedOnReload() {
        Runnable scheduledRetry = installWaitingForOther();

        coordinator.reload();
        verify(taskQueue).submitTask(eq("install-project-encryption-key"), isA(KeyRotationCoordinator.InstallKeyTask.class), any());

        scheduledRetry.run();
        verify(taskQueue, times(1)).submitTask(anyString(), any(), any());
    }

    /** Starts an install that waits for node "other", as {@link #waitingForOther}. */
    private Runnable installWaitingForOther() {
        ClusterState state = clusterStateWith(null, true);
        setup(state, 0L, TimeValue.timeValueDays(30), TimeValue.timeValueMinutes(1), featureAvailable());
        return waitingForOther(state, "installed");
    }

    /**
     * Applies {@code state} while node "other" cannot persist the key, so that nothing is submitted, then lets every node persist it.
     * Returns the retry scheduled after {@link KeyRotationCoordinator#PERSIST_CHECK_INTERVAL}.
     */
    private Runnable waitingForOther(ClusterState state, String operation) {
        canPersist = node -> node.getId().equals("other") == false;
        MockLog.assertThatLogger(
            () -> coordinator.onClusterStateChanged(new ClusterChangedEvent("test", state, state)),
            KeyRotationCoordinator.class,
            new MockLog.SeenEventExpectation(
                "names the node",
                KeyRotationCoordinator.class.getName(),
                Level.WARN,
                "project encryption key not " + operation + " yet: nodes [other] cannot persist it*"
            )
        );
        verify(taskQueue, never()).submitTask(anyString(), any(), any());
        assertEquals("reported by the health indicator", Set.of("other"), coordinator.nodesWithoutPassword());
        ArgumentCaptor<Runnable> retry = ArgumentCaptor.forClass(Runnable.class);
        verify(threadPool).schedule(retry.capture(), eq(KeyRotationCoordinator.PERSIST_CHECK_INTERVAL), any());
        canPersist = node -> true;
        return retry.getValue();
    }

    private <T extends KeyRotationCoordinator.KeyRotationTask> T submitted(String source, Class<T> taskClass) {
        ArgumentCaptor<KeyRotationCoordinator.KeyRotationTask> task = ArgumentCaptor.forClass(KeyRotationCoordinator.KeyRotationTask.class);
        verify(taskQueue).submitTask(eq(source), task.capture(), any());
        return taskClass.cast(task.getValue());
    }

    private static FeatureService featureAvailable() {
        FeatureService featureService = mock(FeatureService.class);
        when(featureService.clusterHasFeature(any(), any())).thenReturn(true);
        return featureService;
    }

    /** An elected master without the key restores it from its own copy on disk once every node can persist it. */
    public void testElectedMasterWithoutKeyRestoresItFromItsOwnDisk() {
        byte[] keyBytes = randomKey();
        ClusterState state = masterWithoutKeyAndACopyOnDisk(keyBytes);

        waitingForOther(state, "restored").run();

        KeyRotationCoordinator.RestoreKeysTask task = submitted(
            "restore-project-encryption-keys",
            KeyRotationCoordinator.RestoreKeysTask.class
        );
        assertArrayEquals(keyBytes, task.keys().getKeys().get("k1").bytes());
        assertEquals(Set.of("local-ephemeral", "other-ephemeral"), task.readyNodeIds());
    }

    private ClusterState masterWithoutKeyAndACopyOnDisk(byte[] keyBytes) {
        ProjectEncryptionKeyMetadata.GatewayKeys keys = new ProjectEncryptionKeyMetadata.GatewayKeys();
        pekEncryption = TestPekEncryption.onNode(NO_OP_ENCRYPTION, keys);
        writeToDisk(new ProjectEncryptionKeyMetadata(Map.of("k1", new KeyEntry(keyBytes, 1L)), "k1", PASSWORD_ID, Map.of(), pekEncryption));
        ClusterState state = clusterStateWith(ProjectEncryptionKeyMetadata.degraded("k1", PASSWORD_ID, pekEncryption, "no key"), true);
        setup(state, 0L, TimeValue.timeValueDays(30));
        return state;
    }

    public void testElectedMasterWithoutACopyOnDiskWarns() {
        ClusterState state = clusterStateWith(ProjectEncryptionKeyMetadata.degraded("k1", PASSWORD_ID, NO_OP_ENCRYPTION, "no key"), true);
        setup(state, 0L, TimeValue.timeValueDays(30));

        MockLog.assertThatLogger(
            () -> coordinator.onClusterStateChanged(new ClusterChangedEvent("test", state, state)),
            KeyRotationCoordinator.class,
            new MockLog.SeenEventExpectation(
                "tells the operator how to recover",
                KeyRotationCoordinator.class.getName(),
                Level.WARN,
                "the elected master holds no project encryption key [k1] and cannot read it from its disk*"
            )
        );
        verify(taskQueue, never()).submitTask(anyString(), any(), any());
    }

    public void testElectedMasterThatCannotPersistTheKeyDoesNotRestoreIt() {
        ClusterState state = clusterStateWith(ProjectEncryptionKeyMetadata.degraded("k1", PASSWORD_ID, NO_OP_ENCRYPTION, "no key"), true);
        setup(state, 0L, TimeValue.timeValueDays(30), TimeValue.timeValueMinutes(1), mock(FeatureService.class), List.of(), Settings.EMPTY);

        MockLog.assertThatLogger(
            () -> coordinator.onClusterStateChanged(new ClusterChangedEvent("test", state, state)),
            KeyRotationCoordinator.class,
            new MockLog.SeenEventExpectation(
                "tells the operator to configure the password",
                KeyRotationCoordinator.class.getName(),
                Level.WARN,
                "the elected master holds no project encryption key [k1] and cannot persist it*"
            )
        );
        verify(taskQueue, never()).submitTask(anyString(), any(), any());
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
        // not needed, e.g. cluster secrets may switch to the new password and drop the old one in one step
        if (randomBoolean()) {
            secure.setString(ProjectEncryptionKeyPasswordSettings.PASSWORD_PREFIX + PASSWORD_ID, PASSWORD_VALUE);
        }
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

    public void testTickDefersKeyLifecycleWhenHandlerApplyTaskSubmitted() {
        long generatedAt = 1_000_000_000L;
        long now = generatedAt + TimeValue.timeValueDays(30).millis() + 1;
        ProjectEncryptionKeyMetadata metadata = new ProjectEncryptionKeyMetadata(
            Map.of("k1", entry(generatedAt)),
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
        verify(taskQueue).submitTask(eq("re-encrypt-test_kc_custom"), isA(KeyRotationCoordinator.ReEncryptApplyTask.class), any());
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

    public void testTickSubmitsApplyTaskWhenHandlerReturnsSameCustom() {
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
            public TestCustom reEncrypt(TestCustom current, UnaryOperator<EncryptedData> rewrapper) {
                calls.incrementAndGet();
                return current;
            }
        };
        setup(clusterStateWith(metadata, seeded, true), now, TimeValue.timeValueDays(30), List.of(identityHandler));

        coordinator.tick();

        ArgumentCaptor<KeyRotationCoordinator.ReEncryptApplyTask> taskCaptor = ArgumentCaptor.forClass(
            KeyRotationCoordinator.ReEncryptApplyTask.class
        );

        assertEquals(1, calls.get());
        verify(taskQueue).submitTask(eq("re-encrypt-" + TestCustom.TYPE), taskCaptor.capture(), any());

        KeyRotationCoordinator.ReEncryptApplyTask task = taskCaptor.getValue();
        assertSame(seeded, task.expectedOld());
        assertSame(seeded, task.newCustom());
        assertEquals("k1", task.expectedActiveKeyId());
    }

    public void testHandlerReturningNullForExistingCustomDoesNotSubmitApplyTask() {
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
        EncryptedDataHandler<TestCustom> nullReturningHandler = new EncryptedDataHandler<>() {
            @Override
            public String customName() {
                return TestCustom.TYPE;
            }

            @Override
            public TestCustom reEncrypt(TestCustom current, UnaryOperator<EncryptedData> rewrapper) {
                calls.incrementAndGet();
                return null;
            }
        };
        setup(clusterStateWith(metadata, seeded, true), now, TimeValue.timeValueDays(30), List.of(nullReturningHandler));

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
            Map.of("older", entry(now - 45 * 60 * 1000L), "old", entry(oldGeneratedAt), "active", entry(activeGeneratedAt)),
            "active",
            PASSWORD_ID,
            Map.of(),
            NO_OP_ENCRYPTION
        );
        setup(clusterStateWith(metadata, true), now, TimeValue.timeValueDays(30), checkInterval);

        coordinator.tick();

        verify(taskQueue).submitTask(
            eq("retire-project-encryption-keys"),
            eq(new KeyRotationCoordinator.RetireKeysTask(now - graceMillis, "active")),
            any()
        );
    }

    public void testTickDoesNotSubmitRetireWhenAllKeysWithinGrace() {
        TimeValue checkInterval = TimeValue.timeValueMinutes(1);
        long now = 1_000_000L;
        long activeGeneratedAt = now - 60 * 1000L;
        long oldGeneratedAt = now - 5 * 60 * 1000L;

        ProjectEncryptionKeyMetadata metadata = new ProjectEncryptionKeyMetadata(
            Map.of("older", entry(now - 8 * 60 * 1000L), "old", entry(oldGeneratedAt), "active", entry(activeGeneratedAt)),
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
            Map.of("older", entry(now - 45 * 60 * 1000L), "old", entry(oldGeneratedAt), "active", entry(activeGeneratedAt)),
            "active",
            PASSWORD_ID,
            Map.of(TestCustom.TYPE, "older"),
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
        return new KeyRotationCoordinator.KeyRotationExecutor(DefaultProjectResolver.INSTANCE, NO_OP_ENCRYPTION, false);
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
            public TestCustom reEncrypt(TestCustom current, UnaryOperator<EncryptedData> rewrapper) {
                calls.incrementAndGet();
                return TestCustom.encryptedUnder(expectedKeyId);
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
