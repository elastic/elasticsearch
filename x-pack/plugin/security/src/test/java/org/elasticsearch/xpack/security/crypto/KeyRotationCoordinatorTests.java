/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.crypto;

import org.apache.logging.log4j.Level;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.project.DefaultProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.MasterServiceTaskQueue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.features.FeatureService;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.crypto.KeyRotationHandler;
import org.elasticsearch.xpack.core.crypto.PrimaryEncryptionKeyMetadata;
import org.elasticsearch.xpack.core.crypto.PrimaryEncryptionKeyMetadata.KeyEntry;
import org.mockito.ArgumentCaptor;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@SuppressWarnings({ "unchecked", "rawtypes" })
public class KeyRotationCoordinatorTests extends ESTestCase {

    private static final ClusterName CLUSTER_NAME = new ClusterName("test");

    private static byte[] randomKey() {
        byte[] keyBytes = new byte[32];
        random().nextBytes(keyBytes);
        return keyBytes;
    }

    private static KeyEntry entry(long generatedAt) {
        return new KeyEntry(randomKey(), generatedAt);
    }

    private static DiscoveryNodes nodes(boolean isLocalMaster) {
        DiscoveryNode local = DiscoveryNodeUtils.create("local");
        DiscoveryNode other = DiscoveryNodeUtils.create("other");
        DiscoveryNodes.Builder builder = DiscoveryNodes.builder().add(local).add(other).localNodeId("local");
        builder.masterNodeId(isLocalMaster ? "local" : "other");
        return builder.build();
    }

    private static ClusterState clusterStateWith(PrimaryEncryptionKeyMetadata pek, boolean isLocalMaster) {
        ProjectMetadata.Builder project = ProjectMetadata.builder(Metadata.DEFAULT_PROJECT_ID);
        if (pek != null) {
            project.putCustom(PrimaryEncryptionKeyMetadata.TYPE, pek);
        }
        return ClusterState.builder(CLUSTER_NAME)
            .metadata(Metadata.builder().put(project.build()).build())
            .nodes(nodes(isLocalMaster))
            .build();
    }

    @SuppressWarnings("rawtypes")
    private record Harness(KeyRotationCoordinator coordinator, MasterServiceTaskQueue taskQueue, ClusterStateListener listener) {}

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private static Harness newHarness(
        ClusterState state,
        long now,
        TimeValue rotationInterval,
        TimeValue checkInterval,
        FeatureService featureService
    ) {
        MasterServiceTaskQueue taskQueue = mock(MasterServiceTaskQueue.class);
        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenReturn(state);
        when(clusterService.createTaskQueue(anyString(), any(), any())).thenReturn(taskQueue);
        ThreadPool threadPool = mock(ThreadPool.class);
        when(threadPool.absoluteTimeInMillis()).thenReturn(now);
        KeyRotationCoordinator coordinator = KeyRotationCoordinator.create(
            clusterService,
            threadPool,
            DefaultProjectResolver.INSTANCE,
            featureService,
            org.elasticsearch.common.settings.Settings.builder()
                .put(KeyRotationCoordinator.ROTATION_INTERVAL_SETTING.getKey(), rotationInterval)
                .put(KeyRotationCoordinator.CHECK_INTERVAL_SETTING.getKey(), checkInterval)
                .build()
        );
        ArgumentCaptor<ClusterStateListener> captor = ArgumentCaptor.forClass(ClusterStateListener.class);
        verify(clusterService).addListener(captor.capture());
        return new Harness(coordinator, taskQueue, captor.getValue());
    }

    private static Harness newHarness(ClusterState state, long now, TimeValue rotationInterval, TimeValue checkInterval) {
        return newHarness(state, now, rotationInterval, checkInterval, mock(FeatureService.class));
    }

    private static Harness newHarness(ClusterState state, long now, TimeValue rotationInterval) {
        return newHarness(state, now, rotationInterval, TimeValue.timeValueMinutes(1));
    }

    // --- Install path (ClusterStateListener) ---

    public void testInstallSubmittedWhenMasterWithoutKeyAndFeatureAvailable() {
        FeatureService featureService = mock(FeatureService.class);
        when(featureService.clusterHasFeature(any(), any())).thenReturn(true);
        Harness h = newHarness(
            clusterStateWith(null, true),
            0L,
            TimeValue.timeValueDays(30),
            TimeValue.timeValueMinutes(1),
            featureService
        );

        ClusterState prev = clusterStateWith(null, true);
        ClusterState next = clusterStateWith(null, true);
        h.listener.clusterChanged(new ClusterChangedEvent("test", next, prev));

        verify(h.taskQueue).submitTask(eq("install-primary-encryption-key"), isA(KeyRotationCoordinator.InstallKeyTask.class), any());
    }

    public void testInstallNotSubmittedWhenNotMaster() {
        FeatureService featureService = mock(FeatureService.class);
        when(featureService.clusterHasFeature(any(), any())).thenReturn(true);
        Harness h = newHarness(
            clusterStateWith(null, false),
            0L,
            TimeValue.timeValueDays(30),
            TimeValue.timeValueMinutes(1),
            featureService
        );

        h.listener.clusterChanged(new ClusterChangedEvent("test", clusterStateWith(null, false), clusterStateWith(null, false)));

        verify(h.taskQueue, never()).submitTask(anyString(), any(), any());
    }

    public void testInstallNotSubmittedWhenFeatureMissing() {
        FeatureService featureService = mock(FeatureService.class);
        when(featureService.clusterHasFeature(any(), any())).thenReturn(false);
        Harness h = newHarness(
            clusterStateWith(null, true),
            0L,
            TimeValue.timeValueDays(30),
            TimeValue.timeValueMinutes(1),
            featureService
        );

        h.listener.clusterChanged(new ClusterChangedEvent("test", clusterStateWith(null, true), clusterStateWith(null, true)));

        verify(h.taskQueue, never()).submitTask(anyString(), any(), any());
    }

    public void testInstallNotSubmittedWhenKeyAlreadyExists() {
        FeatureService featureService = mock(FeatureService.class);
        when(featureService.clusterHasFeature(any(), any())).thenReturn(true);
        PrimaryEncryptionKeyMetadata existing = new PrimaryEncryptionKeyMetadata(Map.of("k1", entry(0L)), "k1");
        Harness h = newHarness(
            clusterStateWith(existing, true),
            0L,
            TimeValue.timeValueDays(30),
            TimeValue.timeValueMinutes(1),
            featureService
        );

        h.listener.clusterChanged(new ClusterChangedEvent("test", clusterStateWith(existing, true), clusterStateWith(null, true)));

        verify(h.taskQueue, never()).submitTask(anyString(), any(), any());
    }

    // --- Tick path: rotation + retire ---

    public void testTickIsNoopWhenRotationDisabled() {
        Harness h = newHarness(clusterStateWith(null, true), 0L, TimeValue.ZERO);
        h.coordinator.tick();
        verify(h.taskQueue, never()).submitTask(anyString(), any(), any());
    }

    public void testTickIsNoopWhenNotMaster() {
        Harness h = newHarness(clusterStateWith(null, false), 0L, TimeValue.timeValueDays(30));
        h.coordinator.tick();
        verify(h.taskQueue, never()).submitTask(anyString(), any(), any());
    }

    public void testTickIsNoopWhenNoMetadataInstalled() {
        Harness h = newHarness(clusterStateWith(null, true), 0L, TimeValue.timeValueDays(30));
        h.coordinator.tick();
        verify(h.taskQueue, never()).submitTask(anyString(), any(), any());
    }

    public void testTickBeginsRotationWhenActiveKeyIsOldEnough() {
        long generatedAt = 1_000_000_000L;
        long now = generatedAt + TimeValue.timeValueDays(30).millis() + 1;
        PrimaryEncryptionKeyMetadata metadata = new PrimaryEncryptionKeyMetadata(Map.of("k1", entry(generatedAt)), "k1");
        Harness h = newHarness(clusterStateWith(metadata, true), now, TimeValue.timeValueDays(30));

        h.coordinator.tick();

        verify(h.taskQueue).submitTask(
            eq("begin-primary-encryption-key-rotation"),
            isA(KeyRotationCoordinator.BeginRotationTask.class),
            any()
        );
    }

    public void testTickDoesNotBeginRotationWhenActiveKeyIsYoung() {
        long generatedAt = 1_000_000_000L;
        long now = generatedAt + 1;
        PrimaryEncryptionKeyMetadata metadata = new PrimaryEncryptionKeyMetadata(Map.of("k1", entry(generatedAt)), "k1");
        Harness h = newHarness(clusterStateWith(metadata, true), now, TimeValue.timeValueDays(30));

        h.coordinator.tick();

        verify(h.taskQueue, never()).submitTask(anyString(), any(), any());
    }

    public void testTickInvokesAllHandlersOnEveryTick() {
        long generatedAt = 100L;
        long now = 200L;
        PrimaryEncryptionKeyMetadata metadata = new PrimaryEncryptionKeyMetadata(Map.of("k1", entry(generatedAt)), "k1");
        Harness h = newHarness(clusterStateWith(metadata, true), now, TimeValue.timeValueDays(30));

        AtomicInteger alphaCalls = new AtomicInteger();
        AtomicInteger betaCalls = new AtomicInteger();
        h.coordinator.registerKeyRotationHandler(handler("alpha", alphaCalls, true));
        h.coordinator.registerKeyRotationHandler(handler("beta", betaCalls, true));

        h.coordinator.tick();

        assertEquals(1, alphaCalls.get());
        assertEquals(1, betaCalls.get());
    }

    public void testTickSubmitsRetireForKeysOlderThanGrace() {
        TimeValue checkInterval = TimeValue.timeValueMinutes(1);
        long now = 1_000_000L;
        long graceMillis = 10 * checkInterval.millis();
        long activeGeneratedAt = now - 15 * 60 * 1000L;
        long oldGeneratedAt = now - 30 * 60 * 1000L;
        PrimaryEncryptionKeyMetadata metadata = new PrimaryEncryptionKeyMetadata(
            Map.of("old", entry(oldGeneratedAt), "active", entry(activeGeneratedAt)),
            "active"
        );
        Harness h = newHarness(clusterStateWith(metadata, true), now, TimeValue.timeValueDays(30), checkInterval);

        h.coordinator.tick();

        verify(h.taskQueue).submitTask(
            eq("retire-primary-encryption-keys"),
            eq(new KeyRotationCoordinator.RetireKeysTask(now - graceMillis)),
            any()
        );
    }

    public void testTickDoesNotSubmitRetireWhenAllKeysWithinGrace() {
        TimeValue checkInterval = TimeValue.timeValueMinutes(1);
        long now = 1_000_000L;
        long activeGeneratedAt = now - 60 * 1000L;
        long oldGeneratedAt = now - 5 * 60 * 1000L;

        PrimaryEncryptionKeyMetadata metadata = new PrimaryEncryptionKeyMetadata(
            Map.of("old", entry(oldGeneratedAt), "active", entry(activeGeneratedAt)),
            "active"
        );
        Harness h = newHarness(clusterStateWith(metadata, true), now, TimeValue.timeValueDays(30), checkInterval);

        h.coordinator.tick();

        verify(h.taskQueue, never()).submitTask(
            eq("retire-primary-encryption-keys"),
            isA(KeyRotationCoordinator.RetireKeysTask.class),
            any()
        );
    }

    public void testTickBeginsRotationAndAlsoInvokesHandlers() {
        long generatedAt = 1_000_000_000L;
        long now = generatedAt + TimeValue.timeValueDays(30).millis() + 1;
        PrimaryEncryptionKeyMetadata metadata = new PrimaryEncryptionKeyMetadata(Map.of("k1", entry(generatedAt)), "k1");
        Harness h = newHarness(clusterStateWith(metadata, true), now, TimeValue.timeValueDays(30));

        AtomicInteger calls = new AtomicInteger();
        h.coordinator.registerKeyRotationHandler(handler("alpha", calls, true));

        h.coordinator.tick();

        assertEquals(1, calls.get());
        verify(h.taskQueue).submitTask(
            eq("begin-primary-encryption-key-rotation"),
            isA(KeyRotationCoordinator.BeginRotationTask.class),
            any()
        );
    }

    public void testStuckRotationLogsWarnWithDuration() throws Exception {
        // A handler that never completes its listener stalls rotation. The next tick should emit a WARN log with the elapsed duration.
        PrimaryEncryptionKeyMetadata metadata = new PrimaryEncryptionKeyMetadata(Map.of("k1", entry(0L)), "k1");
        ClusterState state = clusterStateWith(metadata, true);
        AtomicInteger calls = new AtomicInteger();
        KeyRotationHandler hanging = new KeyRotationHandler() {
            @Override
            public String name() {
                return "hanging";
            }

            @Override
            public void reEncrypt(String activeKeyId, ActionListener<Void> listener) {
                calls.incrementAndGet();
                // Never complete the listener.
            }
        };

        @SuppressWarnings({ "unchecked", "rawtypes" })
        MasterServiceTaskQueue taskQueue = mock(MasterServiceTaskQueue.class);
        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenReturn(state);
        when(clusterService.createTaskQueue(anyString(), any(), any())).thenReturn(taskQueue);
        ThreadPool threadPool = mock(ThreadPool.class);
        long t0 = 1_000L;
        long t1 = t0 + TimeValue.timeValueMinutes(5).millis();
        when(threadPool.absoluteTimeInMillis()).thenReturn(t0, t1);
        KeyRotationCoordinator coordinator = new KeyRotationCoordinator(
            clusterService,
            threadPool,
            DefaultProjectResolver.INSTANCE,
            mock(FeatureService.class),
            TimeValue.timeValueDays(30),
            TimeValue.timeValueMinutes(1)
        );
        coordinator.registerKeyRotationHandler(hanging);

        coordinator.tick();
        assertEquals(1, calls.get());

        try (var mockLog = MockLog.capture(KeyRotationCoordinator.class)) {
            mockLog.addExpectation(
                new MockLog.SeenEventExpectation(
                    "stuck rotation warn",
                    KeyRotationCoordinator.class.getName(),
                    Level.WARN,
                    "rotation already in progress, skipping this tick (in progress for 5m)"
                )
            );
            coordinator.tick();
            mockLog.assertAllExpectationsMatched();
        }
        // Second tick must not invoke the handler again (rotating flag prevents re-entry).
        assertEquals(1, calls.get());
    }

    // --- Handler registry ---

    public void testRegisterKeyRotationHandler() {
        Harness h = newHarness(clusterStateWith(null, true), 0L, TimeValue.timeValueDays(30));
        h.coordinator.registerKeyRotationHandler(handler("neil", new AtomicInteger(), true));
        h.coordinator.registerKeyRotationHandler(handler("geddy", new AtomicInteger(), true));
        assertEquals(Set.of("neil", "geddy"), h.coordinator.getRegisteredHandlerNames());
    }

    public void testRegisterKeyRotationHandlerDuplicateNameThrows() {
        Harness h = newHarness(clusterStateWith(null, true), 0L, TimeValue.timeValueDays(30));
        h.coordinator.registerKeyRotationHandler(handler("alex", new AtomicInteger(), true));
        IllegalStateException ex = expectThrows(
            IllegalStateException.class,
            () -> h.coordinator.registerKeyRotationHandler(handler("alex", new AtomicInteger(), true))
        );
        assertTrue(ex.getMessage(), ex.getMessage().contains("already registered"));
    }

    private static KeyRotationHandler handler(String name, AtomicInteger callCount, boolean succeed) {
        return new KeyRotationHandler() {
            @Override
            public String name() {
                return name;
            }

            @Override
            public void reEncrypt(String activeKeyId, ActionListener<Void> listener) {
                callCount.incrementAndGet();
                if (succeed) {
                    listener.onResponse(null);
                } else {
                    listener.onFailure(new RuntimeException("simulated"));
                }
            }
        };
    }
}
