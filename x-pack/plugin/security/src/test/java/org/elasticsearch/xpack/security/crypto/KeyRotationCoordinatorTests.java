/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.crypto;

import org.apache.logging.log4j.Level;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.crypto.KeyRotationHandler;
import org.elasticsearch.xpack.core.crypto.PrimaryEncryptionKeyMetadata;
import org.elasticsearch.xpack.core.crypto.PrimaryEncryptionKeyMetadata.KeyEntry;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

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

    private KeyRotationCoordinator newCoordinator(
        PrimaryEncryptionKeyService pekService,
        ClusterState state,
        long now,
        TimeValue rotationInterval,
        TimeValue checkInterval
    ) {
        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenReturn(state);
        ThreadPool threadPool = mock(ThreadPool.class);
        when(threadPool.absoluteTimeInMillis()).thenReturn(now);
        return new KeyRotationCoordinator(clusterService, threadPool, pekService, rotationInterval, checkInterval);
    }

    private KeyRotationCoordinator newCoordinator(
        PrimaryEncryptionKeyService pekService,
        ClusterState state,
        long now,
        TimeValue rotationInterval
    ) {
        return newCoordinator(pekService, state, now, rotationInterval, TimeValue.timeValueMinutes(1));
    }

    public void testTickIsNoopWhenRotationDisabled() {
        PrimaryEncryptionKeyService pekService = mock(PrimaryEncryptionKeyService.class);
        ClusterState state = clusterStateWith(null, true);
        KeyRotationCoordinator coordinator = newCoordinator(pekService, state, 0L, TimeValue.ZERO);

        coordinator.tick();
        verify(pekService, never()).submitBeginRotation(state);
        verify(pekService, never()).submitRetireKeys(any(), anyLong());
    }

    public void testTickIsNoopWhenNotMaster() {
        PrimaryEncryptionKeyService pekService = mock(PrimaryEncryptionKeyService.class);
        ClusterState state = clusterStateWith(null, false);
        KeyRotationCoordinator coordinator = newCoordinator(pekService, state, 0L, TimeValue.timeValueDays(30));

        coordinator.tick();
        verify(pekService, never()).submitBeginRotation(state);
    }

    public void testTickIsNoopWhenNoMetadataInstalled() {
        PrimaryEncryptionKeyService pekService = mock(PrimaryEncryptionKeyService.class);
        ClusterState state = clusterStateWith(null, true);
        when(pekService.getCurrentMetadata(any())).thenReturn(null);

        KeyRotationCoordinator coordinator = newCoordinator(pekService, state, 0L, TimeValue.timeValueDays(30));
        coordinator.tick();
        verify(pekService, never()).submitBeginRotation(state);
    }

    public void testTickBeginsRotationWhenActiveKeyIsOldEnough() {
        PrimaryEncryptionKeyService pekService = mock(PrimaryEncryptionKeyService.class);
        long generatedAt = 1_000_000_000L;
        long now = generatedAt + TimeValue.timeValueDays(30).millis() + 1;
        PrimaryEncryptionKeyMetadata metadata = new PrimaryEncryptionKeyMetadata(Map.of("k1", entry(generatedAt)), "k1");
        ClusterState state = clusterStateWith(metadata, true);
        when(pekService.getCurrentMetadata(any())).thenReturn(metadata);

        KeyRotationCoordinator coordinator = newCoordinator(pekService, state, now, TimeValue.timeValueDays(30));
        coordinator.tick();

        verify(pekService).submitBeginRotation(state);
    }

    public void testTickDoesNotBeginRotationWhenActiveKeyIsYoung() {
        PrimaryEncryptionKeyService pekService = mock(PrimaryEncryptionKeyService.class);
        long generatedAt = 1_000_000_000L;
        long now = generatedAt + 1;  // tiny elapsed time
        PrimaryEncryptionKeyMetadata metadata = new PrimaryEncryptionKeyMetadata(Map.of("k1", entry(generatedAt)), "k1");
        ClusterState state = clusterStateWith(metadata, true);
        when(pekService.getCurrentMetadata(any())).thenReturn(metadata);

        KeyRotationCoordinator coordinator = newCoordinator(pekService, state, now, TimeValue.timeValueDays(30));
        coordinator.tick();

        verify(pekService, never()).submitBeginRotation(state);
    }

    public void testTickInvokesAllHandlersOnEveryTick() {
        // Continuous scrub: handlers run every tick regardless of "state".
        PrimaryEncryptionKeyService pekService = mock(PrimaryEncryptionKeyService.class);
        long generatedAt = 100L;
        long now = 200L;
        PrimaryEncryptionKeyMetadata metadata = new PrimaryEncryptionKeyMetadata(Map.of("k1", entry(generatedAt)), "k1");
        ClusterState state = clusterStateWith(metadata, true);

        AtomicInteger alphaCalls = new AtomicInteger();
        AtomicInteger betaCalls = new AtomicInteger();
        KeyRotationHandler alpha = handler("alpha", alphaCalls, true);
        KeyRotationHandler beta = handler("beta", betaCalls, true);

        when(pekService.getCurrentMetadata(any())).thenReturn(metadata);
        when(pekService.getRegisteredHandlers()).thenReturn(List.of(alpha, beta));

        KeyRotationCoordinator coordinator = newCoordinator(pekService, state, now, TimeValue.timeValueDays(30));
        coordinator.tick();

        assertEquals(1, alphaCalls.get());
        assertEquals(1, betaCalls.get());
    }

    public void testTickSubmitsRetireForKeysOlderThanGrace() {
        // Grace = 10 * checkInterval = 10m. A key is retire-eligible when its deactivation time (= the next-newer key's generatedAt)
        // is older than `now - grace`. Active key is 15m old, so the previous key was deactivated 15m ago — past the 10m grace.
        TimeValue checkInterval = TimeValue.timeValueMinutes(1);
        long now = 1_000_000L;
        long graceMillis = 10 * checkInterval.millis();
        long activeGeneratedAt = now - 15 * 60 * 1000L;
        long oldGeneratedAt = now - 30 * 60 * 1000L;
        PrimaryEncryptionKeyService pekService = mock(PrimaryEncryptionKeyService.class);
        PrimaryEncryptionKeyMetadata metadata = new PrimaryEncryptionKeyMetadata(
            Map.of("old", entry(oldGeneratedAt), "active", entry(activeGeneratedAt)),
            "active"
        );
        ClusterState state = clusterStateWith(metadata, true);
        when(pekService.getCurrentMetadata(any())).thenReturn(metadata);
        when(pekService.getRegisteredHandlers()).thenReturn(List.of());

        KeyRotationCoordinator coordinator = newCoordinator(pekService, state, now, TimeValue.timeValueDays(30), checkInterval);
        coordinator.tick();

        verify(pekService).submitRetireKeys(state, now - graceMillis);
    }

    public void testTickDoesNotSubmitRetireWhenAllKeysWithinGrace() {
        // The non-active key was deactivated 1m ago (when the active key was generated) — well within the 10m grace.
        TimeValue checkInterval = TimeValue.timeValueMinutes(1);
        long now = 1_000_000L;
        long activeGeneratedAt = now - 60 * 1000L;
        long oldGeneratedAt = now - 5 * 60 * 1000L;

        PrimaryEncryptionKeyService pekService = mock(PrimaryEncryptionKeyService.class);
        PrimaryEncryptionKeyMetadata metadata = new PrimaryEncryptionKeyMetadata(
            Map.of("old", entry(oldGeneratedAt), "active", entry(activeGeneratedAt)),
            "active"
        );
        ClusterState state = clusterStateWith(metadata, true);
        when(pekService.getCurrentMetadata(any())).thenReturn(metadata);
        when(pekService.getRegisteredHandlers()).thenReturn(List.of());

        KeyRotationCoordinator coordinator = newCoordinator(pekService, state, now, TimeValue.timeValueDays(30), checkInterval);
        coordinator.tick();

        verify(pekService, never()).submitRetireKeys(any(), anyLong());
    }

    public void testTickBeginsRotationAndAlsoInvokesHandlers() {
        // When active key is old enough, the tick should BOTH invoke handlers AND submit BeginRotation.
        PrimaryEncryptionKeyService pekService = mock(PrimaryEncryptionKeyService.class);
        long generatedAt = 1_000_000_000L;
        long now = generatedAt + TimeValue.timeValueDays(30).millis() + 1;
        PrimaryEncryptionKeyMetadata metadata = new PrimaryEncryptionKeyMetadata(Map.of("k1", entry(generatedAt)), "k1");
        ClusterState state = clusterStateWith(metadata, true);

        AtomicInteger calls = new AtomicInteger();
        KeyRotationHandler h = handler("alpha", calls, true);
        when(pekService.getCurrentMetadata(any())).thenReturn(metadata);
        when(pekService.getRegisteredHandlers()).thenReturn(List.of(h));

        KeyRotationCoordinator coordinator = newCoordinator(pekService, state, now, TimeValue.timeValueDays(30));
        coordinator.tick();

        assertEquals(1, calls.get());
        verify(pekService).submitBeginRotation(state);
    }

    public void testStuckRotationLogsWarnWithDuration() throws Exception {
        // A handler that never completes its listener stalls rotation. The next tick should emit a WARN log with the elapsed duration.
        PrimaryEncryptionKeyService pekService = mock(PrimaryEncryptionKeyService.class);
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

        when(pekService.getCurrentMetadata(any())).thenReturn(metadata);
        when(pekService.getRegisteredHandlers()).thenReturn(List.of(hanging));

        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenReturn(state);
        ThreadPool threadPool = mock(ThreadPool.class);
        long t0 = 1_000L;
        long t1 = t0 + TimeValue.timeValueMinutes(5).millis();
        when(threadPool.absoluteTimeInMillis()).thenReturn(t0, t1);
        KeyRotationCoordinator coordinator = new KeyRotationCoordinator(
            clusterService,
            threadPool,
            pekService,
            TimeValue.timeValueDays(30),
            TimeValue.timeValueMinutes(1)
        );

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
