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
import org.elasticsearch.xpack.core.crypto.PrimaryEncryptionKeyMetadata.RotationState;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class KeyRotationCoordinatorTests extends ESTestCase {

    private static final ClusterName CLUSTER_NAME = new ClusterName("test");

    private static byte[] randomKey() {
        byte[] keyBytes = new byte[32];
        random().nextBytes(keyBytes);
        return keyBytes;
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
        TimeValue rotationInterval
    ) {
        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenReturn(state);
        ThreadPool threadPool = mock(ThreadPool.class);
        return new KeyRotationCoordinator(
            clusterService,
            threadPool,
            pekService,
            rotationInterval,
            TimeValue.timeValueMinutes(1),
            () -> now
        );
    }

    public void testTickIsNoopWhenRotationDisabled() {
        PrimaryEncryptionKeyService pekService = mock(PrimaryEncryptionKeyService.class);
        ClusterState state = clusterStateWith(null, true);
        KeyRotationCoordinator coordinator = newCoordinator(pekService, state, 0L, TimeValue.ZERO);

        coordinator.tick();
        verify(pekService, never()).submitBeginRotation();
        verify(pekService, never()).submitRetireKeys();
    }

    public void testTickIsNoopWhenNotMaster() {
        PrimaryEncryptionKeyService pekService = mock(PrimaryEncryptionKeyService.class);
        ClusterState state = clusterStateWith(null, false);
        KeyRotationCoordinator coordinator = newCoordinator(pekService, state, 0L, TimeValue.timeValueDays(30));

        coordinator.tick();
        verify(pekService, never()).submitBeginRotation();
    }

    public void testTickIsNoopWhenNoMetadataInstalled() {
        PrimaryEncryptionKeyService pekService = mock(PrimaryEncryptionKeyService.class);
        ClusterState state = clusterStateWith(null, true);
        when(pekService.getCurrentMetadata(any())).thenReturn(null);

        KeyRotationCoordinator coordinator = newCoordinator(pekService, state, 0L, TimeValue.timeValueDays(30));
        coordinator.tick();
        verify(pekService, never()).submitBeginRotation();
    }

    public void testTickBeginsRotationWhenDue() {
        PrimaryEncryptionKeyService pekService = mock(PrimaryEncryptionKeyService.class);
        long lastRotated = 1_000_000_000L;
        long now = lastRotated + TimeValue.timeValueDays(30).millis() + 1;
        PrimaryEncryptionKeyMetadata metadata = new PrimaryEncryptionKeyMetadata(
            Map.of("k1", randomKey()),
            "k1",
            lastRotated,
            RotationState.STABLE
        );
        ClusterState state = clusterStateWith(metadata, true);
        when(pekService.getCurrentMetadata(any())).thenReturn(metadata);

        KeyRotationCoordinator coordinator = newCoordinator(pekService, state, now, TimeValue.timeValueDays(30));
        coordinator.tick();

        verify(pekService).submitBeginRotation();
    }

    public void testTickDoesNotBeginRotationWhenNotDue() {
        PrimaryEncryptionKeyService pekService = mock(PrimaryEncryptionKeyService.class);
        long lastRotated = 1_000_000_000L;
        long now = lastRotated + 1; // tiny elapsed time
        PrimaryEncryptionKeyMetadata metadata = new PrimaryEncryptionKeyMetadata(
            Map.of("k1", randomKey()),
            "k1",
            lastRotated,
            RotationState.STABLE
        );
        ClusterState state = clusterStateWith(metadata, true);
        when(pekService.getCurrentMetadata(any())).thenReturn(metadata);

        KeyRotationCoordinator coordinator = newCoordinator(pekService, state, now, TimeValue.timeValueDays(30));
        coordinator.tick();

        verify(pekService, never()).submitBeginRotation();
    }

    public void testTickInvokesAllHandlersAndRetiresOnSuccess() {
        PrimaryEncryptionKeyService pekService = mock(PrimaryEncryptionKeyService.class);
        PrimaryEncryptionKeyMetadata metadata = new PrimaryEncryptionKeyMetadata(
            Map.of("k1", randomKey(), "k2", randomKey()),
            "k2",
            0L,
            RotationState.ROTATING
        );
        ClusterState state = clusterStateWith(metadata, true);

        AtomicInteger alphaCalls = new AtomicInteger();
        AtomicInteger betaCalls = new AtomicInteger();
        AtomicInteger gammaCalls = new AtomicInteger();
        KeyRotationHandler alpha = handler("alpha", alphaCalls, true);
        KeyRotationHandler beta = handler("beta", betaCalls, true);
        KeyRotationHandler gamma = handler("gamma", gammaCalls, true);

        when(pekService.getCurrentMetadata(any())).thenReturn(metadata);
        when(pekService.getRegisteredHandlers()).thenReturn(List.<KeyRotationHandler>of(alpha, beta, gamma));

        KeyRotationCoordinator coordinator = newCoordinator(pekService, state, 0L, TimeValue.timeValueDays(30));
        coordinator.tick();

        assertEquals(1, alphaCalls.get());
        assertEquals(1, betaCalls.get());
        assertEquals(1, gammaCalls.get());

        verify(pekService).submitRetireKeys();
    }

    public void testTickRetiresImmediatelyWhenNoHandlersRegistered() {
        PrimaryEncryptionKeyService pekService = mock(PrimaryEncryptionKeyService.class);
        PrimaryEncryptionKeyMetadata metadata = new PrimaryEncryptionKeyMetadata(
            Map.of("k1", randomKey(), "k2", randomKey()),
            "k2",
            0L,
            RotationState.ROTATING
        );
        ClusterState state = clusterStateWith(metadata, true);
        when(pekService.getCurrentMetadata(any())).thenReturn(metadata);
        when(pekService.getRegisteredHandlers()).thenReturn(List.of());

        KeyRotationCoordinator coordinator = newCoordinator(pekService, state, 0L, TimeValue.timeValueDays(30));
        coordinator.tick();

        verify(pekService).submitRetireKeys();
    }

    public void testHandlerFailureDoesNotRetire() {
        PrimaryEncryptionKeyService pekService = mock(PrimaryEncryptionKeyService.class);
        PrimaryEncryptionKeyMetadata metadata = new PrimaryEncryptionKeyMetadata(
            Map.of("k1", randomKey(), "k2", randomKey()),
            "k2",
            0L,
            RotationState.ROTATING
        );
        ClusterState state = clusterStateWith(metadata, true);

        AtomicInteger calls = new AtomicInteger();
        KeyRotationHandler failing = handler("alpha", calls, false);

        when(pekService.getCurrentMetadata(any())).thenReturn(metadata);
        when(pekService.getRegisteredHandlers()).thenReturn(List.<KeyRotationHandler>of(failing));

        KeyRotationCoordinator coordinator = newCoordinator(pekService, state, 0L, TimeValue.timeValueDays(30));
        coordinator.tick();

        assertEquals(1, calls.get());
        verify(pekService, never()).submitRetireKeys();
    }

    public void testPartialFailurePreventsRetire() {
        PrimaryEncryptionKeyService pekService = mock(PrimaryEncryptionKeyService.class);
        PrimaryEncryptionKeyMetadata metadata = new PrimaryEncryptionKeyMetadata(
            Map.of("k1", randomKey(), "k2", randomKey()),
            "k2",
            0L,
            RotationState.ROTATING
        );
        ClusterState state = clusterStateWith(metadata, true);

        AtomicInteger successCalls = new AtomicInteger();
        AtomicInteger failCalls = new AtomicInteger();
        KeyRotationHandler succeeding = handler("alpha", successCalls, true);
        KeyRotationHandler failing = handler("beta", failCalls, false);

        when(pekService.getCurrentMetadata(any())).thenReturn(metadata);
        when(pekService.getRegisteredHandlers()).thenReturn(List.<KeyRotationHandler>of(succeeding, failing));

        KeyRotationCoordinator coordinator = newCoordinator(pekService, state, 0L, TimeValue.timeValueDays(30));
        coordinator.tick();

        assertEquals(1, successCalls.get());
        assertEquals(1, failCalls.get());
        verify(pekService, never()).submitRetireKeys();
    }

    public void testNextTickRetriesAllHandlers() {
        PrimaryEncryptionKeyService pekService = mock(PrimaryEncryptionKeyService.class);
        PrimaryEncryptionKeyMetadata metadata = new PrimaryEncryptionKeyMetadata(
            Map.of("k1", randomKey(), "k2", randomKey()),
            "k2",
            0L,
            RotationState.ROTATING
        );
        ClusterState state = clusterStateWith(metadata, true);

        AtomicInteger calls = new AtomicInteger();
        KeyRotationHandler failing = handler("alpha", calls, false);

        when(pekService.getCurrentMetadata(any())).thenReturn(metadata);
        when(pekService.getRegisteredHandlers()).thenReturn(List.<KeyRotationHandler>of(failing));

        KeyRotationCoordinator coordinator = newCoordinator(pekService, state, 0L, TimeValue.timeValueDays(30));
        coordinator.tick();
        coordinator.tick();
        coordinator.tick();

        assertEquals(3, calls.get());
        verify(pekService, times(0)).submitRetireKeys();
    }

    public void testStuckRotationLogsWarnWithDuration() throws Exception {
        // A handler that never completes its listener stalls rotation. The coordinator should keep
        // ticking and emit a WARN log with the elapsed duration so an operator can notice.
        PrimaryEncryptionKeyService pekService = mock(PrimaryEncryptionKeyService.class);
        PrimaryEncryptionKeyMetadata metadata = new PrimaryEncryptionKeyMetadata(
            Map.of("k1", randomKey(), "k2", randomKey()),
            "k2",
            0L,
            RotationState.ROTATING
        );
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

        AtomicLong now = new AtomicLong(1_000L);
        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenReturn(state);
        ThreadPool threadPool = mock(ThreadPool.class);
        KeyRotationCoordinator coordinator = new KeyRotationCoordinator(
            clusterService,
            threadPool,
            pekService,
            TimeValue.timeValueDays(30),
            TimeValue.timeValueMinutes(1),
            now::get
        );

        // First tick: enters rotate(), invokes the hanging handler
        coordinator.tick();
        assertEquals(1, calls.get());

        // Advance time and tick again. Should short-circuit and emit a WARN with the elapsed duration.
        now.set(1_000L + TimeValue.timeValueMinutes(5).millis());
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
        verify(pekService, never()).submitRetireKeys();
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
