/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.encryption;

import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.project.DefaultProjectResolver;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.encryption.ProjectEncryptionKeyMetadata.KeyEntry;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class KeyRotationExecutorTests extends ESTestCase {

    private static final ClusterName CLUSTER_NAME = new ClusterName("test");
    private static final String PASSWORD_ID = "v1";
    private final ThreadPool threadPool = mockThreadPool();
    private final KeyRotationCoordinator.KeyRotationExecutor executor = new KeyRotationCoordinator.KeyRotationExecutor(
        DefaultProjectResolver.INSTANCE,
        threadPool
    );

    private static ThreadPool mockThreadPool() {
        ThreadPool tp = mock(ThreadPool.class);
        when(tp.absoluteTimeInMillis()).thenReturn(System.currentTimeMillis());
        return tp;
    }

    private static byte[] randomKey() {
        byte[] keyBytes = new byte[PasswordBasedEncryption.PEK_LENGTH_BYTES];
        random().nextBytes(keyBytes);
        return keyBytes;
    }

    private static KeyEntry entry(long generatedAt) {
        return new KeyEntry(randomKey(), generatedAt);
    }

    private static ClusterState stateWith(ProjectEncryptionKeyMetadata metadata) {
        ProjectMetadata.Builder project = ProjectMetadata.builder(Metadata.DEFAULT_PROJECT_ID);
        if (metadata != null) {
            project.putCustom(ProjectEncryptionKeyMetadata.TYPE, metadata);
        }
        return ClusterState.builder(CLUSTER_NAME).metadata(Metadata.builder().put(project.build()).build()).build();
    }

    private static ProjectEncryptionKeyMetadata metadataOf(ClusterState state) {
        return state.metadata().getProject(Metadata.DEFAULT_PROJECT_ID).custom(ProjectEncryptionKeyMetadata.TYPE);
    }

    public void testInstallInitialKeyOnEmptyState() {
        ClusterState state = stateWith(null);
        String newKeyId = ProjectEncryptionKeyMetadata.generateKeyId();
        long generatedAt = System.currentTimeMillis();
        Tuple<ClusterState, Void> result = executor.executeTask(
            new KeyRotationCoordinator.InstallKeyTask(newKeyId, randomKey(), PASSWORD_ID, generatedAt),
            state
        );

        ProjectEncryptionKeyMetadata metadata = metadataOf(result.v1());
        assertNotNull(metadata);
        assertEquals(1, metadata.getKeys().size());
        assertEquals(newKeyId, metadata.getActiveKeyId());
        assertEquals(PASSWORD_ID, metadata.getPasswordId());
        assertEquals(generatedAt, metadata.getGeneratedAt(newKeyId));
    }

    public void testInstallInitialKeyIsNoopWhenKeyExists() {
        ProjectEncryptionKeyMetadata existing = new ProjectEncryptionKeyMetadata(Map.of("k1", entry(42L)), "k1", PASSWORD_ID);
        ClusterState state = stateWith(existing);
        Tuple<ClusterState, Void> result = executor.executeTask(
            new KeyRotationCoordinator.InstallKeyTask(ProjectEncryptionKeyMetadata.generateKeyId(), randomKey(), PASSWORD_ID, 0L),
            state
        );
        assertSame(state, result.v1());
    }

    public void testBeginRotationAddsKeyAndAdvancesActive() {
        long activeBornAt = System.currentTimeMillis() - 60_000L;
        ProjectEncryptionKeyMetadata existing = new ProjectEncryptionKeyMetadata(Map.of("k1", entry(activeBornAt)), "k1", PASSWORD_ID);
        ClusterState state = stateWith(existing);
        String newKeyId = ProjectEncryptionKeyMetadata.generateKeyId();
        long newGeneratedAt = activeBornAt + 1000L;
        Tuple<ClusterState, Void> result = executor.executeTask(
            new KeyRotationCoordinator.BeginRotationTask(newKeyId, randomKey(), PASSWORD_ID, newGeneratedAt),
            state
        );

        ProjectEncryptionKeyMetadata metadata = metadataOf(result.v1());
        assertEquals(2, metadata.getKeys().size());
        assertEquals(newKeyId, metadata.getActiveKeyId());
        assertEquals(PASSWORD_ID, metadata.getPasswordId());
        assertTrue("k1 should remain in the map", metadata.getKeys().containsKey("k1"));
        assertEquals(activeBornAt, metadata.getGeneratedAt("k1"));
        assertEquals(newGeneratedAt, metadata.getGeneratedAt(newKeyId));
    }

    public void testBeginRotationIsNoopWhenNoMetadata() {
        ClusterState state = stateWith(null);
        Tuple<ClusterState, Void> result = executor.executeTask(
            new KeyRotationCoordinator.BeginRotationTask(ProjectEncryptionKeyMetadata.generateKeyId(), randomKey(), PASSWORD_ID, 0L),
            state
        );
        assertSame(state, result.v1());
    }

    public void testBeginRotationDropsWhenPasswordIdDrifted() {
        // Existing metadata under PASSWORD_ID, but the task pre-wrapped under a different id (a password rotation slipped in between
        // the pre-wrap fork and the master-thread apply). Executor must drop and let the next tick re-attempt.
        ProjectEncryptionKeyMetadata existing = new ProjectEncryptionKeyMetadata(Map.of("k1", entry(0L)), "k1", PASSWORD_ID);
        ClusterState state = stateWith(existing);
        Tuple<ClusterState, Void> result = executor.executeTask(
            new KeyRotationCoordinator.BeginRotationTask(ProjectEncryptionKeyMetadata.generateKeyId(), randomKey(), "other-password", 100L),
            state
        );
        assertSame(state, result.v1());
    }

    public void testRotatePasswordSwapsAllEntriesAndPasswordId() {
        ProjectEncryptionKeyMetadata existing = new ProjectEncryptionKeyMetadata(
            Map.of("k1", entry(0L), "k2", entry(100L)),
            "k2",
            PASSWORD_ID,
            Map.of("some_handler", "k1")
        );
        ClusterState state = stateWith(existing);

        Map<String, KeyEntry> rewrapped = Map.of("k1", new KeyEntry(randomKey(), 0L), "k2", new KeyEntry(randomKey(), 100L));
        Tuple<ClusterState, Void> result = executor.executeTask(
            new KeyRotationCoordinator.RotatePasswordTask(rewrapped, "v2", PASSWORD_ID),
            state
        );

        ProjectEncryptionKeyMetadata updated = metadataOf(result.v1());
        assertEquals("v2", updated.getPasswordId());
        assertEquals("k2", updated.getActiveKeyId());
        assertEquals(Set.of("k1", "k2"), updated.getKeys().keySet());
        assertArrayEquals(rewrapped.get("k1").bytes(), updated.getKeys().get("k1").bytes());
        assertArrayEquals(rewrapped.get("k2").bytes(), updated.getKeys().get("k2").bytes());
        assertEquals("handlerKeyIds preserved across password rotation", Map.of("some_handler", "k1"), updated.getHandlerKeyIds());
    }

    public void testRotatePasswordDropsWhenAlreadyOnNewPassword() {
        // Idempotent: a duplicate task arriving after the rotation has already applied is a no-op.
        ProjectEncryptionKeyMetadata existing = new ProjectEncryptionKeyMetadata(Map.of("k1", entry(0L)), "k1", "v2");
        ClusterState state = stateWith(existing);

        Tuple<ClusterState, Void> result = executor.executeTask(
            new KeyRotationCoordinator.RotatePasswordTask(Map.of("k1", new KeyEntry(randomKey(), 0L)), "v2", PASSWORD_ID),
            state
        );
        assertSame(state, result.v1());
    }

    public void testRotatePasswordDropsOnKeySetChange() {
        ProjectEncryptionKeyMetadata existing = new ProjectEncryptionKeyMetadata(
            Map.of("k1", entry(0L), "k2", entry(100L)),
            "k2",
            PASSWORD_ID
        );
        ClusterState state = stateWith(existing);

        // Rewrapped map only has one entry — key set changed between pre-rewrap and apply (a rotation slipped in).
        Tuple<ClusterState, Void> result = executor.executeTask(
            new KeyRotationCoordinator.RotatePasswordTask(Map.of("k1", new KeyEntry(randomKey(), 0L)), "v2", PASSWORD_ID),
            state
        );
        assertSame(state, result.v1());
    }

    public void testRetireKeysDropsOnlyKeysBelowCutoff() {
        // Retirement uses each key's deactivation time (= generatedAt of the next-newer key), not its own generatedAt.
        Map<String, KeyEntry> entries = new HashMap<>();
        entries.put("old", entry(100L));
        entries.put("active", entry(500L));
        entries.put("recent", entry(400L));
        ProjectEncryptionKeyMetadata existing = new ProjectEncryptionKeyMetadata(entries, "active", PASSWORD_ID);
        ClusterState state = stateWith(existing);

        Tuple<ClusterState, Void> result = executor.executeTask(new KeyRotationCoordinator.RetireKeysTask(450L), state);

        ProjectEncryptionKeyMetadata metadata = metadataOf(result.v1());
        assertEquals("only 'old' should have been retired", Set.of("active", "recent"), metadata.getKeys().keySet());
        assertEquals("active", metadata.getActiveKeyId());
    }

    public void testRetireKeysNeverRetiresActiveKey() {
        // Even if the active key's generatedAt is below the cutoff, it must not be retired.
        Map<String, KeyEntry> entries = new HashMap<>();
        entries.put("active", entry(100L));
        entries.put("old", entry(50L));
        ProjectEncryptionKeyMetadata existing = new ProjectEncryptionKeyMetadata(entries, "active", PASSWORD_ID);
        ClusterState state = stateWith(existing);

        long cutoff = 200L;
        Tuple<ClusterState, Void> result = executor.executeTask(new KeyRotationCoordinator.RetireKeysTask(cutoff), state);

        ProjectEncryptionKeyMetadata metadata = metadataOf(result.v1());
        assertEquals(Set.of("active"), metadata.getKeys().keySet());
        assertEquals("active", metadata.getActiveKeyId());
    }

    public void testRetireKeysIsNoopWhenNothingToRetire() {
        ProjectEncryptionKeyMetadata existing = new ProjectEncryptionKeyMetadata(Map.of("active", entry(1000L)), "active", PASSWORD_ID);
        ClusterState state = stateWith(existing);

        Tuple<ClusterState, Void> result = executor.executeTask(new KeyRotationCoordinator.RetireKeysTask(500L), state);
        assertSame(state, result.v1());
    }

    public void testRetireKeysIsNoopWhenNoMetadata() {
        ClusterState state = stateWith(null);
        Tuple<ClusterState, Void> result = executor.executeTask(new KeyRotationCoordinator.RetireKeysTask(100L), state);
        assertSame(state, result.v1());
    }
}
