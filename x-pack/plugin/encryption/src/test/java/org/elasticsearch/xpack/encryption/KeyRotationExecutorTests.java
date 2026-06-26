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
import org.elasticsearch.xpack.encryption.ProjectEncryptionKeyMetadata.KeyEntry;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

public class KeyRotationExecutorTests extends ESTestCase {

    private static final ClusterName CLUSTER_NAME = new ClusterName("test");
    private static final String PASSWORD_ID = "v1";
    private static final ProjectEncryptionKeyMetadata.PekEncryption NO_OP_ENCRYPTION = TestPekEncryption.NO_OP;
    private final KeyRotationCoordinator.KeyRotationExecutor executor = new KeyRotationCoordinator.KeyRotationExecutor(
        DefaultProjectResolver.INSTANCE,
        NO_OP_ENCRYPTION
    );

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
        ProjectEncryptionKeyMetadata existing = new ProjectEncryptionKeyMetadata(
            Map.of("k1", entry(42L)),
            "k1",
            PASSWORD_ID,
            Map.of(),
            NO_OP_ENCRYPTION
        );
        ClusterState state = stateWith(existing);
        Tuple<ClusterState, Void> result = executor.executeTask(
            new KeyRotationCoordinator.InstallKeyTask(ProjectEncryptionKeyMetadata.generateKeyId(), randomKey(), PASSWORD_ID, 0L),
            state
        );
        assertSame(state, result.v1());
    }

    public void testBeginRotationAddsKeyAndAdvancesActive() {
        long activeBornAt = System.currentTimeMillis() - 60_000L;
        ProjectEncryptionKeyMetadata existing = new ProjectEncryptionKeyMetadata(
            Map.of("k1", entry(activeBornAt)),
            "k1",
            PASSWORD_ID,
            Map.of(),
            NO_OP_ENCRYPTION
        );
        ClusterState state = stateWith(existing);
        String newKeyId = ProjectEncryptionKeyMetadata.generateKeyId();
        long newGeneratedAt = activeBornAt + 1000L;
        Tuple<ClusterState, Void> result = executor.executeTask(
            new KeyRotationCoordinator.BeginRotationTask(newKeyId, randomKey(), newGeneratedAt, new AtomicBoolean()),
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
            new KeyRotationCoordinator.BeginRotationTask(
                ProjectEncryptionKeyMetadata.generateKeyId(),
                randomKey(),
                0L,
                new AtomicBoolean()
            ),
            state
        );
        assertSame(state, result.v1());
    }

    public void testRotatePasswordUpdatesPasswordIdOnly() {
        byte[] k1Bytes = randomKey();
        byte[] k2Bytes = randomKey();
        ProjectEncryptionKeyMetadata existing = new ProjectEncryptionKeyMetadata(
            Map.of("k1", new KeyEntry(k1Bytes, 0L), "k2", new KeyEntry(k2Bytes, 100L)),
            "k2",
            PASSWORD_ID,
            Map.of("some_handler", "k1"),
            NO_OP_ENCRYPTION
        );
        ClusterState state = stateWith(existing);

        Tuple<ClusterState, Void> result = executor.executeTask(new KeyRotationCoordinator.UpdatePasswordIdTask("v2", PASSWORD_ID), state);

        ProjectEncryptionKeyMetadata updated = metadataOf(result.v1());
        assertEquals("v2", updated.getPasswordId());
        assertEquals("k2", updated.getActiveKeyId());
        assertEquals(Set.of("k1", "k2"), updated.getKeys().keySet());
        assertArrayEquals("plaintext key bytes must be unchanged", k1Bytes, updated.getKeys().get("k1").bytes());
        assertArrayEquals("plaintext key bytes must be unchanged", k2Bytes, updated.getKeys().get("k2").bytes());
        assertEquals("handlerKeyIds preserved across password id change", Map.of("some_handler", "k1"), updated.getHandlerKeyIds());
    }

    public void testRotatePasswordDropsWhenAlreadyOnNewPassword() {
        ProjectEncryptionKeyMetadata existing = new ProjectEncryptionKeyMetadata(
            Map.of("k1", entry(0L)),
            "k1",
            "v2",
            Map.of(),
            NO_OP_ENCRYPTION
        );
        ClusterState state = stateWith(existing);

        Tuple<ClusterState, Void> result = executor.executeTask(new KeyRotationCoordinator.UpdatePasswordIdTask("v2", PASSWORD_ID), state);
        assertSame(state, result.v1());
    }

    public void testRetireKeysDropsOnlyKeysBelowCutoff() {
        Map<String, KeyEntry> entries = new HashMap<>();
        entries.put("old", entry(100L));
        entries.put("active", entry(500L));
        entries.put("recent", entry(400L));
        ProjectEncryptionKeyMetadata existing = new ProjectEncryptionKeyMetadata(
            entries,
            "active",
            PASSWORD_ID,
            Map.of(),
            NO_OP_ENCRYPTION
        );
        ClusterState state = stateWith(existing);

        Tuple<ClusterState, Void> result = executor.executeTask(new KeyRotationCoordinator.RetireKeysTask(450L), state);

        ProjectEncryptionKeyMetadata metadata = metadataOf(result.v1());
        assertEquals("only 'old' should have been retired", Set.of("active", "recent"), metadata.getKeys().keySet());
        assertEquals("active", metadata.getActiveKeyId());
    }

    public void testRetireKeysNeverRetiresActiveKey() {
        Map<String, KeyEntry> entries = new HashMap<>();
        entries.put("active", entry(100L));
        entries.put("old", entry(50L));
        ProjectEncryptionKeyMetadata existing = new ProjectEncryptionKeyMetadata(
            entries,
            "active",
            PASSWORD_ID,
            Map.of(),
            NO_OP_ENCRYPTION
        );
        ClusterState state = stateWith(existing);

        long cutoff = 200L;
        Tuple<ClusterState, Void> result = executor.executeTask(new KeyRotationCoordinator.RetireKeysTask(cutoff), state);

        ProjectEncryptionKeyMetadata metadata = metadataOf(result.v1());
        assertEquals(Set.of("active"), metadata.getKeys().keySet());
        assertEquals("active", metadata.getActiveKeyId());
    }

    public void testRetireKeysIsNoopWhenNothingToRetire() {
        ProjectEncryptionKeyMetadata existing = new ProjectEncryptionKeyMetadata(
            Map.of("active", entry(1000L)),
            "active",
            PASSWORD_ID,
            Map.of(),
            NO_OP_ENCRYPTION
        );
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
