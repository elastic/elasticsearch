/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.crypto;

import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.project.DefaultProjectResolver;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.crypto.PrimaryEncryptionKeyMetadata;
import org.elasticsearch.xpack.core.crypto.PrimaryEncryptionKeyMetadata.RotationState;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class KeyRotationExecutorTests extends ESTestCase {

    private static final ClusterName CLUSTER_NAME = new ClusterName("test");
    private final PrimaryEncryptionKeyService.KeyRotationExecutor executor = new PrimaryEncryptionKeyService.KeyRotationExecutor(
        DefaultProjectResolver.INSTANCE
    );

    private static byte[] randomKey() {
        byte[] keyBytes = new byte[32];
        random().nextBytes(keyBytes);
        return keyBytes;
    }

    private static ClusterState stateWith(PrimaryEncryptionKeyMetadata metadata) {
        ProjectMetadata.Builder project = ProjectMetadata.builder(Metadata.DEFAULT_PROJECT_ID);
        if (metadata != null) {
            project.putCustom(PrimaryEncryptionKeyMetadata.TYPE, metadata);
        }
        return ClusterState.builder(CLUSTER_NAME).metadata(Metadata.builder().put(project.build()).build()).build();
    }

    private static PrimaryEncryptionKeyMetadata metadataOf(ClusterState state) {
        return state.metadata().getProject(Metadata.DEFAULT_PROJECT_ID).custom(PrimaryEncryptionKeyMetadata.TYPE);
    }

    public void testInstallInitialKeyOnEmptyState() {
        ClusterState state = stateWith(null);
        Tuple<ClusterState, Void> result = executor.executeTask(new PrimaryEncryptionKeyService.InstallKeyTask(), state);

        PrimaryEncryptionKeyMetadata metadata = metadataOf(result.v1());
        assertNotNull(metadata);
        assertEquals(1, metadata.getKeys().size());
        assertNotNull(metadata.getActiveKeyId());
        assertEquals(RotationState.STABLE, metadata.getRotationState());
        assertTrue(metadata.getLastRotatedMillis() > 0);
    }

    public void testInstallInitialKeyIsNoopWhenKeyExists() {
        PrimaryEncryptionKeyMetadata existing = new PrimaryEncryptionKeyMetadata(
            Map.of("k1", randomKey()),
            "k1",
            42L,
            RotationState.STABLE
        );
        ClusterState state = stateWith(existing);
        Tuple<ClusterState, Void> result = executor.executeTask(new PrimaryEncryptionKeyService.InstallKeyTask(), state);
        assertSame(state, result.v1());
    }

    public void testBeginRotationAddsKeyAndFlipsState() {
        byte[] originalKey = randomKey();
        PrimaryEncryptionKeyMetadata existing = new PrimaryEncryptionKeyMetadata(
            Map.of("k1", originalKey),
            "k1",
            100L,
            RotationState.STABLE
        );
        ClusterState state = stateWith(existing);
        Tuple<ClusterState, Void> result = executor.executeTask(new PrimaryEncryptionKeyService.BeginRotationTask(), state);

        PrimaryEncryptionKeyMetadata metadata = metadataOf(result.v1());
        assertEquals(2, metadata.getKeys().size());
        assertNotEquals("k1", metadata.getActiveKeyId());
        assertTrue(metadata.getKeys().containsKey("k1"));
        assertEquals(RotationState.ROTATING, metadata.getRotationState());
        // lastRotatedMillis is preserved during rotation; only updated on retire.
        assertEquals(100L, metadata.getLastRotatedMillis());
    }

    public void testBeginRotationIsNoopWhenAlreadyRotating() {
        PrimaryEncryptionKeyMetadata existing = new PrimaryEncryptionKeyMetadata(
            Map.of("k1", randomKey(), "k2", randomKey()),
            "k2",
            100L,
            RotationState.ROTATING
        );
        ClusterState state = stateWith(existing);
        Tuple<ClusterState, Void> result = executor.executeTask(new PrimaryEncryptionKeyService.BeginRotationTask(), state);
        assertSame(state, result.v1());
    }

    public void testBeginRotationIsNoopWhenNoMetadata() {
        ClusterState state = stateWith(null);
        Tuple<ClusterState, Void> result = executor.executeTask(new PrimaryEncryptionKeyService.BeginRotationTask(), state);
        assertSame(state, result.v1());
    }

    public void testRetireKeysSucceeds() {
        Map<String, byte[]> keys = new HashMap<>();
        keys.put("k1", randomKey());
        keys.put("k2", randomKey());
        keys.put("k3", randomKey());
        PrimaryEncryptionKeyMetadata existing = new PrimaryEncryptionKeyMetadata(keys, "k3", 100L, RotationState.ROTATING);
        ClusterState state = stateWith(existing);
        Tuple<ClusterState, Void> result = executor.executeTask(new PrimaryEncryptionKeyService.RetireKeysTask(), state);
        PrimaryEncryptionKeyMetadata metadata = metadataOf(result.v1());
        assertEquals(Set.of("k3"), metadata.getKeys().keySet());
        assertEquals("k3", metadata.getActiveKeyId());
        assertEquals(RotationState.STABLE, metadata.getRotationState());
        assertTrue(metadata.getLastRotatedMillis() >= 100L);
    }

    public void testRetireKeysIgnoredWhenStable() {
        PrimaryEncryptionKeyMetadata existing = new PrimaryEncryptionKeyMetadata(
            Map.of("k1", randomKey()),
            "k1",
            100L,
            RotationState.STABLE
        );
        ClusterState state = stateWith(existing);
        Tuple<ClusterState, Void> result = executor.executeTask(new PrimaryEncryptionKeyService.RetireKeysTask(), state);
        assertSame(state, result.v1());
    }

    public void testFullRotationCycleStateTransitions() {
        // 1. Install initial key
        ClusterState state = stateWith(null);
        state = executor.executeTask(new PrimaryEncryptionKeyService.InstallKeyTask(), state).v1();
        PrimaryEncryptionKeyMetadata m = metadataOf(state);
        String originalKeyId = m.getActiveKeyId();
        assertEquals(RotationState.STABLE, m.getRotationState());

        // 2. Begin rotation
        state = executor.executeTask(new PrimaryEncryptionKeyService.BeginRotationTask(), state).v1();
        m = metadataOf(state);
        String newKeyId = m.getActiveKeyId();
        assertNotEquals(originalKeyId, newKeyId);
        assertEquals(RotationState.ROTATING, m.getRotationState());
        assertEquals(2, m.getKeys().size());

        // 3. Retire old keys (handlers complete outside cluster state now)
        state = executor.executeTask(new PrimaryEncryptionKeyService.RetireKeysTask(), state).v1();
        m = metadataOf(state);
        assertEquals(Set.of(newKeyId), m.getKeys().keySet());
        assertEquals(RotationState.STABLE, m.getRotationState());
    }
}
