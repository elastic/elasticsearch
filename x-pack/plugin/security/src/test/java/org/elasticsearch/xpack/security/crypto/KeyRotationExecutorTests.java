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
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.crypto.PrimaryEncryptionKeyMetadata;
import org.elasticsearch.xpack.core.crypto.PrimaryEncryptionKeyMetadata.KeyEntry;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class KeyRotationExecutorTests extends ESTestCase {

    private static final ClusterName CLUSTER_NAME = new ClusterName("test");
    private final ThreadPool threadPool = mockThreadPool();
    private final PrimaryEncryptionKeyService.KeyRotationExecutor executor = new PrimaryEncryptionKeyService.KeyRotationExecutor(
        DefaultProjectResolver.INSTANCE,
        threadPool
    );

    private static ThreadPool mockThreadPool() {
        ThreadPool tp = mock(ThreadPool.class);
        when(tp.absoluteTimeInMillis()).thenReturn(System.currentTimeMillis());
        return tp;
    }

    private static byte[] randomKey() {
        byte[] keyBytes = new byte[32];
        random().nextBytes(keyBytes);
        return keyBytes;
    }

    private static KeyEntry entry(long generatedAt) {
        return new KeyEntry(randomKey(), generatedAt);
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
        String activeKeyId = metadata.getActiveKeyId();
        assertNotNull(activeKeyId);
        assertTrue("generatedAt should be set on initial install", metadata.getGeneratedAt(activeKeyId) > 0);
    }

    public void testInstallInitialKeyIsNoopWhenKeyExists() {
        PrimaryEncryptionKeyMetadata existing = new PrimaryEncryptionKeyMetadata(Map.of("k1", entry(42L)), "k1");
        ClusterState state = stateWith(existing);
        Tuple<ClusterState, Void> result = executor.executeTask(new PrimaryEncryptionKeyService.InstallKeyTask(), state);
        assertSame(state, result.v1());
    }

    public void testBeginRotationAddsKeyAndAdvancesActive() {
        long activeBornAt = System.currentTimeMillis() - 60_000L;
        PrimaryEncryptionKeyMetadata existing = new PrimaryEncryptionKeyMetadata(Map.of("k1", entry(activeBornAt)), "k1");
        ClusterState state = stateWith(existing);
        Tuple<ClusterState, Void> result = executor.executeTask(new PrimaryEncryptionKeyService.BeginRotationTask(), state);

        PrimaryEncryptionKeyMetadata metadata = metadataOf(result.v1());
        assertEquals(2, metadata.getKeys().size());
        assertNotEquals("k1", metadata.getActiveKeyId());
        assertTrue("k1 should remain in the map", metadata.getKeys().containsKey("k1"));
        assertEquals(activeBornAt, metadata.getGeneratedAt("k1"));
        assertTrue("new active key has fresh generatedAt", metadata.getGeneratedAt(metadata.getActiveKeyId()) > activeBornAt);
    }

    public void testBeginRotationIsNoopWhenNoMetadata() {
        ClusterState state = stateWith(null);
        Tuple<ClusterState, Void> result = executor.executeTask(new PrimaryEncryptionKeyService.BeginRotationTask(), state);
        assertSame(state, result.v1());
    }

    public void testRetireKeysDropsOnlyKeysBelowCutoff() {
        // Retirement uses each key's deactivation time (= generatedAt of the next-newer key), not its own generatedAt.
        Map<String, KeyEntry> entries = new HashMap<>();
        entries.put("old", entry(100L));
        entries.put("active", entry(500L));
        entries.put("recent", entry(400L));
        PrimaryEncryptionKeyMetadata existing = new PrimaryEncryptionKeyMetadata(entries, "active");
        ClusterState state = stateWith(existing);

        Tuple<ClusterState, Void> result = executor.executeTask(new PrimaryEncryptionKeyService.RetireKeysTask(450L), state);

        PrimaryEncryptionKeyMetadata metadata = metadataOf(result.v1());
        assertEquals("only 'old' should have been retired", Set.of("active", "recent"), metadata.getKeys().keySet());
        assertEquals("active", metadata.getActiveKeyId());
    }

    public void testRetireKeysNeverRetiresActiveKey() {
        // Even if the active key's generatedAt is below the cutoff, it must not be retired.
        Map<String, KeyEntry> entries = new HashMap<>();
        entries.put("active", entry(100L));
        entries.put("old", entry(50L));
        PrimaryEncryptionKeyMetadata existing = new PrimaryEncryptionKeyMetadata(entries, "active");
        ClusterState state = stateWith(existing);

        long cutoff = 200L;
        Tuple<ClusterState, Void> result = executor.executeTask(new PrimaryEncryptionKeyService.RetireKeysTask(cutoff), state);

        PrimaryEncryptionKeyMetadata metadata = metadataOf(result.v1());
        assertEquals(Set.of("active"), metadata.getKeys().keySet());
        assertEquals("active", metadata.getActiveKeyId());
    }

    public void testRetireKeysIsNoopWhenNothingToRetire() {
        PrimaryEncryptionKeyMetadata existing = new PrimaryEncryptionKeyMetadata(Map.of("active", entry(1000L)), "active");
        ClusterState state = stateWith(existing);

        Tuple<ClusterState, Void> result = executor.executeTask(new PrimaryEncryptionKeyService.RetireKeysTask(500L), state);
        assertSame(state, result.v1());
    }

    public void testRetireKeysIsNoopWhenNoMetadata() {
        ClusterState state = stateWith(null);
        Tuple<ClusterState, Void> result = executor.executeTask(new PrimaryEncryptionKeyService.RetireKeysTask(100L), state);
        assertSame(state, result.v1());
    }
}
