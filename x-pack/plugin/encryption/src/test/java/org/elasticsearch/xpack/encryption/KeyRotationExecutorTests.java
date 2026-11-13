/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.encryption;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.project.DefaultProjectResolver;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.encryption.ProjectEncryptionKeyMetadata.KeyEntry;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

public class KeyRotationExecutorTests extends ESTestCase {

    private static final ClusterName CLUSTER_NAME = new ClusterName("test");
    private static final String PASSWORD_ID = "v1";
    private static final ProjectEncryptionKeyMetadata.PekEncryption NO_OP_ENCRYPTION = TestPekEncryption.NO_OP;
    private final KeyRotationCoordinator.KeyRotationExecutor executor = new KeyRotationCoordinator.KeyRotationExecutor(
        DefaultProjectResolver.INSTANCE,
        NO_OP_ENCRYPTION,
        false
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
            new KeyRotationCoordinator.InstallKeyTask(newKeyId, randomKey(), PASSWORD_ID, generatedAt, Set.of()),
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
        Tuple<ClusterState, Void> result = executor.executeTask(installTask(ProjectEncryptionKeyMetadata.generateKeyId(), Set.of()), state);
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
        entries.put("lastDeactivated", entry(450L));
        entries.put("recent", entry(400L));
        ProjectEncryptionKeyMetadata existing = new ProjectEncryptionKeyMetadata(
            entries,
            "active",
            PASSWORD_ID,
            Map.of(),
            NO_OP_ENCRYPTION
        );
        ClusterState state = stateWith(existing);

        Tuple<ClusterState, Void> result = executor.executeTask(new KeyRotationCoordinator.RetireKeysTask(420L, "active"), state);

        ProjectEncryptionKeyMetadata metadata = metadataOf(result.v1());
        assertEquals("only 'old' should have been retired", Set.of("active", "lastDeactivated", "recent"), metadata.getKeys().keySet());
        assertEquals("active", metadata.getActiveKeyId());
    }

    public void testRetireKeysNeverRetiresActiveKey() {
        Map<String, KeyEntry> entries = new HashMap<>();
        entries.put("active", entry(100L));
        entries.put("previous", entry(75L));
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
        Tuple<ClusterState, Void> result = executor.executeTask(new KeyRotationCoordinator.RetireKeysTask(cutoff, "active"), state);

        ProjectEncryptionKeyMetadata metadata = metadataOf(result.v1());
        assertEquals(Set.of("active", "previous"), metadata.getKeys().keySet());
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

        Tuple<ClusterState, Void> result = executor.executeTask(new KeyRotationCoordinator.RetireKeysTask(500L, "active"), state);
        assertSame(state, result.v1());
    }

    /**
     * A retire task submitted in the same tick as a rotation applies after it. Handlers are then pending on the new key, so retiring
     * would remove k1 before their pass moves any late write still under it.
     */
    public void testRetireKeysDropsWhenRotationAppliedSinceSubmit() {
        ProjectEncryptionKeyMetadata existing = new ProjectEncryptionKeyMetadata(
            Map.of("k0", entry(100L), "k1", entry(200L), "k2", entry(300L)),
            "k2",
            PASSWORD_ID,
            Map.of("handler", "k2"),
            NO_OP_ENCRYPTION
        );
        ClusterState rotated = executor.executeTask(
            new KeyRotationCoordinator.BeginRotationTask("k3", randomKey(), 400L, new AtomicBoolean()),
            stateWith(existing)
        ).v1();

        Tuple<ClusterState, Void> result = executor.executeTask(new KeyRotationCoordinator.RetireKeysTask(350L, "k2"), rotated);

        assertSame(rotated, result.v1());
        assertEquals(Set.of("k0", "k1", "k2", "k3"), metadataOf(result.v1()).getKeys().keySet());
    }

    public void testRetireKeysIsNoopWhenNoMetadata() {
        ClusterState state = stateWith(null);
        Tuple<ClusterState, Void> result = executor.executeTask(new KeyRotationCoordinator.RetireKeysTask(100L, "active"), state);
        assertSame(state, result.v1());
    }

    /**
     * The install goes ahead only if every node that writes cluster state to disk reported it can persist the key: one that joined since
     * the check, or restarted since it answered, may not, while a coordinating-only node doesn't need to.
     */
    public void testInstallRequiresEveryNodeThatPersistsClusterStateToBeReady() {
        DiscoveryNode ready = DiscoveryNodeUtils.create("ready");
        DiscoveryNode joined = DiscoveryNodeUtils.create("joined");
        ClusterState state = ClusterState.builder(stateWith(null))
            .nodes(DiscoveryNodes.builder().add(ready).add(joined).add(DiscoveryNodeUtils.builder("coordinating").roles(Set.of()).build()))
            .build();
        String keyId = ProjectEncryptionKeyMetadata.generateKeyId();
        assertSame(state, executor.executeTask(installTask(keyId, Set.of(ready.getEphemeralId())), state).v1());
        String beforeRestart = randomValueOtherThan(joined.getEphemeralId(), ESTestCase::randomIdentifier);
        assertSame(state, executor.executeTask(installTask(keyId, Set.of(ready.getEphemeralId(), beforeRestart)), state).v1());
        assertEquals(
            keyId,
            metadataOf(executor.executeTask(installTask(keyId, Set.of(ready.getEphemeralId(), joined.getEphemeralId())), state).v1())
                .getActiveKeyId()
        );
    }

    /** On stateless only master-eligible nodes persist cluster state, to the object store, so only they must be ready. */
    public void testStatelessInstallRequiresEveryMasterEligibleNodeToBeReady() {
        var statelessExecutor = new KeyRotationCoordinator.KeyRotationExecutor(DefaultProjectResolver.INSTANCE, NO_OP_ENCRYPTION, true);
        DiscoveryNode master = DiscoveryNodeUtils.builder("master").roles(Set.of(DiscoveryNodeRole.MASTER_ROLE)).build();
        ClusterState state = ClusterState.builder(stateWith(null))
            .nodes(
                DiscoveryNodes.builder()
                    .add(master)
                    .add(DiscoveryNodeUtils.builder("index").roles(Set.of(DiscoveryNodeRole.INDEX_ROLE)).build())
                    .add(DiscoveryNodeUtils.builder("search").roles(Set.of(DiscoveryNodeRole.SEARCH_ROLE)).build())
            )
            .build();
        String keyId = ProjectEncryptionKeyMetadata.generateKeyId();
        assertSame(state, statelessExecutor.executeTask(installTask(keyId, Set.of()), state).v1());
        assertEquals(
            keyId,
            metadataOf(statelessExecutor.executeTask(installTask(keyId, Set.of(master.getEphemeralId())), state).v1()).getActiveKeyId()
        );
    }

    /** With passwords from cluster secrets no node is checked, since every node gets the same password. */
    public void testInstallWithoutReadyNodeIdsSkipsTheCheck() {
        ClusterState state = ClusterState.builder(stateWith(null))
            .nodes(DiscoveryNodes.builder().add(DiscoveryNodeUtils.create("master")))
            .build();
        String keyId = ProjectEncryptionKeyMetadata.generateKeyId();
        assertEquals(keyId, metadataOf(executor.executeTask(installTask(keyId, null), state).v1()).getActiveKeyId());
    }

    private static KeyRotationCoordinator.InstallKeyTask installTask(String keyId, @Nullable Set<String> readyNodeIds) {
        return new KeyRotationCoordinator.InstallKeyTask(keyId, randomKey(), PASSWORD_ID, 0L, readyNodeIds);
    }

    /** A restore never undoes a reset, replaces usable metadata, or brings back a different active key. */
    public void testRestoreOnlyReplacesKeylessMetadataForTheSameActiveKey() {
        ProjectEncryptionKeyMetadata keys = new ProjectEncryptionKeyMetadata(
            Map.of("k1", entry(1L)),
            "k1",
            PASSWORD_ID,
            Map.of(),
            NO_OP_ENCRYPTION
        );
        var task = new KeyRotationCoordinator.RestoreKeysTask(keys, Set.of());
        assertSame(keys, metadataOf(executor.executeTask(task, stateWith(degraded("k1"))).v1()));

        for (ClusterState state : List.of(stateWith(degraded("k2")), stateWith(null), stateWith(keys))) {
            assertSame(state, executor.executeTask(task, state).v1());
        }
    }

    /** As for the install, the restore goes ahead only if every node that persists cluster state reported it can persist the key. */
    public void testRestoreRequiresEveryNodeThatPersistsClusterStateToBeReady() {
        ProjectEncryptionKeyMetadata keys = new ProjectEncryptionKeyMetadata(
            Map.of("k1", entry(1L)),
            "k1",
            PASSWORD_ID,
            Map.of(),
            NO_OP_ENCRYPTION
        );
        DiscoveryNode ready = DiscoveryNodeUtils.create("ready");
        DiscoveryNode joined = DiscoveryNodeUtils.create("joined");
        ClusterState state = ClusterState.builder(stateWith(degraded("k1"))).nodes(DiscoveryNodes.builder().add(ready).add(joined)).build();
        assertSame(
            state,
            executor.executeTask(new KeyRotationCoordinator.RestoreKeysTask(keys, Set.of(ready.getEphemeralId())), state).v1()
        );
        ClusterState restored = executor.executeTask(
            new KeyRotationCoordinator.RestoreKeysTask(keys, Set.of(ready.getEphemeralId(), joined.getEphemeralId())),
            state
        ).v1();
        assertSame(keys, metadataOf(restored));
    }

    /** Key-less metadata would turn into metadata holding only a new key, losing the keys other nodes still hold. */
    public void testLifecycleTasksAreDroppedWhileMasterHoldsNoKey() {
        ClusterState state = stateWith(degraded("k1"));
        for (KeyRotationCoordinator.KeyRotationTask task : List.of(
            new KeyRotationCoordinator.BeginRotationTask("k2", randomKey(), 20L, new AtomicBoolean(true)),
            new KeyRotationCoordinator.UpdatePasswordIdTask("v2", PASSWORD_ID),
            new KeyRotationCoordinator.RetireKeysTask(100L, "k1"),
            new KeyRotationCoordinator.ReEncryptApplyTask("handler", null, null, "k1", ActionListener.noop())
        )) {
            assertSame(task.description(), state, executor.executeTask(task, state).v1());
        }
    }

    private static ProjectEncryptionKeyMetadata degraded(String activeKeyId) {
        return ProjectEncryptionKeyMetadata.degraded(activeKeyId, PASSWORD_ID, NO_OP_ENCRYPTION, "no key on the elected master");
    }
}
