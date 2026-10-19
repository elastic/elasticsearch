/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.encryption;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.ProjectState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.SecurityIntegTestCase;
import org.elasticsearch.test.SecuritySettingsSource;
import org.elasticsearch.test.disruption.BlockClusterStateProcessing;
import org.elasticsearch.xpack.encryption.spi.EncryptedData;
import org.elasticsearch.xpack.encryption.spi.EncryptionKeyNotYetAvailableException;
import org.elasticsearch.xpack.encryption.spi.EncryptionService;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

/**
 * Shows that the project encryption key cache is only guaranteed per node. A coordinator that has applied a cluster state picks
 * workers from that state and ships ciphertext to them. A worker that has committed the same state but has not finished applying it
 * still has the old cache, and rejects the ciphertext with {@link EncryptionKeyNotYetAvailableException}.
 *
 * <p>This is the shape of the failure seen during serverless scale-up in ES|QL: {@code NodeEligibilityStrategy.EXTERNAL_WORKER_NODES}
 * selects any data-capable, non-index node from the coordinator's own state, the plan carries {@link EncryptedData}, and
 * {@code DataSourceCredentials.decryptInPlace} runs on the worker against the worker's cache. The fan-out is node-to-node transport,
 * so node readiness (which gates client HTTP traffic and is itself a cluster-state listener) does not protect it.
 *
 * <p>The worker is held with {@link BlockClusterStateProcessing}, the test framework's disruption for exactly this: it parks the
 * node's cluster-applier thread, so the next state is committed on the node but never applied while the hold lasts. The node's
 * {@code ClusterService#state()} keeps showing the previous state and the key cache is not rebuilt. That stands in for a freshly
 * joined node that is still busy applying its first state. The decrypt is invoked on the worker's {@link EncryptionService} directly;
 * it is the same node-local object the ES|QL data-node handler uses.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 3, numClientNodes = 0, supportsDedicatedMasters = false)
public class ProjectEncryptionKeyLaggingNodeIT extends SecurityIntegTestCase {

    private static final String PASSWORD_ID = "v1";
    private static final String PASSWORD = "encryption-test-password";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        Collection<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(EncryptionPlugin.class);
        return plugins;
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        // Rotation stays at its defaults (30 days), so the only key change in this test is the one the test submits itself.
        Settings.Builder builder = Settings.builder().put(super.nodeSettings(nodeOrdinal, otherSettings));
        SecuritySettingsSource.addSecureSettings(builder, secure -> {
            secure.setString(ProjectEncryptionKeyPasswordSettings.ACTIVE_PASSWORD_ID_KEY, PASSWORD_ID);
            secure.setString(ProjectEncryptionKeyPasswordSettings.PASSWORD_PREFIX + PASSWORD_ID, PASSWORD);
        });
        return builder.build();
    }

    public void testWorkerThatHasNotAppliedTheStateCannotDecryptWhatTheCoordinatorEncrypted() throws Exception {
        ensureGreen();
        waitForKeyOnAllNodes();

        String master = internalCluster().getMasterName();
        String[] others = Arrays.stream(internalCluster().getNodeNames()).filter(n -> n.equals(master) == false).toArray(String[]::new);
        assertThat(others.length, equalTo(2));
        String worker = others[0];
        String coordinator = others[1];

        ClusterService workerClusterService = internalCluster().clusterService(worker);
        String k1 = pekOn(worker).getActiveKeyId();

        // Park the worker's applier thread so the next state is committed there but not applied. Released in finally: a parked
        // applier thread would wedge the node for the rest of the test.
        BlockClusterStateProcessing hold = new BlockClusterStateProcessing(worker, random());
        internalCluster().setDisruptionScheme(hold);
        hold.startDisrupting();
        try {
            AtomicReference<Exception> rotationFailure = new AtomicReference<>();
            String k2 = submitNewActiveKey(master, rotationFailure);

            // The coordinator applies the new state as soon as its own commit arrives. It does not wait for the worker.
            assertBusy(() -> {
                assertThat("rotation task failed", rotationFailure.get(), nullValue());
                assertThat(pekOn(coordinator).getActiveKeyId(), equalTo(k2));
            });

            // The worker's applier is parked, so its visible state is still the old one while the coordinator has moved on.
            assertThat(pekOn(worker).getActiveKeyId(), equalTo(k1));
            assertThat(workerClusterService.state().version(), lessThan(internalCluster().clusterService(coordinator).state().version()));

            // From the coordinator's point of view the worker is a valid ES|QL external-split worker (data-capable, no index role).
            DiscoveryNode workerNode = internalCluster().clusterService(coordinator).state().nodes().resolveNode(worker);
            assertTrue(workerNode.canContainData());
            assertFalse(workerNode.hasRole(DiscoveryNodeRole.INDEX_ROLE.roleName()));

            EncryptionService coordinatorService = internalCluster().getInstance(EncryptionService.class, coordinator);
            EncryptionService workerService = internalCluster().getInstance(EncryptionService.class, worker);
            byte[] plaintext = randomByteArrayOfLength(randomIntBetween(1, 256));
            EncryptedData encrypted = coordinatorService.encrypt(plaintext);
            assertThat(encrypted.keyId(), equalTo(k2));

            // The production failure: HTTP 503 "project encryption key [k2] is not yet available", raised on the worker.
            EncryptionKeyNotYetAvailableException e = expectThrows(
                EncryptionKeyNotYetAvailableException.class,
                () -> workerService.decrypt(encrypted)
            );
            assertThat(e.getMessage(), containsString("[" + k2 + "] is not yet available"));
            // The data itself is fine: the coordinator decrypts it. Only the worker is behind.
            assertArrayEquals(plaintext, coordinatorService.decrypt(encrypted));

            // Once the worker is allowed to apply the state, the same ciphertext decrypts there too. Wait for the state first: a
            // decrypt that still misses throws a RuntimeException, which assertBusy would not retry.
            hold.stopDisrupting();
            assertBusy(() -> assertThat(pekOn(worker).getActiveKeyId(), equalTo(k2)));
            assertArrayEquals(plaintext, workerService.decrypt(encrypted));
        } finally {
            hold.stopDisrupting();
            internalCluster().clearDisruptionScheme();
        }
        // Let the master's publication finish (it waits for the worker's commit ack) before the framework's consistency check runs.
        waitNoPendingTasksOnAll();
    }

    private ProjectEncryptionKeyMetadata pekOn(String node) {
        return internalCluster().clusterService(node).state().metadata().getSingleProjectCustom(ProjectEncryptionKeyMetadata.TYPE);
    }

    private void waitForKeyOnAllNodes() throws Exception {
        assertBusy(() -> {
            for (String node : internalCluster().getNodeNames()) {
                assertThat("no PEK on " + node, pekOn(node), notNullValue());
            }
        });
    }

    /**
     * Adds a new active key the same way {@code KeyRotationCoordinator}'s begin-rotation task does, but from the test so the moment
     * of the change is under test control. Returns right after submitting; the publication completes only once every node has applied
     * the state, and the test deliberately holds one node, so callers must not wait on it here.
     */
    private String submitNewActiveKey(String master, AtomicReference<Exception> failure) {
        ClusterService clusterService = internalCluster().clusterService(master);
        ProjectResolver projectResolver = internalCluster().getInstance(ProjectResolver.class, master);
        // The metadata wraps keys for on-disk persistence through this object on the master, so it needs the same password the nodes use.
        MockSecureSettings secure = new MockSecureSettings();
        secure.setString(ProjectEncryptionKeyPasswordSettings.ACTIVE_PASSWORD_ID_KEY, PASSWORD_ID);
        secure.setString(ProjectEncryptionKeyPasswordSettings.PASSWORD_PREFIX + PASSWORD_ID, PASSWORD);
        Settings pekSettings = Settings.builder().setSecureSettings(secure).build();
        ProjectEncryptionKeyMetadata.PekEncryption pekEncryption = new PasswordPekEncryption(() -> pekSettings);

        String newKeyId = ProjectEncryptionKeyMetadata.generateKeyId();
        byte[] newKey = randomByteArrayOfLength(PasswordBasedEncryption.PEK_LENGTH_BYTES);
        clusterService.submitUnbatchedStateUpdateTask("test-add-active-pek", new ClusterStateUpdateTask() {
            @Override
            public ClusterState execute(ClusterState currentState) {
                ProjectState projectState = projectResolver.getProjectState(currentState);
                ProjectEncryptionKeyMetadata existing = projectState.metadata().custom(ProjectEncryptionKeyMetadata.TYPE);
                long newest = existing.getKeys()
                    .values()
                    .stream()
                    .mapToLong(ProjectEncryptionKeyMetadata.KeyEntry::generatedAt)
                    .max()
                    .orElse(0L);
                Map<String, ProjectEncryptionKeyMetadata.KeyEntry> keys = new HashMap<>(existing.getKeys());
                keys.put(newKeyId, new ProjectEncryptionKeyMetadata.KeyEntry(newKey, Math.max(System.currentTimeMillis(), newest + 1)));
                ProjectEncryptionKeyMetadata rotated = new ProjectEncryptionKeyMetadata(
                    keys,
                    newKeyId,
                    existing.getPasswordId(),
                    existing.getHandlerKeyIds(),
                    pekEncryption
                );
                return currentState.copyAndUpdateProject(
                    projectState.projectId(),
                    b -> b.putCustom(ProjectEncryptionKeyMetadata.TYPE, rotated)
                );
            }

            @Override
            public void onFailure(Exception e) {
                failure.set(e);
            }
        });
        return newKeyId;
    }
}
