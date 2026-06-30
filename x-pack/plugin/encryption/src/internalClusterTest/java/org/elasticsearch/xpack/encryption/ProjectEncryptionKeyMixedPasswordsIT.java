/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.encryption;

import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.encryption.spi.EncryptedData;
import org.elasticsearch.xpack.encryption.spi.EncryptionService;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;

import static org.hamcrest.Matchers.notNullValue;

/**
 * Verifies that nodes with different password configurations can form a cluster and use a shared PEK. Since PEK bytes travel as plaintext
 * over the TLS-protected transport layer, each node only needs its own password to wrap/unwrap the key it writes and reads from its own
 * local disk — no cross-node password agreement is required.
 *
 * <p>Two scenarios are covered:
 * <ol>
 *   <li>Same password ID, different password values per node.</li>
 *   <li>Different password IDs per node (each node also carries the others' passwords so it can wrap for its own disk).</li>
 * </ol>
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 3, supportsDedicatedMasters = false)
public class ProjectEncryptionKeyMixedPasswordsIT extends ESIntegTestCase {

    private static final String PASSWORD_ID = "v1";
    private static final String BASE_PASSWORD = "encryption-test-password";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        Collection<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(EncryptionPlugin.class);
        return plugins;
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        Settings.Builder builder = Settings.builder().put(super.nodeSettings(nodeOrdinal, otherSettings));
        MockSecureSettings secure = new MockSecureSettings();
        // Each node has a unique password value, but they all share the same password ID.
        secure.setString(ProjectEncryptionKeyPasswordSettings.ACTIVE_PASSWORD_ID_KEY, PASSWORD_ID);
        secure.setString(ProjectEncryptionKeyPasswordSettings.PASSWORD_PREFIX + PASSWORD_ID, BASE_PASSWORD + "-node-" + nodeOrdinal);
        builder.setSecureSettings(secure);
        return builder.build();
    }

    public void testKeyIsInstalledAndUsableWithDifferentPasswordsPerNode() throws Exception {
        ensureGreen();
        assertBusy(() -> assertThat(getKeyFromMaster(), notNullValue()));

        // All nodes must hold the same active key ID: the plaintext PEK is shared via TLS transport,
        // not derived per-node from a password.
        String[] firstKeyId = { null };
        assertBusy(() -> {
            for (String nodeName : internalCluster().getNodeNames()) {
                ClusterService cs = internalCluster().getInstance(ClusterService.class, nodeName);
                ProjectEncryptionKeyMetadata pek = cs.state().metadata().getSingleProjectCustom(ProjectEncryptionKeyMetadata.TYPE);
                assertThat("PEK must be present on node " + nodeName, pek, notNullValue());
                if (firstKeyId[0] == null) {
                    firstKeyId[0] = pek.getActiveKeyId();
                } else {
                    assertEquals("all nodes must share the same active key ID", firstKeyId[0], pek.getActiveKeyId());
                }
            }
        });

        // Data encrypted on the master must be decryptable on every other node regardless of their
        // individual password values.
        EncryptionService masterSvc = internalCluster().getInstance(EncryptionService.class, internalCluster().getMasterName());
        byte[] payload = "mixed-password-test-payload".getBytes(StandardCharsets.UTF_8);
        EncryptedData encrypted = masterSvc.encrypt(payload);

        for (String nodeName : internalCluster().getNodeNames()) {
            EncryptionService svc = internalCluster().getInstance(EncryptionService.class, nodeName);
            assertArrayEquals(
                "node [" + nodeName + "] must decrypt data encrypted by master despite having a different password value",
                payload,
                svc.decrypt(encrypted)
            );
        }
    }

    public void testKeyPersistsAfterFullRestartWithDifferentPasswordsPerNode() throws Exception {
        ensureGreen();
        assertBusy(() -> assertThat(getKeyFromMaster(), notNullValue()));

        String originalKeyId = getKeyFromMaster().getActiveKeyId();

        // On restart each node reads from its own disk; each disk file was wrapped with that node's own
        // password, so mismatched passwords across nodes are not a problem.
        internalCluster().fullRestart();
        ensureGreen();

        assertBusy(() -> {
            ProjectEncryptionKeyMetadata pek = getKeyFromMaster();
            assertThat("PEK must survive a full cluster restart with per-node passwords", pek, notNullValue());
            assertEquals("active key ID must be unchanged after restart", originalKeyId, pek.getActiveKeyId());
        });

        // Cross-node encryption must still work after the restart.
        EncryptionService masterSvc = internalCluster().getInstance(EncryptionService.class, internalCluster().getMasterName());
        byte[] payload = "post-restart-payload".getBytes(StandardCharsets.UTF_8);
        EncryptedData encrypted = masterSvc.encrypt(payload);

        for (String nodeName : internalCluster().getNodeNames()) {
            EncryptionService svc = internalCluster().getInstance(EncryptionService.class, nodeName);
            assertArrayEquals(
                "node [" + nodeName + "] must decrypt post-restart data despite different password values",
                payload,
                svc.decrypt(encrypted)
            );
        }
    }

    /**
     * Each node uses a different active password ID. Node 0 uses "id-0", node 1 uses "id-1", etc. Every node also carries all other
     * nodes' passwords so it can recover blobs written by other nodes after a restart (in stateful mode each node writes its own local
     * disk, so only its own active password matters for writing; but all passwords must be present for the coordinator's signal check).
     */
    public void testKeyIsInstalledAndUsableWithDifferentPasswordIdsPerNode() throws Exception {
        String[] nodeNames = internalCluster().getNodeNames();
        for (int i = 0; i < nodeNames.length; i++) {
            String nodePasswordId = "id-" + i;
            MockSecureSettings secure = new MockSecureSettings();
            secure.setString(ProjectEncryptionKeyPasswordSettings.ACTIVE_PASSWORD_ID_KEY, nodePasswordId);
            // Each node has its own active password plus all the other nodes' passwords.
            for (int j = 0; j < nodeNames.length; j++) {
                secure.setString(ProjectEncryptionKeyPasswordSettings.PASSWORD_PREFIX + "id-" + j, BASE_PASSWORD + "-id-" + j);
            }
            Settings reloadSettings = Settings.builder().setSecureSettings(secure).build();
            internalCluster().getInstance(PluginsService.class, nodeNames[i])
                .filterPlugins(EncryptionPlugin.class)
                .findFirst()
                .orElseThrow()
                .reload(reloadSettings);
        }

        assertBusy(() -> assertThat(getKeyFromMaster(), notNullValue()));

        assertBusy(() -> {
            String[] firstKeyId = { null };
            for (String nodeName : internalCluster().getNodeNames()) {
                ClusterService cs = internalCluster().getInstance(ClusterService.class, nodeName);
                ProjectEncryptionKeyMetadata pek = cs.state().metadata().getSingleProjectCustom(ProjectEncryptionKeyMetadata.TYPE);
                assertThat("PEK must be present on node " + nodeName, pek, notNullValue());
                if (firstKeyId[0] == null) {
                    firstKeyId[0] = pek.getActiveKeyId();
                } else {
                    assertEquals("all nodes must share the same active key ID", firstKeyId[0], pek.getActiveKeyId());
                }
            }
        });

        EncryptionService masterSvc = internalCluster().getInstance(EncryptionService.class, internalCluster().getMasterName());
        byte[] payload = "mixed-password-id-test-payload".getBytes(StandardCharsets.UTF_8);
        EncryptedData encrypted = masterSvc.encrypt(payload);

        for (String nodeName : internalCluster().getNodeNames()) {
            EncryptionService svc = internalCluster().getInstance(EncryptionService.class, nodeName);
            assertArrayEquals(
                "node [" + nodeName + "] must decrypt data encrypted by master despite having a different active password ID",
                payload,
                svc.decrypt(encrypted)
            );
        }
    }

    private ProjectEncryptionKeyMetadata getKeyFromMaster() {
        ClusterService clusterService = internalCluster().getInstance(ClusterService.class, internalCluster().getMasterName());
        return clusterService.state().metadata().getSingleProjectCustom(ProjectEncryptionKeyMetadata.TYPE);
    }
}
