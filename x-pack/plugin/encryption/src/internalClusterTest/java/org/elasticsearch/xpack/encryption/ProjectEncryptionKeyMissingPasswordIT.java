/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.encryption;

import org.elasticsearch.action.admin.cluster.configuration.AddVotingConfigExclusionsRequest;
import org.elasticsearch.action.admin.cluster.configuration.ClearVotingConfigExclusionsRequest;
import org.elasticsearch.action.admin.cluster.configuration.TransportAddVotingConfigExclusionsAction;
import org.elasticsearch.action.admin.cluster.configuration.TransportClearVotingConfigExclusionsAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.health.HealthIndicatorResult;
import org.elasticsearch.health.HealthStatus;
import org.elasticsearch.health.node.HealthInfo;
import org.elasticsearch.indices.recovery.RecoverySettings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.test.NodeRoles;
import org.elasticsearch.xpack.encryption.spi.EncryptedData;
import org.elasticsearch.xpack.encryption.spi.EncryptionService;
import org.elasticsearch.xpack.encryption.spi.EncryptionServiceState;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.notNullValue;

/**
 * Nodes whose keystore lacks the active PEK password must stay in the cluster after the PEK is installed. Before, such nodes failed
 * every on-disk cluster state write: a master-eligible node rejected every publication and was repeatedly removed from the cluster, and a
 * data-only node silently stopped persisting cluster state. With a majority of master-eligible nodes affected, no publication could
 * commit and the cluster lost its elected master.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0, autoManageMasterNodes = true)
public class ProjectEncryptionKeyMissingPasswordIT extends ESIntegTestCase {

    private static final String PASSWORD_ID = "v1";
    private static final String PASSWORD = "encryption-test-password";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        Collection<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(EncryptionPlugin.class);
        return plugins;
    }

    @Override
    protected boolean autoManageVotingExclusions() {
        // the tests set voting exclusions themselves to decide which master-eligible node wins the election
        return false;
    }

    public void testNodesWithoutPasswordStayInClusterAndRecoverAfterReload() throws Exception {
        runWithMasterEligibleNodesWithoutPassword(1);
    }

    /** Before the fix no publication could commit once the key was installed, so the cluster lost its elected master. */
    public void testMajorityOfMasterEligibleNodesWithoutPasswordKeepsElectedMaster() throws Exception {
        runWithMasterEligibleNodesWithoutPassword(2);
    }

    private void runWithMasterEligibleNodesWithoutPassword(int count) throws Exception {
        List<String> masterNodes = internalCluster().startMasterOnlyNodes(3);
        String dataNodeWithoutPassword = internalCluster().startDataOnlyNode();
        ensureStableCluster(4);

        String master = internalCluster().getMasterName();
        List<String> masterEligibleWithoutPassword = randomSubsetOf(
            count,
            masterNodes.stream().filter(n -> n.equals(master) == false).toList()
        );
        List<String> nodesWithoutPassword = new ArrayList<>(masterEligibleWithoutPassword);
        nodesWithoutPassword.add(dataNodeWithoutPassword);

        for (String node : internalCluster().getNodeNames()) {
            if (nodesWithoutPassword.contains(node) == false) {
                reload(node, passwordSettings());
            }
        }
        assertBusy(() -> assertThat(pekOn(master), notNullValue()));

        // Further metadata changes must still be accepted and applied by the nodes without a password.
        updateClusterSettings(Settings.builder().put(RecoverySettings.INDICES_RECOVERY_MAX_BYTES_PER_SEC_SETTING.getKey(), "51mb"));
        ensureStableCluster(4);
        long masterVersion = internalCluster().clusterService(master).state().version();
        for (String node : nodesWithoutPassword) {
            assertBusy(() -> assertThat(internalCluster().clusterService(node).state().version(), greaterThanOrEqualTo(masterVersion)));
            assertThat(pekOn(node), notNullValue());
            assertEquals(HealthStatus.YELLOW, health(node).status());
            assertEquals(ProjectEncryptionKeyHealthIndicatorService.YELLOW_CANNOT_PERSIST, health(node).symptom());
        }

        byte[] payload = "missing-password-payload".getBytes(StandardCharsets.UTF_8);
        EncryptedData encrypted = internalCluster().getInstance(EncryptionService.class, master).encrypt(payload);
        for (String node : nodesWithoutPassword) {
            assertArrayEquals(payload, internalCluster().getInstance(EncryptionService.class, node).decrypt(encrypted));
        }

        for (String node : nodesWithoutPassword) {
            reload(node, passwordSettings());
        }
        // A master-eligible node persists accepted state before acking it; data-only nodes persist asynchronously, so only the former
        // is guaranteed to have written its key-less copy by now.
        for (String node : masterEligibleWithoutPassword) {
            assertEquals(ProjectEncryptionKeyHealthIndicatorService.YELLOW_NOT_YET_PERSISTED, health(node).symptom());
        }

        updateClusterSettings(Settings.builder().put(RecoverySettings.INDICES_RECOVERY_MAX_BYTES_PER_SEC_SETTING.getKey(), "52mb"));
        for (String node : nodesWithoutPassword) {
            assertBusy(() -> assertEquals(HealthStatus.GREEN, health(node).status()));
        }
        ensureStableCluster(4);
        assertEquals(master, internalCluster().getMasterName());
    }

    public void testReloadThatRemovesActivePasswordIsRejected() throws Exception {
        internalCluster().startNodes(2);
        ensureStableCluster(2);
        for (String node : internalCluster().getNodeNames()) {
            reload(node, passwordSettings());
        }
        String master = internalCluster().getMasterName();
        assertBusy(() -> {
            for (String node : internalCluster().getNodeNames()) {
                assertThat(pekOn(node), notNullValue());
            }
        });

        String node = randomFrom(internalCluster().getNodeNames());
        MockSecureSettings activeIdOnly = new MockSecureSettings();
        activeIdOnly.setString(ProjectEncryptionKeyPasswordSettings.ACTIVE_PASSWORD_ID_KEY, PASSWORD_ID);
        Settings withoutPassword = randomBoolean() ? Settings.EMPTY : Settings.builder().setSecureSettings(activeIdOnly).build();

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> reload(node, withoutPassword));
        assertThat(e.getMessage(), containsString("keeping the previous settings"));
        assertTrue(internalCluster().getInstance(ProjectEncryptionKeyService.class, node).canWrapForDisk());

        updateClusterSettings(Settings.builder().put(RecoverySettings.INDICES_RECOVERY_MAX_BYTES_PER_SEC_SETTING.getKey(), "51mb"));
        ensureStableCluster(2);
        assertEquals(HealthStatus.GREEN, health(node).status());
        assertThat(pekOn(master), notNullValue());
    }

    /**
     * A master-eligible node without a password stores the key-less cluster state, restarts and wins the election before it receives a
     * publication, so it publishes a degraded instance. Encryption is unavailable on every node, but the other nodes must keep the keys
     * on disk, and a later master must restore them from its disk.
     */
    public void testElectedMasterWithoutKeysDoesNotLoseKeys() throws Exception {
        KeyLessMaster cluster = electMasterWithoutKeysFromDisk();

        for (String node : internalCluster().getNodeNames()) {
            assertEquals(
                EncryptionServiceState.UNAVAILABLE_DECRYPTION_FAILED,
                internalCluster().getInstance(ProjectEncryptionKeyService.class, node).state()
            );
            expectThrows(Exception.class, () -> decryptOn(node, cluster.encrypted()));
            HealthIndicatorResult health = health(node);
            assertEquals("no feature stores encrypted data in this test", HealthStatus.YELLOW, health.status());
            assertEquals(ProjectEncryptionKeyHealthIndicatorService.MASTER_HAS_NO_KEY, health.symptom());
        }

        // further metadata changes make every node persist the degraded instance, which must not erase the keys from their disks
        updateClusterSettings(Settings.builder().put(RecoverySettings.INDICES_RECOVERY_MAX_BYTES_PER_SEC_SETTING.getKey(), "51mb"));

        clearVotingConfigExclusions();
        internalCluster().restartNode(cluster.masterWithoutKeys(), new InternalTestCluster.RestartCallback() {
            @Override
            public Settings onNodeStopped(String nodeName) throws Exception {
                assertBusy(() -> {
                    ProjectEncryptionKeyMetadata restored = pekOn(cluster.nodesWithKeys().get(0));
                    assertFalse(restored.isUnwrapFailed());
                    assertEquals(cluster.activeKeyId(), restored.getActiveKeyId());
                });
                return passwordSettings();
            }
        });
        ensureStableCluster(4);
        for (String node : internalCluster().getNodeNames()) {
            assertBusy(() -> assertEquals(HealthStatus.GREEN, health(node).status()));
            assertArrayEquals(cluster.payload(), decryptOn(node, cluster.encrypted()));
        }

        // every node's disk must still hold the current keys, whichever node wins the election this time
        internalCluster().fullRestart(new InternalTestCluster.RestartCallback() {
            @Override
            public Settings onNodeStopped(String nodeName) {
                return nodeName.equals(cluster.masterWithoutKeys()) ? passwordSettings() : Settings.EMPTY;
            }
        });
        ensureStableCluster(4);
        for (String node : internalCluster().getNodeNames()) {
            assertBusy(() -> assertArrayEquals(cluster.payload(), decryptOn(node, cluster.encrypted())));
            assertEquals(cluster.activeKeyId(), internalCluster().getInstance(EncryptionService.class, node).encrypt(new byte[1]).keyId());
        }
    }

    private record KeyLessMaster(
        String masterWithoutKeys,
        List<String> nodesWithKeys,
        String activeKeyId,
        byte[] payload,
        EncryptedData encrypted
    ) {}

    /**
     * Starts two master-eligible nodes and a data node with a password and one master-eligible node without, installs the key, makes the
     * node without a password the only voter and restarts it, so it is elected from its key-less on-disk state.
     */
    private KeyLessMaster electMasterWithoutKeysFromDisk() throws Exception {
        List<String> nodesWithKeys = new ArrayList<>(internalCluster().startMasterOnlyNodes(2, passwordSettings()));
        String withoutPassword = internalCluster().startMasterOnlyNode();
        nodesWithKeys.add(internalCluster().startNode(Settings.builder().put(passwordSettings()).put(NodeRoles.dataOnlyNode())));
        ensureStableCluster(4);
        assertBusy(() -> {
            for (String node : internalCluster().getNodeNames()) {
                assertTrue(internalCluster().getInstance(ProjectEncryptionKeyService.class, node).hasInstalledKey());
            }
        });

        byte[] payload = "key-less-master-payload".getBytes(StandardCharsets.UTF_8);
        EncryptedData encrypted = internalCluster().getInstance(EncryptionService.class, nodesWithKeys.get(0)).encrypt(payload);

        client().execute(
            TransportAddVotingConfigExclusionsAction.TYPE,
            new AddVotingConfigExclusionsRequest(TEST_REQUEST_TIMEOUT, nodesWithKeys.get(0), nodesWithKeys.get(1))
        ).get();
        assertBusy(() -> assertEquals(withoutPassword, internalCluster().getMasterName()));

        internalCluster().restartNode(withoutPassword);
        ensureStableCluster(4);
        assertEquals(withoutPassword, internalCluster().getMasterName());
        assertTrue("elected from a key-less disk copy", pekOn(withoutPassword).isUnwrapFailed());
        return new KeyLessMaster(withoutPassword, nodesWithKeys, encrypted.keyId(), payload, encrypted);
    }

    private static void clearVotingConfigExclusions() throws Exception {
        ClearVotingConfigExclusionsRequest request = new ClearVotingConfigExclusionsRequest(TEST_REQUEST_TIMEOUT);
        request.setWaitForRemoval(false);
        client().execute(TransportClearVotingConfigExclusionsAction.TYPE, request).get();
        assertBusy(() -> {
            ClusterState state = internalCluster().clusterService().state();
            assertEquals(3, state.getLastCommittedConfiguration().getNodeIds().size());
        });
    }

    private static byte[] decryptOn(String node, EncryptedData encrypted) {
        return internalCluster().getInstance(EncryptionService.class, node).decrypt(encrypted);
    }

    private static Settings passwordSettings() {
        MockSecureSettings secure = new MockSecureSettings();
        secure.setString(ProjectEncryptionKeyPasswordSettings.ACTIVE_PASSWORD_ID_KEY, PASSWORD_ID);
        secure.setString(ProjectEncryptionKeyPasswordSettings.PASSWORD_PREFIX + PASSWORD_ID, PASSWORD);
        return Settings.builder().setSecureSettings(secure).build();
    }

    private static void reload(String node, Settings settings) {
        internalCluster().getInstance(PluginsService.class, node)
            .filterPlugins(EncryptionPlugin.class)
            .findFirst()
            .orElseThrow()
            .reload(settings);
    }

    private static ProjectEncryptionKeyMetadata pekOn(String node) {
        ClusterService clusterService = internalCluster().getInstance(ClusterService.class, node);
        return clusterService.state().metadata().getSingleProjectCustom(ProjectEncryptionKeyMetadata.TYPE);
    }

    private static HealthIndicatorResult health(String node) {
        return internalCluster().getInstance(ProjectEncryptionKeyHealthIndicatorService.class, node)
            .calculate(true, 10, HealthInfo.EMPTY_HEALTH_INFO);
    }
}
