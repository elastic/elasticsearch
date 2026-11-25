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
import org.elasticsearch.cluster.coordination.JoinHelper;
import org.elasticsearch.cluster.coordination.JoinValidationService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.health.HealthIndicatorResult;
import org.elasticsearch.health.HealthStatus;
import org.elasticsearch.health.node.HealthInfo;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.test.NodeRoles;
import org.elasticsearch.xpack.encryption.spi.EncryptedData;
import org.elasticsearch.xpack.encryption.spi.EncryptionService;
import org.elasticsearch.xpack.encryption.spi.EncryptionServiceState;
import org.elasticsearch.xpack.encryption.spi.EncryptionServiceUnavailableException;
import org.junit.Before;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

/**
 * A master-eligible or data node that cannot persist the project encryption key cannot persist any cluster state, so once the key is
 * installed such a node is not allowed to join. Installing the key only once every such node can persist it is covered by
 * {@code MissingPasswordRolloutIT}. A node that cannot read the key on its disk still starts; if it is elected master, it reads the key
 * from its disk once it has the password, or a failover to a node that can read its copy restores it.
 *
 * <p>Passwords are added with a secure settings reload rather than at startup, so that a restart drops them.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0, autoManageMasterNodes = false)
public class ProjectEncryptionKeyMissingPasswordIT extends ESIntegTestCase {

    private static final String PASSWORD_ID = "v1";
    private static final String PASSWORD = "encryption-test-password";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        Collection<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(EncryptionPlugin.class);
        return plugins;
    }

    @Before
    public void bootstrapFirstMaster() {
        internalCluster().setBootstrapMasterNodeIndex(0);
    }

    /**
     * Before the cluster has a key, a node that can persist one can't reload its password away, since the master may install a key as
     * soon as the node has reported that it can persist it.
     */
    public void testPasswordCannotBeReloadedAwayBeforeKeyIsInstalled() throws Exception {
        internalCluster().startMasterOnlyNode();
        String dataNode = internalCluster().startDataOnlyNode();
        ensureStableCluster(2);
        // the master has no password, so it doesn't install a key
        reload(dataNode, passwordSettings());

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> reload(dataNode, Settings.EMPTY));
        assertThat(e.getMessage(), containsString("keeping the previous settings"));
    }

    /**
     * The only node without a password restarts, so the master installs the key while it is away. The master rejects it when it rejoins,
     * although the state it validates the join against is cached from before the install. A coordinating-only node doesn't need one.
     */
    public void testNodeThatRestartsWhileKeyIsInstalledCannotRejoinWithoutPassword() throws Exception {
        String master = internalCluster().startMasterOnlyNode(
            Settings.builder().put(JoinValidationService.JOIN_VALIDATION_CACHE_TIMEOUT_SETTING.getKey(), "1h").build()
        );
        ensureStableCluster(1);
        String dataNode = internalCluster().startDataOnlyNode();
        ensureStableCluster(2);
        reload(master, passwordSettings());

        try (MockLog mockLog = MockLog.capture(JoinHelper.class)) {
            mockLog.addExpectation(new EncryptionIntegTestUtils.JoinRejectedExpectation());
            internalCluster().restartNode(dataNode, new InternalTestCluster.RestartCallback() {
                @Override
                public Settings onNodeStopped(String nodeName) throws Exception {
                    assertBusy(() -> assertThat(pekOn(master), notNullValue()), 30, TimeUnit.SECONDS);
                    return Settings.EMPTY;
                }

                @Override
                public boolean validateClusterForming() {
                    return false;
                }
            });
            mockLog.awaitAllExpectationsMatched();
        }
        assertEquals(1, internalCluster().clusterService(master).state().nodes().size());

        String coordinating = internalCluster().startNode(NodeRoles.noRoles());
        ensureStableCluster(2, master);
        // stopped because the test framework's teardown writes cluster state as if to disk on a random node
        internalCluster().stopNode(coordinating);

        // the node retries joining, and is admitted once it can persist the key
        reload(dataNode, passwordSettings());
        ensureStableCluster(2);
        assertBusy(() -> assertEquals(HealthStatus.GREEN, health(dataNode).status()));
    }

    /**
     * The master restarts without its password. Encryption is unavailable until its password is reloaded; it then reads the key from its
     * disk.
     */
    public void testElectedMasterWithoutKeyReadsItOnceItHasThePassword() throws Exception {
        String master = startNodesWithKey(1).get(0);
        byte[] payload = "missing-password-payload".getBytes(StandardCharsets.UTF_8);
        EncryptedData encrypted = internalCluster().getInstance(EncryptionService.class, master).encrypt(payload);

        internalCluster().restartNode(master);
        // the restarted node may not have recovered its state yet
        assertBusy(() -> {
            ProjectEncryptionKeyMetadata metadata = pekOn(master);
            assertThat(metadata, notNullValue());
            assertTrue(metadata.isUnwrapFailed());
        });
        EncryptionServiceUnavailableException e = expectThrows(
            EncryptionServiceUnavailableException.class,
            () -> decryptOn(master, encrypted)
        );
        assertEquals(EncryptionServiceState.UNAVAILABLE_DECRYPTION_FAILED, e.serviceState());
        assertEquals(ProjectEncryptionKeyHealthIndicatorService.MASTER_HAS_NO_KEY, health(master).symptom());

        reload(master, passwordSettings());
        assertBusy(() -> assertFalse(pekOn(master).isUnwrapFailed()));
        assertArrayEquals(payload, decryptOn(master, encrypted));
    }

    /**
     * The elected master lost the password its copy is wrapped with. Failing over to a node that can read its own copy restores the key,
     * once every node can persist it.
     */
    public void testFailoverToNodeThatCanReadItsCopyRestoresTheKey() throws Exception {
        List<String> nodes = startNodesWithKey(2);
        byte[] payload = "failover-payload".getBytes(StandardCharsets.UTF_8);
        EncryptedData encrypted = internalCluster().getInstance(EncryptionService.class, nodes.get(0)).encrypt(payload);

        internalCluster().fullRestart();
        ensureStableCluster(2);
        String master = internalCluster().getMasterName();
        String other = nodes.stream().filter(node -> node.equals(master) == false).findFirst().orElseThrow();
        reload(other, passwordSettings());
        reload(master, EncryptionIntegTestUtils.passwordSettings("v2", "encryption-test-password-v2"));
        assertTrue("the master can't read its copy with a different password", pekOn(master).isUnwrapFailed());

        client().execute(TransportAddVotingConfigExclusionsAction.TYPE, new AddVotingConfigExclusionsRequest(TEST_REQUEST_TIMEOUT, master))
            .get();
        assertBusy(() -> {
            assertThat(internalCluster().getMasterName(), equalTo(other));
            assertFalse(pekOn(other).isUnwrapFailed());
            // the former master gets the restored key from the next publication
            assertFalse(pekOn(master).isUnwrapFailed());
        }, 30, TimeUnit.SECONDS);
        assertArrayEquals(payload, decryptOn(master, encrypted));
        ClearVotingConfigExclusionsRequest clear = new ClearVotingConfigExclusionsRequest(TEST_REQUEST_TIMEOUT);
        // the former master stays in the cluster
        clear.setWaitForRemoval(false);
        client().execute(TransportClearVotingConfigExclusionsAction.TYPE, clear).get();
    }

    private static byte[] decryptOn(String node, EncryptedData encrypted) {
        return internalCluster().getInstance(EncryptionService.class, node).decrypt(encrypted);
    }

    /** Starts the nodes with a password each, and waits for every one of them to hold the key. */
    private List<String> startNodesWithKey(int count) throws Exception {
        List<String> nodes = internalCluster().startNodes(count);
        ensureStableCluster(count);
        for (String node : nodes) {
            reload(node, passwordSettings());
        }
        assertBusy(() -> nodes.forEach(node -> assertThat(pekOn(node), notNullValue())), 30, TimeUnit.SECONDS);
        return nodes;
    }

    private static Settings passwordSettings() {
        return EncryptionIntegTestUtils.passwordSettings(PASSWORD_ID, PASSWORD);
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
