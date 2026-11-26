/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.encryption;

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
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.notNullValue;

/**
 * A master-eligible or data node that cannot persist the project encryption key cannot persist any cluster state, so once the key is
 * installed such a node is not allowed to join, and it can't reload its password away. Installing the key only once every such node
 * can persist it is covered by {@code MissingPasswordRolloutIT}.
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
