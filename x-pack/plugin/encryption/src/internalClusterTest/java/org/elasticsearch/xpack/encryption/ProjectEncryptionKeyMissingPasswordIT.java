/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.encryption;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collection;

import static org.hamcrest.Matchers.containsString;

/**
 * A master-eligible or data node that cannot persist the project encryption key cannot persist any cluster state, so it can't reload
 * its password away. Installing the key only once every such node can persist it is covered by {@code MissingPasswordRolloutIT}.
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

}
