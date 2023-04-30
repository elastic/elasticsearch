/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.coordination;

import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.Collection;
import java.util.List;
import java.util.function.Supplier;

import static org.elasticsearch.node.Node.INITIAL_STATE_TIMEOUT_SETTING;
import static org.hamcrest.Matchers.equalTo;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0, autoManageMasterNodes = false)
public class RestHandlerNodesIT extends ESIntegTestCase {

    public static class TestPlugin extends Plugin implements ActionPlugin {

        volatile Supplier<DiscoveryNodes> nodesInCluster;

        @Override
        public List<RestHandler> getRestHandlers(
            Settings settings,
            RestController restController,
            ClusterSettings clusterSettings,
            IndexScopedSettings indexScopedSettings,
            SettingsFilter settingsFilter,
            IndexNameExpressionResolver indexNameExpressionResolver,
            Supplier<DiscoveryNodes> nodesInCluster
        ) {
            this.nodesInCluster = nodesInCluster;
            return List.of();
        }
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(INITIAL_STATE_TIMEOUT_SETTING.getKey(), "0")
            .build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(TestPlugin.class);
    }

    public void testNodesExposedToRestHandler() {
        internalCluster().setBootstrapMasterNodeIndex(0);
        internalCluster().startDataOnlyNode();

        final var dataNodeSupplier = internalCluster().getInstance(PluginsService.class)
            .filterPlugins(TestPlugin.class)
            .get(0).nodesInCluster;

        assertEquals(DiscoveryNodes.EMPTY_NODES, dataNodeSupplier.get());

        internalCluster().startMasterOnlyNode();
        internalCluster().validateClusterFormed();

        final var masterNodeSupplier = internalCluster().getCurrentMasterNodeInstance(PluginsService.class)
            .filterPlugins(TestPlugin.class)
            .get(0).nodesInCluster;

        assertThat(dataNodeSupplier.get().size(), equalTo(2));
        assertThat(masterNodeSupplier.get().size(), equalTo(2));

        internalCluster().startNode();
        internalCluster().validateClusterFormed();

        assertThat(dataNodeSupplier.get().size(), equalTo(3));
        assertThat(masterNodeSupplier.get().size(), equalTo(3));
    }

}
