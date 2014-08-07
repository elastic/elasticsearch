/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.plugin;

import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.junit.Test;

import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.elasticsearch.test.ElasticsearchIntegrationTest.ClusterScope;
import static org.elasticsearch.test.ElasticsearchIntegrationTest.Scope;

/**
 *
 */
@ClusterScope(scope = Scope.SUITE, numDataNodes = 2)
public class ShieldPluginTests extends ElasticsearchIntegrationTest {

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return ImmutableSettings.settingsBuilder()
                .put("plugin.types", SecurityPlugin.class.getName())
                .put(super.nodeSettings(nodeOrdinal))
                .build();
    }

    @Test
    @TestLogging("_root:INFO,plugins.PluginsService:TRACE")
    public void testThatPluginIsLoaded() {
        logger.info("--> Getting nodes info");
        NodesInfoResponse nodeInfos = internalCluster().transportClient().admin().cluster().prepareNodesInfo().get();
        logger.info("--> Checking nodes info");
        for (NodeInfo nodeInfo : nodeInfos.getNodes()) {
            assertThat(nodeInfo.getPlugins().getInfos(), hasSize(1));
            assertThat(nodeInfo.getPlugins().getInfos().get(0).getName(), is(SecurityPlugin.NAME));
        }
    }

}
