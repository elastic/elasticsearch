/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.plugin;

import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.shield.test.ShieldIntegrationTest;
import org.junit.Test;

import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

public class ShieldPluginTests extends ShieldIntegrationTest {

    @Test
    public void testThatPluginIsLoaded() {
        logger.info("--> Getting nodes info");
        NodesInfoResponse nodeInfos = internalCluster().transportClient().admin().cluster().prepareNodesInfo().get();
        logger.info("--> Checking nodes info that shield plugin is loaded");
        for (NodeInfo nodeInfo : nodeInfos.getNodes()) {
            assertThat(nodeInfo.getPlugins().getInfos(), hasSize(1));
            assertThat(nodeInfo.getPlugins().getInfos().get(0).getName(), is(ShieldPlugin.NAME));
        }
    }

}
