/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.discovery.azure.classic;

import org.elasticsearch.cloud.azure.classic.AbstractAzureComputeServiceTestCase;
import org.elasticsearch.cloud.azure.classic.management.AzureComputeService.Discovery;
import org.elasticsearch.cloud.azure.classic.management.AzureComputeService.Management;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESIntegTestCase;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0, numClientNodes = 0)
public class AzureTwoStartedNodesTests extends AbstractAzureComputeServiceTestCase {

    public void testTwoNodesShouldRunUsingPrivateOrPublicIp() {
        final String hostType = randomFrom(AzureSeedHostsProvider.HostType.values()).getType();
        logger.info("--> using azure host type " + hostType);

        final Settings settings = Settings.builder()
            .put(Management.SERVICE_NAME_SETTING.getKey(), "dummy")
            .put(Discovery.HOST_TYPE_SETTING.getKey(), hostType)
            .build();

        logger.info("--> start first node");
        final String node1 = internalCluster().startNode(settings);
        registerAzureNode(node1);
        assertNotNull(client().admin().cluster().prepareState().setMasterNodeTimeout("1s").get().getState().nodes().getMasterNodeId());

        logger.info("--> start another node");
        final String node2 = internalCluster().startNode(settings);
        registerAzureNode(node2);
        assertNotNull(client().admin().cluster().prepareState().setMasterNodeTimeout("1s").get().getState().nodes().getMasterNodeId());

        // We expect having 2 nodes as part of the cluster, let's test that
        assertNumberOfNodes(2);
    }
}
