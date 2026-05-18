/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.discovery.azure.classic;

import org.elasticsearch.cloud.azure.classic.AbstractAzureComputeServiceTestCase;
import org.elasticsearch.cloud.azure.classic.management.AzureComputeService.Discovery;
import org.elasticsearch.cloud.azure.classic.management.AzureComputeService.Management;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESIntegTestCase;

import static org.hamcrest.Matchers.containsString;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0, numClientNodes = 0)
public class AzureSimpleTests extends AbstractAzureComputeServiceTestCase {

    public void testOneNodeShouldRunUsingPrivateIp() {
        Settings.Builder settings = Settings.builder()
            .put(Management.SERVICE_NAME_SETTING.getKey(), "dummy")
            .put(Discovery.HOST_TYPE_SETTING.getKey(), "private_ip");

        final String node1 = internalCluster().startNode(settings);
        registerAzureNode(node1);
        awaitMasterNode();

        // We expect having 1 node as part of the cluster, let's test that
        assertNumberOfNodes(1);
    }

    public void testOneNodeShouldRunUsingPublicIp() {
        Settings.Builder settings = Settings.builder()
            .put(Management.SERVICE_NAME_SETTING.getKey(), "dummy")
            .put(Discovery.HOST_TYPE_SETTING.getKey(), "public_ip");

        final String node1 = internalCluster().startNode(settings);
        registerAzureNode(node1);
        awaitMasterNode();

        // We expect having 1 node as part of the cluster, let's test that
        assertNumberOfNodes(1);
    }

    public void testOneNodeShouldRunUsingWrongSettings() {
        Settings.Builder settings = Settings.builder()
            .put(Management.SERVICE_NAME_SETTING.getKey(), "dummy")
            .put(Discovery.HOST_TYPE_SETTING.getKey(), "do_not_exist");

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> internalCluster().startNode(settings));
        assertThat(e.getMessage(), containsString("invalid value for host type [do_not_exist]"));
    }
}
