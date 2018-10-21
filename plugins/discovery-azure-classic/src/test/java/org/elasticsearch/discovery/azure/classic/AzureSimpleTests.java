/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.discovery.azure.classic;

import org.elasticsearch.cloud.azure.classic.AbstractAzureComputeServiceTestCase;
import org.elasticsearch.cloud.azure.classic.management.AzureComputeService.Discovery;
import org.elasticsearch.cloud.azure.classic.management.AzureComputeService.Management;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESIntegTestCase;

import static org.hamcrest.Matchers.containsString;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST,
    numDataNodes = 0,
    transportClientRatio = 0.0,
    numClientNodes = 0)
public class AzureSimpleTests extends AbstractAzureComputeServiceTestCase {

    public void testOneNodeShouldRunUsingPrivateIp() {
        Settings.Builder settings = Settings.builder()
                .put(Management.SERVICE_NAME_SETTING.getKey(), "dummy")
                .put(Discovery.HOST_TYPE_SETTING.getKey(), "private_ip");

        final String node1 = internalCluster().startNode(settings);
        registerAzureNode(node1);
        assertNotNull(client().admin().cluster().prepareState().setMasterNodeTimeout("1s").get().getState().nodes().getMasterNodeId());

        // We expect having 1 node as part of the cluster, let's test that
        assertNumberOfNodes(1);
    }

    public void testOneNodeShouldRunUsingPublicIp() {
        Settings.Builder settings = Settings.builder()
                .put(Management.SERVICE_NAME_SETTING.getKey(), "dummy")
                .put(Discovery.HOST_TYPE_SETTING.getKey(), "public_ip");

        final String node1 = internalCluster().startNode(settings);
        registerAzureNode(node1);
        assertNotNull(client().admin().cluster().prepareState().setMasterNodeTimeout("1s").get().getState().nodes().getMasterNodeId());

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
