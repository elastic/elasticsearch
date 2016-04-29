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

package org.elasticsearch.discovery.azure;

import org.elasticsearch.cloud.azure.AbstractAzureComputeServiceTestCase;
import org.elasticsearch.cloud.azure.AzureComputeServiceSimpleMock;
import org.elasticsearch.cloud.azure.management.AzureComputeService.Discovery;
import org.elasticsearch.cloud.azure.management.AzureComputeService.Management;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESIntegTestCase;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.notNullValue;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST,
        numDataNodes = 0,
        transportClientRatio = 0.0,
        numClientNodes = 0)
public class AzureSimpleTests extends AbstractAzureComputeServiceTestCase {
    public AzureSimpleTests() {
        super(AzureComputeServiceSimpleMock.TestPlugin.class);
    }

    public void testOneNodeDhouldRunUsingPrivateIp() {
        Settings.Builder settings = Settings.builder()
                .put(Management.SERVICE_NAME_SETTING.getKey(), "dummy")
                .put(Discovery.HOST_TYPE_SETTING.getKey(), "private_ip");

        logger.info("--> start one node");
        internalCluster().startNode(settings);
        assertThat(client().admin().cluster().prepareState().setMasterNodeTimeout("1s").execute().actionGet().getState().nodes().getMasterNodeId(), notNullValue());

        // We expect having 1 node as part of the cluster, let's test that
        checkNumberOfNodes(1);
    }

    public void testOneNodeShouldRunUsingPublicIp() {
        Settings.Builder settings = Settings.builder()
                .put(Management.SERVICE_NAME_SETTING.getKey(), "dummy")
                .put(Discovery.HOST_TYPE_SETTING.getKey(), "public_ip");

        logger.info("--> start one node");
        internalCluster().startNode(settings);
        assertThat(client().admin().cluster().prepareState().setMasterNodeTimeout("1s").execute().actionGet().getState().nodes().getMasterNodeId(), notNullValue());

        // We expect having 1 node as part of the cluster, let's test that
        checkNumberOfNodes(1);
    }

    public void testOneNodeShouldRunUsingWrongSettings() {
        Settings.Builder settings = Settings.builder()
                .put(Management.SERVICE_NAME_SETTING.getKey(), "dummy")
                .put(Discovery.HOST_TYPE_SETTING.getKey(), "do_not_exist");

        logger.info("--> start one node");
        try {
            internalCluster().startNode(settings);
            fail("Expected IllegalArgumentException on startup");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), containsString("invalid value for host type [do_not_exist]"));
        }
    }
}
