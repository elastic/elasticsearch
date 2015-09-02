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

package org.elasticsearch.cloud.azure;

import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.cloud.azure.management.AzureComputeService.Discovery;
import org.elasticsearch.cloud.azure.management.AzureComputeService.Management;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugin.cloud.azure.CloudAzurePlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.Collection;

public abstract class AbstractAzureComputeServiceTestCase extends ESIntegTestCase {

    private Class<? extends Plugin> mockPlugin;

    public AbstractAzureComputeServiceTestCase(Class<? extends Plugin> mockPlugin) {
        // We want to inject the Azure API Mock
        this.mockPlugin = mockPlugin;
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        Settings.Builder builder = Settings.settingsBuilder()
            .put(super.nodeSettings(nodeOrdinal))
            .put("discovery.type", "azure")
                // We need the network to make the mock working
            .put("node.mode", "network");

        // We add a fake subscription_id to start mock compute service
        builder.put(Management.SUBSCRIPTION_ID, "fake")
            .put(Discovery.REFRESH, "5s")
            .put(Management.KEYSTORE_PATH, "dummy")
            .put(Management.KEYSTORE_PASSWORD, "dummy")
            .put(Management.SERVICE_NAME, "dummy");
        return builder.build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return pluginList(CloudAzurePlugin.class, mockPlugin);
    }

    protected void checkNumberOfNodes(int expected) {
        NodesInfoResponse nodeInfos = client().admin().cluster().prepareNodesInfo().execute().actionGet();
        assertNotNull(nodeInfos);
        assertNotNull(nodeInfos.getNodes());
        assertEquals(expected, nodeInfos.getNodes().length);
    }
}
