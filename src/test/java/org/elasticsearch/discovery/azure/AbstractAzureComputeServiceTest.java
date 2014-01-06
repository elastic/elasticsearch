/*
 * Licensed to ElasticSearch under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.cloud.azure.AbstractAzureTest;
import org.elasticsearch.cloud.azure.AzureComputeService;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;

public abstract class AbstractAzureComputeServiceTest extends AbstractAzureTest {

    private Class<? extends AzureComputeService> mock;

    public AbstractAzureComputeServiceTest(Class<? extends AzureComputeService> mock) {
        // We want to inject the Azure API Mock
        this.mock = mock;
    }

    protected void checkNumberOfNodes(int expected) {
        NodesInfoResponse nodeInfos = client().admin().cluster().prepareNodesInfo().execute().actionGet();
        assertNotNull(nodeInfos);
        assertNotNull(nodeInfos.getNodes());
        assertEquals(expected, nodeInfos.getNodes().length);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        ImmutableSettings.Builder builder = ImmutableSettings.settingsBuilder()
                .put("cloud.azure.api.impl", mock)
                // We add a fake subscription_id to start mock compute service
                .put("cloud.azure.subscription_id", "fake")
                .put("cloud.azure.refresh_interval", "5s");

        return builder.build();
    }
}
