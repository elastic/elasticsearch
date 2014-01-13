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

package org.elasticsearch.cloud.gce.tests;

import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.cloud.gce.GceComputeService;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Assert;

@ElasticsearchIntegrationTest.ClusterScope(scope = ElasticsearchIntegrationTest.Scope.TEST, numNodes = 0)
public abstract class GceAbstractTest extends ElasticsearchIntegrationTest {

    private Class<? extends GceComputeService> mock;

    public GceAbstractTest(Class<? extends GceComputeService> mock) {
        // We want to inject the GCE API Mock
        this.mock = mock;
    }

    protected void checkNumberOfNodes(int expected) {
        NodesInfoResponse nodeInfos = client().admin().cluster().prepareNodesInfo().execute().actionGet();

        Assert.assertNotNull(nodeInfos.getNodes());
        Assert.assertEquals(expected, nodeInfos.getNodes().length);
    }

    protected void nodeBuilder(String filteredTags) {
        ImmutableSettings.Builder nodeSettings = ImmutableSettings.settingsBuilder()
                .put("cloud.gce.api.impl", mock)
                .put("cloud.gce.refresh_interval", "5s");
        if (filteredTags != null) {
            nodeSettings.put("discovery.gce.tags", filteredTags);
        } else {
            nodeSettings.put("discovery.gce.tags", "");
        }
        cluster().startNode(nodeSettings);
    }
}
