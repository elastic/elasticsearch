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
package org.elasticsearch.gateway;

import org.elasticsearch.client.Requests;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalTestCluster;

import java.nio.file.Path;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class DanglingIndicesIT extends ESIntegTestCase {

    public void testDanglingIndices() throws Exception {
        // replicating the case described in https://github.com/elastic/elasticsearch/issues/27073
        final String indexName = "simple1";

        final InternalTestCluster cluster = internalCluster();
        String node1 = cluster.startNode();
        String node2 = cluster.startNode();

        final boolean allocateOnNode2 = randomBoolean();

        createIndex(indexName,
            Settings.builder()
                .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0)
                // explicitly specify shard location
                .put("index.routing.allocation.include._name", allocateOnNode2 ? node2 : node1)
                .build());
        ensureGreen(indexName);

        if (randomBoolean()) {
            index(indexName, "_doc", "1", "f", "f");
        }

        final NodeEnvironment env = internalCluster().getInstance(NodeEnvironment.class, node1);
        final Path[] node1DataPaths = env.nodeDataPaths();

        cluster.stopRandomNode(InternalTestCluster.nameFilter(node1));
        cluster.stopRandomNode(InternalTestCluster.nameFilter(node2));

        // clean up data path on node1 to trigger dangling index being picked up
        IOUtils.rm(node1DataPaths);

        // start node that could have dangling index
        node1 = cluster.startNode();
        final boolean masterNode = randomBoolean();
        try {
            node2 = masterNode ? cluster.startMasterOnlyNode() : cluster.startCoordinatingOnlyNode(Settings.EMPTY);
            assertThat("Node 2 has to fail as it contains shard data",
                allocateOnNode2, equalTo(false));
            assertThat("index exists but has to be red",
                client().admin().cluster().health(Requests.clusterHealthRequest(indexName)).get().getStatus(),
                equalTo(ClusterHealthStatus.RED));
        } catch (IllegalStateException e) {
            assertThat(allocateOnNode2 || masterNode == false, equalTo(true));
            assertThat(e.getMessage(), containsString(" node cannot have shard data "));
        }
    }

}
