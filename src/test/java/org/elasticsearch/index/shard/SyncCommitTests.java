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
package org.elasticsearch.index.shard;

import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.test.ElasticsearchIntegrationTest;

public class SyncCommitTests extends ElasticsearchIntegrationTest {

    public void testSyncCommit() {
        internalCluster().ensureAtLeastNumDataNodes(2);
        prepareCreate("test").setSettings(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1, IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 1).get();
        ensureGreen();
        ClusterStateResponse state = client().admin().cluster().prepareState().get();
        String nodeId = state.getState().getRoutingTable().index("test").shard(0).getShards().get(0).currentNodeId();
        String nodeName = state.getState().getNodes().get(nodeId).name();
        IndicesService indicesService = internalCluster().getInstance(IndicesService.class, nodeName);
        indicesService.indexServiceSafe("test").shardInjectorSafe(0).getInstance(SyncCommitService.class).attemptSyncCommit();
    }
}
