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
package org.elasticsearch.cluster.routing.allocation.decider;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.test.ESIntegTestCase;

import static org.hamcrest.Matchers.equalTo;

public class ShardStateIT extends ESIntegTestCase {

    public void testPrimaryFailureIncreasesTerm() throws Exception {
        internalCluster().ensureAtLeastNumDataNodes(2);
        prepareCreate("test").setSettings(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 2, IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 1).get();
        ensureGreen();
        ClusterState state = client().admin().cluster().prepareState().get().getState();
        IndexMetaData metaData = state.metaData().index("test");
        assertThat(metaData.primaryTerm(0), equalTo(0));
        assertThat(metaData.primaryTerm(1), equalTo(0));

        logger.info("--> disabling allocation to capture shard failure");
        disableAllocation("test");

        final int shard = randomBoolean() ? 0 : 1;
        final String nodeId = state.routingTable().index("test").shard(shard).primaryShard().currentNodeId();
        final String node = state.nodes().get(nodeId).name();
        logger.info("--> failing primary of [{}] on node [{}]", shard, node);
        IndicesService indicesService = internalCluster().getInstance(IndicesService.class, node);
        indicesService.indexService("test").getShard(shard).failShard("simulated test failure", null);

        logger.info("--> waiting for a yellow index");
        assertBusy(() -> assertThat(client().admin().cluster().prepareHealth().get().getStatus(), equalTo(ClusterHealthStatus.YELLOW)));

        state = client().admin().cluster().prepareState().get().getState();
        metaData = state.metaData().index("test");
        assertThat(metaData.primaryTerm(shard), equalTo(1));
        assertThat(metaData.primaryTerm(shard ^ 1), equalTo(0));

        logger.info("--> enabling allocation");
        enableAllocation("test");
        ensureGreen();
        state = client().admin().cluster().prepareState().get().getState();
        metaData = state.metaData().index("test");
        assertThat(metaData.primaryTerm(shard), equalTo(1));
        assertThat(metaData.primaryTerm(shard ^ 1), equalTo(0));
    }
}
