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

package org.elasticsearch.cluster.action.shard;

import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.test.ESIntegTestCase;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class ShardStateActionIT extends ESIntegTestCase {
    public void testMarkShardsResyncFailed() throws Exception {
        internalCluster().startDataOnlyNodes(between(1, 3));
        int numberOfShards = between(1, 10);
        assertAcked(prepareCreate("test", Settings.builder().put(indexSettings())
            .put(SETTING_NUMBER_OF_SHARDS, numberOfShards)
            .put(SETTING_NUMBER_OF_REPLICAS, internalCluster().numDataNodes() - 1)));
        ensureGreen("test");
        final Index index = clusterService().state().metaData().index("test").getIndex();
        final IndexShard shardToMark = internalCluster().getDataNodeInstance(IndicesService.class)
            .indexServiceSafe(index).getShard(between(0, numberOfShards - 1));
        final ShardStateAction shardStateAction = internalCluster().getInstance(ShardStateAction.class);
        shardStateAction.markShardResyncFailed(shardToMark.shardId(),
            shardToMark.routingEntry().allocationId().getId(), shardToMark.getPrimaryTerm(),
            "testMessage", new IOException("TestException"), new ShardStateAction.Listener() {});

        assertBusy(() -> {
            final ShardId shardId = shardToMark.shardId();
            final IndexRoutingTable indexRoutingTable = clusterService().state().routingTable().index(shardId.getIndex());
            final List<ShardRouting> markedShards = indexRoutingTable.shard(shardId.id()).getShards().stream()
                .filter(s -> s.resyncFailedInfo() != null).collect(Collectors.toList());
            assertThat(markedShards, hasSize(1));
            assertThat(markedShards.get(0).allocationId(), equalTo(shardToMark.routingEntry().allocationId()));
            assertThat(markedShards.get(0).resyncFailedInfo().getReason(), equalTo("testMessage"));
            assertThat(markedShards.get(0).resyncFailedInfo().getFailure().getMessage(), equalTo("TestException"));
        });
        ensureGreen("test");
    }
}
