/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.action.admin.cluster.desiredbalance;

import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.allocator.DesiredBalance;
import org.elasticsearch.cluster.routing.allocation.allocator.DesiredBalanceShardsAllocator;
import org.elasticsearch.cluster.routing.allocation.allocator.ShardAssignment;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;

import java.util.Map;

public class TransportGetDesiredBalanceActionTest extends ESIntegTestCase {

    public void testDesiredBalance() throws Exception {
        String index = "test";
        createIndex(index, Settings.builder().put("index.number_of_shards", 2).put("index.number_of_replicas", 0).build());

        BulkResponse bulkResponse = client().prepareBulk()
            .add(new IndexRequest(index).id("1").source("field", "foo 1"))
            .add(new IndexRequest(index).id("2").source("field", "foo 2"))
            .add(new IndexRequest(index).id("3").source("field", "foo 3"))
            .add(new IndexRequest(index).id("4").source("field", "foo 4"))
            .add(new IndexRequest(index).id("5").source("field", "bar"))
            .get();
        assertFalse(bulkResponse.hasFailures());

        DesiredBalanceResponse desiredBalanceResponse = client().execute(GetDesiredBalanceAction.INSTANCE, new DesiredBalanceRequest())
            .get();

        assertEquals(1, desiredBalanceResponse.getRoutingTable().size());
        Map<Integer, DesiredBalanceResponse.DesiredShards> shardsMap = desiredBalanceResponse.getRoutingTable().get(index);
        assertEquals(2, shardsMap.size());
        var entry = shardsMap.entrySet().iterator().next();

        Integer shardId = entry.getKey();
        DesiredBalanceResponse.DesiredShards desiredShards = entry.getValue();
        ShardRouting shard = client().admin()
            .cluster()
            .prepareState()
            .get()
            .getState()
            .routingTable()
            .shardRoutingTable(index, shardId)
            .primaryShard();

        DesiredBalanceShardsAllocator desiredBalanceShardsAllocator = internalCluster().getInstance(DesiredBalanceShardsAllocator.class);
        DesiredBalance desiredBalance = desiredBalanceShardsAllocator.getDesiredBalance();

        assertEquals(shard.state(), desiredShards.current().state());
        assertEquals(shard.primary(), desiredShards.current().primary());
        assertEquals(shardId.intValue(), desiredShards.current().shardId());
        assertEquals(index, desiredShards.current().index());
        assertEquals(shard.currentNodeId(), desiredShards.current().node());
        ShardAssignment assignment = desiredBalance.getAssignment(shard.shardId());
        assertEquals(assignment != null && assignment.nodeIds().contains(shard.currentNodeId()), desiredShards.current().nodeIsDesired());
        assertEquals(shard.relocatingNodeId(), desiredShards.current().relocatingNode());
        assertEquals(
            assignment != null && assignment.nodeIds().contains(shard.relocatingNodeId()),
            desiredShards.current().relocatingNodeIsDesired()
        );
        assertFalse(desiredShards.current().relocatingNodeIsDesired());
    }
}
