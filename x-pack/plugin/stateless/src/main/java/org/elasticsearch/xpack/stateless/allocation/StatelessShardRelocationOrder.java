/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package org.elasticsearch.xpack.stateless.allocation;

import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.allocator.ShardRelocationOrder;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;

import java.util.Iterator;

public class StatelessShardRelocationOrder extends ShardRelocationOrder.DefaultOrder {

    /**
     * If disabled, necessary shard relocations are not ordered explicitly and simply follow the order of shards
     * recorded in {@link org.elasticsearch.cluster.routing.RoutingNode#shards}.
     */
    public static Setting<Boolean> PRIORITIZE_WRITE_SHARD_RELOCATIONS_SETTING = Setting.boolSetting(
        "serverless.cluster.routing.allocation.prioritize_write_shard_necessary_relocations",
        true,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    private volatile boolean prioritizeWriteShardRelocation;

    public StatelessShardRelocationOrder(ClusterSettings clusterSettings) {
        clusterSettings.initializeAndWatch(PRIORITIZE_WRITE_SHARD_RELOCATIONS_SETTING, value -> prioritizeWriteShardRelocation = value);
    }

    @Override
    public Iterator<ShardRouting> forNecessaryMoves(RoutingAllocation allocation, String nodeId) {
        return prioritizeWriteShardRelocation
            ? super.forNecessaryMoves(allocation, nodeId)
            : Iterators.forArray(allocation.routingNodes().node(nodeId).copyShards());
    }

    @Override
    public Iterator<ShardRouting> forBalancing(RoutingAllocation allocation, String nodeId) {
        return super.forBalancing(allocation, nodeId);
    }
}
