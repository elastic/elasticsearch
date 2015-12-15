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

package org.elasticsearch.cluster.routing;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.allocation.decider.AwarenessAllocationDecider;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.math.MathUtils;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardNotFoundException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 *
 */
public class OperationRouting extends AbstractComponent {


    private final AwarenessAllocationDecider awarenessAllocationDecider;

    @Inject
    public OperationRouting(Settings settings, AwarenessAllocationDecider awarenessAllocationDecider) {
        super(settings);
        this.awarenessAllocationDecider = awarenessAllocationDecider;
    }

    public ShardIterator indexShards(ClusterState clusterState, String index, String type, String id, @Nullable String routing) {
        return shards(clusterState, index, id, routing).shardsIt();
    }

    public ShardIterator getShards(ClusterState clusterState, String index, String type, String id, @Nullable String routing, @Nullable String preference) {
        return preferenceActiveShardIterator(shards(clusterState, index, id, routing), clusterState.nodes().localNodeId(), clusterState.nodes(), preference);
    }

    public ShardIterator getShards(ClusterState clusterState, String index, int shardId, @Nullable String preference) {
        final IndexShardRoutingTable indexShard = clusterState.getRoutingTable().shardRoutingTable(index, shardId);
        return preferenceActiveShardIterator(indexShard, clusterState.nodes().localNodeId(), clusterState.nodes(), preference);
    }

    public GroupShardsIterator broadcastDeleteShards(ClusterState clusterState, String index) {
        return indexRoutingTable(clusterState, index).groupByShardsIt();
    }

    public int searchShardsCount(ClusterState clusterState, String[] concreteIndices, @Nullable Map<String, Set<String>> routing) {
        final Set<IndexShardRoutingTable> shards = computeTargetedShards(clusterState, concreteIndices, routing);
        return shards.size();
    }

    public GroupShardsIterator searchShards(ClusterState clusterState, String[] concreteIndices, @Nullable Map<String, Set<String>> routing, @Nullable String preference) {
        final Set<IndexShardRoutingTable> shards = computeTargetedShards(clusterState, concreteIndices, routing);
        final Set<ShardIterator> set = new HashSet<>(shards.size());
        for (IndexShardRoutingTable shard : shards) {
            ShardIterator iterator = preferenceActiveShardIterator(shard, clusterState.nodes().localNodeId(), clusterState.nodes(), preference);
            if (iterator != null) {
                set.add(iterator);
            }
        }
        return new GroupShardsIterator(new ArrayList<>(set));
    }

    private static final Map<String, Set<String>> EMPTY_ROUTING = Collections.emptyMap();

    private Set<IndexShardRoutingTable> computeTargetedShards(ClusterState clusterState, String[] concreteIndices, @Nullable Map<String, Set<String>> routing) {
        routing = routing == null ? EMPTY_ROUTING : routing; // just use an empty map
        final Set<IndexShardRoutingTable> set = new HashSet<>();
        // we use set here and not list since we might get duplicates
        for (String index : concreteIndices) {
            final IndexRoutingTable indexRouting = indexRoutingTable(clusterState, index);
            final Set<String> effectiveRouting = routing.get(index);
            if (effectiveRouting != null) {
                for (String r : effectiveRouting) {
                    int shardId = generateShardId(clusterState, index, null, r);
                    IndexShardRoutingTable indexShard = indexRouting.shard(shardId);
                    if (indexShard == null) {
                        throw new ShardNotFoundException(new ShardId(index, shardId));
                    }
                    // we might get duplicates, but that's ok, they will override one another
                    set.add(indexShard);
                }
            } else {
                for (IndexShardRoutingTable indexShard : indexRouting) {
                    set.add(indexShard);
                }
            }
        }
        return set;
    }

    private ShardIterator preferenceActiveShardIterator(IndexShardRoutingTable indexShard, String localNodeId, DiscoveryNodes nodes, @Nullable String preference) {
        if (preference == null || preference.isEmpty()) {
            String[] awarenessAttributes = awarenessAllocationDecider.awarenessAttributes();
            if (awarenessAttributes.length == 0) {
                return indexShard.activeInitializingShardsRandomIt();
            } else {
                return indexShard.preferAttributesActiveInitializingShardsIt(awarenessAttributes, nodes);
            }
        }
        if (preference.charAt(0) == '_') {
            Preference preferenceType = Preference.parse(preference);
            if (preferenceType == Preference.SHARDS) {
                // starts with _shards, so execute on specific ones
                int index = preference.indexOf(';');

                String shards;
                if (index == -1) {
                    shards = preference.substring(Preference.SHARDS.type().length() + 1);
                } else {
                    shards = preference.substring(Preference.SHARDS.type().length() + 1, index);
                }
                String[] ids = Strings.splitStringByCommaToArray(shards);
                boolean found = false;
                for (String id : ids) {
                    if (Integer.parseInt(id) == indexShard.shardId().id()) {
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    return null;
                }
                // no more preference
                if (index == -1 || index == preference.length() - 1) {
                    String[] awarenessAttributes = awarenessAllocationDecider.awarenessAttributes();
                    if (awarenessAttributes.length == 0) {
                        return indexShard.activeInitializingShardsRandomIt();
                    } else {
                        return indexShard.preferAttributesActiveInitializingShardsIt(awarenessAttributes, nodes);
                    }
                } else {
                    // update the preference and continue
                    preference = preference.substring(index + 1);
                }
            }
            preferenceType = Preference.parse(preference);
            switch (preferenceType) {
                case PREFER_NODE:
                    return indexShard.preferNodeActiveInitializingShardsIt(preference.substring(Preference.PREFER_NODE.type().length() + 1));
                case LOCAL:
                    return indexShard.preferNodeActiveInitializingShardsIt(localNodeId);
                case PRIMARY:
                    return indexShard.primaryActiveInitializingShardIt();
                case REPLICA:
                    return indexShard.replicaActiveInitializingShardIt();
                case PRIMARY_FIRST:
                    return indexShard.primaryFirstActiveInitializingShardsIt();
                case REPLICA_FIRST:
                    return indexShard.replicaFirstActiveInitializingShardsIt();
                case ONLY_LOCAL:
                    return indexShard.onlyNodeActiveInitializingShardsIt(localNodeId);
                case ONLY_NODE:
                    String nodeId = preference.substring(Preference.ONLY_NODE.type().length() + 1);
                    ensureNodeIdExists(nodes, nodeId);
                    return indexShard.onlyNodeActiveInitializingShardsIt(nodeId);
                case ONLY_NODES:
                    String nodeAttribute = preference.substring(Preference.ONLY_NODES.type().length() + 1);
                    return indexShard.onlyNodeSelectorActiveInitializingShardsIt(nodeAttribute, nodes);
                default:
                    throw new IllegalArgumentException("unknown preference [" + preferenceType + "]");
            }
        }
        // if not, then use it as the index
        String[] awarenessAttributes = awarenessAllocationDecider.awarenessAttributes();
        if (awarenessAttributes.length == 0) {
            return indexShard.activeInitializingShardsIt(Murmur3HashFunction.hash(preference));
        } else {
            return indexShard.preferAttributesActiveInitializingShardsIt(awarenessAttributes, nodes, Murmur3HashFunction.hash(preference));
        }
    }

    protected IndexRoutingTable indexRoutingTable(ClusterState clusterState, String index) {
        IndexRoutingTable indexRouting = clusterState.routingTable().index(index);
        if (indexRouting == null) {
            throw new IndexNotFoundException(index);
        }
        return indexRouting;
    }

    protected IndexShardRoutingTable shards(ClusterState clusterState, String index, String id, String routing) {
        int shardId = generateShardId(clusterState, index, id, routing);
        return clusterState.getRoutingTable().shardRoutingTable(index, shardId);
    }

    public ShardId shardId(ClusterState clusterState, String index, String id, @Nullable String routing) {
        return new ShardId(index, generateShardId(clusterState, index, id, routing));
    }

    private int generateShardId(ClusterState clusterState, String index, String id, @Nullable String routing) {
        IndexMetaData indexMetaData = clusterState.metaData().index(index);
        if (indexMetaData == null) {
            throw new IndexNotFoundException(index);
        }
        final int hash;
        if (routing == null) {
            hash = Murmur3HashFunction.hash(id);
        } else {
            hash = Murmur3HashFunction.hash(routing);
        }
        return MathUtils.mod(hash, indexMetaData.getNumberOfShards());
    }

    private void ensureNodeIdExists(DiscoveryNodes nodes, String nodeId) {
        if (!nodes.dataNodes().keys().contains(nodeId)) {
            throw new IllegalArgumentException("No data node with id[" + nodeId + "] found");
        }
    }

}
