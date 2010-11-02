/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
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

package org.elasticsearch.cluster.routing.operation.plain;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.routing.GroupShardsIterator;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.ShardsIterator;
import org.elasticsearch.cluster.routing.operation.OperationRouting;
import org.elasticsearch.cluster.routing.operation.hash.HashFunction;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexShardMissingException;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndexMissingException;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.regex.Pattern;

/**
 * @author kimchy (shay.banon)
 */
public class PlainOperationRouting extends AbstractComponent implements OperationRouting {

    public final static Pattern routingPattern = Pattern.compile(",");

    private final HashFunction hashFunction;

    @Inject public PlainOperationRouting(Settings indexSettings, HashFunction hashFunction) {
        super(indexSettings);
        this.hashFunction = hashFunction;
    }

    @Override public ShardsIterator indexShards(ClusterState clusterState, String index, String type, String id, @Nullable String routing) throws IndexMissingException, IndexShardMissingException {
        return shards(clusterState, index, type, id, routing).shardsIt();
    }

    @Override public ShardsIterator deleteShards(ClusterState clusterState, String index, String type, String id, @Nullable String routing) throws IndexMissingException, IndexShardMissingException {
        return shards(clusterState, index, type, id, routing).shardsIt();
    }

    @Override public ShardsIterator getShards(ClusterState clusterState, String index, String type, String id, @Nullable String routing) throws IndexMissingException, IndexShardMissingException {
        return shards(clusterState, index, type, id, routing).shardsRandomIt();
    }

    @Override public GroupShardsIterator deleteByQueryShards(ClusterState clusterState, String index, @Nullable String routing) throws IndexMissingException {
        if (routing == null) {
            return indexRoutingTable(clusterState, index).groupByShardsIt();
        }

        String[] routings = routingPattern.split(routing);
        if (routing.length() == 0) {
            return indexRoutingTable(clusterState, index).groupByShardsIt();
        }

        // we use set here and not identity set since we might get duplicates
        HashSet<ShardsIterator> set = new HashSet<ShardsIterator>();
        IndexRoutingTable indexRouting = indexRoutingTable(clusterState, index);
        for (String r : routings) {
            int shardId = shardId(clusterState, index, null, null, r);
            IndexShardRoutingTable indexShard = indexRouting.shard(shardId);
            if (indexShard == null) {
                throw new IndexShardMissingException(new ShardId(index, shardId));
            }
            set.add(indexShard.shardsRandomIt());
        }
        return new GroupShardsIterator(set);
    }

    @Override public GroupShardsIterator searchShards(ClusterState clusterState, String[] indices, @Nullable String queryHint, @Nullable String routing) throws IndexMissingException {
        if (indices == null || indices.length == 0) {
            indices = clusterState.metaData().concreteAllIndices();
        }

        String[] routings = null;
        if (routing != null) {
            routings = routingPattern.split(routing);
        }

        if (routings != null && routings.length > 0) {
            // we use set here and not list since we might get duplicates
            HashSet<ShardsIterator> set = new HashSet<ShardsIterator>();
            for (String index : indices) {
                IndexRoutingTable indexRouting = indexRoutingTable(clusterState, index);
                for (String r : routings) {
                    int shardId = shardId(clusterState, index, null, null, r);
                    IndexShardRoutingTable indexShard = indexRouting.shard(shardId);
                    if (indexShard == null) {
                        throw new IndexShardMissingException(new ShardId(index, shardId));
                    }
                    // we might get duplicates, but that's ok, they will override one another
                    set.add(indexShard.shardsRandomIt());
                }
            }
            return new GroupShardsIterator(set);
        } else {
            // we use list here since we know we are not going to create duplicates
            ArrayList<ShardsIterator> set = new ArrayList<ShardsIterator>();
            for (String index : indices) {
                IndexRoutingTable indexRouting = indexRoutingTable(clusterState, index);
                for (IndexShardRoutingTable indexShard : indexRouting) {
                    set.add(indexShard.shardsRandomIt());
                }
            }
            return new GroupShardsIterator(set);
        }
    }

    public IndexMetaData indexMetaData(ClusterState clusterState, String index) {
        IndexMetaData indexMetaData = clusterState.metaData().index(index);
        if (indexMetaData == null) {
            throw new IndexMissingException(new Index(index));
        }
        return indexMetaData;
    }

    protected IndexRoutingTable indexRoutingTable(ClusterState clusterState, String index) {
        IndexRoutingTable indexRouting = clusterState.routingTable().index(index);
        if (indexRouting == null) {
            throw new IndexMissingException(new Index(index));
        }
        return indexRouting;
    }


    // either routing is set, or type/id are set

    protected IndexShardRoutingTable shards(ClusterState clusterState, String index, String type, String id, String routing) {
        int shardId = shardId(clusterState, index, type, id, routing);
        IndexShardRoutingTable indexShard = indexRoutingTable(clusterState, index).shard(shardId);
        if (indexShard == null) {
            throw new IndexShardMissingException(new ShardId(index, shardId));
        }
        return indexShard;
    }

    private int shardId(ClusterState clusterState, String index, String type, @Nullable String id, @Nullable String routing) {
        if (routing == null) {
            return Math.abs(hash(type, id)) % indexMetaData(clusterState, index).numberOfShards();
        }
        return Math.abs(hash(routing)) % indexMetaData(clusterState, index).numberOfShards();
    }

    protected int hash(String routing) {
        return hashFunction.hash(routing);
    }

    protected int hash(String type, String id) {
        return hashFunction.hash(type, id);
    }
}
