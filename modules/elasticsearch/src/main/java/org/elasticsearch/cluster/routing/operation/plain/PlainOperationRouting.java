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
import org.elasticsearch.common.collect.IdentityHashSet;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexShardMissingException;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndexMissingException;

import javax.annotation.Nullable;

/**
 * @author kimchy (shay.banon)
 */
public class PlainOperationRouting extends AbstractComponent implements OperationRouting {

    private final HashFunction hashFunction;

    @Inject public PlainOperationRouting(Settings indexSettings, HashFunction hashFunction) {
        super(indexSettings);
        this.hashFunction = hashFunction;
    }

    @Override public ShardsIterator indexShards(ClusterState clusterState, String index, String type, String id) throws IndexMissingException, IndexShardMissingException {
        return shards(clusterState, index, type, id).shardsIt();
    }

    @Override public ShardsIterator deleteShards(ClusterState clusterState, String index, String type, String id) throws IndexMissingException, IndexShardMissingException {
        return shards(clusterState, index, type, id).shardsIt();
    }

    @Override public ShardsIterator getShards(ClusterState clusterState, String index, String type, String id) throws IndexMissingException, IndexShardMissingException {
        return shards(clusterState, index, type, id).shardsRandomIt();
    }

    @Override public GroupShardsIterator deleteByQueryShards(ClusterState clusterState, String index) throws IndexMissingException {
        return indexRoutingTable(clusterState, index).groupByShardsIt();
    }

    @Override public GroupShardsIterator searchShards(ClusterState clusterState, String[] indices, @Nullable String queryHint) throws IndexMissingException {
        if (indices == null || indices.length == 0) {
            indices = clusterState.metaData().concreteAllIndices();
        }

        IdentityHashSet<ShardsIterator> set = new IdentityHashSet<ShardsIterator>();
        for (String index : indices) {
            IndexRoutingTable indexRouting = indexRoutingTable(clusterState, index);
            for (IndexShardRoutingTable indexShard : indexRouting) {
                set.add(indexShard.shardsRandomIt());
            }
        }
        return new GroupShardsIterator(set);
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


    protected IndexShardRoutingTable shards(ClusterState clusterState, String index, String type, String id) {
        int shardId = Math.abs(hash(type, id)) % indexMetaData(clusterState, index).numberOfShards();
        IndexShardRoutingTable indexShard = indexRoutingTable(clusterState, index).shard(shardId);
        if (indexShard == null) {
            throw new IndexShardMissingException(new ShardId(index, shardId));
        }
        return indexShard;
    }

    protected int hash(String type, String id) {
        return hashFunction.hash(type, id);
    }
}
