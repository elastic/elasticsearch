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

package org.elasticsearch.index.routing.plain;

import com.google.inject.Inject;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.routing.GroupShardsIterator;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.ShardsIterator;
import org.elasticsearch.index.AbstractIndexComponent;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexShardMissingException;
import org.elasticsearch.index.routing.OperationRouting;
import org.elasticsearch.index.routing.hash.HashFunction;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndexMissingException;
import org.elasticsearch.util.IdentityHashSet;
import org.elasticsearch.util.Nullable;
import org.elasticsearch.util.settings.Settings;

/**
 * @author kimchy (Shay Banon)
 */
public class PlainOperationRouting extends AbstractIndexComponent implements OperationRouting {

    private final HashFunction hashFunction;

    @Inject public PlainOperationRouting(Index index, @IndexSettings Settings indexSettings, HashFunction hashFunction) {
        super(index, indexSettings);
        this.hashFunction = hashFunction;
    }

    @Override public ShardsIterator indexShards(ClusterState clusterState, String type, String id) throws IndexMissingException, IndexShardMissingException {
        return shards(clusterState, type, id).shardsIt();
    }

    @Override public ShardsIterator deleteShards(ClusterState clusterState, String type, String id) throws IndexMissingException, IndexShardMissingException {
        return shards(clusterState, type, id).shardsIt();
    }

    @Override public ShardsIterator getShards(ClusterState clusterState, String type, String id) throws IndexMissingException, IndexShardMissingException {
        return shards(clusterState, type, id).shardsRandomIt();
    }

    @Override public GroupShardsIterator deleteByQueryShards(ClusterState clusterState) throws IndexMissingException {
        return indexRoutingTable(clusterState).groupByShardsIt();
    }

    @Override public GroupShardsIterator searchShards(ClusterState clusterState, @Nullable String queryHint) throws IndexMissingException {
        IdentityHashSet<ShardsIterator> set = new IdentityHashSet<ShardsIterator>();
        IndexRoutingTable indexRouting = indexRoutingTable(clusterState);
        for (IndexShardRoutingTable indexShard : indexRouting) {
            set.add(indexShard.shardsRandomIt());
        }
        return new GroupShardsIterator(set);
    }

    public IndexMetaData indexMetaData(ClusterState clusterState) {
        IndexMetaData indexMetaData = clusterState.metaData().index(index.name());
        if (indexMetaData == null) {
            throw new IndexMissingException(index);
        }
        return indexMetaData;
    }

    protected IndexRoutingTable indexRoutingTable(ClusterState clusterState) {
        IndexRoutingTable indexRouting = clusterState.routingTable().index(index.name());
        if (indexRouting == null) {
            throw new IndexMissingException(index);
        }
        return indexRouting;
    }


    protected IndexShardRoutingTable shards(ClusterState clusterState, String type, String id) {
        int shardId = Math.abs(hash(type, id)) % indexMetaData(clusterState).numberOfShards();
        IndexShardRoutingTable indexShard = indexRoutingTable(clusterState).shard(shardId);
        if (indexShard == null) {
            throw new IndexShardMissingException(new ShardId(index, shardId));
        }
        return indexShard;
    }

    protected int hash(String type, String id) {
        return hashFunction.hash(type, id);
    }
}
