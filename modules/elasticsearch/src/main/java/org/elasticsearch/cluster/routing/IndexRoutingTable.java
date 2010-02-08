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

package org.elasticsearch.cluster.routing;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.UnmodifiableIterator;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.util.IdentityHashSet;
import org.elasticsearch.util.concurrent.Immutable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * @author kimchy (Shay Banon)
 */
@Immutable
public class IndexRoutingTable implements Iterable<IndexShardRoutingTable> {

    private final String index;

    // note, we assume that when the index routing is created, ShardRoutings are created for all possible number of
    // shards with state set to UNASSIGNED
    private final ImmutableMap<Integer, IndexShardRoutingTable> shards;

    IndexRoutingTable(String index, Map<Integer, IndexShardRoutingTable> shards) {
        this.index = index;
        this.shards = ImmutableMap.copyOf(shards);
    }

    public String index() {
        return this.index;
    }

    @Override public UnmodifiableIterator<IndexShardRoutingTable> iterator() {
        return shards.values().iterator();
    }

    public ImmutableMap<Integer, IndexShardRoutingTable> shards() {
        return shards;
    }

    public IndexShardRoutingTable shard(int shardId) {
        return shards.get(shardId);
    }

    public GroupShardsIterator groupByShardsIt() {
        IdentityHashSet<ShardsIterator> set = new IdentityHashSet<ShardsIterator>();
        for (IndexShardRoutingTable indexShard : this) {
            set.add(indexShard.shardsIt());
        }
        return new GroupShardsIterator(set);
    }

    public void validate() throws RoutingValidationException {
    }

    public static class Builder {

        private final String index;

        private final Map<Integer, IndexShardRoutingTable> shards = new HashMap<Integer, IndexShardRoutingTable>();

        public Builder(String index) {
            this.index = index;
        }

        public static IndexRoutingTable readFrom(DataInput in) throws IOException, ClassNotFoundException {
            String index = in.readUTF();
            Builder builder = new Builder(index);

            int size = in.readInt();
            for (int i = 0; i < size; i++) {
                builder.addIndexShard(IndexShardRoutingTable.Builder.readFromThin(in, index));
            }

            return builder.build();
        }

        public static void writeTo(IndexRoutingTable index, DataOutput out) throws IOException {
            out.writeUTF(index.index());
            out.writeInt(index.shards.size());
            for (IndexShardRoutingTable indexShard : index) {
                IndexShardRoutingTable.Builder.writeToThin(indexShard, out);
            }
        }

        /**
         * Initializes a new empry index
         */
        public Builder initializeEmpty(IndexMetaData indexMetaData) {
            for (int shardId = 0; shardId < indexMetaData.numberOfShards(); shardId++) {
                for (int i = 0; i <= indexMetaData.numberOfReplicas(); i++) {
                    addShard(shardId, null, i == 0, ShardRoutingState.UNASSIGNED);
                }
            }
            return this;
        }

        public Builder addIndexShard(IndexShardRoutingTable indexShard) {
            shards.put(indexShard.shardId().id(), indexShard);
            return this;
        }

        public Builder addShard(ShardRouting shard) {
            return internalAddShard(new ImmutableShardRouting(shard));
        }

        public Builder addShard(int shardId, String nodeId, boolean primary, ShardRoutingState state) {
            ImmutableShardRouting shard = new ImmutableShardRouting(index, shardId, nodeId, primary, state);
            return internalAddShard(shard);
        }

        private Builder internalAddShard(ImmutableShardRouting shard) {
            IndexShardRoutingTable indexShard = shards.get(shard.id());
            if (indexShard == null) {
                indexShard = new IndexShardRoutingTable.Builder(shard.shardId()).addShard(shard).build();
            } else {
                indexShard = new IndexShardRoutingTable.Builder(indexShard).addShard(shard).build();
            }
            shards.put(indexShard.shardId().id(), indexShard);
            return this;
        }

        public IndexRoutingTable build() throws RoutingValidationException {
            IndexRoutingTable indexRoutingTable = new IndexRoutingTable(index, ImmutableMap.copyOf(shards));
            indexRoutingTable.validate();
            return indexRoutingTable;
        }
    }


    public String prettyPrint() {
        StringBuilder sb = new StringBuilder("-- Index[" + index + "]\n");
        for (IndexShardRoutingTable indexShard : this) {
            sb.append("----ShardId[").append(indexShard.shardId()).append("]\n");
            for (ShardRouting shard : indexShard) {
                sb.append("--------").append(shard.shortSummary()).append("\n");
            }
        }
        return sb.toString();
    }


}
