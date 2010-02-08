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
import com.google.common.collect.Lists;
import com.google.common.collect.UnmodifiableIterator;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.index.Index;
import org.elasticsearch.indices.IndexMissingException;
import org.elasticsearch.util.concurrent.Immutable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import static com.google.common.collect.Maps.*;

/**
 * @author kimchy (Shay Banon)
 */
@Immutable
public class RoutingTable implements Iterable<IndexRoutingTable> {

    public static final RoutingTable EMPTY_ROUTING_TABLE = newRoutingTableBuilder().build();

    // index to IndexRoutingTable map
    private final ImmutableMap<String, IndexRoutingTable> indicesRouting;

    RoutingTable(Map<String, IndexRoutingTable> indicesRouting) {
        this.indicesRouting = ImmutableMap.copyOf(indicesRouting);
    }

    @Override public UnmodifiableIterator<IndexRoutingTable> iterator() {
        return indicesRouting.values().iterator();
    }

    public boolean hasIndex(String index) {
        return indicesRouting.containsKey(index);
    }

    public IndexRoutingTable index(String index) {
        return indicesRouting.get(index);
    }

    public Map<String, IndexRoutingTable> indicesRouting() {
        return indicesRouting;
    }

    public RoutingNodes routingNodes(MetaData metaData) {
        return new RoutingNodes(metaData, this);
    }

    public List<ShardRouting> allShards(String... indices) throws IndexMissingException {
        List<ShardRouting> shards = Lists.newArrayList();
        if (indices == null || indices.length == 0) {
            indices = indicesRouting.keySet().toArray(new String[indicesRouting.keySet().size()]);
        }
        for (String index : indices) {
            IndexRoutingTable indexRoutingTable = index(index);
            if (indexRoutingTable == null) {
                throw new IndexMissingException(new Index(index));
            }
            for (IndexShardRoutingTable indexShardRoutingTable : indexRoutingTable) {
                for (ShardRouting shardRouting : indexShardRoutingTable) {
                    shards.add(shardRouting);
                }
            }
        }
        return shards;
    }

    public static Builder newRoutingTableBuilder() {
        return new Builder();
    }

    public static class Builder {

        private final Map<String, IndexRoutingTable> indicesRouting = newHashMap();

        public Builder add(IndexRoutingTable indexRoutingTable) {
            indexRoutingTable.validate();
            indicesRouting.put(indexRoutingTable.index(), indexRoutingTable);
            return this;
        }

        public Builder add(IndexRoutingTable.Builder indexRoutingTableBuilder) {
            add(indexRoutingTableBuilder.build());
            return this;
        }

        public Builder updateNodes(RoutingNodes routingNodes) {
            Map<String, IndexRoutingTable.Builder> indexRoutingTableBuilders = newHashMap();
            for (RoutingNode routingNode : routingNodes) {
                for (MutableShardRouting shardRoutingEntry : routingNode) {
                    // every relocating shard has a double entry, ignore the target one.
                    if (shardRoutingEntry.state() == ShardRoutingState.INITIALIZING && shardRoutingEntry.relocatingNodeId() != null)
                        continue;

                    String index = shardRoutingEntry.index();
                    IndexRoutingTable.Builder indexBuilder = indexRoutingTableBuilders.get(index);
                    if (indexBuilder == null) {
                        indexBuilder = new IndexRoutingTable.Builder(index);
                        indexRoutingTableBuilders.put(index, indexBuilder);
                    }
                    indexBuilder.addShard(new ImmutableShardRouting(shardRoutingEntry));
                }
            }
            for (MutableShardRouting shardRoutingEntry : routingNodes.unassigned()) {
                String index = shardRoutingEntry.index();
                IndexRoutingTable.Builder indexBuilder = indexRoutingTableBuilders.get(index);
                if (indexBuilder == null) {
                    indexBuilder = new IndexRoutingTable.Builder(index);
                    indexRoutingTableBuilders.put(index, indexBuilder);
                }
                indexBuilder.addShard(new ImmutableShardRouting(shardRoutingEntry));
            }
            for (IndexRoutingTable.Builder indexBuilder : indexRoutingTableBuilders.values()) {
                add(indexBuilder);
            }
            return this;
        }

        public RoutingTable build() {
            return new RoutingTable(indicesRouting);
        }

        public static RoutingTable readFrom(DataInput in) throws IOException, ClassNotFoundException {
            Builder builder = new Builder();
            int size = in.readInt();
            for (int i = 0; i < size; i++) {
                IndexRoutingTable index = IndexRoutingTable.Builder.readFrom(in);
                builder.add(index);
            }

            return builder.build();
        }

        public static void writeTo(RoutingTable table, DataOutput out) throws IOException {
            out.writeInt(table.indicesRouting.size());
            for (IndexRoutingTable index : table.indicesRouting.values()) {
                IndexRoutingTable.Builder.writeTo(index, out);
            }
        }
    }

    public String prettyPrint() {
        StringBuilder sb = new StringBuilder("Routing Table:\n");
        for (Map.Entry<String, IndexRoutingTable> entry : indicesRouting.entrySet()) {
            sb.append(entry.getValue().prettyPrint()).append('\n');
        }
        return sb.toString();
    }


}
