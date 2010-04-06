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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.UnmodifiableIterator;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.util.concurrent.ThreadLocalRandom;
import org.elasticsearch.util.io.stream.StreamInput;
import org.elasticsearch.util.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.collect.Lists.*;

/**
 * @author kimchy (Shay Banon)
 */
public class IndexShardRoutingTable implements Iterable<ShardRouting> {

    private final ShardId shardId;

    private final ImmutableList<ShardRouting> shards;

    private final AtomicInteger counter;

    IndexShardRoutingTable(ShardId shardId, ImmutableList<ShardRouting> shards) {
        this.shardId = shardId;
        this.shards = shards;
        this.counter = new AtomicInteger(ThreadLocalRandom.current().nextInt(shards.size()));
    }

    public ShardId shardId() {
        return shardId;
    }

    @Override public UnmodifiableIterator<ShardRouting> iterator() {
        return shards.iterator();
    }

    public int size() {
        return shards.size();
    }

    public ImmutableList<ShardRouting> shards() {
        return shards;
    }

    public ShardsIterator shardsIt() {
        return new IndexShardsIterator(0);
    }

    public ShardsIterator shardsRandomIt() {
        return new IndexShardsIterator(nextCounter());
    }

    public ShardRouting primaryShard() {
        for (ShardRouting shardRouting : this) {
            if (shardRouting.primary()) {
                return shardRouting;
            }
        }
        return null;
    }

    public List<ShardRouting> backupsShards() {
        List<ShardRouting> backupShards = newArrayListWithExpectedSize(2);
        for (ShardRouting shardRouting : this) {
            if (!shardRouting.primary()) {
                backupShards.add(shardRouting);
            }
        }
        return backupShards;
    }

    int nextCounter() {
        return counter.getAndIncrement();
    }

    ShardRouting shardModulo(int shardId) {
        return shards.get((Math.abs(shardId) % size()));
    }

    /**
     * <p>The class can be used from different threads, though not designed to be used concurrently
     * from different threads.
     */
    class IndexShardsIterator implements ShardsIterator, Iterator<ShardRouting> {

        private final int origIndex;

        private volatile int index;

        private volatile int counter = 0;

        private IndexShardsIterator(int index) {
            this.origIndex = index;
            this.index = index;
        }

        @Override public Iterator<ShardRouting> iterator() {
            return this;
        }

        @Override public ShardsIterator reset() {
            counter = 0;
            index = origIndex;
            return this;
        }

        @Override public boolean hasNext() {
            return counter < size();
        }

        @Override public ShardRouting next() throws NoSuchElementException {
            if (!hasNext()) {
                throw new NoSuchElementException("No shard found");
            }
            counter++;
            return shardModulo(index++);
        }

        @Override public boolean hasNextActive() {
            int counter = this.counter;
            int index = this.index;
            while (counter++ < size()) {
                ShardRouting shardRouting = shardModulo(index++);
                if (shardRouting.active()) {
                    return true;
                }
            }
            return false;
        }

        @Override public ShardRouting nextActive() throws NoSuchElementException {
            ShardRouting shardRouting = nextActiveOrNull();
            if (shardRouting == null) {
                throw new NoSuchElementException("No active shard found");
            }
            return shardRouting;
        }

        @Override public ShardRouting nextActiveOrNull() throws NoSuchElementException {
            while (counter++ < size()) {
                ShardRouting shardRouting = shardModulo(index++);
                if (shardRouting.active()) {
                    return shardRouting;
                }
            }
            return null;
        }

        @Override public void remove() {
            throw new UnsupportedOperationException();
        }

        @Override public int size() {
            return IndexShardRoutingTable.this.size();
        }

        @Override public int sizeActive() {
            int shardsActive = 0;
            for (ShardRouting shardRouting : IndexShardRoutingTable.this.shards()) {
                if (shardRouting.active()) {
                    shardsActive++;
                }
            }
            return shardsActive;
        }

        @Override public ShardId shardId() {
            return IndexShardRoutingTable.this.shardId();
        }
    }

    public static class Builder {

        private ShardId shardId;

        private final List<ShardRouting> shards;

        public Builder(IndexShardRoutingTable indexShard) {
            this.shardId = indexShard.shardId;
            this.shards = newArrayList(indexShard.shards);
        }

        public Builder(ShardId shardId) {
            this.shardId = shardId;
            this.shards = newArrayList();
        }

        public Builder addShard(ImmutableShardRouting shardEntry) {
            for (ShardRouting shard : shards) {
                // don't add two that map to the same node id
                // we rely on the fact that a node does not have primary and backup of the same shard
                if (shard.assignedToNode() && shardEntry.assignedToNode()
                        && shard.currentNodeId().equals(shardEntry.currentNodeId())) {
                    return this;
                }
            }
            shards.add(shardEntry);
            return this;
        }

        public IndexShardRoutingTable build() {
            return new IndexShardRoutingTable(shardId, ImmutableList.copyOf(shards));
        }

        public static IndexShardRoutingTable readFrom(StreamInput in) throws IOException {
            String index = in.readUTF();
            return readFromThin(in, index);
        }

        public static IndexShardRoutingTable readFromThin(StreamInput in, String index) throws IOException {
            int iShardId = in.readVInt();
            ShardId shardId = new ShardId(index, iShardId);
            Builder builder = new Builder(shardId);

            int size = in.readVInt();
            for (int i = 0; i < size; i++) {
                ImmutableShardRouting shard = ImmutableShardRouting.readShardRoutingEntry(in, index, iShardId);
                builder.addShard(shard);
            }

            return builder.build();
        }

        public static void writeTo(IndexShardRoutingTable indexShard, StreamOutput out) throws IOException {
            out.writeUTF(indexShard.shardId().index().name());
            writeToThin(indexShard, out);
        }

        public static void writeToThin(IndexShardRoutingTable indexShard, StreamOutput out) throws IOException {
            out.writeVInt(indexShard.shardId.id());
            out.writeVInt(indexShard.shards.size());
            for (ShardRouting entry : indexShard) {
                entry.writeToThin(out);
            }
        }

    }
}
