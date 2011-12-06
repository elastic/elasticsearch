/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
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

package org.elasticsearch.action.admin.indices.status;

import com.google.common.collect.Iterators;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.flush.FlushStats;
import org.elasticsearch.index.merge.MergeStats;
import org.elasticsearch.index.refresh.RefreshStats;
import org.elasticsearch.index.shard.ShardId;

import java.util.Iterator;

/**
 *
 */
public class IndexShardStatus implements Iterable<ShardStatus> {

    private final ShardId shardId;

    private final ShardStatus[] shards;

    IndexShardStatus(ShardId shardId, ShardStatus[] shards) {
        this.shardId = shardId;
        this.shards = shards;
    }

    public ShardId shardId() {
        return this.shardId;
    }

    public ShardId getShardId() {
        return shardId();
    }

    public ShardStatus[] shards() {
        return this.shards;
    }

    public ShardStatus[] getShards() {
        return shards();
    }

    public ShardStatus getAt(int position) {
        return shards[position];
    }

    /**
     * Returns only the primary shards store size in bytes.
     */
    public ByteSizeValue primaryStoreSize() {
        long bytes = -1;
        for (ShardStatus shard : shards()) {
            if (!shard.shardRouting().primary()) {
                // only sum docs for the primaries
                continue;
            }
            if (shard.storeSize() != null) {
                if (bytes == -1) {
                    bytes = 0;
                }
                bytes += shard.storeSize().bytes();
            }
        }
        if (bytes == -1) {
            return null;
        }
        return new ByteSizeValue(bytes);
    }

    /**
     * Returns only the primary shards store size in bytes.
     */
    public ByteSizeValue getPrimaryStoreSize() {
        return primaryStoreSize();
    }

    /**
     * Returns the full store size in bytes, of both primaries and replicas.
     */
    public ByteSizeValue storeSize() {
        long bytes = -1;
        for (ShardStatus shard : shards()) {
            if (shard.storeSize() != null) {
                if (bytes == -1) {
                    bytes = 0;
                }
                bytes += shard.storeSize().bytes();
            }
        }
        if (bytes == -1) {
            return null;
        }
        return new ByteSizeValue(bytes);
    }

    /**
     * Returns the full store size in bytes, of both primaries and replicas.
     */
    public ByteSizeValue getStoreSize() {
        return storeSize();
    }

    public long translogOperations() {
        long translogOperations = -1;
        for (ShardStatus shard : shards()) {
            if (shard.translogOperations() != -1) {
                if (translogOperations == -1) {
                    translogOperations = 0;
                }
                translogOperations += shard.translogOperations();
            }
        }
        return translogOperations;
    }

    public long getTranslogOperations() {
        return translogOperations();
    }

    private transient DocsStatus docs;

    public DocsStatus docs() {
        if (docs != null) {
            return docs;
        }
        DocsStatus docs = null;
        for (ShardStatus shard : shards()) {
            if (!shard.shardRouting().primary()) {
                // only sum docs for the primaries
                continue;
            }
            if (shard.docs() == null) {
                continue;
            }
            if (docs == null) {
                docs = new DocsStatus();
            }
            docs.numDocs += shard.docs().numDocs();
            docs.maxDoc += shard.docs().maxDoc();
            docs.deletedDocs += shard.docs().deletedDocs();
        }
        this.docs = docs;
        return this.docs;
    }

    public DocsStatus getDocs() {
        return docs();
    }

    /**
     * Total merges of this shard replication group.
     */
    public MergeStats mergeStats() {
        MergeStats mergeStats = new MergeStats();
        for (ShardStatus shard : shards) {
            mergeStats.add(shard.mergeStats());
        }
        return mergeStats;
    }

    /**
     * Total merges of this shard replication group.
     */
    public MergeStats getMergeStats() {
        return this.mergeStats();
    }

    public RefreshStats refreshStats() {
        RefreshStats refreshStats = new RefreshStats();
        for (ShardStatus shard : shards) {
            refreshStats.add(shard.refreshStats());
        }
        return refreshStats;
    }

    public RefreshStats getRefreshStats() {
        return refreshStats();
    }

    public FlushStats flushStats() {
        FlushStats flushStats = new FlushStats();
        for (ShardStatus shard : shards) {
            flushStats.add(shard.flushStats);
        }
        return flushStats;
    }

    public FlushStats getFlushStats() {
        return flushStats();
    }

    @Override
    public Iterator<ShardStatus> iterator() {
        return Iterators.forArray(shards);
    }

}