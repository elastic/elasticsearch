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

package org.elasticsearch.action.admin.indices.status;

import com.google.common.collect.Maps;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.flush.FlushStats;
import org.elasticsearch.index.merge.MergeStats;
import org.elasticsearch.index.refresh.RefreshStats;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static com.google.common.collect.Lists.newArrayList;

/**
 * This class will be removed in future versions
 * Use the recovery API instead
 */
@Deprecated
public class IndexStatus implements Iterable<IndexShardStatus> {

    private final String index;

    private final Map<Integer, IndexShardStatus> indexShards;

    IndexStatus(String index, ShardStatus[] shards) {
        this.index = index;

        Map<Integer, List<ShardStatus>> tmpIndexShards = Maps.newHashMap();
        for (ShardStatus shard : shards) {
            List<ShardStatus> lst = tmpIndexShards.get(shard.getShardRouting().id());
            if (lst == null) {
                lst = newArrayList();
                tmpIndexShards.put(shard.getShardRouting().id(), lst);
            }
            lst.add(shard);
        }
        indexShards = Maps.newHashMap();
        for (Map.Entry<Integer, List<ShardStatus>> entry : tmpIndexShards.entrySet()) {
            indexShards.put(entry.getKey(), new IndexShardStatus(entry.getValue().get(0).getShardRouting().shardId(), entry.getValue().toArray(new ShardStatus[entry.getValue().size()])));
        }
    }

    public String getIndex() {
        return this.index;
    }

    /**
     * A shard id to index shard status map (note, index shard status is the replication shard group that maps
     * to the shard id).
     */
    public Map<Integer, IndexShardStatus> getShards() {
        return this.indexShards;
    }

    /**
     * Returns only the primary shards store size in bytes.
     */
    public ByteSizeValue getPrimaryStoreSize() {
        long bytes = -1;
        for (IndexShardStatus shard : this) {
            if (shard.getPrimaryStoreSize() != null) {
                if (bytes == -1) {
                    bytes = 0;
                }
                bytes += shard.getPrimaryStoreSize().bytes();
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
        long bytes = -1;
        for (IndexShardStatus shard : this) {
            if (shard.getStoreSize() != null) {
                if (bytes == -1) {
                    bytes = 0;
                }
                bytes += shard.getStoreSize().bytes();
            }
        }
        if (bytes == -1) {
            return null;
        }
        return new ByteSizeValue(bytes);
    }

    public long getTranslogOperations() {
        long translogOperations = -1;
        for (IndexShardStatus shard : this) {
            if (shard.getTranslogOperations() != -1) {
                if (translogOperations == -1) {
                    translogOperations = 0;
                }
                translogOperations += shard.getTranslogOperations();
            }
        }
        return translogOperations;
    }

    private transient DocsStatus docs;

    public DocsStatus getDocs() {
        if (docs != null) {
            return docs;
        }
        DocsStatus docs = null;
        for (IndexShardStatus shard : this) {
            if (shard.getDocs() == null) {
                continue;
            }
            if (docs == null) {
                docs = new DocsStatus();
            }
            docs.numDocs += shard.getDocs().getNumDocs();
            docs.maxDoc += shard.getDocs().getMaxDoc();
            docs.deletedDocs += shard.getDocs().getDeletedDocs();
        }
        this.docs = docs;
        return docs;
    }

    /**
     * Total merges of this index.
     */
    public MergeStats getMergeStats() {
        MergeStats mergeStats = new MergeStats();
        for (IndexShardStatus shard : this) {
            mergeStats.add(shard.getMergeStats());
        }
        return mergeStats;
    }

    public RefreshStats getRefreshStats() {
        RefreshStats refreshStats = new RefreshStats();
        for (IndexShardStatus shard : this) {
            refreshStats.add(shard.getRefreshStats());
        }
        return refreshStats;
    }

    public FlushStats getFlushStats() {
        FlushStats flushStats = new FlushStats();
        for (IndexShardStatus shard : this) {
            flushStats.add(shard.getFlushStats());
        }
        return flushStats;
    }

    @Override
    public Iterator<IndexShardStatus> iterator() {
        return indexShards.values().iterator();
    }

}
