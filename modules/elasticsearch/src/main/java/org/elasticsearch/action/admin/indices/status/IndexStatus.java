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

package org.elasticsearch.action.admin.indices.status;

import org.elasticsearch.common.collect.Maps;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.SizeValue;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.common.collect.Lists.*;

/**
 * @author kimchy (shay.banon)
 */
public class IndexStatus implements Iterable<IndexShardStatus> {

    public static class Docs {
        public static final Docs UNKNOWN = new Docs();

        long numDocs = -1;
        long maxDoc = -1;
        long deletedDocs = -1;

        public long numDocs() {
            return numDocs;
        }

        public long getNumDocs() {
            return numDocs();
        }

        public long maxDoc() {
            return maxDoc;
        }

        public long getMaxDoc() {
            return maxDoc();
        }

        public long deletedDocs() {
            return deletedDocs;
        }

        public long getDeletedDocs() {
            return deletedDocs();
        }
    }

    private final String index;

    private final Map<Integer, IndexShardStatus> indexShards;

    private final Settings settings;

    IndexStatus(String index, Settings settings, ShardStatus[] shards) {
        this.index = index;
        this.settings = settings;

        Map<Integer, List<ShardStatus>> tmpIndexShards = Maps.newHashMap();
        for (ShardStatus shard : shards) {
            List<ShardStatus> lst = tmpIndexShards.get(shard.shardRouting().id());
            if (lst == null) {
                lst = newArrayList();
                tmpIndexShards.put(shard.shardRouting().id(), lst);
            }
            lst.add(shard);
        }
        indexShards = Maps.newHashMap();
        for (Map.Entry<Integer, List<ShardStatus>> entry : tmpIndexShards.entrySet()) {
            indexShards.put(entry.getKey(), new IndexShardStatus(entry.getValue().get(0).shardRouting().shardId(), entry.getValue().toArray(new ShardStatus[entry.getValue().size()])));
        }
    }

    public String index() {
        return this.index;
    }

    public String getIndex() {
        return index();
    }

    /**
     * A shard id to index shard status map (note, index shard status is the replication shard group that maps
     * to the shard id).
     */
    public Map<Integer, IndexShardStatus> shards() {
        return this.indexShards;
    }

    public Map<Integer, IndexShardStatus> getShards() {
        return shards();
    }

    public Settings settings() {
        return this.settings;
    }

    public Settings getSettings() {
        return settings();
    }

    public SizeValue storeSize() {
        long bytes = -1;
        for (IndexShardStatus shard : this) {
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
        return new SizeValue(bytes);
    }

    public SizeValue getStoreSize() {
        return storeSize();
    }

    public SizeValue estimatedFlushableMemorySize() {
        long bytes = -1;
        for (IndexShardStatus shard : this) {
            if (shard.estimatedFlushableMemorySize() != null) {
                if (bytes == -1) {
                    bytes = 0;
                }
                bytes += shard.estimatedFlushableMemorySize().bytes();
            }
        }
        if (bytes == -1) {
            return null;
        }
        return new SizeValue(bytes);
    }

    public SizeValue getEstimatedFlushableMemorySize() {
        return estimatedFlushableMemorySize();
    }

    public long translogOperations() {
        long translogOperations = -1;
        for (IndexShardStatus shard : this) {
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

    public Docs docs() {
        Docs docs = new Docs();
        for (IndexShardStatus shard : this) {
            if (shard.docs().numDocs() != -1) {
                if (docs.numDocs == -1) {
                    docs.numDocs = 0;
                }
                docs.numDocs += shard.docs().numDocs();
            }
            if (shard.docs().maxDoc() != -1) {
                if (docs.maxDoc == -1) {
                    docs.maxDoc = 0;
                }
                docs.maxDoc += shard.docs().maxDoc();
            }
            if (shard.docs().deletedDocs() != -1) {
                if (docs.deletedDocs == -1) {
                    docs.deletedDocs = 0;
                }
                docs.deletedDocs += shard.docs().deletedDocs();
            }
        }
        return docs;
    }

    public Docs getDocs() {
        return docs();
    }

    @Override public Iterator<IndexShardStatus> iterator() {
        return indexShards.values().iterator();
    }

}
