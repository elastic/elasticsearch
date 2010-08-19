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

import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.shard.ShardId;

import java.util.Iterator;

/**
 * @author kimchy (shay.banon)
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

    @Override public Iterator<ShardStatus> iterator() {
        return Iterators.forArray(shards);
    }

}