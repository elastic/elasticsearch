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

import com.google.common.collect.Iterators;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.util.SizeValue;

import java.util.Iterator;

/**
 * @author kimchy (Shay Banon)
 */
public class IndexShardStatus implements Iterable<ShardStatus> {

    public static class Docs {
        public static final Docs UNKNOWN = new Docs();

        int numDocs = -1;
        int maxDoc = -1;
        int deletedDocs = -1;

        public int numDocs() {
            return numDocs;
        }

        public int maxDoc() {
            return maxDoc;
        }

        public int deletedDocs() {
            return deletedDocs;
        }
    }

    private final ShardId shardId;

    private final ShardStatus[] shards;

    IndexShardStatus(ShardId shardId, ShardStatus[] shards) {
        this.shardId = shardId;
        this.shards = shards;
    }

    public ShardId shardId() {
        return this.shardId;
    }

    public ShardStatus[] shards() {
        return this.shards;
    }

    public SizeValue storeSize() {
        long bytes = -1;
        for (ShardStatus shard : shards()) {
            if (shard.storeSize().bytes() != SizeValue.UNKNOWN.bytes()) {
                if (bytes == -1) {
                    bytes = 0;
                }
                bytes += shard.storeSize().bytes();
            }
        }
        return new SizeValue(bytes);
    }

    public SizeValue estimatedFlushableMemorySize() {
        long bytes = -1;
        for (ShardStatus shard : shards()) {
            if (shard.estimatedFlushableMemorySize().bytes() != SizeValue.UNKNOWN.bytes()) {
                if (bytes == -1) {
                    bytes = 0;
                }
                bytes += shard.estimatedFlushableMemorySize().bytes();
            }
        }
        return new SizeValue(bytes);
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

    public Docs docs() {
        Docs docs = new Docs();
        for (ShardStatus shard : shards()) {
            if (!shard.shardRouting().primary()) {
                // only sum docs for the primaries
                continue;
            }
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

    @Override public Iterator<ShardStatus> iterator() {
        return Iterators.forArray(shards);
    }

}