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

package org.elasticsearch.index.store;

import org.apache.lucene.store.StoreRateLimiting;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.CloseableIndexComponent;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.store.IndicesStore;

import java.io.IOException;

/**
 * Index store is an index level information of the {@link Store} each shard will use.
 */
public interface IndexStore extends CloseableIndexComponent {

    /**
     * Is the store a persistent store that can survive full restarts.
     */
    boolean persistent();

    IndicesStore indicesStore();

    /**
     * Returns the rate limiting, either of the index is explicitly configured, or
     * the node level one (defaults to the node level one).
     */
    StoreRateLimiting rateLimiting();

    /**
     * The shard store class that should be used for each shard.
     */
    Class<? extends DirectoryService> shardDirectory();

    /**
     * Returns the backing store total space. Return <tt>-1</tt> if not available.
     */
    ByteSizeValue backingStoreTotalSpace();

    /**
     * Returns the backing store free space. Return <tt>-1</tt> if not available.
     */
    ByteSizeValue backingStoreFreeSpace();

    /**
     * Returns <tt>true</tt> if this shard is allocated on this node. Allocated means
     * that it has storage files that can be deleted using {@link #deleteUnallocated(org.elasticsearch.index.shard.ShardId)}.
     */
    boolean canDeleteUnallocated(ShardId shardId);

    /**
     * Deletes this shard store since its no longer allocated.
     */
    void deleteUnallocated(ShardId shardId) throws IOException;
}
