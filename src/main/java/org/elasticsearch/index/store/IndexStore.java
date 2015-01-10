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

package org.elasticsearch.index.store;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.index.shard.ShardId;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;

/**
 * Index store is an index level information of the {@link Store} each shard will use.
 */
public interface IndexStore extends Closeable {

    /**
     * The shard store class that should be used for each shard.
     */
    Class<? extends DirectoryService> shardDirectory();

    /**
     * Returns <tt>true</tt> if this shard is allocated on this node. Allocated means
     * that it has storage files that can be deleted using {@code deleteUnallocated(ShardId, Settings)}.
     */
    boolean canDeleteUnallocated(ShardId shardId, @IndexSettings Settings indexSettings);

    /**
     * Deletes this shard store since its no longer allocated.
     */
    void deleteUnallocated(ShardId shardId, @IndexSettings Settings indexSettings) throws IOException;

    /**
     * Return an array of all index folder locations for a given shard
     */
    Path[] shardIndexLocations(ShardId shardId);

    /**
     * Return an array of all translog folder locations for a given shard
     */
    Path[] shardTranslogLocations(ShardId shardId);
}
