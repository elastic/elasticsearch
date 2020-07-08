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

package org.elasticsearch.plugins;

import org.apache.lucene.store.Directory;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.shard.ShardPath;
import org.elasticsearch.indices.recovery.RecoveryState;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

/**
 * A plugin that provides alternative directory implementations.
 */
public interface IndexStorePlugin {

    /**
     * An interface that describes how to create a new directory instance per shard.
     */
    @FunctionalInterface
    interface DirectoryFactory {
        /**
         * Creates a new directory per shard. This method is called once per shard on shard creation.
         * @param indexSettings the shards index settings
         * @param shardPath the path the shard is using
         * @return a new lucene directory instance
         * @throws IOException if an IOException occurs while opening the directory
         */
        Directory newDirectory(IndexSettings indexSettings, ShardPath shardPath) throws IOException;
    }

    /**
     * The {@link DirectoryFactory} mappings for this plugin. When an index is created the store type setting
     * {@link org.elasticsearch.index.IndexModule#INDEX_STORE_TYPE_SETTING} on the index will be examined and either use the default or a
     * built-in type, or looked up among all the directory factories from {@link IndexStorePlugin} plugins.
     *
     * @return a map from store type to an directory factory
     */
    Map<String, DirectoryFactory> getDirectoryFactories();

    /**
     * An interface that allows to create a new {@link RecoveryState} per shard.
     */
    @FunctionalInterface
    interface RecoveryStateFactory {
        /**
         * Creates a new {@link RecoveryState} per shard. This method is called once per shard on shard creation.
         * @return a new RecoveryState instance
         */
        RecoveryState newRecoveryState(ShardRouting shardRouting, DiscoveryNode targetNode, @Nullable DiscoveryNode sourceNode);
    }

    /**
     * The {@link RecoveryStateFactory} mappings for this plugin. When an index is created the recovery type setting
     * {@link org.elasticsearch.index.IndexModule#INDEX_RECOVERY_TYPE_SETTING} on the index will be examined and either use the default
     * or looked up among all the recovery state factories from {@link IndexStorePlugin} plugins.
     *
     * @return a map from recovery type to an recovery state factory
     */
    default Map<String, RecoveryStateFactory> getRecoveryStateFactories() {
        return Collections.emptyMap();
    }
}
