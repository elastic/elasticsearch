/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugins;

import org.apache.lucene.store.Directory;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.EngineException;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardPath;
import org.elasticsearch.indices.recovery.RecoveryState;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
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

        /**
         * Creates a new directory per shard. This method is called once per shard on shard creation.
         * @param indexSettings the shards index settings
         * @param shardPath the path the shard is using
         * @param shardRouting the {@link ShardRouting}
         * @return a new lucene directory instance
         * @throws IOException if an IOException occurs while opening the directory
         */
        default Directory newDirectory(IndexSettings indexSettings, ShardPath shardPath, ShardRouting shardRouting) throws IOException {
            return newDirectory(indexSettings, shardPath);
        }
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

    /**
     * {@link IndexFoldersDeletionListener} are invoked before the folders of a shard or an index are deleted from disk.
     */
    interface IndexFoldersDeletionListener {
        /**
         * Invoked before the folders of an index are deleted from disk. The list of folders contains {@link Path}s that may or may not
         * exist on disk. Shard locks are expected to be acquired at the time this method is invoked.
         *
         * @param index         the {@link Index} of the index whose folders are going to be deleted
         * @param indexSettings settings for the index whose folders are going to be deleted
         * @param indexPaths    the paths of the folders that are going to be deleted
         */
        void beforeIndexFoldersDeleted(Index index, IndexSettings indexSettings, Path[] indexPaths);

        /**
         * Invoked before the folders of a shard are deleted from disk. The list of folders contains {@link Path}s that may or may not
         * exist on disk. Shard locks are expected to be acquired at the time this method is invoked.
         *
         * @param shardId       the {@link ShardId} of the shard whose folders are going to be deleted
         * @param indexSettings index settings of the shard whose folders are going to be deleted
         * @param shardPaths    the paths of the folders that are going to be deleted
         */
        void beforeShardFoldersDeleted(ShardId shardId, IndexSettings indexSettings, Path[] shardPaths);
    }

    /**
     * The {@link IndexFoldersDeletionListener} listeners for this plugin. When the folders of an index or a shard are deleted from disk,
     * these listeners are invoked before the deletion happens in order to allow plugin to clean up any resources if needed.
     *
     * @return a list of {@link IndexFoldersDeletionListener} listeners
     */
    default List<IndexFoldersDeletionListener> getIndexFoldersDeletionListeners() {
        return Collections.emptyList();
    }

    /**
     * An interface that allows plugins to override the {@link org.apache.lucene.index.IndexCommit} of which a snapshot is taken. By default
     * we snapshot the latest such commit.
     */
    @FunctionalInterface
    interface SnapshotCommitSupplier {
        Engine.IndexCommitRef acquireIndexCommitForSnapshot(Engine engine) throws EngineException;
    }

    /**
     * The {@link SnapshotCommitSupplier} mappings for this plugin. When an index is created the store type setting
     * {@link org.elasticsearch.index.IndexModule#INDEX_STORE_TYPE_SETTING} on the index will determine whether the snapshot commit supplier
     * should be overridden and, if so, which override to use.
     *
     * @return a collection of snapshot commit suppliers, keyed by the value of
     *         {@link org.elasticsearch.index.IndexModule#INDEX_STORE_TYPE_SETTING}.
     */
    // TODO: remove unused API extension point
    default Map<String, SnapshotCommitSupplier> getSnapshotCommitSuppliers() {
        return Collections.emptyMap();
    }

}
