/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.searchablesnapshots.cache;

import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardPath;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Cleans any leftover searchable snapshot caches when a node is starting up.
 */
public class NodeEnvironmentCacheCleaner implements Runnable {

    private final NodeEnvironment nodeEnvironment;

    public NodeEnvironmentCacheCleaner(NodeEnvironment nodeEnvironment) {
        this.nodeEnvironment = nodeEnvironment;
    }

    @Override
    public void run() {
        try {
            for (NodeEnvironment.NodePath nodePath : nodeEnvironment.nodePaths()) {
                for (String indexUUID : nodeEnvironment.availableIndexFoldersForPath(nodePath)) {
                    for (ShardId shardId : nodeEnvironment.findAllShardIds(new Index("_unknown_", indexUUID))) {
                        final Path shardDataPath = nodePath.resolve(shardId);
                        final ShardPath shardPath = new ShardPath(false, shardDataPath, shardDataPath, shardId);
                        final Path shardCachePath = CacheService.getShardCachePath(shardPath);
                        if (Files.isDirectory(shardCachePath)) {
                            IOUtils.rm(shardCachePath);
                        }
                    }
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
