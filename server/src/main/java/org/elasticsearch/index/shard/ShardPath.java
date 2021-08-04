/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.index.shard;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.util.Strings;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.env.ShardLock;
import org.elasticsearch.index.IndexSettings;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;
import java.util.function.Consumer;

public final class ShardPath {
    public static final String INDEX_FOLDER_NAME = "index";
    public static final String TRANSLOG_FOLDER_NAME = "translog";

    private final Path path;
    private final ShardId shardId;
    private final Path shardStatePath;
    private final boolean isCustomDataPath;

    public ShardPath(boolean isCustomDataPath, Path dataPath, Path shardStatePath, ShardId shardId) {
        assert dataPath.getFileName().toString().equals(Integer.toString(shardId.id())) :
            "dataPath must end with the shard ID but didn't: " + dataPath.toString();
        assert shardStatePath.getFileName().toString().equals(Integer.toString(shardId.id())) :
            "shardStatePath must end with the shard ID but didn't: " + dataPath.toString();
        assert dataPath.getParent().getFileName().toString().equals(shardId.getIndex().getUUID()) :
            "dataPath must end with index path id but didn't: " + dataPath.toString();
        assert shardStatePath.getParent().getFileName().toString().equals(shardId.getIndex().getUUID()) :
            "shardStatePath must end with index path id but didn't: " + dataPath.toString();
        if (isCustomDataPath && dataPath.equals(shardStatePath)) {
            throw new IllegalArgumentException("shard state path must be different to the data path when using custom data paths");
        }
        this.isCustomDataPath = isCustomDataPath;
        this.path = dataPath;
        this.shardId = shardId;
        this.shardStatePath = shardStatePath;
    }

    public Path resolveTranslog() {
        return path.resolve(TRANSLOG_FOLDER_NAME);
    }

    public Path resolveIndex() {
        return path.resolve(INDEX_FOLDER_NAME);
    }

    public Path getDataPath() {
        return path;
    }

    public boolean exists() {
        return Files.exists(path);
    }

    public ShardId getShardId() {
        return shardId;
    }

    public Path getShardStatePath() {
        return shardStatePath;
    }

    /**
     * Returns the data-path root for this shard. The root is a parent of {@link #getDataPath()} without the index name
     * and the shard ID.
     */
    public Path getRootDataPath() {
        Path noIndexShardId = getDataPath().getParent().getParent();
        return isCustomDataPath ? noIndexShardId : noIndexShardId.getParent(); // also strip the indices folder
    }

    /**
     * Returns the state-path root for this shard. The root is a parent of {@link #getRootStatePath()} ()} without the index name
     * and the shard ID.
     */
    public Path getRootStatePath() {
        return getShardStatePath().getParent().getParent().getParent(); // also strip the indices folder
    }

    /**
     * Returns <code>true</code> iff the data location is a custom data location and therefore outside of the nodes configured data paths.
     */
    public boolean isCustomDataPath() {
        return isCustomDataPath;
    }

    /**
     * This method resolves the node's shard path using the given {@link NodeEnvironment}.
     * <b>Note:</b> this method resolves custom data locations for the shard if such a custom data path is provided.
     */
    public static ShardPath loadShardPath(Logger logger, NodeEnvironment env,
                                          ShardId shardId, String customDataPath) throws IOException {
        final Path shardPath = env.availableShardPath(shardId);
        final Path sharedDataPath = env.sharedDataPath();
        return loadShardPath(logger, shardId, customDataPath, shardPath, sharedDataPath);
    }

    /**
     * This method resolves the node's shard path using the given data paths.
     * <b>Note:</b> this method resolves custom data locations for the shard.
     */
    public static ShardPath loadShardPath(Logger logger, ShardId shardId, String customDataPath, Path shardPath,
                                          Path sharedDataPath) throws IOException {
        final String indexUUID = shardId.getIndex().getUUID();
        // EMPTY is safe here because we never call namedObject
        ShardStateMetadata load = ShardStateMetadata.FORMAT.loadLatestState(logger, NamedXContentRegistry.EMPTY, shardPath);
        if (load != null) {
            if (load.indexUUID.equals(indexUUID) == false && IndexMetadata.INDEX_UUID_NA_VALUE.equals(load.indexUUID) == false) {
                logger.warn("{} found shard on path: [{}] with a different index UUID - this "
                    + "shard seems to be leftover from a different index with the same name. "
                    + "Remove the leftover shard in order to reuse the path with the current index", shardId, shardPath);
                throw new IllegalStateException(shardId + " index UUID in shard state was: " + load.indexUUID
                    + " expected: " + indexUUID + " on shard path: " + shardPath);
            }
            final Path dataPath;
            final boolean hasCustomDataPath = Strings.isNotEmpty(customDataPath);
            if (hasCustomDataPath) {
                dataPath = NodeEnvironment.resolveCustomLocation(customDataPath, shardId, sharedDataPath);
            } else {
                dataPath = shardPath;
            }
            logger.debug("{} loaded data path [{}], state path [{}]", shardId, dataPath, shardPath);
            return new ShardPath(hasCustomDataPath, dataPath, shardPath, shardId);
        }
        return null;
    }

    /**
     * This method tries to delete left-over shards where the index name has been reused but the UUID is different
     * to allow the new shard to be allocated.
     */
    public static void deleteLeftoverShardDirectory(
        final Logger logger,
        final NodeEnvironment env,
        final ShardLock lock,
        final IndexSettings indexSettings,
        final Consumer<Path> listener
    ) throws IOException {
        final String indexUUID = indexSettings.getUUID();
        final Path path = env.availableShardPath(lock.getShardId());
        // EMPTY is safe here because we never call namedObject
        ShardStateMetadata load = ShardStateMetadata.FORMAT.loadLatestState(logger, NamedXContentRegistry.EMPTY, path);
        if (load != null) {
            if (load.indexUUID.equals(indexUUID) == false && IndexMetadata.INDEX_UUID_NA_VALUE.equals(load.indexUUID) == false) {
                logger.warn("{} deleting leftover shard on path: [{}] with a different index UUID", lock.getShardId(), path);
                assert Files.isDirectory(path) : path + " is not a directory";
                NodeEnvironment.acquireFSLockForPaths(indexSettings, path);
                listener.accept(path);
                IOUtils.rm(path);
            }
        }
    }

    public static ShardPath selectNewPathForShard(NodeEnvironment env, ShardId shardId, IndexSettings indexSettings) {

        final Path dataPath;
        final Path statePath;

        if (indexSettings.hasCustomDataPath()) {
            dataPath = env.resolveCustomLocation(indexSettings.customDataPath(), shardId);
            statePath = env.nodePath().resolve(shardId);
        } else {
            dataPath = statePath = env.nodePath().resolve(shardId);
        }
        return new ShardPath(indexSettings.hasCustomDataPath(), dataPath, statePath, shardId);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final ShardPath shardPath = (ShardPath) o;
        if (Objects.equals(shardId, shardPath.shardId) == false) {
            return false;
        }
        if (Objects.equals(path, shardPath.path) == false) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = path != null ? path.hashCode() : 0;
        result = 31 * result + (shardId != null ? shardId.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "ShardPath{" +
                "path=" + path +
                ", shard=" + shardId +
                '}';
    }
}
