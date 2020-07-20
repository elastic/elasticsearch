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
package org.elasticsearch.index.shard;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.util.Strings;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.env.ShardLock;
import org.elasticsearch.index.IndexSettings;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public final class ShardPath {
    public static final String INDEX_FOLDER_NAME = "index";
    public static final String TRANSLOG_FOLDER_NAME = "translog";

    private final Path path;
    private final ShardId shardId;
    private final Path shardStatePath;
    private final boolean isCustomDataPath;
    private final NodeEnvironment.NodePath nodePath;

    public ShardPath(boolean isCustomDataPath, Path dataPath, Path shardStatePath, NodeEnvironment.NodePath nodePath, ShardId shardId) {
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
        this.nodePath = nodePath;
    }

    public ShardPath(boolean isCustomDataPath, Path dataPath, Path shardStatePath, ShardId shardId) {
        this(isCustomDataPath, dataPath, shardStatePath, null, shardId);
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
     * Returns the node path of this shard, it could be null if the shard has custom data path.
     */
    public NodeEnvironment.NodePath getNodePath() {
        return nodePath;
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
     * This method walks through the nodes shard paths to find the data and state path for the given shard. If multiple
     * directories with a valid shard state exist the one with the highest version will be used.
     * <b>Note:</b> this method resolves custom data locations for the shard if such a custom data path is provided.
     */
    public static ShardPath loadShardPath(Logger logger, NodeEnvironment env,
                                          ShardId shardId, String customDataPath) throws IOException {
        final Path sharedDataPath = env.sharedDataPath();
        return loadShardPath(logger, shardId, customDataPath, env.nodePaths(), sharedDataPath);
    }

    /**
     * This method walks through the nodes shard paths to find the data and state path for the given shard. If multiple
     * directories with a valid shard state exist the one with the highest version will be used.
     * <b>Note:</b> this method resolves custom data locations for the shard.
     */
    public static ShardPath loadShardPath(Logger logger, ShardId shardId, String customDataPath,
                                          final NodeEnvironment.NodePath[] nodePaths,
                                          Path sharedDataPath) throws IOException {
        final String indexUUID = shardId.getIndex().getUUID();
        Path loadedPath = null;
        NodeEnvironment.NodePath loadedNodePath = null;
        for (NodeEnvironment.NodePath nodePath : nodePaths) {
            // EMPTY is safe here because we never call namedObject
            Path path = nodePath.resolve(shardId);
            ShardStateMetadata load = ShardStateMetadata.FORMAT.loadLatestState(logger, NamedXContentRegistry.EMPTY, path);
            if (load != null) {
                if (load.indexUUID.equals(indexUUID) == false && IndexMetadata.INDEX_UUID_NA_VALUE.equals(load.indexUUID) == false) {
                    logger.warn("{} found shard on path: [{}] with a different index UUID - this "
                        + "shard seems to be leftover from a different index with the same name. "
                        + "Remove the leftover shard in order to reuse the path with the current index", shardId, path);
                    throw new IllegalStateException(shardId + " index UUID in shard state was: " + load.indexUUID
                        + " expected: " + indexUUID + " on shard path: " + path);
                }
                if (loadedPath == null) {
                    loadedPath = path;
                    loadedNodePath = nodePath;
                } else{
                    throw new IllegalStateException(shardId + " more than one shard state found");
                }
            }

        }
        if (loadedPath == null) {
            return null;
        } else {
            final Path dataPath;
            final Path statePath = loadedPath;
            final boolean hasCustomDataPath = Strings.isNotEmpty(customDataPath);
            if (hasCustomDataPath) {
                dataPath = NodeEnvironment.resolveCustomLocation(customDataPath, shardId, sharedDataPath);
                // ignore custom data path
                loadedNodePath = null;
            } else {
                dataPath = statePath;
            }
            logger.debug("{} loaded data path [{}], state path [{}]", shardId, dataPath, statePath);
            return new ShardPath(hasCustomDataPath, dataPath, statePath, loadedNodePath, shardId);
        }
    }

    /**
     * This method tries to delete left-over shards where the index name has been reused but the UUID is different
     * to allow the new shard to be allocated.
     */
    public static void deleteLeftoverShardDirectory(Logger logger, NodeEnvironment env,
                                                            ShardLock lock, IndexSettings indexSettings) throws IOException {
        final String indexUUID = indexSettings.getUUID();
        final Path[] paths = env.availableShardPaths(lock.getShardId());
        for (Path path : paths) {
            // EMPTY is safe here because we never call namedObject
            ShardStateMetadata load = ShardStateMetadata.FORMAT.loadLatestState(logger, NamedXContentRegistry.EMPTY, path);
            if (load != null) {
                if (load.indexUUID.equals(indexUUID) == false && IndexMetadata.INDEX_UUID_NA_VALUE.equals(load.indexUUID) == false) {
                    logger.warn("{} deleting leftover shard on path: [{}] with a different index UUID", lock.getShardId(), path);
                    assert Files.isDirectory(path) : path + " is not a directory";
                    NodeEnvironment.acquireFSLockForPaths(indexSettings, paths);
                    IOUtils.rm(path);
                }
            }
        }
    }

    public static ShardPath selectNewPathForShard(NodeEnvironment env, ShardId shardId, IndexSettings indexSettings,
                                                  long avgShardSizeInBytes) throws IOException {

        final Path dataPath;
        final Path statePath;
        final NodeEnvironment.NodePath selectedNodePath;
        final String indexName = shardId.getIndexName();

        if (indexSettings.hasCustomDataPath()) {
            dataPath = env.resolveCustomLocation(indexSettings.customDataPath(), shardId);
            statePath = env.nodePaths()[0].resolve(shardId);
            // ignore custom data path
            selectedNodePath = null;
        } else {
            BigInteger totFreeSpace = BigInteger.ZERO;
            for (NodeEnvironment.NodePath nodePath : env.nodePaths()) {
                totFreeSpace = totFreeSpace.add(BigInteger.valueOf(nodePath.fileStore.getUsableSpace()));
            }

            // TODO: this is a hack!!  We should instead keep track of incoming (relocated) shards since we know
            // how large they will be once they're done copying, instead of a silly guess for such cases:

            // Very rough heuristic of how much disk space we expect the shard will use over its lifetime, the max of current average
            // shard size across the cluster and 5% of the total available free space on this node:
            BigInteger estShardSizeInBytes = BigInteger.valueOf(avgShardSizeInBytes).max(totFreeSpace.divide(BigInteger.valueOf(20)));

            // TODO - do we need something more extensible? Yet, this does the job for now...
            final NodeEnvironment.NodePath[] paths = env.nodePaths();

            // If no better path is chosen, use the one with the most space by default
            NodeEnvironment.NodePath bestPath = getPathWithMostFreeSpace(env);

            if (paths.length != 1) {
                // Compute how much space there is on each path
                final Map<NodeEnvironment.NodePath, BigInteger> pathsToSpace = new HashMap<>(paths.length);
                for (NodeEnvironment.NodePath nodePath : paths) {
                    FileStore fileStore = nodePath.fileStore;
                    BigInteger usableBytes = BigInteger.valueOf(fileStore.getUsableSpace());
                    pathsToSpace.put(nodePath, usableBytes);
                }

                bestPath = Arrays.stream(paths)
                        // Filter out paths that have enough space
                        .filter((path) -> pathsToSpace.get(path).subtract(estShardSizeInBytes).compareTo(BigInteger.ZERO) > 0)
                        // Sort by the number of shards for this index
                        .sorted((p1, p2) -> {
                                int cmp = Long.compare(p1.getNumShards(indexName), p2.getNumShards(indexName));
                                if (cmp == 0) {
                                    // if the number of shards is equal, tie-break with the number of total shards
                                    cmp = Integer.compare(p1.getNumShards(), p2.getNumShards());
                                    if (cmp == 0) {
                                        // if the number of shards is equal, tie-break with the usable bytes
                                        cmp = pathsToSpace.get(p2).compareTo(pathsToSpace.get(p1));
                                    }
                                }
                                return cmp;
                            })
                        // Return the first result
                        .findFirst()
                        // Or the existing best path if there aren't any that fit the criteria
                        .orElse(bestPath);
            }

            selectedNodePath = bestPath;
            statePath = bestPath.resolve(shardId);
            dataPath = statePath;
        }
        return new ShardPath(indexSettings.hasCustomDataPath(), dataPath, statePath, selectedNodePath, shardId);
    }

    static NodeEnvironment.NodePath getPathWithMostFreeSpace(NodeEnvironment env) throws IOException {
        final NodeEnvironment.NodePath[] paths = env.nodePaths();
        NodeEnvironment.NodePath bestPath = null;
        long maxUsableBytes = Long.MIN_VALUE;
        for (NodeEnvironment.NodePath nodePath : paths) {
            FileStore fileStore = nodePath.fileStore;
            long usableBytes = fileStore.getUsableSpace(); // NB usable bytes doesn't account for reserved space (e.g. incoming recoveries)
            assert usableBytes >= 0 : "usable bytes must be >= 0, got: " + usableBytes;

            if (bestPath == null || usableBytes > maxUsableBytes) {
                // This path has been determined to be "better" based on the usable bytes
                maxUsableBytes = usableBytes;
                bestPath = nodePath;
            }
        }
        return bestPath;
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
