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

import org.elasticsearch.ElasticsearchIllegalStateException;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.settings.IndexSettings;

import java.io.IOException;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public final class ShardPath {
    public static final String INDEX_FOLDER_NAME = "index";
    public static final String TRANSLOG_FOLDER_NAME = "translog";

    private final Path path;
    private final String indexUUID;
    private final ShardId shardId;
    private final Path shardStatePath;


    public ShardPath(Path path, Path shardStatePath, String indexUUID, ShardId shardId) {
        this.path = path;
        this.indexUUID = indexUUID;
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

    public String getIndexUUID() {
        return indexUUID;
    }

    public ShardId getShardId() {
        return shardId;
    }

    public Path getShardStatePath() {
        return shardStatePath;
    }

    /**
     * This method walks through the nodes shard paths to find the data and state path for the given shard. If multiple
     * directories with a valid shard state exist the one with the highest version will be used.
     * <b>Note:</b> this method resolves custom data locations for the shard.
     */
    public static ShardPath loadShardPath(ESLogger logger, NodeEnvironment env, ShardId shardId, @IndexSettings Settings indexSettings) throws IOException {
        final String indexUUID = indexSettings.get(IndexMetaData.SETTING_UUID, IndexMetaData.INDEX_UUID_NA_VALUE);
        final Path[] paths = env.availableShardPaths(shardId);
        Path loadedPath = null;
        for (Path path : paths) {
            ShardStateMetaData load = ShardStateMetaData.FORMAT.loadLatestState(logger, path);
            if (load != null) {
                if ((load.indexUUID.equals(indexUUID) || IndexMetaData.INDEX_UUID_NA_VALUE.equals(load.indexUUID)) == false) {
                    throw new ElasticsearchIllegalStateException(shardId + " index UUID in shard state was: " + load.indexUUID + " excepted: " + indexUUID + " on shard path: " + path);
                }
                if (loadedPath == null) {
                    loadedPath = path;
                } else{
                    throw new ElasticsearchIllegalStateException(shardId + " more than one shard state found");
                }
            }

        }
        if (loadedPath == null) {
            return null;
        } else {
            final Path dataPath;
            final Path statePath = loadedPath;
            if (NodeEnvironment.hasCustomDataPath(indexSettings)) {
                dataPath = env.resolveCustomLocation(indexSettings, shardId);
            } else {
                dataPath = statePath;
            }
            logger.debug("{} loaded  data path [{}], state path [{}]", shardId, dataPath, statePath);
            return new ShardPath(dataPath, statePath, indexUUID, shardId);
        }
    }

    // TODO - do we need something more extensible? Yet, this does the job for now...
    public static ShardPath selectNewPathForShard(NodeEnvironment env, ShardId shardId, @IndexSettings Settings indexSettings) throws IOException {
        final String indexUUID = indexSettings.get(IndexMetaData.SETTING_UUID, IndexMetaData.INDEX_UUID_NA_VALUE);
        final NodeEnvironment.NodePath[] paths = env.nodePaths();
        final List<Tuple<Path, Long>> minUsedPaths = new ArrayList<>();
        for (NodeEnvironment.NodePath nodePath : paths) {
            final Path shardPath = nodePath.resolve(shardId);
            FileStore fileStore = nodePath.fileStore;
            long usableSpace = fileStore.getUsableSpace();
            if (minUsedPaths.isEmpty() || minUsedPaths.get(0).v2() == usableSpace) {
                minUsedPaths.add(new Tuple<>(shardPath, usableSpace));
            } else if (minUsedPaths.get(0).v2() < usableSpace) {
                minUsedPaths.clear();
                minUsedPaths.add(new Tuple<>(shardPath, usableSpace));
            }
        }
        Path minUsed = minUsedPaths.get(shardId.id() % minUsedPaths.size()).v1();
        final Path dataPath;
        final Path statePath = minUsed;
        if (NodeEnvironment.hasCustomDataPath(indexSettings)) {
            dataPath = env.resolveCustomLocation(indexSettings, shardId);
        } else {
            dataPath = statePath;
        }
        return new ShardPath(dataPath, statePath, indexUUID, shardId);
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
        if (shardId != null ? !shardId.equals(shardPath.shardId) : shardPath.shardId != null) {
            return false;
        }
        if (indexUUID != null ? !indexUUID.equals(shardPath.indexUUID) : shardPath.indexUUID != null) {
            return false;
        }
        if (path != null ? !path.equals(shardPath.path) : shardPath.path != null) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = path != null ? path.hashCode() : 0;
        result = 31 * result + (indexUUID != null ? indexUUID.hashCode() : 0);
        result = 31 * result + (shardId != null ? shardId.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "ShardPath{" +
                "path=" + path +
                ", indexUUID='" + indexUUID + '\'' +
                ", shard=" + shardId +
                '}';
    }
}
