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
package org.elasticsearch.common.util;

import java.nio.charset.StandardCharsets;
import com.google.common.primitives.Ints;
import org.apache.lucene.index.CheckIndex;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.Lock;
import org.apache.lucene.store.LockObtainFailedException;
import org.apache.lucene.store.SimpleFSDirectory;
import org.apache.lucene.util.IOUtils;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.io.FileSystemUtils;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.env.ShardLock;
import org.elasticsearch.gateway.MetaDataStateFormat;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardPath;
import org.elasticsearch.index.shard.ShardStateMetaData;

import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.DirectoryStream;
import java.nio.file.FileStore;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 */
public class MultiDataPathUpgrader {

    private final NodeEnvironment nodeEnvironment;
    private final ESLogger logger = Loggers.getLogger(getClass());


    /**
     * Creates a new upgrader instance
     * @param nodeEnvironment the node env to operate on.
     *
     */
    public MultiDataPathUpgrader(NodeEnvironment nodeEnvironment) {
        this.nodeEnvironment = nodeEnvironment;
    }


    /**
     * Upgrades the given shard Id from multiple shard paths into the given target path.
     *
     * @see #pickShardPath(org.elasticsearch.index.shard.ShardId)
     */
    public void upgrade(ShardId shard, ShardPath targetPath) throws IOException {
        final Path[] paths = nodeEnvironment.availableShardPaths(shard); // custom data path doesn't need upgrading
        if (isTargetPathConfigured(paths, targetPath) == false) {
            throw new IllegalArgumentException("shard path must be one of the shards data paths");
        }
        assert needsUpgrading(shard) : "Should not upgrade a path that needs no upgrading";
        logger.info("{} upgrading multi data dir to {}", shard, targetPath.getDataPath());
        final ShardStateMetaData loaded = ShardStateMetaData.FORMAT.loadLatestState(logger, paths);
        if (loaded == null) {
            throw new IllegalStateException(shard + " no shard state found in any of: " + Arrays.toString(paths) + " please check and remove them if possible");
        }
        logger.info("{} loaded shard state {}", shard, loaded);

        ShardStateMetaData.FORMAT.write(loaded, loaded.version, targetPath.getShardStatePath());
        Files.createDirectories(targetPath.resolveIndex());
        try (SimpleFSDirectory directory = new SimpleFSDirectory(targetPath.resolveIndex())) {
            try (final Lock lock = directory.obtainLock(IndexWriter.WRITE_LOCK_NAME)) {
                upgradeFiles(shard, targetPath, targetPath.resolveIndex(), ShardPath.INDEX_FOLDER_NAME, paths);
            } catch (LockObtainFailedException ex) {
                throw new IllegalStateException("Can't obtain lock on " + targetPath.resolveIndex(), ex);
            }

        }


        upgradeFiles(shard, targetPath, targetPath.resolveTranslog(), ShardPath.TRANSLOG_FOLDER_NAME, paths);

        logger.info("{} wipe upgraded directories", shard);
        for (Path path : paths) {
            if (path.equals(targetPath.getShardStatePath()) == false) {
                logger.info("{} wipe shard directories: [{}]", shard, path);
                IOUtils.rm(path);
            }
        }

        if (FileSystemUtils.files(targetPath.resolveIndex()).length == 0) {
            throw new IllegalStateException("index folder [" + targetPath.resolveIndex() + "] is empty");
        }

        if (FileSystemUtils.files(targetPath.resolveTranslog()).length == 0) {
            throw new IllegalStateException("translog folder [" + targetPath.resolveTranslog() + "] is empty");
        }
    }

    /**
     * Runs check-index on the target shard and throws an exception if it failed
     */
    public void checkIndex(ShardPath targetPath) throws IOException {
        BytesStreamOutput os = new BytesStreamOutput();
        PrintStream out = new PrintStream(os, false, StandardCharsets.UTF_8.name());
        try (Directory directory = new SimpleFSDirectory(targetPath.resolveIndex());
            final CheckIndex checkIndex = new CheckIndex(directory)) {
            checkIndex.setInfoStream(out);
            CheckIndex.Status status = checkIndex.checkIndex();
            out.flush();
            if (!status.clean) {
                logger.warn("check index [failure]\n{}", new String(os.bytes().toBytes(), StandardCharsets.UTF_8));
                throw new IllegalStateException("index check failure");
            }
        }
    }

    /**
     * Returns true iff the given shard needs upgrading.
     */
    public boolean needsUpgrading(ShardId shard) {
        final Path[] paths = nodeEnvironment.availableShardPaths(shard);
         // custom data path doesn't need upgrading neither single path envs
        if (paths.length > 1) {
            int numPathsExist = 0;
            for (Path path : paths) {
                if (Files.exists(path.resolve(MetaDataStateFormat.STATE_DIR_NAME))) {
                    numPathsExist++;
                    if (numPathsExist > 1) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    /**
     * Picks a target ShardPath to allocate and upgrade the given shard to. It picks the target based on a simple
     * heuristic:
     * <ul>
     *  <li>if the smallest datapath has 2x more space available that the shards total size the datapath with the most bytes for that shard is picked to minimize the amount of bytes to copy</li>
     *  <li>otherwise the largest available datapath is used as the target no matter how big of a slice of the shard it already holds.</li>
     * </ul>
     */
    public ShardPath pickShardPath(ShardId shard) throws IOException {
        if (needsUpgrading(shard) == false) {
            throw new IllegalStateException("Shard doesn't need upgrading");
        }
        final NodeEnvironment.NodePath[] paths = nodeEnvironment.nodePaths();

        // if we need upgrading make sure we have all paths.
        for (NodeEnvironment.NodePath path : paths) {
            Files.createDirectories(path.resolve(shard));
        }
        final ShardFileInfo[] shardFileInfo = getShardFileInfo(shard, paths);
        long totalBytesUsedByShard = 0;
        long leastUsableSpace = Long.MAX_VALUE;
        long mostUsableSpace = Long.MIN_VALUE;
        assert shardFileInfo.length ==  nodeEnvironment.availableShardPaths(shard).length;
        for (ShardFileInfo info : shardFileInfo) {
            totalBytesUsedByShard += info.spaceUsedByShard;
            leastUsableSpace = Math.min(leastUsableSpace, info.usableSpace + info.spaceUsedByShard);
            mostUsableSpace = Math.max(mostUsableSpace, info.usableSpace + info.spaceUsedByShard);
        }

        if (mostUsableSpace < totalBytesUsedByShard) {
            throw new IllegalStateException("Can't upgrade path available space: " + new ByteSizeValue(mostUsableSpace) + " required space: " + new ByteSizeValue(totalBytesUsedByShard));
        }
        ShardFileInfo target = shardFileInfo[0];
        if (leastUsableSpace >= (2 * totalBytesUsedByShard)) {
            for (ShardFileInfo info : shardFileInfo) {
                if (info.spaceUsedByShard > target.spaceUsedByShard) {
                    target = info;
                }
            }
        } else {
            for (ShardFileInfo info : shardFileInfo) {
                if (info.usableSpace > target.usableSpace) {
                    target = info;
                }
            }
        }
        return new ShardPath(false, target.path, target.path, IndexMetaData.INDEX_UUID_NA_VALUE /* we don't know */, shard);
    }

    private ShardFileInfo[] getShardFileInfo(ShardId shard, NodeEnvironment.NodePath[] paths) throws IOException {
        final ShardFileInfo[] info = new ShardFileInfo[paths.length];
        for (int i = 0; i < info.length; i++) {
            Path path = paths[i].resolve(shard);
            final long usabelSpace = getUsabelSpace(paths[i]);
            info[i] = new ShardFileInfo(path, usabelSpace, getSpaceUsedByShard(path));
        }
        return info;
    }

    protected long getSpaceUsedByShard(Path path) throws IOException {
        final long[] spaceUsedByShard = new long[] {0};
        if (Files.exists(path)) {
            Files.walkFileTree(path, new FileVisitor<Path>() {
                @Override
                public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                    if (attrs.isRegularFile()) {
                        spaceUsedByShard[0] += attrs.size();
                    }
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult visitFileFailed(Path file, IOException exc) throws IOException {
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                    return FileVisitResult.CONTINUE;
                }
            });
        }
        return spaceUsedByShard[0];
    }

    protected long getUsabelSpace(NodeEnvironment.NodePath path) throws IOException {
        FileStore fileStore = path.fileStore;
        return fileStore.getUsableSpace();
    }

    static class ShardFileInfo {
        final Path path;
        final long usableSpace;
        final long spaceUsedByShard;

        ShardFileInfo(Path path, long usableSpace, long spaceUsedByShard) {
            this.path = path;
            this.usableSpace = usableSpace;
            this.spaceUsedByShard = spaceUsedByShard;
        }
    }



    private void upgradeFiles(ShardId shard, ShardPath targetPath, final Path targetDir, String folderName, Path[] paths) throws IOException {
        List<Path> movedFiles = new ArrayList<>();
        for (Path path : paths) {
            if (path.equals(targetPath.getDataPath()) == false) {
                final Path sourceDir = path.resolve(folderName);
                if (Files.exists(sourceDir)) {
                    logger.info("{} upgrading [{}] from [{}] to [{}]", shard, folderName, sourceDir, targetDir);
                    try (DirectoryStream<Path> stream = Files.newDirectoryStream(sourceDir)) {
                        Files.createDirectories(targetDir);
                        for (Path file : stream) {
                            if (IndexWriter.WRITE_LOCK_NAME.equals(file.getFileName().toString()) || Files.isDirectory(file)) {
                                continue; // skip write.lock
                            }
                            logger.info("{} move file [{}] size: [{}]", shard, file.getFileName(), Files.size(file));
                            final Path targetFile = targetDir.resolve(file.getFileName());
                            /* We are pessimistic and do a copy first to the other path and then and atomic move to rename it such that
                               in the worst case the file exists twice but is never lost or half written.*/
                            final Path targetTempFile = Files.createTempFile(targetDir, "upgrade_", "_" + file.getFileName().toString());
                            Files.copy(file, targetTempFile, StandardCopyOption.COPY_ATTRIBUTES, StandardCopyOption.REPLACE_EXISTING);
                            Files.move(targetTempFile, targetFile, StandardCopyOption.ATOMIC_MOVE); // we are on the same FS - this must work otherwise all bets are off
                            Files.delete(file);
                            movedFiles.add(targetFile);
                        }
                    }
                }
            }
        }
        if (movedFiles.isEmpty() == false) {
            // fsync later it might be on disk already
            logger.info("{} fsync files", shard);
            for (Path moved : movedFiles) {
                logger.info("{} syncing [{}]", shard, moved.getFileName());
                IOUtils.fsync(moved, false);
            }
            logger.info("{} syncing directory [{}]", shard, targetDir);
            IOUtils.fsync(targetDir, true);
        }
    }


    /**
     * Returns <code>true</code> iff the target path is one of the given paths.
     */
    private boolean isTargetPathConfigured(final Path[] paths, ShardPath targetPath) {
        for (Path path : paths) {
            if (path.equals(targetPath.getDataPath())) {
                return true;
            }
        }
        return false;
    }

    /**
     * Runs an upgrade on all shards located under the given node environment if there is more than 1 data.path configured
     * otherwise this method will return immediately.
     */
    public static void upgradeMultiDataPath(NodeEnvironment nodeEnv, ESLogger logger) throws IOException {
        if (nodeEnv.nodeDataPaths().length > 1) {
            final MultiDataPathUpgrader upgrader = new MultiDataPathUpgrader(nodeEnv);
            final Set<String> allIndices = nodeEnv.findAllIndices();

            for (String index : allIndices) {
                for (ShardId shardId : findAllShardIds(nodeEnv.indexPaths(new Index(index)))) {
                    try (ShardLock lock = nodeEnv.shardLock(shardId, 0)) {
                        if (upgrader.needsUpgrading(shardId)) {
                            final ShardPath shardPath = upgrader.pickShardPath(shardId);
                            upgrader.upgrade(shardId, shardPath);
                            // we have to check if the index path exists since we might
                            // have only upgraded the shard state that is written under /indexname/shardid/_state
                            // in the case we upgraded a dedicated index directory index
                            if (Files.exists(shardPath.resolveIndex())) {
                                upgrader.checkIndex(shardPath);
                            }
                        } else {
                            logger.debug("{} no upgrade needed - already upgraded");
                        }
                    }
                }
            }
        }
    }

    private static Set<ShardId> findAllShardIds(Path... locations) throws IOException {
        final Set<ShardId> shardIds = new HashSet<>();
        for (final Path location : locations) {
            if (Files.isDirectory(location)) {
                shardIds.addAll(findAllShardsForIndex(location));
            }
        }
        return shardIds;
    }

    private static Set<ShardId> findAllShardsForIndex(Path indexPath) throws IOException {
        Set<ShardId> shardIds = new HashSet<>();
        if (Files.isDirectory(indexPath)) {
            try (DirectoryStream<Path> stream = Files.newDirectoryStream(indexPath)) {
                String currentIndex = indexPath.getFileName().toString();
                for (Path shardPath : stream) {
                    if (Files.isDirectory(shardPath)) {
                        Integer shardId = Ints.tryParse(shardPath.getFileName().toString());
                        if (shardId != null) {
                            ShardId id = new ShardId(currentIndex, shardId);
                            shardIds.add(id);
                        }
                    }
                }
            }
        }
        return shardIds;
    }

}
