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

package org.elasticsearch.env;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.common.primitives.Ints;
import org.apache.lucene.store.Lock;
import org.apache.lucene.store.LockObtainFailedException;
import org.apache.lucene.store.NativeFSLockFactory;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.XIOUtils;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.ElasticsearchIllegalStateException;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.FileSystemUtils;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.index.shard.ShardId;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A component that holds all data paths for a single node.
 */
public class NodeEnvironment extends AbstractComponent implements Closeable{

    /* ${data.paths}/nodes/{node.id} */
    private final Path[] nodePaths;
    /* ${data.paths}/nodes/{node.id}/indices */
    private final Path[] nodeIndicesPaths;
    private final Lock[] locks;
    private final boolean addNodeId;

    private final int localNodeId;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final Map<ShardId, InternalShardLock> shardLocks = new HashMap<>();

    private final boolean customPathsEnabled;

    // Setting to automatically append node id to custom data paths
    public static final String ADD_NODE_ID_TO_CUSTOM_PATH = "node.add_id_to_custom_path";
    // Setting to enable custom index.data_path setting for new indices
    public static final String SETTING_CUSTOM_DATA_PATH_ENABLED = "node.enable_custom_paths";

    public static final String NODES_FOLDER = "nodes";
    public static final String INDICES_FOLDER = "indices";
    public static final String NODE_LOCK_FILENAME = "node.lock";

    @Inject
    public NodeEnvironment(Settings settings, Environment environment) throws IOException {
        super(settings);

        this.addNodeId = settings.getAsBoolean(ADD_NODE_ID_TO_CUSTOM_PATH, true);
        this.customPathsEnabled = settings.getAsBoolean(SETTING_CUSTOM_DATA_PATH_ENABLED, false);

        if (!DiscoveryNode.nodeRequiresLocalStorage(settings)) {
            nodePaths = null;
            nodeIndicesPaths = null;
            locks = null;
            localNodeId = -1;
            return;
        }

        final Path[] nodePaths = new Path[environment.dataWithClusterFiles().length];
        final Lock[] locks = new Lock[environment.dataWithClusterFiles().length];
        int localNodeId = -1;
        IOException lastException = null;
        int maxLocalStorageNodes = settings.getAsInt("node.max_local_storage_nodes", 50);
        for (int possibleLockId = 0; possibleLockId < maxLocalStorageNodes; possibleLockId++) {
            for (int dirIndex = 0; dirIndex < environment.dataWithClusterFiles().length; dirIndex++) {
                Path dir = environment.dataWithClusterFiles()[dirIndex].toPath().resolve(Paths.get(NODES_FOLDER, Integer.toString(possibleLockId)));
                if (Files.exists(dir) == false) {
                    Files.createDirectories(dir);
                }
                logger.trace("obtaining node lock on {} ...", dir.toAbsolutePath());
                try {
                    NativeFSLockFactory lockFactory = new NativeFSLockFactory(dir.toFile());
                    Lock tmpLock = lockFactory.makeLock("node.lock");
                    boolean obtained = tmpLock.obtain();
                    if (obtained) {
                        locks[dirIndex] = tmpLock;
                        nodePaths[dirIndex] = dir;
                        localNodeId = possibleLockId;
                    } else {
                        logger.trace("failed to obtain node lock on {}", dir.toAbsolutePath());
                        // release all the ones that were obtained up until now
                        for (int i = 0; i < locks.length; i++) {
                            if (locks[i] != null) {
                                IOUtils.closeWhileHandlingException(locks[i]);
                            }
                            locks[i] = null;
                        }
                        break;
                    }
                } catch (IOException e) {
                    logger.trace("failed to obtain node lock on {}", e, dir.toAbsolutePath());
                    lastException = new IOException("failed to obtain lock on " + dir.toAbsolutePath(), e);
                    // release all the ones that were obtained up until now
                    for (int i = 0; i < locks.length; i++) {
                        IOUtils.closeWhileHandlingException(locks[i]);
                        locks[i] = null;
                    }
                    break;
                }
            }
            if (locks[0] != null) {
                // we found a lock, break
                break;
            }
        }
        if (locks[0] == null) {
            throw new ElasticsearchIllegalStateException("Failed to obtain node lock, is the following location writable?: "
                    + Arrays.toString(environment.dataWithClusterFiles()), lastException);
        }

        this.localNodeId = localNodeId;
        this.locks = locks;
        this.nodePaths = nodePaths;


        if (logger.isDebugEnabled()) {
            logger.debug("using node location [{}], local_node_id [{}]", nodePaths, localNodeId);
        }
        if (logger.isTraceEnabled()) {
            StringBuilder sb = new StringBuilder("node data locations details:\n");
            for (Path file : nodePaths) {
                sb.append(" -> ")
                        .append(file.toAbsolutePath())
                        .append(", free_space [")
                        .append(new ByteSizeValue(Files.getFileStore(file).getUnallocatedSpace()))
                        .append("], usable_space [")
                        .append(new ByteSizeValue(Files.getFileStore(file).getUsableSpace()))
                        .append("]\n");
            }
            logger.trace(sb.toString());
        }

        this.nodeIndicesPaths = new Path[nodePaths.length];
        for (int i = 0; i < nodePaths.length; i++) {
            nodeIndicesPaths[i] = nodePaths[i].resolve(INDICES_FOLDER);
        }
    }



    /**
     * Deletes a shard data directory iff the shards locks were successfully acquired.
     *
     * @param shardId the id of the shard to delete to delete
     * @throws IOException if an IOException occurs
     */
    public void deleteShardDirectorySafe(ShardId shardId, @IndexSettings Settings indexSettings) throws IOException {
        // This is to ensure someone doesn't use ImmutableSettings.EMPTY
        assert indexSettings != ImmutableSettings.EMPTY;
        final Path[] paths = shardPaths(shardId);
        logger.trace("deleting shard {} directory, paths: [{}]", shardId, paths);
        try (ShardLock lock = shardLock(shardId)) {
            deleteShardDirectoryUnderLock(lock, indexSettings);
        }
    }

    /**
     * Deletes a shard data directory. Note: this method assumes that the shard lock is acquired
     *
     * @param lock the shards lock
     * @throws IOException if an IOException occurs
     */
    public void deleteShardDirectoryUnderLock(ShardLock lock, @IndexSettings Settings indexSettings) throws IOException {
        assert indexSettings != ImmutableSettings.EMPTY;
        final ShardId shardId = lock.getShardId();
        assert isShardLocked(shardId) : "shard " + shardId + " is not locked";
        final Path[] paths = shardPaths(shardId);
        XIOUtils.rm(paths);
        if (hasCustomDataPath(indexSettings)) {
            Path customLocation = resolveCustomLocation(indexSettings, shardId);
            logger.trace("deleting custom shard {} directory [{}]", shardId, customLocation);
            XIOUtils.rm(customLocation);
        }
        logger.trace("deleted shard {} directory, paths: [{}]", shardId, paths);
        assert FileSystemUtils.exists(paths) == false;
    }

    private boolean isShardLocked(ShardId id) {
        try {
            shardLock(id, 0).close();
            return false;
        } catch (IOException ex) {
            return true;
        }
    }

    /**
     * Deletes an indexes data directory recursively iff all of the indexes
     * shards locks were successfully acquired. If any of the indexes shard directories can't be locked
     * non of the shards will be deleted
     *
     * @param index the index to delete
     * @param lockTimeoutMS how long to wait for acquiring the indices shard locks
     * @param indexSettings settings for the index being deleted
     * @throws Exception if any of the shards data directories can't be locked or deleted
     */
    public void deleteIndexDirectorySafe(Index index, long lockTimeoutMS, @IndexSettings Settings indexSettings) throws IOException {
        // This is to ensure someone doesn't use ImmutableSettings.EMPTY
        assert indexSettings != ImmutableSettings.EMPTY;
        final List<ShardLock> locks = lockAllForIndex(index, indexSettings, lockTimeoutMS);
        try {
            deleteIndexDirectoryUnderLock(index, indexSettings);
        } finally {
            IOUtils.closeWhileHandlingException(locks);
        }
    }

    /**
     * Deletes an indexes data directory recursively.
     * Note: this method assumes that the shard lock is acquired
     *
     * @param index the index to delete
     * @param indexSettings settings for the index being deleted
     */
    public void deleteIndexDirectoryUnderLock(Index index, @IndexSettings Settings indexSettings) throws IOException {
        // This is to ensure someone doesn't use ImmutableSettings.EMPTY
        assert indexSettings != ImmutableSettings.EMPTY;
        final Path[] indexPaths = indexPaths(index);
        logger.trace("deleting index {} directory, paths({}): [{}]", index, indexPaths.length, indexPaths);
        XIOUtils.rm(indexPaths);
        if (hasCustomDataPath(indexSettings)) {
            Path customLocation = resolveCustomLocation(indexSettings, index.name());
            logger.trace("deleting custom index {} directory [{}]", index, customLocation);
            XIOUtils.rm(customLocation);
        }
    }


    /**
     * Tries to lock all local shards for the given index. If any of the shard locks can't be acquired
     * an {@link LockObtainFailedException} is thrown and all previously acquired locks are released.
     *
     * @param index the index to lock shards for
     * @param lockTimeoutMS how long to wait for acquiring the indices shard locks
     * @return the {@link ShardLock} instances for this index.
     * @throws IOException if an IOException occurs.
     */
    public List<ShardLock> lockAllForIndex(Index index, @IndexSettings Settings settings, long lockTimeoutMS) throws IOException {
        final Integer numShards = settings.getAsInt(IndexMetaData.SETTING_NUMBER_OF_SHARDS, null);
        if (numShards == null || numShards <= 0) {
            throw new IllegalArgumentException("settings must contain a non-null > 0 number of shards");
        }
        logger.trace("locking all shards for index {} - [{}]", index, numShards);
        List<ShardLock> allLocks = new ArrayList<>(numShards);
        boolean success = false;
        long startTime = System.currentTimeMillis();
        try {
            for (int i = 0; i < numShards; i++) {
                long timeoutLeft = Math.max(0, lockTimeoutMS - (System.currentTimeMillis() - startTime));
                allLocks.add(shardLock(new ShardId(index, i), timeoutLeft));
            }
            success = true;
        } finally {
            if (success == false) {
                logger.trace("unable to lock all shards for index {}", index);
                IOUtils.closeWhileHandlingException(allLocks);
            }
        }
        return allLocks;
    }

    /**
     * Tries to lock the given shards ID. A shard lock is required to perform any kind of
     * write operation on a shards data directory like deleting files, creating a new index writer
     * or recover from a different shard instance into it. If the shard lock can not be acquired
     * an {@link LockObtainFailedException} is thrown.
     *
     * Note: this method will return immediately if the lock can't be acquired.
     *
     * @param id the shard ID to lock
     * @return the shard lock. Call {@link ShardLock#close()} to release the lock
     * @throws IOException if an IOException occurs.
     */
    public ShardLock shardLock(ShardId id) throws IOException {
        return shardLock(id, 0);
    }

    /**
     * Tries to lock the given shards ID. A shard lock is required to perform any kind of
     * write operation on a shards data directory like deleting files, creating a new index writer
     * or recover from a different shard instance into it. If the shard lock can not be acquired
     * an {@link org.apache.lucene.store.LockObtainFailedException} is thrown
     * @param id the shard ID to lock
     * @param lockTimeoutMS the lock timeout in milliseconds
     * @return the shard lock. Call {@link ShardLock#close()} to release the lock
     * @throws IOException if an IOException occurs.
     */
    public ShardLock shardLock(final ShardId id, long lockTimeoutMS) throws IOException {
        logger.trace("acquiring node shardlock on [{}], timeout [{}]", id, lockTimeoutMS);
        final InternalShardLock shardLock;
        final boolean acquired;
        synchronized (shardLocks) {
            if (shardLocks.containsKey(id)) {
                shardLock = shardLocks.get(id);
                shardLock.incWaitCount();
                acquired = false;
            } else {
                shardLock = new InternalShardLock(id);
                shardLocks.put(id, shardLock);
                acquired = true;
            }
        }
        if (acquired == false) {
            boolean success = false;
            try {
                shardLock.acquire(lockTimeoutMS);
                success = true;
            } finally {
                if (success == false) {
                    shardLock.decWaitCount();
                }
            }
        }
        logger.trace("successfully acquired shardlock for [{}]", id);
        return new ShardLock(id) { // new instance prevents double closing
            @Override
            protected void closeInternal() {
                shardLock.release();
                logger.trace("released shard lock for [{}]", id);
            }
        };
    }

    /**
     * Returns all currently lock shards
     */
    public Set<ShardId> lockedShards() {
        synchronized (shardLocks) {
            ImmutableSet.Builder<ShardId> builder = ImmutableSet.builder();
            return builder.addAll(shardLocks.keySet()).build();
        }
    }

    private final class InternalShardLock {
        /*
         * This class holds a mutex for exclusive access and timeout / wait semantics
         * and a reference count to cleanup the shard lock instance form the internal data
         * structure if nobody is waiting for it. the wait count is guarded by the same lock
         * that is used to mutate the map holding the shard locks to ensure exclusive access
         */
        private final Semaphore mutex = new Semaphore(1);
        private int waitCount = 1; // guarded by shardLocks
        private ShardId shardId;

        InternalShardLock(ShardId id) {
            shardId = id;
            mutex.acquireUninterruptibly();
        }

        protected void release() {
            mutex.release();
            decWaitCount();
        }

        void incWaitCount() {
            synchronized (shardLocks) {
                assert waitCount > 0 : "waitCount is " + waitCount + " but should be > 0";
                waitCount++;
            }
        }

        private void decWaitCount() {
            synchronized (shardLocks) {
                assert waitCount > 0 : "waitCount is " + waitCount + " but should be > 0";
                --waitCount;
                logger.trace("shard lock wait count for [{}] is now [{}]", shardId, waitCount);
                if (waitCount == 0) {
                    logger.trace("last shard lock wait decremented, removing lock for [{}]", shardId);
                    InternalShardLock remove = shardLocks.remove(shardId);
                    assert remove != null : "Removed lock was null";
                }
            }
        }

        void acquire(long timeoutInMillis) throws LockObtainFailedException{
            try {
                if (mutex.tryAcquire(timeoutInMillis, TimeUnit.MILLISECONDS) == false) {
                    throw new LockObtainFailedException("Can't lock shard " + shardId + ", timed out after " + timeoutInMillis + "ms");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new LockObtainFailedException("Can't lock shard " + shardId + ", interrupted", e);
            }
        }
    }

    public int localNodeId() {
        return this.localNodeId;
    }

    public boolean hasNodeFile() {
        return nodePaths != null && locks != null;
    }

    /**
     * Returns an array of all of the nodes data locations.
     * @throws org.elasticsearch.ElasticsearchIllegalStateException if the node is not configured to store local locations
     */
    public Path[] nodeDataPaths() {
        assert assertEnvIsLocked();
        if (nodePaths == null || locks == null) {
            throw new ElasticsearchIllegalStateException("node is not configured to store local location");
        }
        return nodePaths;
    }

    /**
     * Returns an array of all of the nodes data locations.
     * @deprecated use {@link #nodeDataPaths()}  instead
     */
    @Deprecated
    public File[] nodeDataLocations() {
       return toFiles(nodeDataPaths());
    }

    /**
     * Returns all data paths for the given index.
     * @deprecated use {@link #indexPaths(org.elasticsearch.index.Index)} instead
     */
    @Deprecated
    public File[] indexLocations(Index index) {
        return toFiles(indexPaths(index));
    }

    /**
     * Returns all data paths for the given shards ID
     * @deprecated use {@link #shardPaths(org.elasticsearch.index.shard.ShardId)} instead
     */
    @Deprecated
    public File[] shardLocations(ShardId shardId) {
       return toFiles(shardPaths(shardId));
    }

    /**
     * Returns all data paths for the given shards ID
     * @deprecated use {@code shardDataPaths} instead
     */
    @Deprecated
    public File[] shardDataLocations(ShardId shardId, @IndexSettings Settings settings) {
        return toFiles(shardDataPaths(shardId, settings));
    }

    /**
     * Returns all data paths excluding custom index paths.
     */
    public Path[] indexPaths(Index index) {
        assert assertEnvIsLocked();
        Path[] indexPaths = new Path[nodeIndicesPaths.length];
        for (int i = 0; i < nodeIndicesPaths.length; i++) {
            indexPaths[i] = nodeIndicesPaths[i].resolve(index.name());
        }
        return indexPaths;
    }

    /**
     * Returns all paths where lucene data will be stored, if a index.data_path
     * setting is present, will return the custom data path to be used
     */
    public Path[] shardDataPaths(ShardId shardId, @IndexSettings Settings indexSettings) {
        assert indexSettings != ImmutableSettings.EMPTY;
        assert assertEnvIsLocked();
        if (hasCustomDataPath(indexSettings)) {
            return new Path[] {resolveCustomLocation(indexSettings, shardId)};
        } else {
            return shardPaths(shardId);
        }
    }

    /**
     * Returns all shard paths excluding custom shard path
     */
    public Path[] shardPaths(ShardId shardId) {
        assert assertEnvIsLocked();
        final Path[] nodePaths = nodeDataPaths();
        final Path[] shardLocations = new Path[nodePaths.length];
        for (int i = 0; i < nodePaths.length; i++) {
            shardLocations[i] = nodePaths[i].resolve(Paths.get(INDICES_FOLDER,
                    shardId.index().name(),
                    Integer.toString(shardId.id())));
        }
        return shardLocations;
    }

    public Set<String> findAllIndices() throws Exception {
        if (nodePaths == null || locks == null) {
            throw new ElasticsearchIllegalStateException("node is not configured to store local location");
        }
        assert assertEnvIsLocked();
        Set<String> indices = Sets.newHashSet();
        for (Path indicesLocation : nodeIndicesPaths) {

            if (Files.exists(indicesLocation) && Files.isDirectory(indicesLocation)) {
                try (DirectoryStream<Path> stream = Files.newDirectoryStream(indicesLocation)) {
                    for (Path index : stream) {
                        if (Files.isDirectory(index)) {
                            indices.add(index.getFileName().toString());
                        }
                    }
                }
            }
        }
        return indices;
    }

    /**
     * Tries to find all allocated shards for the given index or for all indices iff the given index is {@code null}
     * on the current node. NOTE: This methods is prone to race-conditions on the filesystem layer since it might not
     * see directories created concurrently or while it's traversing.
     * @param index the index to filter shards for or {@code null} if all shards for all indices should be listed
     * @return a set of shard IDs
     * @throws IOException if an IOException occurs
     */
    public Set<ShardId> findAllShardIds(final Index index) throws IOException {
        if (nodePaths == null || locks == null) {
            throw new ElasticsearchIllegalStateException("node is not configured to store local location");
        }
        assert assertEnvIsLocked();
        return findAllShardIds(index == null ? null : index.getName(), nodeIndicesPaths);
    }

    private static Set<ShardId> findAllShardIds(@Nullable final String index, Path... locations) throws IOException {
        final Set<ShardId> shardIds = Sets.newHashSet();
        for (final Path location : locations) {
            if (Files.isDirectory(location)) {
                try (DirectoryStream<Path> indexStream = Files.newDirectoryStream(location)) {
                    for (Path indexPath : indexStream) {
                        if (index == null || index.equals(indexPath.getFileName().toString())) {
                            shardIds.addAll(findAllShardsForIndex(indexPath));
                        }
                    }
                }
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

    /**
     * Tries to find all allocated shards for all indices iff the given index on the current node. NOTE: This methods
     * is prone to race-conditions on the filesystem layer since it might not see directories created concurrently or
     * while it's traversing.
     *
     * @return a set of shard IDs
     * @throws IOException if an IOException occurs
     */
    public Set<ShardId> findAllShardIds() throws IOException {
        return findAllShardIds(null);
    }

    public void close() {
        if (closed.compareAndSet(false, true) && locks != null) {
            for (Lock lock : locks) {
                try {
                    logger.trace("releasing lock [{}]", lock);
                    lock.close();
                } catch (IOException e) {
                    logger.trace("failed to release lock [{}]", e, lock);
                }
            }
        }
    }


    private boolean assertEnvIsLocked() {
        if (!closed.get() && locks != null) {
            for (Lock lock : locks) {
                try {
                    assert lock.isLocked() : "Lock: " + lock + "is not locked";
                } catch (IOException e) {
                    logger.warn("lock assertion failed", e);
                    return false;
                }
            }
        }
        return true;
    }

    /**
     * This method tries to write an empty file and moves it using an atomic move operation.
     * This method throws an {@link ElasticsearchIllegalStateException} if this operation is
     * not supported by the filesystem. This test is executed on each of the data directories.
     * This method cleans up all files even in the case of an error.
     */
    public void ensureAtomicMoveSupported() throws IOException {
        final Path[] nodePaths = nodeDataPaths();
        for (Path directory : nodePaths) {
            assert Files.isDirectory(directory) : directory + " is not a directory";
            final Path src = directory.resolve("__es__.tmp");
            Files.createFile(src);
            final Path target = directory.resolve("__es__.final");
            try {
                Files.move(src, target, StandardCopyOption.ATOMIC_MOVE);
            } catch (AtomicMoveNotSupportedException ex) {
                throw new ElasticsearchIllegalStateException("atomic_move is not supported by the filesystem on path ["
                        + directory
                        + "] atomic_move is required for elasticsearch to work correctly.", ex);
            } finally {
                Files.deleteIfExists(src);
                Files.deleteIfExists(target);
            }
        }
    }


    /**
     * Returns an array of {@link File} build from the correspondent element
     * in the input array using {@link java.nio.file.Path#toFile()} )}
     * @param files the files to get paths for
     */
    @Deprecated // this is only a transition API
    private static File[] toFiles(Path... files) {
        File[] paths = new File[files.length];
        for (int i = 0; i < files.length; i++) {
            paths[i] = files[i].toFile();
        }
        return paths;
    }

    /** return true if custom paths are allowed for indices */
    public boolean isCustomPathsEnabled() {
        return customPathsEnabled;
    }

    /**
     * @param indexSettings settings for an index
     * @return true if the index has a custom data path
     */
    public static boolean hasCustomDataPath(@IndexSettings Settings indexSettings) {
        return indexSettings.get(IndexMetaData.SETTING_DATA_PATH) != null;
    }

    /**
     * Resolve the custom path for a index's shard.
     * Uses the {@code IndexMetaData.SETTING_DATA_PATH} setting to determine
     * the root path for the index.
     *
     * @param indexSettings settings for the index
     */
    private Path resolveCustomLocation(@IndexSettings Settings indexSettings) {
        assert indexSettings != ImmutableSettings.EMPTY;
        String customDataDir = indexSettings.get(IndexMetaData.SETTING_DATA_PATH);
        if (customDataDir != null) {
            // This assert is because this should be caught by MetaDataCreateIndexService
            assert customPathsEnabled;
            if (addNodeId) {
                return Paths.get(customDataDir, Integer.toString(this.localNodeId));
            } else {
                return Paths.get(customDataDir);
            }
        } else {
            throw new ElasticsearchIllegalArgumentException("no custom " + IndexMetaData.SETTING_DATA_PATH + " setting available");
        }
    }

    /**
     * Resolve the custom path for a index's shard.
     * Uses the {@code IndexMetaData.SETTING_DATA_PATH} setting to determine
     * the root path for the index.
     *
     * @param indexSettings settings for the index
     * @param indexName index to resolve the path for
     */
    private Path resolveCustomLocation(@IndexSettings Settings indexSettings, final String indexName) {
        return resolveCustomLocation(indexSettings).resolve(indexName);
    }

    /**
     * Resolve the custom path for a index's shard.
     * Uses the {@code IndexMetaData.SETTING_DATA_PATH} setting to determine
     * the root path for the index.
     *
     * @param indexSettings settings for the index
     * @param shardId shard to resolve the path to
     */
    public Path resolveCustomLocation(@IndexSettings Settings indexSettings, final ShardId shardId) {
        return resolveCustomLocation(indexSettings, shardId.index().name()).resolve(Integer.toString(shardId.id()));
    }
}
