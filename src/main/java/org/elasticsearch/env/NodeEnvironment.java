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
import org.apache.lucene.store.*;
import org.apache.lucene.util.IOUtils;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.ElasticsearchIllegalStateException;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.FileSystemUtils;
import org.elasticsearch.common.io.PathUtils;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.monitor.fs.FsStats;
import org.elasticsearch.monitor.fs.JmxFsProbe;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A component that holds all data paths for a single node.
 */
public class NodeEnvironment extends AbstractComponent implements Closeable {

    public static class NodePath {
        /* ${data.paths}/nodes/{node.id} */
        public final Path path;
        /* ${data.paths}/nodes/{node.id}/indices */
        public final Path indicesPath;
        /** Cached FileStore from path */
        public final FileStore fileStore;
        /** Cached result of Lucene's {@code IOUtils.spins} on path.  This is a trilean value: null means we could not determine it (we are
         *  not running on Linux, or we hit an exception trying), True means the device possibly spins and False means it does not. */
        public final Boolean spins;

        public NodePath(Path path, Environment environment) throws IOException {
            this.path = path;
            this.indicesPath = path.resolve(INDICES_FOLDER);
            this.fileStore = environment.getFileStore(path);
            if (fileStore.supportsFileAttributeView("lucene")) {
                this.spins = (Boolean) fileStore.getAttribute("lucene:spins");
            } else {
                this.spins = null;
            }
        }

        /**
         * Resolves the given shards directory against this NodePath
         */
        public Path resolve(ShardId shardId) {
            return resolve(shardId.index()).resolve(Integer.toString(shardId.id()));
        }

        /**
         * Resolves the given indexes directory against this NodePath
         */
        public Path resolve(Index index) {
            return indicesPath.resolve(index.name());
        }

        @Override
        public String toString() {
            return "NodePath{" +
                    "path=" + path +
                    ", spins=" + spins +
                    '}';
        }
    }

    private final NodePath[] nodePaths;
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
            locks = null;
            localNodeId = -1;
            return;
        }

        final NodePath[] nodePaths = new NodePath[environment.dataWithClusterFiles().length];
        final Lock[] locks = new Lock[nodePaths.length];

        int localNodeId = -1;
        IOException lastException = null;
        int maxLocalStorageNodes = settings.getAsInt("node.max_local_storage_nodes", 50);
        for (int possibleLockId = 0; possibleLockId < maxLocalStorageNodes; possibleLockId++) {
            for (int dirIndex = 0; dirIndex < environment.dataWithClusterFiles().length; dirIndex++) {
                // TODO: wtf with resolve(get())
                Path dir = environment.dataWithClusterFiles()[dirIndex].resolve(PathUtils.get(NODES_FOLDER, Integer.toString(possibleLockId)));
                Files.createDirectories(dir);
                
                try (Directory luceneDir = FSDirectory.open(dir, NativeFSLockFactory.INSTANCE)) {
                    logger.trace("obtaining node lock on {} ...", dir.toAbsolutePath());
                    Lock tmpLock = luceneDir.makeLock(NODE_LOCK_FILENAME);
                    boolean obtained = tmpLock.obtain();
                    if (obtained) {
                        nodePaths[dirIndex] = new NodePath(dir, environment);
                        locks[dirIndex] = tmpLock;
                        localNodeId = possibleLockId;
                    } else {
                        logger.trace("failed to obtain node lock on {}", dir.toAbsolutePath());
                        // release all the ones that were obtained up until now
                        releaseAndNullLocks(locks);
                        break;
                    }
                } catch (IOException e) {
                    logger.trace("failed to obtain node lock on {}", e, dir.toAbsolutePath());
                    lastException = new IOException("failed to obtain lock on " + dir.toAbsolutePath(), e);
                    // release all the ones that were obtained up until now
                    releaseAndNullLocks(locks);
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

        maybeLogPathDetails();
    }

    private static void releaseAndNullLocks(Lock[] locks) {
        for (int i = 0; i < locks.length; i++) {
            if (locks[i] != null) {
                IOUtils.closeWhileHandlingException(locks[i]);
            }
            locks[i] = null;
        }
    }

    private void maybeLogPathDetails() throws IOException {

        // We do some I/O in here, so skip this if DEBUG/INFO are not enabled:
        if (logger.isDebugEnabled()) {
            // Log one line per path.data:
            StringBuilder sb = new StringBuilder("node data locations details:");
            for (NodePath nodePath : nodePaths) {
                sb.append('\n').append(" -> ").append(nodePath.path.toAbsolutePath());

                String spinsDesc;
                if (nodePath.spins == null) {
                    spinsDesc = "unknown";
                } else if (nodePath.spins) {
                    spinsDesc = "possibly";
                } else {
                    spinsDesc = "no";
                }

                FsStats.Info fsInfo = JmxFsProbe.getFSInfo(nodePath);
                sb.append(", free_space [")
                    .append(fsInfo.getFree())
                    .append("], usable_space [")
                    .append(fsInfo.getAvailable())
                    .append("], total_space [")
                    .append(fsInfo.getTotal())
                    .append("], spins? [")
                    .append(spinsDesc)
                    .append("], mount [")
                    .append(fsInfo.getMount())
                    .append("], type [")
                    .append(fsInfo.getType())
                    .append(']');
            }
            logger.debug(sb.toString());
        } else if (logger.isInfoEnabled()) {
            FsStats.Info totFSInfo = new FsStats.Info();
            Set<String> allTypes = new HashSet<>();
            Set<String> allSpins = new HashSet<>();
            Set<String> allMounts = new HashSet<>();
            for (NodePath nodePath : nodePaths) {
                // TODO: can/should I use the chosen FsProbe instead (i.e. sigar if it's available)?
                FsStats.Info fsInfo = JmxFsProbe.getFSInfo(nodePath);
                String mount = fsInfo.getMount();
                if (allMounts.contains(mount) == false) {
                    allMounts.add(mount);
                    String type = fsInfo.getType();
                    if (type != null) {
                        allTypes.add(type);
                    }
                    Boolean spins = fsInfo.getSpins();
                    if (spins == null) {
                        allSpins.add("unknown");
                    } else if (spins.booleanValue()) {
                        allSpins.add("possibly");
                    } else {
                        allSpins.add("no");
                    }
                    totFSInfo.add(fsInfo);
                }
            }

            // Just log a 1-line summary:
            logger.info(String.format(Locale.ROOT,
                                      "using [%d] data paths, mounts [%s], net usable_space [%s], net total_space [%s], spins? [%s], types [%s]",
                                      nodePaths.length,
                                      allMounts,
                                      totFSInfo.getAvailable(),
                                      totFSInfo.getTotal(),
                                      toString(allSpins),
                                      toString(allTypes)));
        }
    }

    private static String toString(Collection<String> items) {
        StringBuilder b = new StringBuilder();
        for(String item : items) {
            if (b.length() > 0) {
                b.append(", ");
            }
            b.append(item);
        }
        return b.toString();
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
        final Path[] paths = availableShardPaths(shardId);
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
        final Path[] paths = availableShardPaths(shardId);
        IOUtils.rm(paths);
        if (hasCustomDataPath(indexSettings)) {
            Path customLocation = resolveCustomLocation(indexSettings, shardId);
            logger.trace("deleting custom shard {} directory [{}]", shardId, customLocation);
            IOUtils.rm(customLocation);
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
        IOUtils.rm(indexPaths);
        if (hasCustomDataPath(indexSettings)) {
            Path customLocation = resolveCustomLocation(indexSettings, index.name());
            logger.trace("deleting custom index {} directory [{}]", index, customLocation);
            IOUtils.rm(customLocation);
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
        Path[] paths = new Path[nodePaths.length];
        for(int i=0;i<paths.length;i++) {
            paths[i] = nodePaths[i].path;
        }
        return paths;
    }

    /**
     * Returns an array of all of the {@link NodePath}s.
     */
    public NodePath[] nodePaths() {
        assert assertEnvIsLocked();
        if (nodePaths == null || locks == null) {
            throw new ElasticsearchIllegalStateException("node is not configured to store local location");
        }
        return nodePaths;
    }

    /**
     * Returns all index paths.
     */
    public Path[] indexPaths(Index index) {
        assert assertEnvIsLocked();
        Path[] indexPaths = new Path[nodePaths.length];
        for (int i = 0; i < nodePaths.length; i++) {
            indexPaths[i] = nodePaths[i].indicesPath.resolve(index.name());
        }
        return indexPaths;
    }

    /**
     * Returns all shard paths excluding custom shard path. Note: Shards are only allocated on one of the
     * returned paths. The returned array may contain paths to non-existing directories.
     *
     * @see #hasCustomDataPath(org.elasticsearch.common.settings.Settings)
     * @see #resolveCustomLocation(org.elasticsearch.common.settings.Settings, org.elasticsearch.index.shard.ShardId)
     *
     */
    public Path[] availableShardPaths(ShardId shardId) {
        assert assertEnvIsLocked();
        final NodePath[] nodePaths = nodePaths();
        final Path[] shardLocations = new Path[nodePaths.length];
        for (int i = 0; i < nodePaths.length; i++) {
            shardLocations[i] = nodePaths[i].resolve(shardId);
        }
        return shardLocations;
    }

    public Set<String> findAllIndices() throws IOException {
        if (nodePaths == null || locks == null) {
            throw new ElasticsearchIllegalStateException("node is not configured to store local location");
        }
        assert assertEnvIsLocked();
        Set<String> indices = Sets.newHashSet();
        for (NodePath nodePath : nodePaths) {
            Path indicesLocation = nodePath.indicesPath;
            if (Files.isDirectory(indicesLocation)) {
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

    @Override
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
        final NodePath[] nodePaths = nodePaths();
        for (NodePath nodePath : nodePaths) {
            assert Files.isDirectory(nodePath.path) : nodePath.path + " is not a directory";
            final Path src = nodePath.path.resolve("__es__.tmp");
            Files.createFile(src);
            final Path target = nodePath.path.resolve("__es__.final");
            try {
                Files.move(src, target, StandardCopyOption.ATOMIC_MOVE);
            } catch (AtomicMoveNotSupportedException ex) {
                throw new ElasticsearchIllegalStateException("atomic_move is not supported by the filesystem on path ["
                        + nodePath.path
                        + "] atomic_move is required for elasticsearch to work correctly.", ex);
            } finally {
                Files.deleteIfExists(src);
                Files.deleteIfExists(target);
            }
        }
    }

    Settings getSettings() { // for testing
        return settings;
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
                return PathUtils.get(customDataDir, Integer.toString(this.localNodeId));
            } else {
                return PathUtils.get(customDataDir);
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
