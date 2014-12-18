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
import org.apache.lucene.store.*;
import org.apache.lucene.util.IOUtils;
import org.elasticsearch.ElasticsearchIllegalStateException;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;

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
public class NodeEnvironment extends AbstractComponent implements Closeable{

    /* ${data.paths}/nodes/{node.id} */
    private final Path[] nodePaths;
    /* ${data.paths}/nodes/{node.id}/indices */
    private final Path[] nodeIndicesPaths;
    private final Lock[] locks;

    private final int localNodeId;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final Map<ShardId, InternalShardLock> shardLocks = new HashMap<>();

    @Inject
    public NodeEnvironment(Settings settings, Environment environment) throws IOException {
        super(settings);

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
                Path dir = environment.dataWithClusterFiles()[dirIndex].resolve(Paths.get("nodes", Integer.toString(possibleLockId)));
                if (Files.exists(dir) == false) {
                    Files.createDirectories(dir);
                }
                
                try (Directory luceneDir = FSDirectory.open(dir, NativeFSLockFactory.INSTANCE)) {
                    logger.trace("obtaining node lock on {} ...", dir.toAbsolutePath());
                    Lock tmpLock = luceneDir.makeLock("node.lock");
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
            throw new ElasticsearchIllegalStateException("Failed to obtain node lock, is the following location writable?: " + Arrays.toString(environment.dataWithClusterFiles()), lastException);
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
                sb.append(" -> ").append(file.toAbsolutePath()).append(", free_space [").append(new ByteSizeValue(Files.getFileStore(file).getUnallocatedSpace())).append("], usable_space [").append(new ByteSizeValue(Files.getFileStore(file).getUsableSpace())).append("]\n");
            }
            logger.trace(sb.toString());
        }

        this.nodeIndicesPaths = new Path[nodePaths.length];
        for (int i = 0; i < nodePaths.length; i++) {
            nodeIndicesPaths[i] = nodePaths[i].resolve("indices");
        }
    }



    /**
     * Deletes a shard data directory iff the shards locks were successfully acquired.
     *
     * @param shardId the id of the shard to delete to delete
     * @throws IOException if an IOException occurs
     */
    public void deleteShardDirectorySafe(ShardId shardId) throws IOException {
        final Path[] paths = shardPaths(shardId);
        try (Closeable lock = shardLock(shardId)) {
            IOUtils.rm(paths);
        }
    }

    /**
     * Deletes an indexes data directory recursively iff all of the indexes
     * shards locks were successfully acquired. If any of the indexes shard directories can't be locked
     * non of the shards will be deleted
     *
     * @param index the index to delete
     * @param lockTimeoutMS how long to wait for acquiring the indices shard locks
     * @throws Exception if any of the shards data directories can't be locked or deleted
     */
    public void deleteIndexDirectorySafe(Index index, long lockTimeoutMS) throws IOException {
        final List<ShardLock> locks = lockAllForIndex(index, lockTimeoutMS);
        try {
            final Path[] indexPaths = new Path[nodeIndicesPaths.length];
            for (int i = 0; i < indexPaths.length; i++) {
                indexPaths[i] = nodeIndicesPaths[i].resolve(index.name());
            }
            IOUtils.rm(indexPaths);
        } finally {
            IOUtils.closeWhileHandlingException(locks);
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
    public List<ShardLock> lockAllForIndex(Index index, long lockTimeoutMS) throws IOException {
        Set<ShardId> allShardIds = findAllShardIds(index);
        List<ShardLock> allLocks = new ArrayList<>();
        boolean success = false;
        long startTime = System.currentTimeMillis();
        try {
            for (ShardId shardId : allShardIds) {
                long timeoutLeft = Math.max(0, lockTimeoutMS - (System.currentTimeMillis() - startTime));
                allLocks.add(shardLock(shardId, timeoutLeft));
            }
            success = true;
        } finally {
            if (success == false) {
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
        return new ShardLock(id) { // new instance prevents double closing
            @Override
            protected void closeInternal() {
                shardLock.release();
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
                if (--waitCount == 0) {
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
     * Returns all data paths for the given index.
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
     * Returns all data paths for the given shards ID
     */
    public Path[] shardPaths(ShardId shardId) {
        assert assertEnvIsLocked();
        final Path[] nodePaths = nodeDataPaths();
        final Path[] shardLocations = new Path[nodePaths.length];
        for (int i = 0; i < nodePaths.length; i++) {
            shardLocations[i] = nodePaths[i].resolve(Paths.get("indices", shardId.index().name(), Integer.toString(shardId.id())));
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
     * Tries to find all allocated shards for the given index or for all indices iff the given index is <code>null</code>
     * on the current node. NOTE: This methods is prone to race-conditions on the filesystem layer since it might not
     * see directories created concurrently or while it's traversing.
     * @param index the index to filter shards for or <code>null</code> if all shards for all indices should be listed
     * @return a set of shard IDs
     * @throws IOException if an IOException occurs
     */
    public Set<ShardId> findAllShardIds(@Nullable final Index index) throws IOException {
        if (nodePaths == null || locks == null) {
            throw new ElasticsearchIllegalStateException("node is not configured to store local location");
        }
        assert assertEnvIsLocked();
        return findAllShardIds(index == null ? null : index.getName(), nodeIndicesPaths);
    }

    private static Set<ShardId> findAllShardIds(@Nullable final String index, Path... locations) throws IOException {
        final Set<ShardId> shardIds = Sets.newHashSet();
        for (final Path location : locations) {
            if (Files.exists(location) && Files.isDirectory(location)) {
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
        if (Files.exists(indexPath) && Files.isDirectory(indexPath)) {
            try (DirectoryStream<Path> stream = Files.newDirectoryStream(indexPath)) {
                String currentIndex = indexPath.getFileName().toString();
                for (Path shardPath : stream) {
                    if (Files.exists(shardPath) && Files.isDirectory(shardPath)) {
                        Integer shardId = Ints.tryParse(shardPath.getFileName().toString());
                        if (shardId != null) {
                            shardIds.add(new ShardId(currentIndex, shardId));
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
                throw new ElasticsearchIllegalStateException("atomic_move is not supported by the filesystem on path [" + directory + "] atomic_move is required for elasticsearch to work correctly.", ex);
            } finally {
                Files.deleteIfExists(src);
                Files.deleteIfExists(target);
            }
        }
    }

    Settings getSettings() { // for testing
        return settings;
    }

}
