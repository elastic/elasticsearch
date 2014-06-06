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

import com.google.common.collect.Sets;
import com.google.common.primitives.Ints;
import org.apache.lucene.store.Lock;
import org.apache.lucene.store.XNativeFSLockFactory;
import org.apache.lucene.util.IOUtils;
import org.elasticsearch.ElasticsearchIllegalStateException;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.FileSystemUtils;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 *
 */
public class NodeEnvironment extends AbstractComponent {

    private final File[] nodeFiles;
    private final File[] nodeIndicesLocations;

    private final Lock[] locks;

    private final int localNodeId;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    @Inject
    public NodeEnvironment(Settings settings, Environment environment) {
        super(settings);

        if (!DiscoveryNode.nodeRequiresLocalStorage(settings)) {
            nodeFiles = null;
            nodeIndicesLocations = null;
            locks = null;
            localNodeId = -1;
            return;
        }

        File[] nodesFiles = new File[environment.dataWithClusterFiles().length];
        Lock[] locks = new Lock[environment.dataWithClusterFiles().length];
        int localNodeId = -1;
        IOException lastException = null;
        int maxLocalStorageNodes = settings.getAsInt("node.max_local_storage_nodes", 50);
        for (int possibleLockId = 0; possibleLockId < maxLocalStorageNodes; possibleLockId++) {
            for (int dirIndex = 0; dirIndex < environment.dataWithClusterFiles().length; dirIndex++) {
                File dir = new File(new File(environment.dataWithClusterFiles()[dirIndex], "nodes"), Integer.toString(possibleLockId));
                if (!dir.exists()) {
                    FileSystemUtils.mkdirs(dir);
                }
                logger.trace("obtaining node lock on {} ...", dir.getAbsolutePath());
                try {
                    XNativeFSLockFactory lockFactory = new XNativeFSLockFactory(dir);
                    Lock tmpLock = lockFactory.makeLock("node.lock");
                    boolean obtained = tmpLock.obtain();
                    if (obtained) {
                        locks[dirIndex] = tmpLock;
                        nodesFiles[dirIndex] = dir;
                        localNodeId = possibleLockId;
                    } else {
                        logger.trace("failed to obtain node lock on {}", dir.getAbsolutePath());
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
                    logger.trace("failed to obtain node lock on {}", e, dir.getAbsolutePath());
                    lastException = new IOException("failed to obtain lock on " + dir.getAbsolutePath(), e);
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
        this.nodeFiles = nodesFiles;
        if (logger.isDebugEnabled()) {
            logger.debug("using node location [{}], local_node_id [{}]", nodesFiles, localNodeId);
        }
        if (logger.isTraceEnabled()) {
            StringBuilder sb = new StringBuilder("node data locations details:\n");
            for (File file : nodesFiles) {
                sb.append(" -> ").append(file.getAbsolutePath()).append(", free_space [").append(new ByteSizeValue(file.getFreeSpace())).append("], usable_space [").append(new ByteSizeValue(file.getUsableSpace())).append("]\n");
            }
            logger.trace(sb.toString());
        }

        this.nodeIndicesLocations = new File[nodeFiles.length];
        for (int i = 0; i < nodeFiles.length; i++) {
            nodeIndicesLocations[i] = new File(nodeFiles[i], "indices");
        }
    }

    public int localNodeId() {
        return this.localNodeId;
    }

    public boolean hasNodeFile() {
        return nodeFiles != null && locks != null;
    }

    public File[] nodeDataLocations() {
        assert assertEnvIsLocked();
        if (nodeFiles == null || locks == null) {
            throw new ElasticsearchIllegalStateException("node is not configured to store local location");
        }
        return nodeFiles;
    }

    public File[] indicesLocations() {
        assert assertEnvIsLocked();
        return nodeIndicesLocations;
    }

    public File[] indexLocations(Index index) {
        assert assertEnvIsLocked();
        File[] indexLocations = new File[nodeFiles.length];
        for (int i = 0; i < nodeFiles.length; i++) {
            indexLocations[i] = new File(new File(nodeFiles[i], "indices"), index.name());
        }
        return indexLocations;
    }

    public File[] shardLocations(ShardId shardId) {
        assert assertEnvIsLocked();
        File[] shardLocations = new File[nodeFiles.length];
        for (int i = 0; i < nodeFiles.length; i++) {
            shardLocations[i] = new File(new File(new File(nodeFiles[i], "indices"), shardId.index().name()), Integer.toString(shardId.id()));
        }
        return shardLocations;
    }

    public Set<String> findAllIndices() throws Exception {
        if (nodeFiles == null || locks == null) {
            throw new ElasticsearchIllegalStateException("node is not configured to store local location");
        }
        assert assertEnvIsLocked();
        Set<String> indices = Sets.newHashSet();
        for (File indicesLocation : nodeIndicesLocations) {
            File[] indicesList = indicesLocation.listFiles();
            if (indicesList == null) {
                continue;
            }
            for (File indexLocation : indicesList) {
                if (indexLocation.isDirectory()) {
                    indices.add(indexLocation.getName());
                }
            }
        }
        return indices;
    }

    public Set<ShardId> findAllShardIds() throws Exception {
        if (nodeFiles == null || locks == null) {
            throw new ElasticsearchIllegalStateException("node is not configured to store local location");
        }
        assert assertEnvIsLocked();
        Set<ShardId> shardIds = Sets.newHashSet();
        for (File indicesLocation : nodeIndicesLocations) {
            File[] indicesList = indicesLocation.listFiles();
            if (indicesList == null) {
                continue;
            }
            for (File indexLocation : indicesList) {
                if (!indexLocation.isDirectory()) {
                    continue;
                }
                String indexName = indexLocation.getName();
                File[] shardsList = indexLocation.listFiles();
                if (shardsList == null) {
                    continue;
                }
                for (File shardLocation : shardsList) {
                    if (!shardLocation.isDirectory()) {
                        continue;
                    }
                    Integer shardId = Ints.tryParse(shardLocation.getName());
                    if (shardId != null) {
                        shardIds.add(new ShardId(indexName, shardId));
                    }
                }
            }
        }
        return shardIds;
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
}
