/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import org.apache.lucene.store.Lock;
import org.apache.lucene.store.NativeFSLockFactory;
import org.elasticsearch.ElasticSearchIllegalStateException;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.FileSystemUtils;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;

import java.io.File;
import java.io.IOException;

/**
 * @author kimchy (shay.banon)
 */
public class NodeEnvironment extends AbstractComponent {

    private final File[] nodeFiles;
    private final File[] nodeIndicesLocations;

    private final Lock[] locks;

    private final int localNodeId;

    @Inject public NodeEnvironment(Settings settings, Environment environment) throws IOException {
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
                    NativeFSLockFactory lockFactory = new NativeFSLockFactory(dir);
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
                                try {
                                    locks[i].release();
                                } catch (Exception e1) {
                                    // ignore
                                }
                            }
                            locks[i] = null;
                        }
                        break;
                    }
                } catch (IOException e) {
                    logger.trace("failed to obtain node lock on {}", e, dir.getAbsolutePath());
                    lastException = e;
                    // release all the ones that were obtained up until now
                    for (int i = 0; i < locks.length; i++) {
                        if (locks[i] != null) {
                            try {
                                locks[i].release();
                            } catch (Exception e1) {
                                // ignore
                            }
                        }
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
            throw new IOException("Failed to obtain node lock", lastException);
        }

        this.localNodeId = localNodeId;
        this.locks = locks;
        this.nodeFiles = nodesFiles;
        if (logger.isDebugEnabled()) {
            logger.debug("using node location [{}], local_node_id [{}]", nodesFiles, localNodeId);
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
        if (nodeFiles == null || locks == null) {
            throw new ElasticSearchIllegalStateException("node is not configured to store local location");
        }
        return nodeFiles;
    }

    public File[] indicesLocations() {
        return nodeIndicesLocations;
    }

    public File[] indexLocations(Index index) {
        File[] indexLocations = new File[nodeFiles.length];
        for (int i = 0; i < nodeFiles.length; i++) {
            indexLocations[i] = new File(new File(nodeFiles[i], "indices"), index.name());
        }
        return indexLocations;
    }

    public File[] shardLocations(ShardId shardId) {
        File[] shardLocations = new File[nodeFiles.length];
        for (int i = 0; i < nodeFiles.length; i++) {
            shardLocations[i] = new File(new File(new File(nodeFiles[i], "indices"), shardId.index().name()), Integer.toString(shardId.id()));
        }
        return shardLocations;
    }

    public void close() {
        if (locks != null) {
            for (Lock lock : locks) {
                try {
                    lock.release();
                } catch (IOException e) {
                    // ignore
                }
            }
        }
    }
}
