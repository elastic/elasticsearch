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
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;

import java.io.File;
import java.io.IOException;

/**
 * @author kimchy (shay.banon)
 */
public class NodeEnvironment extends AbstractComponent {

    private final File nodeFile;

    private final Lock lock;

    private final int localNodeId;

    @Inject public NodeEnvironment(Settings settings, Environment environment) throws IOException {
        super(settings);

        if (!DiscoveryNode.nodeRequiresLocalStorage(settings)) {
            nodeFile = null;
            lock = null;
            localNodeId = -1;
            return;
        }

        Lock lock = null;
        File dir = null;
        int localNodeId = -1;
        IOException lastException = null;
        for (int i = 0; i < 50; i++) {
            dir = new File(new File(environment.dataWithClusterFile(), "nodes"), Integer.toString(i));
            if (!dir.exists()) {
                dir.mkdirs();
            }
            logger.trace("obtaining node lock on {} ...", dir.getAbsolutePath());
            try {
                NativeFSLockFactory lockFactory = new NativeFSLockFactory(dir);
                Lock tmpLock = lockFactory.makeLock("node.lock");
                boolean obtained = tmpLock.obtain();
                if (obtained) {
                    lock = tmpLock;
                    localNodeId = i;
                    break;
                } else {
                    logger.trace("failed to obtain node lock on {}", dir.getAbsolutePath());
                }
            } catch (IOException e) {
                logger.trace("failed to obtain node lock on {}", e, dir.getAbsolutePath());
                lastException = e;
            }
        }
        if (lock == null) {
            throw new IOException("Failed to obtain node lock", lastException);
        }
        this.localNodeId = localNodeId;
        this.lock = lock;
        this.nodeFile = dir;
        if (logger.isDebugEnabled()) {
            logger.debug("using node location [{}], local_node_id [{}]", dir, localNodeId);
        }
    }

    public int localNodeId() {
        return this.localNodeId;
    }

    public boolean hasNodeFile() {
        return nodeFile != null && lock != null;
    }

    public File nodeDataLocation() {
        if (nodeFile == null || lock == null) {
            throw new ElasticSearchIllegalStateException("node is not configured to store local location");
        }
        return nodeFile;
    }

    public File indicesLocation() {
        return new File(nodeDataLocation(), "indices");
    }

    public File indexLocation(Index index) {
        return new File(indicesLocation(), index.name());
    }

    public File shardLocation(ShardId shardId) {
        return new File(indexLocation(shardId.index()), Integer.toString(shardId.id()));
    }

    public void close() {
        if (lock != null) {
            try {
                lock.release();
            } catch (IOException e) {
                // ignore
            }
        }
    }
}
