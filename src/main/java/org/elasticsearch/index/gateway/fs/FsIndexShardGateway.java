/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
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

package org.elasticsearch.index.gateway.fs;

import org.apache.lucene.store.Lock;
import org.apache.lucene.store.NativeFSLockFactory;
import org.elasticsearch.ElasticSearchIllegalStateException;
import org.elasticsearch.common.blobstore.fs.AbstractFsBlobContainer;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.gateway.IndexGateway;
import org.elasticsearch.index.gateway.blobstore.BlobStoreIndexShardGateway;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.service.IndexShard;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;

/**
 *
 */
public class FsIndexShardGateway extends BlobStoreIndexShardGateway {

    private final boolean snapshotLock;

    @Inject
    public FsIndexShardGateway(ShardId shardId, @IndexSettings Settings indexSettings, ThreadPool threadPool, IndexGateway fsIndexGateway,
                               IndexShard indexShard, Store store) {
        super(shardId, indexSettings, threadPool, fsIndexGateway, indexShard, store);
        this.snapshotLock = indexSettings.getAsBoolean("gateway.fs.snapshot_lock", true);
    }

    @Override
    public String type() {
        return "fs";
    }

    @Override
    public SnapshotLock obtainSnapshotLock() throws Exception {
        if (!snapshotLock) {
            return NO_SNAPSHOT_LOCK;
        }
        AbstractFsBlobContainer fsBlobContainer = (AbstractFsBlobContainer) blobContainer;
        NativeFSLockFactory lockFactory = new NativeFSLockFactory(fsBlobContainer.filePath());

        Lock lock = lockFactory.makeLock("snapshot.lock");
        boolean obtained = lock.obtain();
        if (!obtained) {
            throw new ElasticSearchIllegalStateException("failed to obtain snapshot lock [" + lock + "]");
        }
        return new FsSnapshotLock(lock);
    }

    public class FsSnapshotLock implements SnapshotLock {
        private final Lock lock;

        public FsSnapshotLock(Lock lock) {
            this.lock = lock;
        }

        @Override
        public void release() {
            try {
                lock.release();
            } catch (IOException e) {
                logger.warn("failed to release snapshot lock [{}]", e, lock);
            }
        }
    }
}
