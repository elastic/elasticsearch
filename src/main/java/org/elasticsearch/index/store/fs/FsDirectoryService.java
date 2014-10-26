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

package org.elasticsearch.index.store.fs;

import org.apache.lucene.store.*;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.common.io.FileSystemUtils;
import org.elasticsearch.common.metrics.CounterMetric;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.index.shard.AbstractIndexShardComponent;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.DirectoryService;
import org.elasticsearch.index.store.DirectoryUtils;
import org.elasticsearch.index.store.IndexStore;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InterruptedIOException;

/**
 */
public abstract class FsDirectoryService extends AbstractIndexShardComponent implements DirectoryService, StoreRateLimiting.Listener, StoreRateLimiting.Provider {

    protected final FsIndexStore indexStore;

    private final CounterMetric rateLimitingTimeInNanos = new CounterMetric();

    public FsDirectoryService(ShardId shardId, @IndexSettings Settings indexSettings, IndexStore indexStore) {
        super(shardId, indexSettings);
        this.indexStore = (FsIndexStore) indexStore;
    }

    @Override
    public long throttleTimeInNanos() {
        return rateLimitingTimeInNanos.count();
    }

    @Override
    public StoreRateLimiting rateLimiting() {
        return indexStore.rateLimiting();
    }

    protected final LockFactory buildLockFactory() throws IOException {
        String fsLock = componentSettings.get("lock", componentSettings.get("fs_lock", "native"));
        LockFactory lockFactory = NoLockFactory.getNoLockFactory();
        if (fsLock.equals("native")) {
            lockFactory = new NativeFSLockFactory();
        } else if (fsLock.equals("simple")) {
            lockFactory = new SimpleFSLockFactory();
        } else if (fsLock.equals("none")) {
            lockFactory = NoLockFactory.getNoLockFactory();
        }
        return lockFactory;
    }
    
    @Override
    public Directory[] build() throws IOException {
        File[] locations = indexStore.shardIndexLocations(shardId);
        Directory[] dirs = new Directory[locations.length];
        for (int i = 0; i < dirs.length; i++) {
            FileSystemUtils.mkdirs(locations[i]);
            Directory wrapped = newFSDirectory(locations[i], buildLockFactory());
            dirs[i] = new RateLimitedFSDirectory(wrapped, this, this) ;
        }
        return dirs;
    }
    
    protected abstract Directory newFSDirectory(File location, LockFactory lockFactory) throws IOException;

    @Override
    public void onPause(long nanos) {
        rateLimitingTimeInNanos.inc(nanos);
    }
}
