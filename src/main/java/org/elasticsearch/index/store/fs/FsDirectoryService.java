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

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.apache.lucene.store.*;
import org.elasticsearch.common.io.FileSystemUtils;
import org.elasticsearch.common.metrics.CounterMetric;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.DirectoryService;
import org.elasticsearch.index.store.IndexStore;
import org.elasticsearch.index.store.StoreException;

/**
 */
public abstract class FsDirectoryService extends DirectoryService {

    protected final IndexStore indexStore;

    public FsDirectoryService(ShardId shardId, @IndexSettings Settings indexSettings, IndexStore indexStore) {
        super(shardId, indexSettings);
        this.indexStore = indexStore;
    }

    protected final LockFactory buildLockFactory() throws IOException {
        String fsLock = componentSettings.get("lock", componentSettings.get("fs_lock", "native"));
        LockFactory lockFactory;
        if (fsLock.equals("native")) {
            lockFactory = NativeFSLockFactory.INSTANCE;
        } else if (fsLock.equals("simple")) {
            lockFactory = SimpleFSLockFactory.INSTANCE;
        } else {
            throw new StoreException(shardId, "unrecognized fs_lock \"" + fsLock + "\": must be native or simple");
        }
        return lockFactory;
    }

    
    @Override
    public Directory[] build() throws IOException {
        Path[] locations = indexStore.shardIndexLocations(shardId);
        Directory[] dirs = new Directory[locations.length];
        for (int i = 0; i < dirs.length; i++) {
            Files.createDirectories(locations[i]);
            dirs[i] = newFSDirectory(locations[i], buildLockFactory());
        }
        return dirs;
    }
    
    protected abstract Directory newFSDirectory(Path location, LockFactory lockFactory) throws IOException;
}
