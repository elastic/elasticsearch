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

package org.elasticsearch.index.store.fs;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.LockFactory;
import org.apache.lucene.store.SimpleFSDirectory;
import org.elasticsearch.cache.memory.ByteBufferCache;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.lucene.store.SwitchDirectory;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.IndexStore;

import java.io.File;
import java.io.IOException;

/**
 * @author kimchy (shay.banon)
 */
public class SimpleFsStore extends FsStore {

    private SimpleFSDirectory fsDirectory;

    private final Directory directory;

    private final boolean suggestUseCompoundFile;

    @Inject public SimpleFsStore(ShardId shardId, @IndexSettings Settings indexSettings, IndexStore indexStore, ByteBufferCache byteBufferCache) throws IOException {
        super(shardId, indexSettings);
        LockFactory lockFactory = buildLockFactory();
        File location = ((FsIndexStore) indexStore).shardIndexLocation(shardId);
        location.mkdirs();
        this.fsDirectory = new SimpleFSDirectory(location, lockFactory);

        SwitchDirectory switchDirectory = buildSwitchDirectoryIfNeeded(fsDirectory, byteBufferCache);
        if (switchDirectory != null) {
            suggestUseCompoundFile = false;
            logger.debug("Using [simple_fs] Store with path [{}], cache [true] with extensions [{}]", fsDirectory.getFile(), switchDirectory.primaryExtensions());
            directory = wrapDirectory(switchDirectory);
        } else {
            suggestUseCompoundFile = true;
            directory = wrapDirectory(fsDirectory);
            logger.debug("Using [simple_fs] Store with path [{}]", fsDirectory.getFile());
        }
    }

    @Override public FSDirectory fsDirectory() {
        return fsDirectory;
    }

    @Override public Directory directory() {
        return directory;
    }

    @Override public boolean suggestUseCompoundFile() {
        return suggestUseCompoundFile;
    }
}
