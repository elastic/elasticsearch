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
import org.apache.lucene.store.NIOFSDirectory;
import org.elasticsearch.cache.memory.ByteBufferCache;
import org.elasticsearch.common.collect.Tuple;
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
public class NioFsStore extends FsStore {

    private final NIOFSDirectory fsDirectory;

    private final Directory directory;

    private final boolean suggestUseCompoundFile;

    @Inject public NioFsStore(ShardId shardId, @IndexSettings Settings indexSettings, IndexStore indexStore, ByteBufferCache byteBufferCache) throws IOException {
        super(shardId, indexSettings, indexStore);
        LockFactory lockFactory = buildLockFactory();
        File location = ((FsIndexStore) indexStore).shardIndexLocation(shardId);
        location.mkdirs();
        this.fsDirectory = new NIOFSDirectory(location, lockFactory);

        boolean suggestUseCompoundFile;
        Tuple<SwitchDirectory, Boolean> switchDirectory = buildSwitchDirectoryIfNeeded(fsDirectory, byteBufferCache);
        if (switchDirectory != null) {
            suggestUseCompoundFile = DEFAULT_SUGGEST_USE_COMPOUND_FILE;
            if (switchDirectory.v2() != null) {
                suggestUseCompoundFile = switchDirectory.v2();
            }
            logger.debug("using [nio_fs] store with path [{}], cache [true] with extensions [{}]", fsDirectory.getDirectory(), switchDirectory.v1().primaryExtensions());
            directory = wrapDirectory(switchDirectory.v1());
        } else {
            suggestUseCompoundFile = DEFAULT_SUGGEST_USE_COMPOUND_FILE;
            directory = wrapDirectory(fsDirectory);
            logger.debug("using [nio_fs] store with path [{}]", fsDirectory.getDirectory());
        }
        this.suggestUseCompoundFile = suggestUseCompoundFile;
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
