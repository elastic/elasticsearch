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

import com.google.inject.Inject;
import org.apache.lucene.store.NIOFSDirectory;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.LocalNodeId;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.util.settings.Settings;

import java.io.File;
import java.io.IOException;

import static org.elasticsearch.index.store.fs.FsStores.*;

/**
 * @author kimchy (Shay Banon)
 */
public class NioFsStore extends AbstractFsStore<NIOFSDirectory> {

    private final boolean syncToDisk;

    private final NIOFSDirectory directory;

    @Inject public NioFsStore(ShardId shardId, @IndexSettings Settings indexSettings, Environment environment, @LocalNodeId String localNodeId) throws IOException {
        super(shardId, indexSettings);
        // by default, we don't need to sync to disk, since we use the gateway
        this.syncToDisk = componentSettings.getAsBoolean("syncToDisk", false);
        this.directory = new CustomNioFSDirectory(createStoreFilePath(environment.workWithClusterFile(), localNodeId, shardId), syncToDisk);
        logger.debug("Using [NioFs] Store with path [{}], syncToDisk [{}]", directory.getFile(), syncToDisk);
    }

    @Override public NIOFSDirectory directory() {
        return directory;
    }

    private static class CustomNioFSDirectory extends NIOFSDirectory {

        private final boolean syncToDisk;

        private CustomNioFSDirectory(File path, boolean syncToDisk) throws IOException {
            super(path);
            this.syncToDisk = syncToDisk;
        }

        @Override public void sync(String name) throws IOException {
            if (!syncToDisk) {
                return;
            }
            super.sync(name);
        }
    }
}
