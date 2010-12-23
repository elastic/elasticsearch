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

import org.elasticsearch.ElasticSearchIllegalStateException;
import org.elasticsearch.common.io.FileSystemUtils;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.service.IndexService;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.support.AbstractIndexStore;

import java.io.File;
import java.io.IOException;

/**
 * @author kimchy (shay.banon)
 */
public abstract class FsIndexStore extends AbstractIndexStore {

    private final NodeEnvironment nodeEnv;

    private final File location;

    public FsIndexStore(Index index, @IndexSettings Settings indexSettings, IndexService indexService, NodeEnvironment nodeEnv) {
        super(index, indexSettings, indexService);
        this.nodeEnv = nodeEnv;
        if (nodeEnv.hasNodeFile()) {
            this.location = nodeEnv.indexLocation(index);
        } else {
            this.location = null;
        }
    }

    @Override public boolean persistent() {
        return true;
    }

    @Override public ByteSizeValue backingStoreTotalSpace() {
        if (location == null) {
            return new ByteSizeValue(0);
        }
        long totalSpace = location.getTotalSpace();
        if (totalSpace == 0) {
            totalSpace = 0;
        }
        return new ByteSizeValue(totalSpace);
    }

    @Override public ByteSizeValue backingStoreFreeSpace() {
        if (location == null) {
            return new ByteSizeValue(0);
        }
        long usableSpace = location.getUsableSpace();
        if (usableSpace == 0) {
            usableSpace = 0;
        }
        return new ByteSizeValue(usableSpace);
    }

    @Override public boolean canDeleteUnallocated(ShardId shardId) {
        if (location == null) {
            return false;
        }
        if (indexService.hasShard(shardId.id())) {
            return false;
        }
        return shardLocation(shardId).exists();
    }

    @Override public void deleteUnallocated(ShardId shardId) throws IOException {
        if (location == null) {
            return;
        }
        if (indexService.hasShard(shardId.id())) {
            throw new ElasticSearchIllegalStateException(shardId + " allocated, can't be deleted");
        }
        FileSystemUtils.deleteRecursively(shardLocation(shardId));
    }

    public File shardLocation(ShardId shardId) {
        return nodeEnv.shardLocation(shardId);
    }

    public File shardIndexLocation(ShardId shardId) {
        return new File(shardLocation(shardId), "index");
    }

    // not used currently, but here to state that this store also defined a file based translog location

    public File shardTranslogLocation(ShardId shardId) {
        return new File(shardLocation(shardId), "translog");
    }
}
