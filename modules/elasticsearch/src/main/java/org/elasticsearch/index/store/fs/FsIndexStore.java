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
import org.elasticsearch.common.collect.ImmutableMap;
import org.elasticsearch.common.collect.Lists;
import org.elasticsearch.common.collect.Maps;
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
import java.util.List;
import java.util.Map;

/**
 * @author kimchy (shay.banon)
 */
public abstract class FsIndexStore extends AbstractIndexStore {

    private final File location;

    public FsIndexStore(Index index, @IndexSettings Settings indexSettings, IndexService indexService, NodeEnvironment nodeEnv) {
        super(index, indexSettings, indexService);
        this.location = new File(new File(nodeEnv.nodeFile(), "indices"), index.name());

        if (!location.exists()) {
            for (int i = 0; i < 5; i++) {
                if (location.mkdirs()) {
                    break;
                }
            }
        }
    }

    @Override public boolean persistent() {
        return true;
    }

    @Override public ByteSizeValue backingStoreTotalSpace() {
        long totalSpace = location.getTotalSpace();
        if (totalSpace == 0) {
            totalSpace = -1;
        }
        return new ByteSizeValue(totalSpace);
    }

    @Override public ByteSizeValue backingStoreFreeSpace() {
        long usableSpace = location.getUsableSpace();
        if (usableSpace == 0) {
            usableSpace = -1;
        }
        return new ByteSizeValue(usableSpace);
    }

    @Override public void deleteUnallocated(ShardId shardId) throws IOException {
        if (indexService.hasShard(shardId.id())) {
            throw new ElasticSearchIllegalStateException(shardId + " allocated, can't be deleted");
        }
        FileSystemUtils.deleteRecursively(shardLocation(shardId));
    }

    @Override public StoreFilesMetaData[] listUnallocatedStores() throws IOException {
        File[] shardLocations = location.listFiles();
        if (shardLocations == null || shardLocations.length == 0) {
            return new StoreFilesMetaData[0];
        }
        List<StoreFilesMetaData> shards = Lists.newArrayList();
        for (File shardLocation : shardLocations) {
            int shardId = Integer.parseInt(shardLocation.getName());
            if (!indexService.hasShard(shardId)) {
                shards.add(listUnallocatedStoreMetaData(new ShardId(index, shardId)));
            }
        }
        return shards.toArray(new StoreFilesMetaData[shards.size()]);
    }

    @Override protected StoreFilesMetaData listUnallocatedStoreMetaData(ShardId shardId) throws IOException {
        File shardIndexLocation = shardIndexLocation(shardId);
        if (!shardIndexLocation.exists()) {
            return new StoreFilesMetaData(false, shardId, ImmutableMap.<String, StoreFileMetaData>of());
        }
        Map<String, StoreFileMetaData> files = Maps.newHashMap();
        for (File file : shardIndexLocation.listFiles()) {
            files.put(file.getName(), new StoreFileMetaData(file.getName(), file.length()));
        }
        return new StoreFilesMetaData(false, shardId, files);
    }

    public File location() {
        return location;
    }

    public File shardLocation(ShardId shardId) {
        return new File(location, Integer.toString(shardId.id()));
    }

    public File shardIndexLocation(ShardId shardId) {
        return new File(shardLocation(shardId), "index");
    }

    // not used currently, but here to state that this store also defined a file based translog location

    public File shardTranslogLocation(ShardId shardId) {
        return new File(shardLocation(shardId), "translog");
    }
}
