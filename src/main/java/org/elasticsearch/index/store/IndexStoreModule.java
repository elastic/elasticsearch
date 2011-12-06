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

package org.elasticsearch.index.store;

import com.google.common.collect.ImmutableList;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.util.Constants;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.inject.Modules;
import org.elasticsearch.common.inject.SpawnModules;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.store.fs.MmapFsIndexStoreModule;
import org.elasticsearch.index.store.fs.NioFsIndexStoreModule;
import org.elasticsearch.index.store.fs.SimpleFsIndexStoreModule;
import org.elasticsearch.index.store.memory.MemoryIndexStoreModule;
import org.elasticsearch.index.store.ram.RamIndexStoreModule;

/**
 *
 */
public class IndexStoreModule extends AbstractModule implements SpawnModules {

    private final Settings settings;

    public IndexStoreModule(Settings settings) {
        this.settings = settings;
    }

    @Override
    public Iterable<? extends Module> spawnModules() {
        Class<? extends Module> indexStoreModule = NioFsIndexStoreModule.class;
        // Same logic as FSDirectory#open ...
        if ((Constants.WINDOWS || Constants.SUN_OS)
                && Constants.JRE_IS_64BIT && MMapDirectory.UNMAP_SUPPORTED) {
            indexStoreModule = MmapFsIndexStoreModule.class;
        } else if (Constants.WINDOWS) {
            indexStoreModule = SimpleFsIndexStoreModule.class;
        }
        String storeType = settings.get("index.store.type");
        if ("ram".equalsIgnoreCase(storeType)) {
            indexStoreModule = RamIndexStoreModule.class;
        } else if ("memory".equalsIgnoreCase(storeType)) {
            indexStoreModule = MemoryIndexStoreModule.class;
        } else if ("fs".equalsIgnoreCase(storeType)) {
            // nothing to set here ... (we default to fs)
        } else if ("simplefs".equalsIgnoreCase(storeType) || "simple_fs".equals(storeType)) {
            indexStoreModule = SimpleFsIndexStoreModule.class;
        } else if ("niofs".equalsIgnoreCase(storeType) || "nio_fs".equalsIgnoreCase(storeType)) {
            indexStoreModule = NioFsIndexStoreModule.class;
        } else if ("mmapfs".equalsIgnoreCase(storeType) || "mmap_fs".equalsIgnoreCase(storeType)) {
            indexStoreModule = MmapFsIndexStoreModule.class;
        } else if (storeType != null) {
            indexStoreModule = settings.getAsClass("index.store.type", indexStoreModule, "org.elasticsearch.index.store.", "IndexStoreModule");
        }
        return ImmutableList.of(Modules.createModule(indexStoreModule, settings));
    }

    @Override
    protected void configure() {
    }
}