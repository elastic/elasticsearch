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

package org.elasticsearch.index.store;

import com.google.inject.AbstractModule;
import com.google.inject.Module;
import org.elasticsearch.index.store.bytebuffer.ByteBufferStoreModule;
import org.elasticsearch.index.store.fs.MmapFsStoreModule;
import org.elasticsearch.index.store.fs.NioFsStoreModule;
import org.elasticsearch.index.store.fs.SimpleFsStoreModule;
import org.elasticsearch.index.store.memory.MemoryStoreModule;
import org.elasticsearch.index.store.ram.RamStoreModule;
import org.elasticsearch.util.OsUtils;
import org.elasticsearch.util.guice.ModulesFactory;
import org.elasticsearch.util.settings.Settings;

/**
 * @author kimchy (Shay Banon)
 */
public class StoreModule extends AbstractModule {

    private final Settings settings;

    public StoreModule(Settings settings) {
        this.settings = settings;
    }

    @Override protected void configure() {
        Class<? extends Module> storeModule = NioFsStoreModule.class;
        if (OsUtils.WINDOWS) {
            storeModule = SimpleFsStoreModule.class;
        }
        String storeType = settings.get("index.store.type");
        if ("ram".equalsIgnoreCase(storeType)) {
            storeModule = RamStoreModule.class;
        } else if ("memory".equalsIgnoreCase(storeType)) {
            storeModule = MemoryStoreModule.class;
        } else if ("bytebuffer".equalsIgnoreCase(storeType)) {
            storeModule = ByteBufferStoreModule.class;
        } else if ("fs".equalsIgnoreCase(storeType)) {
            // nothing to set here ... (we default to fs)
        } else if ("simplefs".equalsIgnoreCase(storeType)) {
            storeModule = SimpleFsStoreModule.class;
        } else if ("niofs".equalsIgnoreCase(storeType)) {
            storeModule = NioFsStoreModule.class;
        } else if ("mmapfs".equalsIgnoreCase(storeType)) {
            storeModule = MmapFsStoreModule.class;
        } else if (storeType != null) {
            storeModule = settings.getAsClass("index.store.type", storeModule, "org.elasticsearch.index.store.", "StoreModule");
        }
        ModulesFactory.createModule(storeModule, settings).configure(binder());
        bind(StoreManagement.class).asEagerSingleton();
    }
}
