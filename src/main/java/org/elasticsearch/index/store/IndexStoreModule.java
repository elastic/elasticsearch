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

package org.elasticsearch.index.store;

import com.google.common.collect.ImmutableList;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.util.Constants;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.inject.Modules;
import org.elasticsearch.common.inject.SpawnModules;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.store.fs.DefaultFsIndexStoreModule;
import org.elasticsearch.index.store.fs.MmapFsIndexStoreModule;
import org.elasticsearch.index.store.fs.NioFsIndexStoreModule;
import org.elasticsearch.index.store.fs.SimpleFsIndexStoreModule;
import org.elasticsearch.index.store.ram.RamIndexStoreModule;

/**
 *
 */
public class IndexStoreModule extends AbstractModule implements SpawnModules {

    private final Settings settings;

    public static enum Type {
        NIOFS {
            public boolean match(String setting) {
                return super.match(setting) || "nio_fs".equalsIgnoreCase(setting);
            }
        },
        MMAPFS {
            public boolean match(String setting) {
                return super.match(setting) || "mmap_fs".equalsIgnoreCase(setting);
            }
        },

        SIMPLEFS {
            public boolean match(String setting) {
                return super.match(setting) || "simple_fs".equalsIgnoreCase(setting);
            }
        },
        RAM {
            public  boolean fsStore() {
                return false;
            }
        },
        MEMORY {
            public  boolean fsStore() {
                return false;
            }
        },
        FS,
        DEFAULT,;

        /**
         * Returns true iff this store type is a filesystem based store.
         */
        public  boolean fsStore() {
            return true;
        }

        /**
         * Returns true iff this settings matches the type.
         */
        public boolean match(String setting) {
            return this.name().equalsIgnoreCase(setting);
        }
    }

    public IndexStoreModule(Settings settings) {
        this.settings = settings;
    }

    @Override
    public Iterable<? extends Module> spawnModules() {
        Class<? extends Module> indexStoreModule = NioFsIndexStoreModule.class;
        if ((Constants.WINDOWS || Constants.SUN_OS || Constants.LINUX)
                && Constants.JRE_IS_64BIT && MMapDirectory.UNMAP_SUPPORTED) {
            if (Constants.WINDOWS) {
                indexStoreModule = MmapFsIndexStoreModule.class;
            } else {
                // on linux and friends we only mmap dedicated files
                indexStoreModule = DefaultFsIndexStoreModule.class;
            }
        } else if (Constants.WINDOWS) {
            indexStoreModule = SimpleFsIndexStoreModule.class;
        }
        String storeType = settings.get("index.store.type");
        if (Type.RAM.name().equalsIgnoreCase(storeType)) {
            indexStoreModule = RamIndexStoreModule.class;
        } else if (Type.MEMORY.match(storeType)) {
            indexStoreModule = RamIndexStoreModule.class;
        } else if (Type.FS.match(storeType)) {
            // nothing to set here ... (we default to fs)
        } else if (Type.SIMPLEFS.match(storeType)) {
            indexStoreModule = SimpleFsIndexStoreModule.class;
        } else if (Type.NIOFS.match(storeType)) {
            indexStoreModule = NioFsIndexStoreModule.class;
        } else if (Type.MMAPFS.match(storeType)) {
            indexStoreModule = MmapFsIndexStoreModule.class;
        } else if (Type.DEFAULT.match(storeType)) {
            indexStoreModule = DefaultFsIndexStoreModule.class;
        } else if (storeType != null) {
            indexStoreModule = settings.getAsClass("index.store.type", indexStoreModule, "org.elasticsearch.index.store.", "IndexStoreModule");
        }
        return ImmutableList.of(Modules.createModule(indexStoreModule, settings));
    }

    @Override
    protected void configure() {
    }
}