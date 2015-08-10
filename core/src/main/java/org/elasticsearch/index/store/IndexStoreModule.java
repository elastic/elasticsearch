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
import org.elasticsearch.common.inject.*;
import org.elasticsearch.common.settings.Settings;

import java.util.Locale;

/**
 *
 */
public class IndexStoreModule extends AbstractModule implements SpawnModules {

    public static final String STORE_TYPE = "index.store.type";

    private final Settings settings;

    public enum Type {
        NIOFS,
        MMAPFS,
        SIMPLEFS,
        FS,
        DEFAULT;

        public String getSettingsKey() {
            return this.name().toLowerCase(Locale.ROOT);
        }
        /**
         * Returns true iff this settings matches the type.
         */
        public boolean match(String setting) {
            return getSettingsKey().equals(setting);
        }
    }

    public IndexStoreModule(Settings settings) {
        this.settings = settings;
    }

    @Override
    public Iterable<? extends Module> spawnModules() {
        final String storeType = settings.get(STORE_TYPE, Type.DEFAULT.getSettingsKey());
        for (Type type : Type.values()) {
            if (type.match(storeType)) {
                return ImmutableList.of(new DefaultStoreModule());
            }
        }
        final Class<? extends Module> indexStoreModule = settings.getAsClass(STORE_TYPE, null, "org.elasticsearch.index.store.", "IndexStoreModule");
        return ImmutableList.of(Modules.createModule(indexStoreModule, settings));
    }

    @Override
    protected void configure() {}

    private static class DefaultStoreModule extends AbstractModule {
        @Override
        protected void configure() {
            bind(IndexStore.class).asEagerSingleton();
        }
    }
}