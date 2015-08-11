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

import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.settings.Settings;

import java.util.HashMap;
import java.util.Map;
import java.util.Locale;

/**
 *
 */
public class IndexStoreModule extends AbstractModule {

    public static final String STORE_TYPE = "index.store.type";

    private final Settings settings;
    private final Map<String, Class<? extends IndexStore>> storeTypes = new HashMap<>();

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

    public void addIndexStore(String type, Class<? extends IndexStore> clazz) {
        storeTypes.put(type, clazz);
    }

    private static boolean isBuiltinType(String storeType) {
        for (Type type : Type.values()) {
            if (type.match(storeType)) {
                return true;
            }
        }
        return false;
    }

    @Override
    protected void configure() {
        final String storeType = settings.get(STORE_TYPE);
        if (storeType == null || isBuiltinType(storeType)) {
            bind(IndexStore.class).asEagerSingleton();
        } else {
            Class<? extends IndexStore> clazz = storeTypes.get(storeType);
            if (clazz == null) {
                throw new IllegalArgumentException("Unknown store type [" + storeType + "]");
            }
            bind(IndexStore.class).to(clazz).asEagerSingleton();
        }
    }
}