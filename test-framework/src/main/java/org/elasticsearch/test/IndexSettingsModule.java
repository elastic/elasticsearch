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
package org.elasticsearch.test;

import org.elasticsearch.Version;
import org.elasticsearch.cache.recycler.PageCacheRecycler;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.NodeServicesProvider;
import org.elasticsearch.indices.IndicesWarmer;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.indices.cache.query.IndicesQueryCache;
import org.elasticsearch.indices.fielddata.cache.IndicesFieldDataCache;
import org.elasticsearch.indices.fielddata.cache.IndicesFieldDataCacheListener;
import org.elasticsearch.indices.memory.IndexingMemoryController;
import org.elasticsearch.script.ScriptContextRegistry;
import org.elasticsearch.script.ScriptEngineService;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.mustache.MustacheScriptEngineService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.watcher.ResourceWatcherService;

import java.io.IOException;
import java.util.*;
import java.util.function.Consumer;

public class IndexSettingsModule extends AbstractModule {

    private final Index index;
    private final Settings settings;

    public IndexSettingsModule(Index index, Settings settings) {
        this.settings = settings;
        this.index = index;

    }
    @Override
    protected void configure() {
        bind(IndexSettings.class).toInstance(newIndexSettings(index, settings, Collections.EMPTY_LIST));
    }

    public static IndexSettings newIndexSettings(Index index, Settings settings, Collection<Consumer<Settings>> updateListeners) {
        Settings build = Settings.settingsBuilder().put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT)
                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 1)
                .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
                .put(settings)
                .build();
        IndexMetaData metaData = IndexMetaData.builder(index.getName()).settings(build).build();
        return new IndexSettings(metaData, Settings.EMPTY, Collections.EMPTY_LIST);
    }
}
