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

package org.elasticsearch.index;

import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.util.Providers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.engine.EngineFactory;
import org.elasticsearch.index.engine.InternalEngineFactory;
import org.elasticsearch.index.fielddata.IndexFieldDataService;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.shard.IndexEventListener;
import org.elasticsearch.index.shard.IndexSearcherWrapper;

import java.util.HashSet;
import java.util.Set;
import java.util.function.Consumer;

/**
 *
 */
public class IndexModule extends AbstractModule {

    private final IndexSettings indexSettings;
    // pkg private so tests can mock
    Class<? extends EngineFactory> engineFactoryImpl = InternalEngineFactory.class;
    Class<? extends IndexSearcherWrapper> indexSearcherWrapper = null;
    private final Set<Consumer<Settings>> settingsConsumers = new HashSet<>();
    private final Set<IndexEventListener> indexEventListeners = new HashSet<>();
    private IndexEventListener listener;


    public IndexModule(IndexSettings indexSettings) {
        this.indexSettings = indexSettings;
    }

    /**
     * Adds a settings consumer for this index
     */
    public void addIndexSettingsListener(Consumer<Settings> listener) {
        if (listener == null) {
            throw new IllegalArgumentException("listener must not be null");
        }

        if (settingsConsumers.contains(listener)) {
            throw new IllegalStateException("listener already registered");
        }
        settingsConsumers.add(listener);
    }

    /**
     * Returns the index {@link Settings} for this index
     */
    public Settings getSettings() {
        return indexSettings.getSettings();
    }

    /**
     * Returns the index this module is associated with
     */
    public Index getIndex() {
        return indexSettings.getIndex();
    }

    /**
     * Adds an {@link IndexEventListener} for this index. All listeners added here
     * are maintained for the entire index lifecycle on this node. Once an index is closed or deleted these
     * listeners go out of scope.
     * <p>
     * Note: an index might be created on a node multiple times. For instance if the last shard from an index is
     * relocated to another node the internal representation will be destroyed which includes the registered listeners.
     * Once the node holds at least one shard of an index all modules are reloaded and listeners are registered again.
     * Listeners can't be unregistered they will stay alive for the entire time the index is allocated on a node.
     * </p>
     */
    public void addIndexEventListener(IndexEventListener listener) {
        if (this.listener != null) {
            throw new IllegalStateException("can't add listener after listeners are frozen");
        }
        if (listener == null) {
            throw new IllegalArgumentException("listener must not be null");
        }
        if (indexEventListeners.contains(listener)) {
            throw new IllegalArgumentException("listener already added");
        }

        this.indexEventListeners.add(listener);
    }

    public IndexEventListener freeze() {
        // TODO somehow we need to make this pkg private...
        if (listener == null) {
            listener = new CompositeIndexEventListener(indexSettings, indexEventListeners);
        }
        return listener;
    }

    @Override
    protected void configure() {
        bind(EngineFactory.class).to(engineFactoryImpl).asEagerSingleton();
        if (indexSearcherWrapper == null) {
            bind(IndexSearcherWrapper.class).toProvider(Providers.of(null));
        } else {
            bind(IndexSearcherWrapper.class).to(indexSearcherWrapper).asEagerSingleton();
        }
        bind(IndexEventListener.class).toInstance(freeze());
        bind(IndexService.class).asEagerSingleton();
        bind(IndexServicesProvider.class).asEagerSingleton();
        bind(MapperService.class).asEagerSingleton();
        bind(IndexFieldDataService.class).asEagerSingleton();
        bind(IndexSettings.class).toInstance(new IndexSettings(indexSettings.getIndexMetaData(), indexSettings.getNodeSettings(), settingsConsumers));
    }

}
