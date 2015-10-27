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

import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.util.Providers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.engine.EngineFactory;
import org.elasticsearch.index.engine.InternalEngineFactory;
import org.elasticsearch.index.fielddata.IndexFieldDataService;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.shard.IndexEventListener;
import org.elasticsearch.index.shard.IndexSearcherWrapper;
import org.elasticsearch.index.similarity.BM25SimilarityProvider;
import org.elasticsearch.index.similarity.SimilarityProvider;
import org.elasticsearch.index.similarity.SimilarityService;
import org.elasticsearch.index.store.IndexStore;
import org.elasticsearch.index.store.IndexStoreConfig;

import java.util.*;
import java.util.function.BiFunction;
import java.util.function.Consumer;

/**
 * IndexModule represents the central extension point for index level custom implementations like:
 * <ul>
 *     <li>{@link SimilarityProvider} - New {@link SimilarityProvider} implementations can be registered through {@link #addSimilarity(String, BiFunction)}
 *         while existing Providers can be referenced through Settings under the {@link IndexModule#SIMILARITY_SETTINGS_PREFIX} prefix
 *         along with the "type" value.  For example, to reference the {@link BM25SimilarityProvider}, the configuration
 *         <tt>"index.similarity.my_similarity.type : "BM25"</tt> can be used.</li>
 *      <li>{@link IndexStore} - Custom {@link IndexStore} instances can be registered via {@link #addIndexStore(String, BiFunction)}</li>
 *      <li>{@link IndexEventListener} - Custom {@link IndexEventListener} instances can be registered via {@link #addIndexEventListener(IndexEventListener)}</li>
 *      <li>Settings update listener - Custom settings update listener can be registered via {@link #addIndexSettingsListener(Consumer)}</li>
 * </ul>
 */
public class IndexModule extends AbstractModule {

    public static final String STORE_TYPE = "index.store.type";
    public static final String SIMILARITY_SETTINGS_PREFIX = "index.similarity";
    private final IndexSettings indexSettings;
    private final IndexStoreConfig indexStoreConfig;
    // pkg private so tests can mock
    Class<? extends EngineFactory> engineFactoryImpl = InternalEngineFactory.class;
    Class<? extends IndexSearcherWrapper> indexSearcherWrapper = null;
    private final Set<Consumer<Settings>> settingsConsumers = new HashSet<>();
    private final Set<IndexEventListener> indexEventListeners = new HashSet<>();
    private IndexEventListener listener;
    private final Map<String, BiFunction<String, Settings, SimilarityProvider>> similarities = new HashMap<>();
    private final Map<String, BiFunction<IndexSettings, IndexStoreConfig, IndexStore>> storeTypes = new HashMap<>();


    public IndexModule(IndexSettings indexSettings, IndexStoreConfig indexStoreConfig) {
        this.indexStoreConfig = indexStoreConfig;
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

    /**
     * Adds an {@link IndexStore} type to this index module. Typically stores are registered with a refrence to
     * it's constructor:
     * <pre>
     *     indexModule.addIndexStore("my_store_type", MyStore::new);
     * </pre>
     *
     * @param type the type to register
     * @param provider the instance provider / factory method
     */
    public void addIndexStore(String type, BiFunction<IndexSettings, IndexStoreConfig, IndexStore> provider) {
        if (storeTypes.containsKey(type)) {
            throw new IllegalArgumentException("key [" + type +"] already registerd");
        }
        storeTypes.put(type, provider);
    }


    /**
     * Registers the given {@link SimilarityProvider} with the given name
     *
     * @param name Name of the SimilarityProvider
     * @param similarity SimilarityProvider to register
     */
    public void addSimilarity(String name, BiFunction<String, Settings, SimilarityProvider> similarity) {
        if (similarities.containsKey(name) || SimilarityService.BUILT_IN.containsKey(name)) {
            throw new IllegalArgumentException("similarity for name: [" + name + " is already registered");
        }
        similarities.put(name, similarity);
    }


    public IndexEventListener freeze() {
        // TODO somehow we need to make this pkg private...
        if (listener == null) {
            listener = new CompositeIndexEventListener(indexSettings, indexEventListeners);
        }
        return listener;
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
        final IndexSettings settings = new IndexSettings(indexSettings.getIndexMetaData(), indexSettings.getNodeSettings(), settingsConsumers);
        bind(IndexSettings.class).toInstance(settings);

        final String storeType = settings.getSettings().get(STORE_TYPE);
        final IndexStore store;
        if (storeType == null || isBuiltinType(storeType)) {
            store = new IndexStore(settings, indexStoreConfig);
        } else {
            BiFunction<IndexSettings, IndexStoreConfig, IndexStore> factory = storeTypes.get(storeType);
            if (factory == null) {
                throw new IllegalArgumentException("Unknown store type [" + storeType + "]");
            }
            store = factory.apply(settings, indexStoreConfig);
            if (store == null) {
                throw new IllegalStateException("store must not be null");
            }
        }
        bind(IndexStore.class).toInstance(store);
        bind(SimilarityService.class).toInstance(new SimilarityService(settings, similarities));
    }

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
}
