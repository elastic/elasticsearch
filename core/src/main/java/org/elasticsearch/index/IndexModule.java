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

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.analysis.AnalysisRegistry;
import org.elasticsearch.index.cache.query.QueryCache;
import org.elasticsearch.index.cache.query.IndexQueryCache;
import org.elasticsearch.index.cache.query.DisabledQueryCache;
import org.elasticsearch.index.engine.EngineFactory;
import org.elasticsearch.index.shard.IndexEventListener;
import org.elasticsearch.index.shard.IndexSearcherWrapper;
import org.elasticsearch.index.shard.IndexingOperationListener;
import org.elasticsearch.index.shard.SearchOperationListener;
import org.elasticsearch.index.similarity.BM25SimilarityProvider;
import org.elasticsearch.index.similarity.SimilarityProvider;
import org.elasticsearch.index.similarity.SimilarityService;
import org.elasticsearch.index.store.IndexStore;
import org.elasticsearch.index.store.IndexStoreConfig;
import org.elasticsearch.indices.IndicesQueryCache;
import org.elasticsearch.indices.fielddata.cache.IndicesFieldDataCache;
import org.elasticsearch.indices.mapper.MapperRegistry;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * IndexModule represents the central extension point for index level custom implementations like:
 * <ul>
 *     <li>{@link SimilarityProvider} - New {@link SimilarityProvider} implementations can be registered through
 *     {@link #addSimilarity(String, BiFunction)}while existing Providers can be referenced through Settings under the
 *     {@link IndexModule#SIMILARITY_SETTINGS_PREFIX} prefix along with the "type" value.  For example, to reference the
 *     {@link BM25SimilarityProvider}, the configuration <tt>"index.similarity.my_similarity.type : "BM25"</tt> can be used.</li>
 *      <li>{@link IndexStore} - Custom {@link IndexStore} instances can be registered via {@link #addIndexStore(String, BiFunction)}</li>
 *      <li>{@link IndexEventListener} - Custom {@link IndexEventListener} instances can be registered via
 *      {@link #addIndexEventListener(IndexEventListener)}</li>
 *      <li>Settings update listener - Custom settings update listener can be registered via
 *      {@link #addSettingsUpdateConsumer(Setting, Consumer)}</li>
 * </ul>
 */
public final class IndexModule {

    public static final Setting<String> INDEX_STORE_TYPE_SETTING =
        new Setting<>("index.store.type", "", Function.identity(), Property.IndexScope, Property.NodeScope);
    public static final String SIMILARITY_SETTINGS_PREFIX = "index.similarity";

    // whether to use the query cache
    public static final Setting<Boolean> INDEX_QUERY_CACHE_ENABLED_SETTING =
            Setting.boolSetting("index.queries.cache.enabled", true, Property.IndexScope);

    // for test purposes only
    public static final Setting<Boolean> INDEX_QUERY_CACHE_EVERYTHING_SETTING =
        Setting.boolSetting("index.queries.cache.everything", false, Property.IndexScope);

    private final IndexSettings indexSettings;
    private final IndexStoreConfig indexStoreConfig;
    private final AnalysisRegistry analysisRegistry;
    // pkg private so tests can mock
    final SetOnce<EngineFactory> engineFactory = new SetOnce<>();
    private SetOnce<IndexSearcherWrapperFactory> indexSearcherWrapper = new SetOnce<>();
    private final Set<IndexEventListener> indexEventListeners = new HashSet<>();
    private final Map<String, BiFunction<String, Settings, SimilarityProvider>> similarities = new HashMap<>();
    private final Map<String, BiFunction<IndexSettings, IndexStoreConfig, IndexStore>> storeTypes = new HashMap<>();
    private final SetOnce<BiFunction<IndexSettings, IndicesQueryCache, QueryCache>> forceQueryCacheProvider = new SetOnce<>();
    private final List<SearchOperationListener> searchOperationListeners = new ArrayList<>();
    private final List<IndexingOperationListener> indexOperationListeners = new ArrayList<>();
    private final AtomicBoolean frozen = new AtomicBoolean(false);

    public IndexModule(IndexSettings indexSettings, IndexStoreConfig indexStoreConfig, AnalysisRegistry analysisRegistry) {
        this.indexStoreConfig = indexStoreConfig;
        this.indexSettings = indexSettings;
        this.analysisRegistry = analysisRegistry;
        this.searchOperationListeners.add(new SearchSlowLog(indexSettings));
        this.indexOperationListeners.add(new IndexingSlowLog(indexSettings));
    }

    /**
     * Adds a Setting and it's consumer for this index.
     */
    public <T> void addSettingsUpdateConsumer(Setting<T> setting, Consumer<T> consumer) {
        ensureNotFrozen();
        if (setting == null) {
            throw new IllegalArgumentException("setting must not be null");
        }
        indexSettings.getScopedSettings().addSettingsUpdateConsumer(setting, consumer);
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
        ensureNotFrozen();
        if (listener == null) {
            throw new IllegalArgumentException("listener must not be null");
        }
        if (indexEventListeners.contains(listener)) {
            throw new IllegalArgumentException("listener already added");
        }

        this.indexEventListeners.add(listener);
    }

    /**
     * Adds an {@link SearchOperationListener} for this index. All listeners added here
     * are maintained for the entire index lifecycle on this node. Once an index is closed or deleted these
     * listeners go out of scope.
     * <p>
     * Note: an index might be created on a node multiple times. For instance if the last shard from an index is
     * relocated to another node the internal representation will be destroyed which includes the registered listeners.
     * Once the node holds at least one shard of an index all modules are reloaded and listeners are registered again.
     * Listeners can't be unregistered they will stay alive for the entire time the index is allocated on a node.
     * </p>
     */
    public void addSearchOperationListener(SearchOperationListener listener) {
        ensureNotFrozen();
        if (listener == null) {
            throw new IllegalArgumentException("listener must not be null");
        }
        if (searchOperationListeners.contains(listener)) {
            throw new IllegalArgumentException("listener already added");
        }

        this.searchOperationListeners.add(listener);
    }

    /**
     * Adds an {@link IndexingOperationListener} for this index. All listeners added here
     * are maintained for the entire index lifecycle on this node. Once an index is closed or deleted these
     * listeners go out of scope.
     * <p>
     * Note: an index might be created on a node multiple times. For instance if the last shard from an index is
     * relocated to another node the internal representation will be destroyed which includes the registered listeners.
     * Once the node holds at least one shard of an index all modules are reloaded and listeners are registered again.
     * Listeners can't be unregistered they will stay alive for the entire time the index is allocated on a node.
     * </p>
     */
    public void addIndexOperationListener(IndexingOperationListener listener) {
        ensureNotFrozen();
        if (listener == null) {
            throw new IllegalArgumentException("listener must not be null");
        }
        if (indexOperationListeners.contains(listener)) {
            throw new IllegalArgumentException("listener already added");
        }

        this.indexOperationListeners.add(listener);
    }

    /**
     * Adds an {@link IndexStore} type to this index module. Typically stores are registered with a reference to
     * it's constructor:
     * <pre>
     *     indexModule.addIndexStore("my_store_type", MyStore::new);
     * </pre>
     *
     * @param type the type to register
     * @param provider the instance provider / factory method
     */
    public void addIndexStore(String type, BiFunction<IndexSettings, IndexStoreConfig, IndexStore> provider) {
        ensureNotFrozen();
        if (storeTypes.containsKey(type)) {
            throw new IllegalArgumentException("key [" + type +"] already registered");
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
        ensureNotFrozen();
        if (similarities.containsKey(name) || SimilarityService.BUILT_IN.containsKey(name)) {
            throw new IllegalArgumentException("similarity for name: [" + name + " is already registered");
        }
        similarities.put(name, similarity);
    }

    /**
     * Sets a {@link org.elasticsearch.index.IndexModule.IndexSearcherWrapperFactory} that is called once the IndexService
     * is fully constructed.
     * Note: this method can only be called once per index. Multiple wrappers are not supported.
     */
    public void setSearcherWrapper(IndexSearcherWrapperFactory indexSearcherWrapperFactory) {
        ensureNotFrozen();
        this.indexSearcherWrapper.set(indexSearcherWrapperFactory);
    }

    IndexEventListener freeze() { // pkg private for testing
        if (this.frozen.compareAndSet(false, true)) {
            return new CompositeIndexEventListener(indexSettings, indexEventListeners);
        } else {
            throw new IllegalStateException("already frozen");
        }
    }

    private static boolean isBuiltinType(String storeType) {
        for (Type type : Type.values()) {
            if (type.match(storeType)) {
                return true;
            }
        }
        return false;
    }

    public enum Type {
        NIOFS,
        MMAPFS,
        SIMPLEFS,
        FS,
        @Deprecated
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

    /**
     * Factory for creating new {@link IndexSearcherWrapper} instances
     */
    public interface IndexSearcherWrapperFactory {
        /**
         * Returns a new IndexSearcherWrapper. This method is called once per index per node
         */
        IndexSearcherWrapper newWrapper(final IndexService indexService);
    }

    public IndexService newIndexService(NodeEnvironment environment, IndexService.ShardStoreDeleter shardStoreDeleter,
                                        NodeServicesProvider servicesProvider, IndicesQueryCache indicesQueryCache,
                                        MapperRegistry mapperRegistry, IndicesFieldDataCache indicesFieldDataCache) throws IOException {
        final IndexEventListener eventListener = freeze();
        IndexSearcherWrapperFactory searcherWrapperFactory = indexSearcherWrapper.get() == null
            ? (shard) -> null : indexSearcherWrapper.get();
        eventListener.beforeIndexCreated(indexSettings.getIndex(), indexSettings.getSettings());
        final String storeType = indexSettings.getValue(INDEX_STORE_TYPE_SETTING);
        final IndexStore store;
        if (Strings.isEmpty(storeType) || isBuiltinType(storeType)) {
            store = new IndexStore(indexSettings, indexStoreConfig);
        } else {
            BiFunction<IndexSettings, IndexStoreConfig, IndexStore> factory = storeTypes.get(storeType);
            if (factory == null) {
                throw new IllegalArgumentException("Unknown store type [" + storeType + "]");
            }
            store = factory.apply(indexSettings, indexStoreConfig);
            if (store == null) {
                throw new IllegalStateException("store must not be null");
            }
        }
        indexSettings.getScopedSettings().addSettingsUpdateConsumer(IndexStore.INDEX_STORE_THROTTLE_TYPE_SETTING, store::setType);
        indexSettings.getScopedSettings().addSettingsUpdateConsumer(IndexStore.INDEX_STORE_THROTTLE_MAX_BYTES_PER_SEC_SETTING,
            store::setMaxRate);
        final QueryCache queryCache;
        if (indexSettings.getValue(INDEX_QUERY_CACHE_ENABLED_SETTING)) {
            BiFunction<IndexSettings, IndicesQueryCache, QueryCache> queryCacheProvider = forceQueryCacheProvider.get();
            if (queryCacheProvider == null) {
                queryCache = new IndexQueryCache(indexSettings, indicesQueryCache);
            } else {
                queryCache = queryCacheProvider.apply(indexSettings, indicesQueryCache);
            }
        } else {
            queryCache = new DisabledQueryCache(indexSettings);
        }
        return new IndexService(indexSettings, environment, new SimilarityService(indexSettings, similarities), shardStoreDeleter,
            analysisRegistry, engineFactory.get(), servicesProvider, queryCache, store, eventListener, searcherWrapperFactory,
            mapperRegistry, indicesFieldDataCache, searchOperationListeners, indexOperationListeners);
    }

    /**
     * Forces a certain query cache to use instead of the default one. If this is set
     * and query caching is not disabled with {@code index.queries.cache.enabled}, then
     * the given provider will be used.
     * NOTE: this can only be set once
     *
     * @see #INDEX_QUERY_CACHE_ENABLED_SETTING
     */
    public void forceQueryCacheProvider(BiFunction<IndexSettings, IndicesQueryCache, QueryCache> queryCacheProvider) {
        ensureNotFrozen();
        this.forceQueryCacheProvider.set(queryCacheProvider);
    }

    private void ensureNotFrozen() {
        if (this.frozen.get()) {
            throw new IllegalStateException("Can't modify IndexModule once the index service has been created");
        }
    }

}
