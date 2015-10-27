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

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FieldInvertState;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.*;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.search.similarities.Similarity;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.inject.ModuleTestCase;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.cache.query.QueryCache;
import org.elasticsearch.index.cache.query.index.IndexQueryCache;
import org.elasticsearch.index.cache.query.none.NoneQueryCache;
import org.elasticsearch.index.engine.EngineConfig;
import org.elasticsearch.index.engine.EngineException;
import org.elasticsearch.index.engine.EngineFactory;
import org.elasticsearch.index.engine.InternalEngineFactory;
import org.elasticsearch.index.shard.IndexEventListener;
import org.elasticsearch.index.shard.IndexSearcherWrapper;
import org.elasticsearch.index.similarity.SimilarityProvider;
import org.elasticsearch.index.similarity.SimilarityService;
import org.elasticsearch.index.store.IndexStore;
import org.elasticsearch.index.store.IndexStoreConfig;
import org.elasticsearch.indices.IndicesWarmer;
import org.elasticsearch.test.IndexSettingsModule;
import org.elasticsearch.test.engine.MockEngineFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

public class IndexModuleTests extends ModuleTestCase {
    private final IndicesWarmer warmer = new IndicesWarmer(Settings.EMPTY, null);
    public void testWrapperIsBound() {
        final Index index = new Index("foo");
        final Settings settings = Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT).build();
        IndexSettings indexSettings = IndexSettingsModule.newIndexSettings(index, settings, Collections.EMPTY_LIST);
        IndexModule module = new IndexModule(indexSettings, null, null, warmer);
        assertInstanceBinding(module, IndexModule.IndexSearcherWrapperFactory.class, (x) -> x.newWrapper(null) == null);
        module.setSearcherWrapper((s) ->  new Wrapper());
        assertInstanceBinding(module, IndexModule.IndexSearcherWrapperFactory.class, (x) -> x.newWrapper(null) instanceof Wrapper);
    }

    public void testEngineFactoryBound() {
        final Index index = new Index("foo");
        final Settings settings = Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT).build();
        IndexSettings indexSettings = IndexSettingsModule.newIndexSettings(index, settings, Collections.EMPTY_LIST);
        IndexModule module = new IndexModule(indexSettings, null, null, warmer);
        assertBinding(module, EngineFactory.class, InternalEngineFactory.class);
        module.engineFactoryImpl = MockEngineFactory.class;
        assertBinding(module, EngineFactory.class, MockEngineFactory.class);
    }

    public void testRegisterIndexStore() {
        final Index index = new Index("foo");
        final Settings settings = Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT).put(IndexModule.STORE_TYPE, "foo_store").build();
        IndexSettings indexSettings = IndexSettingsModule.newIndexSettings(index, settings, Collections.EMPTY_LIST);
        IndexModule module = new IndexModule(indexSettings, null, null, warmer);
        module.addIndexStore("foo_store", FooStore::new);
        assertInstanceBinding(module, IndexStore.class, (x) -> x.getClass() == FooStore.class);
        try {
            module.addIndexStore("foo_store", FooStore::new);
            fail("already registered");
        } catch (IllegalArgumentException ex) {
            // fine
        }
    }

    public void testOtherServiceBound() {
        final AtomicBoolean atomicBoolean = new AtomicBoolean(false);
        final IndexEventListener eventListener = new IndexEventListener() {
            @Override
            public void beforeIndexDeleted(IndexService indexService) {
                atomicBoolean.set(true);
            }
        };
        final Index index = new Index("foo");
        final Settings settings = Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT).build();
        IndexSettings indexSettings = IndexSettingsModule.newIndexSettings(index, settings, Collections.EMPTY_LIST);
        IndexModule module = new IndexModule(indexSettings, null, null, warmer);
        Consumer<Settings> listener = (s) -> {};
        module.addIndexSettingsListener(listener);
        module.addIndexEventListener(eventListener);
        assertBinding(module, IndexService.class, IndexService.class);
        assertBinding(module, IndexServicesProvider.class, IndexServicesProvider.class);
        assertInstanceBinding(module, IndexEventListener.class, (x) -> {
            x.beforeIndexDeleted(null);
            return atomicBoolean.get();
        });
        assertInstanceBinding(module, IndexSettings.class, (x) -> x.getSettings().getAsMap().equals(indexSettings.getSettings().getAsMap()));
        assertInstanceBinding(module, IndexSettings.class, (x) -> x.getIndex().equals(indexSettings.getIndex()));
        assertInstanceBinding(module, IndexSettings.class, (x) -> x.getUpdateListeners().get(0) == listener);
        assertInstanceBinding(module, IndexStore.class, (x) -> x.getClass() == IndexStore.class);
    }


    public void testListener() {
        final Index index = new Index("foo");
        final Settings settings = Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT).build();
        IndexSettings indexSettings = IndexSettingsModule.newIndexSettings(index, settings, Collections.EMPTY_LIST);
        IndexModule module = new IndexModule(indexSettings, null, null, warmer);
        Consumer<Settings> listener = (s) -> {
        };
        module.addIndexSettingsListener(listener);

        try {
            module.addIndexSettingsListener(listener);
            fail("already added");
        } catch (IllegalStateException ex) {

        }

        try {
            module.addIndexSettingsListener(null);
            fail("must not be null");
        } catch (IllegalArgumentException ex) {

        }
        assertInstanceBinding(module, IndexSettings.class, (x) -> x.getUpdateListeners().size() == 1);
        assertInstanceBinding(module, IndexSettings.class, (x) -> x.getUpdateListeners().get(0) == listener);
    }

    public void testAddSimilarity() {
        Settings indexSettings = Settings.settingsBuilder()
                .put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT)
                .put("index.similarity.my_similarity.type", "test_similarity")
                .put("index.similarity.my_similarity.key", "there is a key")
                .build();
        IndexModule module = new IndexModule(IndexSettingsModule.newIndexSettings(new Index("foo"), indexSettings, Collections.EMPTY_LIST), null, null, warmer);
        module.addSimilarity("test_similarity", (string, settings) -> new SimilarityProvider() {
            @Override
            public String name() {
                return string;
            }

            @Override
            public Similarity get() {
                return new TestSimilarity(settings.get("key"));
            }
        });
        assertInstanceBinding(module, SimilarityService.class, (inst) -> {
            if (inst instanceof SimilarityService) {
                assertNotNull(inst.getSimilarity("my_similarity"));
                assertTrue(inst.getSimilarity("my_similarity").get() instanceof TestSimilarity);
                assertEquals("my_similarity", inst.getSimilarity("my_similarity").name());
                assertEquals("there is a key" , ((TestSimilarity)inst.getSimilarity("my_similarity").get()).key);
                return true;
            }
            return false;
        });
    }

    public void testSetupUnknownSimilarity() {
        Settings indexSettings = Settings.settingsBuilder()
                .put("index.similarity.my_similarity.type", "test_similarity")
                .put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT)
                .build();
        IndexModule module = new IndexModule(IndexSettingsModule.newIndexSettings(new Index("foo"), indexSettings, Collections.EMPTY_LIST), null, null, warmer);
        try {
            assertInstanceBinding(module, SimilarityService.class, (inst) -> inst instanceof SimilarityService);
        } catch (IllegalArgumentException ex) {
            assertEquals("Unknown Similarity type [test_similarity] for [my_similarity]", ex.getMessage());
        }
    }


    public void testSetupWithoutType() {
        Settings indexSettings = Settings.settingsBuilder()
                .put("index.similarity.my_similarity.foo", "bar")
                .put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT)
                .build();
        IndexModule module = new IndexModule(IndexSettingsModule.newIndexSettings(new Index("foo"), indexSettings, Collections.EMPTY_LIST), null, null, warmer);
        try {
            assertInstanceBinding(module, SimilarityService.class, (inst) -> inst instanceof SimilarityService);
        } catch (IllegalArgumentException ex) {
            assertEquals("Similarity [my_similarity] must have an associated type", ex.getMessage());
        }
    }

    public void testCannotRegisterProvidedImplementations() {
        Settings indexSettings = Settings.settingsBuilder()
                .put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT).build();
        IndexModule module = new IndexModule(IndexSettingsModule.newIndexSettings(new Index("foo"), indexSettings, Collections.EMPTY_LIST), null, null, warmer);
        try {
            module.registerQueryCache("index", IndexQueryCache::new);
            fail("only once");
        } catch (IllegalArgumentException e) {
            assertEquals(e.getMessage(), "Can't register the same [query_cache] more than once for [index]");
        }

        try {
            module.registerQueryCache("none", (settings, x) -> new NoneQueryCache(settings));
            fail("only once");
        } catch (IllegalArgumentException e) {
            assertEquals(e.getMessage(), "Can't register the same [query_cache] more than once for [none]");
        }

        try {
            module.registerQueryCache("index", null);
            fail("must not be null");
        } catch (IllegalArgumentException e) {
            assertEquals(e.getMessage(), "provider must not be null");
        }
    }

    public void testRegisterCustomQueryCache() {
        Settings indexSettings = Settings.settingsBuilder()
                .put(IndexModule.QUERY_CACHE_TYPE, "custom")
                .put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT).build();
        IndexModule module = new IndexModule(IndexSettingsModule.newIndexSettings(new Index("foo"), indexSettings, Collections.EMPTY_LIST), null, null, warmer);
        module.registerQueryCache("custom", (a, b) -> new CustomQueryCache());
        try {
            module.registerQueryCache("custom", (a, b) -> new CustomQueryCache());
            fail("only once");
        } catch (IllegalArgumentException e) {
            assertEquals(e.getMessage(), "Can't register the same [query_cache] more than once for [custom]");
        }
        assertInstanceBinding(module, QueryCache.class, (x) -> x instanceof CustomQueryCache);
    }

    public void testDefaultQueryCacheImplIsSelected() {
        Settings indexSettings = Settings.settingsBuilder()
                .put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT).build();
        IndexModule module = new IndexModule(IndexSettingsModule.newIndexSettings(new Index("foo"), indexSettings, Collections.EMPTY_LIST), null, null, warmer);
        assertInstanceBinding(module, QueryCache.class, (x) -> x instanceof IndexQueryCache);
    }

    class CustomQueryCache implements QueryCache {

        @Override
        public void clear(String reason) {
        }

        @Override
        public void close() throws IOException {
        }

        @Override
        public Index index() {
            return new Index("test");
        }

        @Override
        public Weight doCache(Weight weight, QueryCachingPolicy policy) {
            return weight;
        }
    }



    private static class TestSimilarity extends Similarity {
        private final Similarity delegate = new BM25Similarity();
        private final String key;


        public TestSimilarity(String key) {
            if (key == null) {
                throw new AssertionError("key is null");
            }
            this.key = key;
        }

        @Override
        public long computeNorm(FieldInvertState state) {
            return delegate.computeNorm(state);
        }

        @Override
        public SimWeight computeWeight(CollectionStatistics collectionStats, TermStatistics... termStats) {
            return delegate.computeWeight(collectionStats, termStats);
        }

        @Override
        public SimScorer simScorer(SimWeight weight, LeafReaderContext context) throws IOException {
            return delegate.simScorer(weight, context);
        }
    }

    public static final class Wrapper extends IndexSearcherWrapper {

        @Override
        public DirectoryReader wrap(DirectoryReader reader) {
            return null;
        }

        @Override
        public IndexSearcher wrap(EngineConfig engineConfig, IndexSearcher searcher) throws EngineException {
            return null;
        }
    }

    public static final class FooStore extends IndexStore {

        public FooStore(IndexSettings indexSettings, IndexStoreConfig config) {
            super(indexSettings, config);
        }
    }

}
