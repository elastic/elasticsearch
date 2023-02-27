/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.entsearch.engine;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.indices.SystemIndexDescriptor;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SystemIndexPlugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.entsearch.engine.EngineIndexService.ENGINE_CONCRETE_INDEX_NAME;
import static org.hamcrest.CoreMatchers.equalTo;

public class EngineIndexServiceTests extends ESSingleNodeTestCase {
    private static final int NUM_INDICES = 10;
    private static final long UPDATED_AT = System.currentTimeMillis();

    private EngineIndexService engineService;
    private ClusterService clusterService;

    @Before
    public void setup() throws Exception {
        clusterService = getInstanceFromNode(ClusterService.class);
        BigArrays bigArrays = getInstanceFromNode(BigArrays.class);
        this.engineService = new EngineIndexService(client(), clusterService, writableRegistry(), bigArrays);
        for (int i = 0; i < NUM_INDICES; i++) {
            client().admin().indices().prepareCreate("index_" + i).execute().get();
        }
    }

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.getPlugins());
        plugins.add(TestPlugin.class);
        return plugins;
    }

    public void testCreateEngine() throws Exception {
        final Engine engine = new Engine("my_engine", new String[] { "index_1" }, null);

        IndexResponse resp = awaitPutEngine(engine);
        assertThat(resp.status(), equalTo(RestStatus.CREATED));
        assertThat(resp.getIndex(), equalTo(ENGINE_CONCRETE_INDEX_NAME));

        Engine getEngine = awaitGetEngine(engine.name());
        assertThat(getEngine, equalTo(engine));
        checkAliases(engine);
    }

    private void checkAliases(Engine engine) {
        Metadata metadata = clusterService.state().metadata();
        final String aliasName = "engine-" + engine.name();
        assertTrue(metadata.hasAlias(aliasName));
        final Set<String> aliasedIndices = metadata.aliasedIndices(aliasName)
            .stream()
            .map(index -> index.getName())
            .collect(Collectors.toSet());
        assertThat(aliasedIndices, equalTo(Set.of(engine.indices())));
    }

    public void testUpdateEngine() throws Exception {
        {
            final Engine engine = new Engine("my_engine", new String[] { "index_1", "index_2" }, null);
            IndexResponse resp = awaitPutEngine(engine);
            assertThat(resp.status(), equalTo(RestStatus.CREATED));
            assertThat(resp.getIndex(), equalTo(ENGINE_CONCRETE_INDEX_NAME));

            Engine getEngine = awaitGetEngine(engine.name());
            assertThat(getEngine, equalTo(engine));
        }

        final Engine engine = new Engine("my_engine", new String[] { "index_3", "index_4" }, "my_engine_analytics_collection");
        IndexResponse newResp = awaitPutEngine(engine);
        assertThat(newResp.status(), equalTo(RestStatus.OK));
        assertThat(newResp.getIndex(), equalTo(ENGINE_CONCRETE_INDEX_NAME));
        Engine getNewEngine = awaitGetEngine(engine.name());
        assertThat(engine, equalTo(getNewEngine));
        checkAliases(engine);
    }

    public void testListEngine() throws Exception {
        for (int i = 0; i < NUM_INDICES; i++) {
            final Engine engine = new Engine("my_engine_" + i, new String[] { "index_" + i }, null);
            IndexResponse resp = awaitPutEngine(engine);
            assertThat(resp.status(), equalTo(RestStatus.CREATED));
            assertThat(resp.getIndex(), equalTo(ENGINE_CONCRETE_INDEX_NAME));
        }

        {
            SearchResponse resp = awaitListEngine("*:*", 0, 10);
            assertNotNull(resp.getHits());
            assertThat(resp.getHits().getHits().length, equalTo(10));
            assertThat(resp.getHits().getTotalHits().value, equalTo(10L));

            SearchHits searchHits = resp.getHits();
            for (int i = 0; i < NUM_INDICES; i++) {
                SearchHit hit = searchHits.getAt(i);
                assertNotNull(hit.getFields().get("name"));
                assertThat(hit.getFields().get("name").getValues(), equalTo(Arrays.asList("my_engine_" + i)));
                assertNotNull(hit.getFields().get("indices"));
                assertThat(hit.getFields().get("indices").getValues(), equalTo(Arrays.asList("index_" + i)));
            }
        }

        {
            SearchResponse resp = awaitListEngine("*:*", 5, 10);
            assertNotNull(resp.getHits());
            assertThat(resp.getHits().getHits().length, equalTo(5));
            assertThat(resp.getHits().getTotalHits().value, equalTo(10L));

            SearchHits searchHits = resp.getHits();
            for (int i = 0; i < 5; i++) {
                int index = i + 5;
                SearchHit hit = searchHits.getAt(i);
                assertNotNull(hit.getFields().get("name"));
                assertThat(hit.getFields().get("name").getValues(), equalTo(Arrays.asList("my_engine_" + index)));
                assertNotNull(hit.getFields().get("indices"));
                assertThat(hit.getFields().get("indices").getValues(), equalTo(Arrays.asList("index_" + index)));
            }
        }
    }

    public void testListEngineWithQuery() throws Exception {
        for (int i = 0; i < 10; i++) {
            final Engine engine = new Engine("my_engine_" + i, new String[] { "index_" + i }, null);
            IndexResponse resp = awaitPutEngine(engine);
            assertThat(resp.status(), equalTo(RestStatus.CREATED));
            assertThat(resp.getIndex(), equalTo(ENGINE_CONCRETE_INDEX_NAME));
        }

        {
            for (String queryString : new String[] {
                "*my_engine_4*",
                "name:my_engine_4",
                "my_engine_4",
                "*_4",
                "indices:index_4",
                "index_4",
                "*_4" }) {
                SearchResponse resp = awaitListEngine(queryString, 0, 10);
                assertNotNull(resp.getHits());
                assertThat(resp.getHits().getHits().length, equalTo(1));
                assertThat(resp.getHits().getTotalHits().value, equalTo(1L));

                SearchHits searchHits = resp.getHits();
                SearchHit hit = searchHits.getAt(0);
                assertNotNull(hit.getFields().get("name"));
                assertThat(hit.getFields().get("name").getValues(), equalTo(Arrays.asList("my_engine_4")));
                assertNotNull(hit.getFields().get("indices"));
                assertThat(hit.getFields().get("indices").getValues(), equalTo(Arrays.asList("index_4")));
            }
        }
    }

    public void testDeleteEngine() throws Exception {
        for (int i = 0; i < 5; i++) {
            final Engine engine = new Engine("my_engine_" + i, new String[] { "index_" + i }, null);
            IndexResponse resp = awaitPutEngine(engine);
            assertThat(resp.status(), equalTo(RestStatus.CREATED));
            assertThat(resp.getIndex(), equalTo(ENGINE_CONCRETE_INDEX_NAME));

            Engine getEngine = awaitGetEngine(engine.name());
            assertThat(getEngine, equalTo(engine));
        }

        DeleteResponse resp = awaitDeleteEngine("my_engine_4");
        assertThat(resp.status(), equalTo(RestStatus.OK));
        expectThrows(ResourceNotFoundException.class, () -> awaitGetEngine("my_engine_4"));

        {
            SearchResponse searchResponse = awaitListEngine("*:*", 0, 10);
            assertNotNull(searchResponse.getHits());
            assertThat(searchResponse.getHits().getHits().length, equalTo(4));
            assertThat(searchResponse.getHits().getTotalHits().value, equalTo(4L));

            SearchHits searchHits = searchResponse.getHits();
            for (int i = 0; i < 4; i++) {
                SearchHit hit = searchHits.getAt(i);
                assertNotNull(hit.getFields().get("name"));
                assertThat(hit.getFields().get("name").getValues(), equalTo(Arrays.asList("my_engine_" + i)));
                assertNotNull(hit.getFields().get("indices"));
                assertThat(hit.getFields().get("indices").getValues(), equalTo(Arrays.asList("index_" + i)));
            }
        }
    }

    private IndexResponse awaitPutEngine(Engine engine) throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        final AtomicReference<IndexResponse> resp = new AtomicReference<>(null);
        final AtomicReference<Exception> exc = new AtomicReference<>(null);
        engineService.putEngine(engine, new ActionListener<>() {
            @Override
            public void onResponse(IndexResponse indexResponse) {
                resp.set(indexResponse);
                latch.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                exc.set(e);
                latch.countDown();
            }
        });
        assertTrue(latch.await(5, TimeUnit.SECONDS));
        if (exc.get() != null) {
            throw exc.get();
        }
        assertNotNull(resp.get());
        return resp.get();
    }

    private Engine awaitGetEngine(String name) throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        final AtomicReference<Engine> resp = new AtomicReference<>(null);
        final AtomicReference<Exception> exc = new AtomicReference<>(null);
        engineService.getEngine(name, new ActionListener<>() {
            @Override
            public void onResponse(Engine engine) {
                resp.set(engine);
                latch.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                exc.set(e);
                latch.countDown();
            }
        });
        assertTrue(latch.await(5, TimeUnit.SECONDS));
        if (exc.get() != null) {
            throw exc.get();
        }
        assertNotNull(resp.get());
        return resp.get();
    }

    private DeleteResponse awaitDeleteEngine(String name) throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        final AtomicReference<DeleteResponse> resp = new AtomicReference<>(null);
        final AtomicReference<Exception> exc = new AtomicReference<>(null);
        engineService.deleteEngine(name, new ActionListener<>() {
            @Override
            public void onResponse(DeleteResponse deleteResponse) {
                resp.set(deleteResponse);
                latch.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                exc.set(e);
                latch.countDown();
            }
        });
        assertTrue(latch.await(5, TimeUnit.SECONDS));
        if (exc.get() != null) {
            throw exc.get();
        }
        assertNotNull(resp.get());
        return resp.get();
    }

    private SearchResponse awaitListEngine(String queryString, int from, int size) throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        final AtomicReference<SearchResponse> resp = new AtomicReference<>(null);
        final AtomicReference<Exception> exc = new AtomicReference<>(null);
        engineService.listEngine(queryString, from, size, new ActionListener<>() {
            @Override
            public void onResponse(SearchResponse searchResponse) {
                resp.set(searchResponse);
                latch.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                exc.set(e);
                latch.countDown();
            }
        });
        assertTrue(latch.await(5, TimeUnit.SECONDS));
        if (exc.get() != null) {
            throw exc.get();
        }
        assertNotNull(resp.get());
        return resp.get();
    }

    /**
     * Test plugin to register the {@link EngineIndexService} system index descriptor.
     */
    public static class TestPlugin extends Plugin implements SystemIndexPlugin {
        @Override
        public Collection<SystemIndexDescriptor> getSystemIndexDescriptors(Settings settings) {
            return List.of(EngineIndexService.getSystemIndexDescriptor());
        }

        @Override
        public String getFeatureName() {
            return this.getClass().getSimpleName();
        }

        @Override
        public String getFeatureDescription() {
            return this.getClass().getCanonicalName();
        }
    }
}
