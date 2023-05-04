/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.search;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.indices.SystemIndexDescriptor;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SystemIndexPlugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.application.search.SearchApplicationIndexService.SEARCH_APPLICATION_CONCRETE_INDEX_NAME;
import static org.hamcrest.CoreMatchers.equalTo;

public class SearchApplicationIndexServiceTests extends ESSingleNodeTestCase {
    private static final int NUM_INDICES = 10;
    private static final long UPDATED_AT = System.currentTimeMillis();

    private SearchApplicationIndexService searchAppService;
    private ClusterService clusterService;

    @Before
    public void setup() throws Exception {
        clusterService = getInstanceFromNode(ClusterService.class);
        BigArrays bigArrays = getInstanceFromNode(BigArrays.class);
        this.searchAppService = new SearchApplicationIndexService(client(), clusterService, writableRegistry(), bigArrays);
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

    public void testEmptyState() throws Exception {
        expectThrows(ResourceNotFoundException.class, () -> awaitGetSearchApplication("i-dont-exist"));
        expectThrows(ResourceNotFoundException.class, () -> awaitDeleteSearchApplication("i-dont-exist"));

        SearchApplicationIndexService.SearchApplicationResult listResults = awaitListSearchApplication("*", 0, 10);
        assertThat(listResults.totalResults(), equalTo(0L));
    }

    public void testCreateSearchApplication() throws Exception {
        final SearchApplication searchApp = new SearchApplication(
            "my_search_app",
            new String[] { "index_1" },
            null,
            System.currentTimeMillis(),
            null
        );

        IndexResponse resp = awaitPutSearchApplication(searchApp, true);
        assertThat(resp.status(), equalTo(RestStatus.CREATED));
        assertThat(resp.getIndex(), equalTo(SEARCH_APPLICATION_CONCRETE_INDEX_NAME));

        SearchApplication getSearchApp = awaitGetSearchApplication(searchApp.name());
        assertThat(getSearchApp, equalTo(searchApp));
        checkAliases(searchApp);

        assertThat(getSearchApp.searchApplicationTemplate(), equalTo(SearchApplicationTemplate.DEFAULT_TEMPLATE));

        expectThrows(VersionConflictEngineException.class, () -> awaitPutSearchApplication(searchApp, true));
    }

    private void checkAliases(SearchApplication searchApp) {
        Metadata metadata = clusterService.state().metadata();
        final String aliasName = searchApp.name();
        assertTrue(metadata.hasAlias(aliasName));
        final Set<String> aliasedIndices = metadata.aliasedIndices(aliasName)
            .stream()
            .map(index -> index.getName())
            .collect(Collectors.toSet());
        assertThat(aliasedIndices, equalTo(Set.of(searchApp.indices())));
    }

    public void testUpdateSearchApplication() throws Exception {
        {
            final SearchApplication searchApp = new SearchApplication(
                "my_search_app",
                new String[] { "index_1", "index_2" },
                null,
                System.currentTimeMillis(),
                SearchApplicationTestUtils.getRandomSearchApplicationTemplate()
            );
            IndexResponse resp = awaitPutSearchApplication(searchApp, false);
            assertThat(resp.status(), equalTo(RestStatus.CREATED));
            assertThat(resp.getIndex(), equalTo(SEARCH_APPLICATION_CONCRETE_INDEX_NAME));

            SearchApplication getSearchApp = awaitGetSearchApplication(searchApp.name());
            assertThat(getSearchApp, equalTo(searchApp));
        }

        final SearchApplication searchApp = new SearchApplication(
            "my_search_app",
            new String[] { "index_3", "index_4" },
            "my_search_app_analytics_collection",
            System.currentTimeMillis(),
            SearchApplicationTestUtils.getRandomSearchApplicationTemplate()
        );
        IndexResponse newResp = awaitPutSearchApplication(searchApp, false);
        assertThat(newResp.status(), equalTo(RestStatus.OK));
        assertThat(newResp.getIndex(), equalTo(SEARCH_APPLICATION_CONCRETE_INDEX_NAME));
        SearchApplication getNewSearchApp = awaitGetSearchApplication(searchApp.name());
        assertThat(searchApp, equalTo(getNewSearchApp));
        assertThat(searchApp.searchApplicationTemplate(), equalTo(getNewSearchApp.searchApplicationTemplate()));
        checkAliases(searchApp);
    }

    public void testListSearchApplication() throws Exception {
        for (int i = 0; i < NUM_INDICES; i++) {
            final SearchApplication searchApp = new SearchApplication(
                "my_search_app_" + i,
                new String[] { "index_" + i },
                null,
                System.currentTimeMillis(),
                null
            );
            IndexResponse resp = awaitPutSearchApplication(searchApp, false);
            assertThat(resp.status(), equalTo(RestStatus.CREATED));
            assertThat(resp.getIndex(), equalTo(SEARCH_APPLICATION_CONCRETE_INDEX_NAME));
        }

        {
            SearchApplicationIndexService.SearchApplicationResult searchResponse = awaitListSearchApplication("*:*", 0, 10);
            final List<SearchApplicationListItem> apps = searchResponse.items();
            assertNotNull(apps);
            assertThat(apps.size(), equalTo(10));
            assertThat(searchResponse.totalResults(), equalTo(10L));

            for (int i = 0; i < NUM_INDICES; i++) {
                SearchApplicationListItem app = apps.get(i);
                assertThat(app.name(), equalTo("my_search_app_" + i));
                assertThat(app.indices(), equalTo(new String[] { "index_" + i }));
            }
        }

        {
            SearchApplicationIndexService.SearchApplicationResult searchResponse = awaitListSearchApplication("*:*", 5, 10);
            final List<SearchApplicationListItem> apps = searchResponse.items();
            assertNotNull(apps);
            assertThat(apps.size(), equalTo(5));
            assertThat(searchResponse.totalResults(), equalTo(10L));

            for (int i = 0; i < 5; i++) {
                int index = i + 5;
                SearchApplicationListItem app = apps.get(i);
                assertThat(app.name(), equalTo("my_search_app_" + index));
                assertThat(app.indices(), equalTo(new String[] { "index_" + index }));
            }
        }
    }

    public void testListSearchApplicationWithQuery() throws Exception {
        for (int i = 0; i < 10; i++) {
            final SearchApplication app = new SearchApplication(
                "my_search_app_" + i,
                new String[] { "index_" + i },
                null,
                System.currentTimeMillis(),
                null
            );
            IndexResponse resp = awaitPutSearchApplication(app, false);
            assertThat(resp.status(), equalTo(RestStatus.CREATED));
            assertThat(resp.getIndex(), equalTo(SEARCH_APPLICATION_CONCRETE_INDEX_NAME));
        }

        {
            for (String queryString : new String[] {
                "*my_search_app_4*",
                "name:my_search_app_4",
                "my_search_app_4",
                "*_4",
                "indices:index_4",
                "index_4",
                "*_4" }) {

                SearchApplicationIndexService.SearchApplicationResult searchResponse = awaitListSearchApplication(queryString, 0, 10);
                final List<SearchApplicationListItem> apps = searchResponse.items();
                assertNotNull(apps);
                assertThat(apps.size(), equalTo(1));
                assertThat(searchResponse.totalResults(), equalTo(1L));
                assertThat(apps.get(0).name(), equalTo("my_search_app_4"));
                assertThat(apps.get(0).indices(), equalTo(new String[] { "index_4" }));
            }
        }
    }

    public void testDeleteSearchApplication() throws Exception {
        for (int i = 0; i < 5; i++) {
            final SearchApplication app = new SearchApplication(
                "my_search_app_" + i,
                new String[] { "index_" + i },
                null,
                System.currentTimeMillis(),
                null
            );
            IndexResponse resp = awaitPutSearchApplication(app, false);
            assertThat(resp.status(), equalTo(RestStatus.CREATED));
            assertThat(resp.getIndex(), equalTo(SEARCH_APPLICATION_CONCRETE_INDEX_NAME));

            SearchApplication getSearchApp = awaitGetSearchApplication(app.name());
            assertThat(getSearchApp, equalTo(app));
        }

        DeleteResponse resp = awaitDeleteSearchApplication("my_search_app_4");
        assertThat(resp.status(), equalTo(RestStatus.OK));
        expectThrows(ResourceNotFoundException.class, () -> awaitGetSearchApplication("my_search_app_4"));
        GetAliasesResponse response = searchAppService.getAlias("my_search_app_4");
        assertTrue(response.getAliases().isEmpty());

        {
            SearchApplicationIndexService.SearchApplicationResult searchResponse = awaitListSearchApplication("*:*", 0, 10);
            final List<SearchApplicationListItem> apps = searchResponse.items();
            assertNotNull(apps);
            assertThat(apps.size(), equalTo(4));
            assertThat(searchResponse.totalResults(), equalTo(4L));

            for (int i = 0; i < 4; i++) {
                SearchApplicationListItem app = apps.get(i);
                assertThat(app.name(), equalTo("my_search_app_" + i));
                assertThat(app.indices(), equalTo(new String[] { "index_" + i }));
            }
        }
    }

    private IndexResponse awaitPutSearchApplication(SearchApplication app, boolean create) throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        final AtomicReference<IndexResponse> resp = new AtomicReference<>(null);
        final AtomicReference<Exception> exc = new AtomicReference<>(null);
        searchAppService.putSearchApplication(app, create, new ActionListener<>() {
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

    private SearchApplication awaitGetSearchApplication(String name) throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        final AtomicReference<SearchApplication> resp = new AtomicReference<>(null);
        final AtomicReference<Exception> exc = new AtomicReference<>(null);
        searchAppService.getSearchApplication(name, new ActionListener<>() {
            @Override
            public void onResponse(SearchApplication app) {
                resp.set(app);
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

    private DeleteResponse awaitDeleteSearchApplication(String name) throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        final AtomicReference<DeleteResponse> resp = new AtomicReference<>(null);
        final AtomicReference<Exception> exc = new AtomicReference<>(null);
        searchAppService.deleteSearchApplicationAndAlias(name, new ActionListener<>() {
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

    private SearchApplicationIndexService.SearchApplicationResult awaitListSearchApplication(String queryString, int from, int size)
        throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        final AtomicReference<SearchApplicationIndexService.SearchApplicationResult> resp = new AtomicReference<>(null);
        final AtomicReference<Exception> exc = new AtomicReference<>(null);
        searchAppService.listSearchApplication(queryString, from, size, new ActionListener<>() {
            @Override
            public void onResponse(SearchApplicationIndexService.SearchApplicationResult result) {
                resp.set(result);
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
     * Test plugin to register the {@link SearchApplicationIndexService} system index descriptor.
     */
    public static class TestPlugin extends Plugin implements SystemIndexPlugin {
        @Override
        public Collection<SystemIndexDescriptor> getSystemIndexDescriptors(Settings settings) {
            return List.of(SearchApplicationIndexService.getSystemIndexDescriptor());
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
