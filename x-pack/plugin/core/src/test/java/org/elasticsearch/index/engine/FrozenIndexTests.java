/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.index.engine;

import org.elasticsearch.action.admin.indices.open.OpenIndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardTestCase;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.xpack.core.XPackClient;
import org.elasticsearch.xpack.core.XPackPlugin;
import org.elasticsearch.xpack.core.action.TransportOpenIndexAndFreezeAction;
import org.hamcrest.Matchers;

import java.util.Collection;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;

public class FrozenIndexTests extends ESSingleNodeTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(XPackPlugin.class);
    }

    public void testCloseFreezeAndOpen() throws ExecutionException, InterruptedException {
        createIndex("index", Settings.builder().put("index.number_of_shards", 2).build());
        client().prepareIndex("index", "type", "1").setSource("field", "value").setRefreshPolicy(IMMEDIATE).get();
        client().prepareIndex("index", "type", "2").setSource("field", "value").setRefreshPolicy(IMMEDIATE).get();
        client().prepareIndex("index", "type", "3").setSource("field", "value").setRefreshPolicy(IMMEDIATE).get();

        client().admin().indices().prepareFlush("index").get();
        client().admin().indices().prepareClose("index").get();
        XPackClient xPackClient = new XPackClient(client());
        PlainActionFuture<OpenIndexResponse> future = new PlainActionFuture<>();
        xPackClient.openAndFreeze(new TransportOpenIndexAndFreezeAction.OpenIndexAndFreezeRequest("index"), future);
        future.get();
        expectThrows(ClusterBlockException.class, () -> client().prepareIndex("index", "type", "4").setSource("field", "value")
            .setRefreshPolicy(IMMEDIATE).get());
        IndicesService indexServices = getInstanceFromNode(IndicesService.class);
        Index index = resolveIndex("index");
        IndexService indexService = indexServices.indexServiceSafe(index);
        IndexShard shard = indexService.getShard(0);
        Engine engine = IndexShardTestCase.getEngine(shard);
        assertEquals(0, shard.refreshStats().getTotal());
        boolean useDFS = randomBoolean();
        assertHitCount(client().prepareSearch().setIndicesOptions(IndicesOptions.STRICT_EXPAND_OPEN_FORBID_CLOSED)
            .setSearchType(useDFS ? SearchType.DFS_QUERY_THEN_FETCH : SearchType.QUERY_THEN_FETCH).get(), 3);
        assertThat(engine, Matchers.instanceOf(FrozenEngine.class));
        assertEquals(useDFS ? 3 : 2, shard.refreshStats().getTotal());
        assertFalse(((FrozenEngine)engine).isReaderOpen());
        assertTrue(indexService.getIndexSettings().isSearchThrottled());
        try (Engine.Searcher searcher = shard.acquireSearcher("test")) {
            assertNotNull(FrozenEngine.unwrapLazyReader(searcher.getDirectoryReader()));
        }
        // now scroll
        SearchResponse searchResponse = client().prepareSearch().setIndicesOptions(IndicesOptions.STRICT_EXPAND_OPEN_FORBID_CLOSED)
            .setScroll(TimeValue.timeValueMinutes(1)).setSize(1).get();
        do {
            assertHitCount(searchResponse, 3);
            assertEquals(1, searchResponse.getHits().getHits().length);
            SearchService searchService = getInstanceFromNode(SearchService.class);
            assertThat(searchService.getActiveContexts(), Matchers.greaterThanOrEqualTo(1));
            for (int i = 0; i < 2; i++) {
                shard = indexService.getShard(i);
                engine = IndexShardTestCase.getEngine(shard);
                assertFalse(((FrozenEngine) engine).isReaderOpen());
            }
            searchResponse = client().prepareSearchScroll(searchResponse.getScrollId()).setScroll(TimeValue.timeValueMinutes(1)).get();
        } while (searchResponse.getHits().getHits().length > 0);

    }

    public void testFreezeAndUnfreeze() throws ExecutionException, InterruptedException {
        createIndex("index", Settings.builder().put("index.number_of_shards", 2).build());
        client().prepareIndex("index", "type", "1").setSource("field", "value").setRefreshPolicy(IMMEDIATE).get();
        client().prepareIndex("index", "type", "2").setSource("field", "value").setRefreshPolicy(IMMEDIATE).get();
        client().prepareIndex("index", "type", "3").setSource("field", "value").setRefreshPolicy(IMMEDIATE).get();

        client().admin().indices().prepareFlush("index").get();
        client().admin().indices().prepareClose("index").get();
        XPackClient xPackClient = new XPackClient(client());
        PlainActionFuture<OpenIndexResponse> future = new PlainActionFuture<>();
        TransportOpenIndexAndFreezeAction.OpenIndexAndFreezeRequest request =
            new TransportOpenIndexAndFreezeAction.OpenIndexAndFreezeRequest("index");
        xPackClient.openAndFreeze(request, future);
        future.get();
        {
            IndicesService indexServices = getInstanceFromNode(IndicesService.class);
            Index index = resolveIndex("index");
            IndexService indexService = indexServices.indexServiceSafe(index);
            assertTrue(indexService.getIndexSettings().isSearchThrottled());
            IndexShard shard = indexService.getShard(0);
            Engine engine = IndexShardTestCase.getEngine(shard);
            assertEquals(0, shard.refreshStats().getTotal());
            client().admin().indices().prepareClose("index").get();
        }
        request.setFreeze(false);
        PlainActionFuture<OpenIndexResponse> future1= new PlainActionFuture<>();
        xPackClient.openAndFreeze(request, future1);
        future1.get();
        client().admin().indices().prepareUpdateSettings("index").setSettings(Settings.builder().put("index.blocks.write", false)).get();
        {
            IndicesService indexServices = getInstanceFromNode(IndicesService.class);
            Index index = resolveIndex("index");
            IndexService indexService = indexServices.indexServiceSafe(index);
            assertFalse(indexService.getIndexSettings().isSearchThrottled());
            IndexShard shard = indexService.getShard(0);
            Engine engine = IndexShardTestCase.getEngine(shard);
            assertThat(engine, Matchers.instanceOf(InternalEngine.class));
        }
        client().prepareIndex("index", "type", "4").setSource("field", "value").setRefreshPolicy(IMMEDIATE).get();
    }
}
