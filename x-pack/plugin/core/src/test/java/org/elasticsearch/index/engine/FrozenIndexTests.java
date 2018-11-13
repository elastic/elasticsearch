/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.index.engine;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardTestCase;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.xpack.core.XPackClient;
import org.elasticsearch.xpack.core.XPackPlugin;
import org.elasticsearch.xpack.core.action.TransportFreezeIndexAction;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;

public class FrozenIndexTests extends ESSingleNodeTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(XPackPlugin.class);
    }

    public void testCloseFreezeAndOpen() throws ExecutionException, InterruptedException {
        createIndex("index", Settings.builder().put("index.number_of_shards", 2).build());
        client().prepareIndex("index", "_doc", "1").setSource("field", "value").setRefreshPolicy(IMMEDIATE).get();
        client().prepareIndex("index", "_doc", "2").setSource("field", "value").setRefreshPolicy(IMMEDIATE).get();
        client().prepareIndex("index", "_doc", "3").setSource("field", "value").setRefreshPolicy(IMMEDIATE).get();

        client().admin().indices().prepareFlush("index").get();
        client().admin().indices().prepareClose("index").get();
        XPackClient xPackClient = new XPackClient(client());
        PlainActionFuture<AcknowledgedResponse> future = new PlainActionFuture<>();
        xPackClient.freeze(new TransportFreezeIndexAction.FreezeRequest("index"), future);
        assertAcked(future.get());
        assertAcked(client().admin().indices().prepareOpen("index"));
        expectThrows(ClusterBlockException.class, () -> client().prepareIndex("index", "_doc", "4").setSource("field", "value")
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

    public void testSearchAndGetAPIsAreThrottled() throws ExecutionException, InterruptedException, IOException {
        XContentBuilder mapping = XContentFactory.jsonBuilder().startObject().startObject("_doc")
            .startObject("properties").startObject("field").field("type", "text").field("term_vector", "with_positions_offsets_payloads")
            .endObject().endObject()
            .endObject().endObject();
        createIndex("index", Settings.builder().put("index.number_of_shards", 2).build(), "_doc", mapping);
        for (int i = 0; i < 10; i++) {
            client().prepareIndex("index", "_doc", "" + i).setSource("field", "foo bar baz").get();
        }
        client().admin().indices().prepareFlush("index").get();
        client().admin().indices().prepareClose("index").get();
        XPackClient xPackClient = new XPackClient(client());
        PlainActionFuture<AcknowledgedResponse> future = new PlainActionFuture<>();
        TransportFreezeIndexAction.FreezeRequest request =
            new TransportFreezeIndexAction.FreezeRequest("index");
        xPackClient.freeze(request, future);
        assertAcked(future.get());
        assertAcked(client().admin().indices().prepareOpen("index"));
        int numRequests = randomIntBetween(20, 50);
        CountDownLatch latch = new CountDownLatch(numRequests);
        ActionListener listener = ActionListener.wrap(latch::countDown);
        int numRefreshes = 0;
        for (int i = 0; i < numRequests; i++) {
            numRefreshes++;
            switch (randomIntBetween(0, 3)) {
                case 0:
                    client().prepareGet("index", "_doc", "" + randomIntBetween(0, 9)).execute(listener);
                    break;
                case 1:
                    client().prepareSearch("index").setIndicesOptions(IndicesOptions.STRICT_EXPAND_OPEN_FORBID_CLOSED)
                        .setSearchType(SearchType.QUERY_THEN_FETCH).execute(listener);
                    // in total 4 refreshes 1x query & 1x fetch per shard (we have 2)
                    numRefreshes += 3;
                    break;
                case 2:
                    client().prepareTermVectors("index", "_doc", "" + randomIntBetween(0, 9)).execute(listener);
                    break;
                case 3:
                    client().prepareExplain("index", "_doc", "" + randomIntBetween(0, 9)).setQuery(new MatchAllQueryBuilder())
                        .execute(listener);
                    break;
                    default:
                        assert false;
            }
        }
        latch.await();
        IndicesStatsResponse index = client().admin().indices().prepareStats("index").clear().setRefresh(true).get();
        assertEquals(numRefreshes, index.getTotal().refresh.getTotal());
    }

    public void testFreezeAndUnfreeze() throws ExecutionException, InterruptedException {
        createIndex("index", Settings.builder().put("index.number_of_shards", 2).build());
        client().prepareIndex("index", "_doc", "1").setSource("field", "value").setRefreshPolicy(IMMEDIATE).get();
        client().prepareIndex("index", "_doc", "2").setSource("field", "value").setRefreshPolicy(IMMEDIATE).get();
        client().prepareIndex("index", "_doc", "3").setSource("field", "value").setRefreshPolicy(IMMEDIATE).get();

        client().admin().indices().prepareFlush("index").get();
        client().admin().indices().prepareClose("index").get();
        XPackClient xPackClient = new XPackClient(client());
        PlainActionFuture<AcknowledgedResponse> future = new PlainActionFuture<>();
        TransportFreezeIndexAction.FreezeRequest request =
            new TransportFreezeIndexAction.FreezeRequest("index");
        xPackClient.freeze(request, future);
        assertAcked(future.get());
        assertAcked(client().admin().indices().prepareOpen("index"));
        {
            IndicesService indexServices = getInstanceFromNode(IndicesService.class);
            Index index = resolveIndex("index");
            IndexService indexService = indexServices.indexServiceSafe(index);
            assertTrue(indexService.getIndexSettings().isSearchThrottled());
            IndexShard shard = indexService.getShard(0);
            assertEquals(0, shard.refreshStats().getTotal());
        }
        client().admin().indices().prepareClose("index").get();
        request.setFreeze(false);
        PlainActionFuture<AcknowledgedResponse> future1= new PlainActionFuture<>();
        xPackClient.freeze(request, future1);
        assertAcked(future1.get());
        assertAcked(client().admin().indices().prepareOpen("index"));
        {
            IndicesService indexServices = getInstanceFromNode(IndicesService.class);
            Index index = resolveIndex("index");
            IndexService indexService = indexServices.indexServiceSafe(index);
            assertFalse(indexService.getIndexSettings().isSearchThrottled());
            IndexShard shard = indexService.getShard(0);
            Engine engine = IndexShardTestCase.getEngine(shard);
            assertThat(engine, Matchers.instanceOf(InternalEngine.class));
        }
        client().prepareIndex("index", "_doc", "4").setSource("field", "value").setRefreshPolicy(IMMEDIATE).get();
    }

    public void testIndexMustBeClosed() {
        createIndex("test-idx", Settings.builder().put("index.number_of_shards", 2).build());
        XPackClient xPackClient = new XPackClient(client());
        PlainActionFuture<AcknowledgedResponse> future = new PlainActionFuture<>();
        TransportFreezeIndexAction.FreezeRequest request =
            new TransportFreezeIndexAction.FreezeRequest("test-idx");
        xPackClient.freeze(request, future);
        ExecutionException executionException = expectThrows(ExecutionException.class, () -> future.get());
        assertThat(executionException.getCause(), Matchers.instanceOf(IllegalStateException.class));
        assertEquals("index [test-idx] is not closed", executionException.getCause().getMessage());
    }
}
