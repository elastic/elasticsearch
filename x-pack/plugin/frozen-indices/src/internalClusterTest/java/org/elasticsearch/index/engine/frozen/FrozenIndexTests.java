/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.index.engine.frozen;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.ClosePointInTimeAction;
import org.elasticsearch.action.search.ClosePointInTimeRequest;
import org.elasticsearch.action.search.OpenPointInTimeAction;
import org.elasticsearch.action.search.OpenPointInTimeRequest;
import org.elasticsearch.action.search.OpenPointInTimeResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.search.TransportSearchAction;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.InternalEngine;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.shard.IndexLongFieldRange;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardTestCase;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.protocol.xpack.frozen.FreezeRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchContextMissingException;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.builder.PointInTimeBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.search.internal.ShardSearchContextId;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.core.LocalStateCompositeXPackPlugin;
import org.elasticsearch.xpack.core.frozen.action.FreezeIndexAction;
import org.elasticsearch.xpack.frozen.FrozenIndices;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.time.Instant;
import java.util.Collection;
import java.util.EnumSet;

import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.sameInstance;

public class FrozenIndexTests extends ESSingleNodeTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(FrozenIndices.class, LocalStateCompositeXPackPlugin.class);
    }

    String openReaders(TimeValue keepAlive, String... indices) {
        OpenPointInTimeRequest request = new OpenPointInTimeRequest(indices).indicesOptions(IndicesOptions.STRICT_EXPAND_OPEN_FORBID_CLOSED)
            .keepAlive(keepAlive);
        final OpenPointInTimeResponse response = client().execute(OpenPointInTimeAction.INSTANCE, request).actionGet();
        return response.getPointInTimeId();
    }

    public void testCloseFreezeAndOpen() throws Exception {
        String indexName = "index";
        createIndex(indexName, Settings.builder().put("index.number_of_shards", 2).build());
        client().prepareIndex(indexName).setId("1").setSource("field", "value").setRefreshPolicy(IMMEDIATE).get();
        client().prepareIndex(indexName).setId("2").setSource("field", "value").setRefreshPolicy(IMMEDIATE).get();
        client().prepareIndex(indexName).setId("3").setSource("field", "value").setRefreshPolicy(IMMEDIATE).get();
        assertAcked(client().execute(FreezeIndexAction.INSTANCE, new FreezeRequest(indexName)).actionGet());
        expectThrows(
            ClusterBlockException.class,
            () -> client().prepareIndex(indexName).setId("4").setSource("field", "value").setRefreshPolicy(IMMEDIATE).get()
        );
        IndicesService indexServices = getInstanceFromNode(IndicesService.class);
        Index index = resolveIndex(indexName);
        IndexService indexService = indexServices.indexServiceSafe(index);
        IndexShard shard = indexService.getShard(0);
        Engine engine = IndexShardTestCase.getEngine(shard);
        assertEquals(0, shard.refreshStats().getTotal());
        boolean useDFS = randomBoolean();
        assertHitCount(
            client().prepareSearch()
                .setIndicesOptions(IndicesOptions.STRICT_EXPAND_OPEN_FORBID_CLOSED)
                .setSearchType(useDFS ? SearchType.DFS_QUERY_THEN_FETCH : SearchType.QUERY_THEN_FETCH)
                .get(),
            3
        );
        assertThat(engine, Matchers.instanceOf(FrozenEngine.class));
        assertEquals(useDFS ? 3 : 2, shard.refreshStats().getTotal());
        assertFalse(((FrozenEngine) engine).isReaderOpen());
        assertTrue(indexService.getIndexSettings().isSearchThrottled());

        // now scroll
        SearchResponse searchResponse = client().prepareSearch()
            .setIndicesOptions(IndicesOptions.STRICT_EXPAND_OPEN_FORBID_CLOSED)
            .setScroll(TimeValue.timeValueMinutes(1))
            .setSize(1)
            .get();
        do {
            assertHitCount(searchResponse, 3);
            assertEquals(1, searchResponse.getHits().getHits().length);
            SearchService searchService = getInstanceFromNode(SearchService.class);
            assertThat(searchService.getActiveContexts(), Matchers.greaterThanOrEqualTo(1));
            for (int i = 0; i < 2; i++) {
                shard = indexService.getShard(i);
                engine = IndexShardTestCase.getEngine(shard);
                // scrolls keep the reader open
                assertTrue(((FrozenEngine) engine).isReaderOpen());
            }
            searchResponse = client().prepareSearchScroll(searchResponse.getScrollId()).setScroll(TimeValue.timeValueMinutes(1)).get();
        } while (searchResponse.getHits().getHits().length > 0);
        client().prepareClearScroll().addScrollId(searchResponse.getScrollId()).get();

        String pitId = openReaders(TimeValue.timeValueMinutes(1), indexName);
        try {
            for (int from = 0; from < 3; from++) {
                searchResponse = client().prepareSearch()
                    .setIndicesOptions(IndicesOptions.STRICT_EXPAND_OPEN_FORBID_CLOSED)
                    .setPointInTime(new PointInTimeBuilder(pitId))
                    .setSize(1)
                    .setFrom(from)
                    .get();
                assertHitCount(searchResponse, 3);
                assertEquals(1, searchResponse.getHits().getHits().length);
                SearchService searchService = getInstanceFromNode(SearchService.class);
                assertThat(searchService.getActiveContexts(), Matchers.greaterThanOrEqualTo(1));
                for (int i = 0; i < 2; i++) {
                    shard = indexService.getShard(i);
                    engine = IndexShardTestCase.getEngine(shard);
                    assertFalse(((FrozenEngine) engine).isReaderOpen());
                }
            }
            assertWarnings(TransportSearchAction.FROZEN_INDICES_DEPRECATION_MESSAGE.replace("{}", indexName));
        } finally {
            client().execute(ClosePointInTimeAction.INSTANCE, new ClosePointInTimeRequest(pitId)).get();
        }
    }

    public void testSearchAndGetAPIsAreThrottled() throws IOException {
        XContentBuilder mapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("_doc")
            .startObject("properties")
            .startObject("field")
            .field("type", "text")
            .field("term_vector", "with_positions_offsets_payloads")
            .endObject()
            .endObject()
            .endObject()
            .endObject();
        String indexName = "index";
        createIndex(indexName, Settings.builder().put("index.number_of_shards", 2).build(), mapping);
        for (int i = 0; i < 10; i++) {
            client().prepareIndex(indexName).setId("" + i).setSource("field", "foo bar baz").get();
        }
        assertAcked(client().execute(FreezeIndexAction.INSTANCE, new FreezeRequest(indexName)).actionGet());
        int numRequests = randomIntBetween(20, 50);
        int numRefreshes = 0;
        int numSearches = 0;
        for (int i = 0; i < numRequests; i++) {
            numRefreshes++;
            // make sure that we don't share the frozen reader in concurrent requests since we acquire the
            // searcher and rewrite the request outside of the search-throttle thread pool
            switch (between(0, 3)) {
                case 0 -> client().prepareGet(indexName, "" + randomIntBetween(0, 9)).get();
                case 1 -> {
                    numSearches++;
                    client().prepareSearch(indexName)
                        .setIndicesOptions(IndicesOptions.STRICT_EXPAND_OPEN_FORBID_CLOSED)
                        .setSearchType(SearchType.QUERY_THEN_FETCH)
                        .get();
                    // in total 4 refreshes 1x query & 1x fetch per shard (we have 2)
                    numRefreshes += 3;
                }
                case 2 -> client().prepareTermVectors(indexName, "" + randomIntBetween(0, 9)).get();
                case 3 -> client().prepareExplain(indexName, "" + randomIntBetween(0, 9)).setQuery(new MatchAllQueryBuilder()).get();
                default -> throw new AssertionError("unexpected value");
            }
        }
        IndicesStatsResponse index = indicesAdmin().prepareStats(indexName).clear().setRefresh(true).get();
        assertEquals(numRefreshes, index.getTotal().refresh.getTotal());
        if (numSearches > 0) {
            assertWarnings(TransportSearchAction.FROZEN_INDICES_DEPRECATION_MESSAGE.replace("{}", indexName));
        }
    }

    public void testFreezeAndUnfreeze() {
        final IndexService originalIndexService = createIndex("index", Settings.builder().put("index.number_of_shards", 2).build());
        assertThat(originalIndexService.getMetadata().getTimestampRange(), sameInstance(IndexLongFieldRange.UNKNOWN));

        client().prepareIndex("index").setId("1").setSource("field", "value").setRefreshPolicy(IMMEDIATE).get();
        client().prepareIndex("index").setId("2").setSource("field", "value").setRefreshPolicy(IMMEDIATE).get();
        client().prepareIndex("index").setId("3").setSource("field", "value").setRefreshPolicy(IMMEDIATE).get();

        if (randomBoolean()) {
            // sometimes close it
            assertAcked(indicesAdmin().prepareClose("index").get());
        }
        assertAcked(client().execute(FreezeIndexAction.INSTANCE, new FreezeRequest("index")).actionGet());
        {
            IndicesService indexServices = getInstanceFromNode(IndicesService.class);
            Index index = resolveIndex("index");
            IndexService indexService = indexServices.indexServiceSafe(index);
            assertTrue(indexService.getIndexSettings().isSearchThrottled());
            assertTrue(FrozenEngine.INDEX_FROZEN.get(indexService.getIndexSettings().getSettings()));
            assertTrue(FrozenEngine.INDEX_FROZEN.exists(indexService.getIndexSettings().getSettings()));
            IndexShard shard = indexService.getShard(0);
            assertEquals(0, shard.refreshStats().getTotal());
            assertThat(indexService.getMetadata().getTimestampRange(), sameInstance(IndexLongFieldRange.UNKNOWN));
        }
        assertAcked(client().execute(FreezeIndexAction.INSTANCE, new FreezeRequest("index").setFreeze(false)).actionGet());
        {
            IndicesService indexServices = getInstanceFromNode(IndicesService.class);
            Index index = resolveIndex("index");
            IndexService indexService = indexServices.indexServiceSafe(index);
            assertFalse(indexService.getIndexSettings().isSearchThrottled());
            assertFalse(FrozenEngine.INDEX_FROZEN.get(indexService.getIndexSettings().getSettings()));
            assertFalse(FrozenEngine.INDEX_FROZEN.exists(indexService.getIndexSettings().getSettings()));
            IndexShard shard = indexService.getShard(0);
            Engine engine = IndexShardTestCase.getEngine(shard);
            assertThat(engine, Matchers.instanceOf(InternalEngine.class));
            assertThat(indexService.getMetadata().getTimestampRange(), sameInstance(IndexLongFieldRange.UNKNOWN));
        }
        client().prepareIndex("index").setId("4").setSource("field", "value").setRefreshPolicy(IMMEDIATE).get();
    }

    private void assertIndexFrozen(String idx) {
        IndicesService indexServices = getInstanceFromNode(IndicesService.class);
        Index index = resolveIndex(idx);
        IndexService indexService = indexServices.indexServiceSafe(index);
        assertTrue(indexService.getIndexSettings().isSearchThrottled());
        assertTrue(FrozenEngine.INDEX_FROZEN.get(indexService.getIndexSettings().getSettings()));
    }

    public void testDoubleFreeze() {
        createIndex("test-idx", Settings.builder().put("index.number_of_shards", 2).build());
        assertAcked(client().execute(FreezeIndexAction.INSTANCE, new FreezeRequest("test-idx")).actionGet());
        ResourceNotFoundException exception = expectThrows(
            ResourceNotFoundException.class,
            () -> client().execute(
                FreezeIndexAction.INSTANCE,
                new FreezeRequest("test-idx").indicesOptions(
                    new IndicesOptions(EnumSet.noneOf(IndicesOptions.Option.class), EnumSet.of(IndicesOptions.WildcardStates.OPEN))
                )
            ).actionGet()
        );
        assertEquals("no index found to freeze", exception.getMessage());
    }

    public void testUnfreezeClosedIndices() {
        createIndex("idx", Settings.builder().put("index.number_of_shards", 1).build());
        client().prepareIndex("idx").setId("1").setSource("field", "value").setRefreshPolicy(IMMEDIATE).get();
        createIndex("idx-closed", Settings.builder().put("index.number_of_shards", 1).build());
        client().prepareIndex("idx-closed").setId("1").setSource("field", "value").setRefreshPolicy(IMMEDIATE).get();
        assertAcked(client().execute(FreezeIndexAction.INSTANCE, new FreezeRequest("idx")).actionGet());
        assertAcked(indicesAdmin().prepareClose("idx-closed").get());
        assertAcked(
            client().execute(
                FreezeIndexAction.INSTANCE,
                new FreezeRequest("idx*").setFreeze(false).indicesOptions(IndicesOptions.strictExpand())
            ).actionGet()
        );
        ClusterStateResponse stateResponse = client().admin().cluster().prepareState().get();
        assertEquals(IndexMetadata.State.CLOSE, stateResponse.getState().getMetadata().index("idx-closed").getState());
        assertEquals(IndexMetadata.State.OPEN, stateResponse.getState().getMetadata().index("idx").getState());
        assertHitCount(client().prepareSearch().get(), 1L);
    }

    public void testFreezePattern() {
        String indexName = "test-idx";
        createIndex(indexName, Settings.builder().put("index.number_of_shards", 1).build());
        client().prepareIndex(indexName).setId("1").setSource("field", "value").setRefreshPolicy(IMMEDIATE).get();
        createIndex("test-idx-1", Settings.builder().put("index.number_of_shards", 1).build());
        client().prepareIndex("test-idx-1").setId("1").setSource("field", "value").setRefreshPolicy(IMMEDIATE).get();
        assertAcked(client().execute(FreezeIndexAction.INSTANCE, new FreezeRequest(indexName)).actionGet());
        assertIndexFrozen(indexName);

        IndicesStatsResponse index = indicesAdmin().prepareStats(indexName).clear().setRefresh(true).get();
        assertEquals(0, index.getTotal().refresh.getTotal());
        assertHitCount(client().prepareSearch(indexName).setIndicesOptions(IndicesOptions.STRICT_EXPAND_OPEN_FORBID_CLOSED).get(), 1);
        index = indicesAdmin().prepareStats(indexName).clear().setRefresh(true).get();
        assertEquals(1, index.getTotal().refresh.getTotal());

        assertAcked(client().execute(FreezeIndexAction.INSTANCE, new FreezeRequest("test*")).actionGet());
        assertIndexFrozen(indexName);
        assertIndexFrozen("test-idx-1");
        index = indicesAdmin().prepareStats(indexName).clear().setRefresh(true).get();
        assertEquals(1, index.getTotal().refresh.getTotal());
        index = indicesAdmin().prepareStats("test-idx-1").clear().setRefresh(true).get();
        assertEquals(0, index.getTotal().refresh.getTotal());
        assertWarnings(TransportSearchAction.FROZEN_INDICES_DEPRECATION_MESSAGE.replace("{}", indexName));
    }

    public void testCanMatch() throws IOException {
        createIndex("index");
        client().prepareIndex("index").setId("1").setSource("field", "2010-01-05T02:00").setRefreshPolicy(IMMEDIATE).execute().actionGet();
        client().prepareIndex("index").setId("2").setSource("field", "2010-01-06T02:00").setRefreshPolicy(IMMEDIATE).execute().actionGet();
        {
            IndicesService indexServices = getInstanceFromNode(IndicesService.class);
            Index index = resolveIndex("index");
            IndexService indexService = indexServices.indexServiceSafe(index);
            IndexShard shard = indexService.getShard(0);
            assertFalse(indexService.getIndexSettings().isSearchThrottled());
            SearchService searchService = getInstanceFromNode(SearchService.class);
            SearchRequest searchRequest = new SearchRequest().allowPartialSearchResults(true);
            assertTrue(
                searchService.canMatch(
                    new ShardSearchRequest(OriginalIndices.NONE, searchRequest, shard.shardId(), 0, 1, AliasFilter.EMPTY, 1f, -1, null)
                ).canMatch()
            );

            SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
            searchRequest.source(sourceBuilder);
            sourceBuilder.query(QueryBuilders.rangeQuery("field").gte("2010-01-03||+2d").lte("2010-01-04||+2d/d"));
            assertTrue(
                searchService.canMatch(
                    new ShardSearchRequest(OriginalIndices.NONE, searchRequest, shard.shardId(), 0, 1, AliasFilter.EMPTY, 1f, -1, null)
                ).canMatch()
            );

            sourceBuilder.query(QueryBuilders.rangeQuery("field").gt("2010-01-06T02:00").lt("2010-01-07T02:00"));
            assertFalse(
                searchService.canMatch(
                    new ShardSearchRequest(OriginalIndices.NONE, searchRequest, shard.shardId(), 0, 1, AliasFilter.EMPTY, 1f, -1, null)
                ).canMatch()
            );
        }

        assertAcked(client().execute(FreezeIndexAction.INSTANCE, new FreezeRequest("index")).actionGet());
        {

            IndicesService indexServices = getInstanceFromNode(IndicesService.class);
            Index index = resolveIndex("index");
            IndexService indexService = indexServices.indexServiceSafe(index);
            IndexShard shard = indexService.getShard(0);
            assertTrue(indexService.getIndexSettings().isSearchThrottled());
            SearchService searchService = getInstanceFromNode(SearchService.class);
            SearchRequest searchRequest = new SearchRequest().allowPartialSearchResults(true);
            assertTrue(
                searchService.canMatch(
                    new ShardSearchRequest(OriginalIndices.NONE, searchRequest, shard.shardId(), 0, 1, AliasFilter.EMPTY, 1f, -1, null)
                ).canMatch()
            );

            SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
            sourceBuilder.query(QueryBuilders.rangeQuery("field").gte("2010-01-03||+2d").lte("2010-01-04||+2d/d"));
            searchRequest.source(sourceBuilder);
            assertTrue(
                searchService.canMatch(
                    new ShardSearchRequest(OriginalIndices.NONE, searchRequest, shard.shardId(), 0, 1, AliasFilter.EMPTY, 1f, -1, null)
                ).canMatch()
            );

            sourceBuilder.query(QueryBuilders.rangeQuery("field").gt("2010-01-06T02:00").lt("2010-01-07T02:00"));
            assertFalse(
                searchService.canMatch(
                    new ShardSearchRequest(OriginalIndices.NONE, searchRequest, shard.shardId(), 0, 1, AliasFilter.EMPTY, 1f, -1, null)
                ).canMatch()
            );

            IndicesStatsResponse response = indicesAdmin().prepareStats("index").clear().setRefresh(true).get();
            assertEquals(0, response.getTotal().refresh.getTotal());

            // Retry with point in time
            PlainActionFuture<ShardSearchContextId> openContextFuture = new PlainActionFuture<>();
            searchService.openReaderContext(shard.shardId(), TimeValue.timeValueSeconds(60), openContextFuture);
            final ShardSearchContextId contextId = openContextFuture.actionGet(TimeValue.timeValueSeconds(60));
            assertNotNull(contextId.getSearcherId());
            sourceBuilder.query(QueryBuilders.rangeQuery("field").gt("2010-01-06T02:00").lt("2010-01-07T02:00"));
            assertFalse(
                searchService.canMatch(
                    new ShardSearchRequest(
                        OriginalIndices.NONE,
                        searchRequest,
                        shard.shardId(),
                        0,
                        1,
                        AliasFilter.EMPTY,
                        1f,
                        -1,
                        null,
                        contextId,
                        null
                    )
                ).canMatch()
            );

            assertTrue(searchService.freeReaderContext(contextId));
            sourceBuilder.query(QueryBuilders.rangeQuery("field").gt("2010-01-06T02:00").lt("2010-01-07T02:00"));
            assertFalse(
                searchService.canMatch(
                    new ShardSearchRequest(
                        OriginalIndices.NONE,
                        searchRequest,
                        shard.shardId(),
                        0,
                        1,
                        AliasFilter.EMPTY,
                        1f,
                        -1,
                        null,
                        contextId,
                        null
                    )
                ).canMatch()
            );

            expectThrows(SearchContextMissingException.class, () -> {
                ShardSearchContextId withoutCommitId = new ShardSearchContextId(contextId.getSessionId(), contextId.getId(), null);
                sourceBuilder.query(QueryBuilders.rangeQuery("field").gt("2010-01-06T02:00").lt("2010-01-07T02:00"));
                assertFalse(
                    searchService.canMatch(
                        new ShardSearchRequest(
                            OriginalIndices.NONE,
                            searchRequest,
                            shard.shardId(),
                            0,
                            1,
                            AliasFilter.EMPTY,
                            1f,
                            -1,
                            null,
                            withoutCommitId,
                            null
                        )
                    ).canMatch()
                );
            });
        }
    }

    public void testWriteToFrozenIndex() {
        createIndex("idx", Settings.builder().put("index.number_of_shards", 1).build());
        client().prepareIndex("idx").setId("1").setSource("field", "value").setRefreshPolicy(IMMEDIATE).get();
        assertAcked(client().execute(FreezeIndexAction.INSTANCE, new FreezeRequest("idx")).actionGet());
        assertIndexFrozen("idx");
        expectThrows(
            ClusterBlockException.class,
            () -> client().prepareIndex("idx").setId("2").setSource("field", "value").setRefreshPolicy(IMMEDIATE).get()
        );
    }

    public void testIgnoreUnavailable() {
        createIndex("idx", Settings.builder().put("index.number_of_shards", 1).build());
        createIndex("idx-close", Settings.builder().put("index.number_of_shards", 1).build());
        assertAcked(indicesAdmin().prepareClose("idx-close"));
        assertAcked(
            client().execute(
                FreezeIndexAction.INSTANCE,
                new FreezeRequest("idx*", "not_available").indicesOptions(
                    IndicesOptions.fromParameters(null, "true", null, null, IndicesOptions.strictExpandOpen())
                )
            ).actionGet()
        );
        assertIndexFrozen("idx");
        assertEquals(
            IndexMetadata.State.CLOSE,
            client().admin().cluster().prepareState().get().getState().metadata().index("idx-close").getState()
        );
    }

    public void testUnfreezeClosedIndex() {
        createIndex("idx", Settings.builder().put("index.number_of_shards", 1).build());
        assertAcked(client().execute(FreezeIndexAction.INSTANCE, new FreezeRequest("idx")).actionGet());
        assertAcked(indicesAdmin().prepareClose("idx"));
        assertEquals(
            IndexMetadata.State.CLOSE,
            client().admin().cluster().prepareState().get().getState().metadata().index("idx").getState()
        );
        expectThrows(
            IndexNotFoundException.class,
            () -> client().execute(
                FreezeIndexAction.INSTANCE,
                new FreezeRequest("id*").setFreeze(false)
                    .indicesOptions(
                        new IndicesOptions(EnumSet.noneOf(IndicesOptions.Option.class), EnumSet.of(IndicesOptions.WildcardStates.OPEN))
                    )
            ).actionGet()
        );
        // we don't resolve to closed indices
        assertAcked(client().execute(FreezeIndexAction.INSTANCE, new FreezeRequest("idx").setFreeze(false)).actionGet());
        assertEquals(
            IndexMetadata.State.OPEN,
            client().admin().cluster().prepareState().get().getState().metadata().index("idx").getState()
        );
    }

    public void testFreezeIndexIncreasesIndexSettingsVersion() {
        final String index = "test";
        createIndex(index, indexSettings(1, 0).build());
        client().prepareIndex(index).setSource("field", "value").execute().actionGet();

        final long settingsVersion = client().admin()
            .cluster()
            .prepareState()
            .get()
            .getState()
            .metadata()
            .index(index)
            .getSettingsVersion();

        assertAcked(client().execute(FreezeIndexAction.INSTANCE, new FreezeRequest(index)).actionGet());
        assertIndexFrozen(index);
        assertThat(
            client().admin().cluster().prepareState().get().getState().metadata().index(index).getSettingsVersion(),
            greaterThan(settingsVersion)
        );
    }

    public void testFreezeEmptyIndexWithTranslogOps() throws Exception {
        final String indexName = "empty";
        createIndex(indexName, indexSettings(1, 0).put("index.refresh_interval", TimeValue.MINUS_ONE).build());

        final long nbNoOps = randomIntBetween(1, 10);
        for (long i = 0; i < nbNoOps; i++) {
            final DeleteResponse deleteResponse = client().prepareDelete(indexName, Long.toString(i)).get();
            assertThat(deleteResponse.status(), is(RestStatus.NOT_FOUND));
        }

        final IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        assertBusy(() -> {
            final Index index = client().admin().cluster().prepareState().get().getState().metadata().index(indexName).getIndex();
            final IndexService indexService = indicesService.indexService(index);
            assertThat(indexService.hasShard(0), is(true));
            assertThat(indexService.getShard(0).getLastKnownGlobalCheckpoint(), greaterThanOrEqualTo(nbNoOps - 1L));
        });

        assertAcked(client().execute(FreezeIndexAction.INSTANCE, new FreezeRequest(indexName)).actionGet());
        assertIndexFrozen(indexName);
    }

    public void testRecoveryState() {
        final String indexName = "index_recovery_state";
        createIndex(indexName, Settings.builder().put("index.number_of_replicas", 0).build());

        final long nbDocs = randomIntBetween(0, 50);
        for (long i = 0; i < nbDocs; i++) {
            final IndexResponse indexResponse = client().prepareIndex(indexName).setId(Long.toString(i)).setSource("field", i).get();
            assertThat(indexResponse.status(), is(RestStatus.CREATED));
        }

        assertAcked(client().execute(FreezeIndexAction.INSTANCE, new FreezeRequest(indexName)).actionGet());
        assertIndexFrozen(indexName);

        final IndexMetadata indexMetadata = client().admin().cluster().prepareState().get().getState().metadata().index(indexName);
        final IndexService indexService = getInstanceFromNode(IndicesService.class).indexService(indexMetadata.getIndex());
        for (int i = 0; i < indexMetadata.getNumberOfShards(); i++) {
            final IndexShard indexShard = indexService.getShardOrNull(i);
            assertThat("Shard [" + i + "] is missing for index " + indexMetadata.getIndex(), indexShard, notNullValue());
            final RecoveryState recoveryState = indexShard.recoveryState();
            assertThat(recoveryState.getRecoverySource(), is(RecoverySource.ExistingStoreRecoverySource.INSTANCE));
            assertThat(recoveryState.getStage(), is(RecoveryState.Stage.DONE));
            assertThat(recoveryState.getTargetNode(), notNullValue());
            assertThat(recoveryState.getIndex().totalFileCount(), greaterThan(0));
            assertThat(recoveryState.getIndex().reusedFileCount(), greaterThan(0));
            assertThat(recoveryState.getTranslog().recoveredOperations(), equalTo(0));
            assertThat(recoveryState.getTranslog().totalOperations(), equalTo(0));
            assertThat(recoveryState.getTranslog().recoveredPercent(), equalTo(100.0f));
        }
    }

    public void testTranslogStats() {
        final String indexName = "test";
        IndexService indexService = createIndex(indexName, Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0).build());

        final int nbDocs = randomIntBetween(0, 50);
        int uncommittedOps = 0;
        for (long i = 0; i < nbDocs; i++) {
            final IndexResponse indexResponse = client().prepareIndex(indexName).setId(Long.toString(i)).setSource("field", i).get();
            assertThat(indexResponse.status(), is(RestStatus.CREATED));
            if (rarely()) {
                indicesAdmin().prepareFlush(indexName).get();
                uncommittedOps = 0;
            } else {
                uncommittedOps += 1;
            }
        }

        IndicesStatsResponse stats = indicesAdmin().prepareStats(indexName).clear().setTranslog(true).get();
        assertThat(stats.getIndex(indexName), notNullValue());
        assertThat(
            stats.getIndex(indexName).getPrimaries().getTranslog().estimatedNumberOfOperations(),
            equalTo(indexService.getIndexSettings().isSoftDeleteEnabled() ? uncommittedOps : nbDocs)
        );
        assertThat(stats.getIndex(indexName).getPrimaries().getTranslog().getUncommittedOperations(), equalTo(uncommittedOps));

        assertAcked(client().execute(FreezeIndexAction.INSTANCE, new FreezeRequest(indexName)).actionGet());
        assertIndexFrozen(indexName);

        IndicesOptions indicesOptions = IndicesOptions.STRICT_EXPAND_OPEN;
        stats = indicesAdmin().prepareStats(indexName).setIndicesOptions(indicesOptions).clear().setTranslog(true).get();
        assertThat(stats.getIndex(indexName), notNullValue());
        assertThat(
            stats.getIndex(indexName).getPrimaries().getTranslog().estimatedNumberOfOperations(),
            equalTo(indexService.getIndexSettings().isSoftDeleteEnabled() ? 0 : nbDocs)
        );
        assertThat(stats.getIndex(indexName).getPrimaries().getTranslog().getUncommittedOperations(), equalTo(0));
    }

    public void testComputesTimestampRangeFromMilliseconds() {
        final int shardCount = between(1, 3);
        createIndex("index", Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, shardCount).build());
        client().prepareIndex("index").setSource(DataStream.TimestampField.FIXED_TIMESTAMP_FIELD, "2010-01-05T01:02:03.456Z").get();
        client().prepareIndex("index").setSource(DataStream.TimestampField.FIXED_TIMESTAMP_FIELD, "2010-01-06T02:03:04.567Z").get();

        assertAcked(client().execute(FreezeIndexAction.INSTANCE, new FreezeRequest("index")).actionGet());

        final IndexLongFieldRange timestampFieldRange = client().admin()
            .cluster()
            .prepareState()
            .get()
            .getState()
            .metadata()
            .index("index")
            .getTimestampRange();
        assertThat(timestampFieldRange, not(sameInstance(IndexLongFieldRange.UNKNOWN)));
        assertThat(timestampFieldRange, not(sameInstance(IndexLongFieldRange.EMPTY)));
        assertTrue(timestampFieldRange.isComplete());
        assertThat(timestampFieldRange.getMin(), equalTo(Instant.parse("2010-01-05T01:02:03.456Z").toEpochMilli()));
        assertThat(timestampFieldRange.getMax(), equalTo(Instant.parse("2010-01-06T02:03:04.567Z").toEpochMilli()));
    }

    public void testComputesTimestampRangeFromNanoseconds() throws IOException {

        final XContentBuilder mapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("properties")
            .startObject(DataStream.TimestampField.FIXED_TIMESTAMP_FIELD)
            .field("type", "date_nanos")
            .field("format", "strict_date_optional_time_nanos")
            .endObject()
            .endObject()
            .endObject();

        final int shardCount = between(1, 3);
        createIndex("index", Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, shardCount).build(), mapping);
        client().prepareIndex("index").setSource(DataStream.TimestampField.FIXED_TIMESTAMP_FIELD, "2010-01-05T01:02:03.456789012Z").get();
        client().prepareIndex("index").setSource(DataStream.TimestampField.FIXED_TIMESTAMP_FIELD, "2010-01-06T02:03:04.567890123Z").get();

        assertAcked(client().execute(FreezeIndexAction.INSTANCE, new FreezeRequest("index")).actionGet());

        final IndexLongFieldRange timestampFieldRange = client().admin()
            .cluster()
            .prepareState()
            .get()
            .getState()
            .metadata()
            .index("index")
            .getTimestampRange();
        assertThat(timestampFieldRange, not(sameInstance(IndexLongFieldRange.UNKNOWN)));
        assertThat(timestampFieldRange, not(sameInstance(IndexLongFieldRange.EMPTY)));
        assertTrue(timestampFieldRange.isComplete());
        final DateFieldMapper.Resolution resolution = DateFieldMapper.Resolution.NANOSECONDS;
        assertThat(timestampFieldRange.getMin(), equalTo(resolution.convert(Instant.parse("2010-01-05T01:02:03.456789012Z"))));
        assertThat(timestampFieldRange.getMax(), equalTo(resolution.convert(Instant.parse("2010-01-06T02:03:04.567890123Z"))));
    }

}
