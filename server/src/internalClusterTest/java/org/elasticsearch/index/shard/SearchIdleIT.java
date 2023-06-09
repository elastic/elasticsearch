/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.shard;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsRequest;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.admin.indices.stats.ShardStats;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.MultiGetRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.query.ExistsQueryBuilder;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.index.refresh.RefreshStats;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.XContentType;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Phaser;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntToLongFunction;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoSearchHits;
import static org.hamcrest.Matchers.equalTo;

public class SearchIdleIT extends ESSingleNodeTestCase {

    public void testAutomaticRefreshSearch() throws InterruptedException {
        runTestAutomaticRefresh(numDocs -> client().prepareSearch("test").get().getHits().getTotalHits().value);
    }

    public void testAutomaticRefreshGet() throws InterruptedException {
        runTestAutomaticRefresh(numDocs -> {
            int count = 0;
            for (int i = 0; i < numDocs; i++) {
                final GetRequest request = new GetRequest();
                request.realtime(false);
                request.index("test");
                request.id("" + i);
                if (client().get(request).actionGet().isExists()) {
                    count++;
                }
            }
            return count;
        });
    }

    public void testAutomaticRefreshMultiGet() throws InterruptedException {
        runTestAutomaticRefresh(numDocs -> {
            final MultiGetRequest request = new MultiGetRequest();
            request.realtime(false);
            for (int i = 0; i < numDocs; i++) {
                request.add("test", "" + i);
            }
            return Arrays.stream(client().multiGet(request).actionGet().getResponses()).filter(r -> r.getResponse().isExists()).count();
        });
    }

    private void runTestAutomaticRefresh(final IntToLongFunction count) throws InterruptedException {
        TimeValue randomTimeValue = randomFrom(random(), null, TimeValue.ZERO, TimeValue.timeValueMillis(randomIntBetween(0, 1000)));
        Settings.Builder builder = Settings.builder();
        if (randomTimeValue != null) {
            builder.put(IndexSettings.INDEX_SEARCH_IDLE_AFTER.getKey(), randomTimeValue);
        }
        IndexService indexService = createIndex("test", builder.build());
        assertFalse(indexService.getIndexSettings().isExplicitRefresh());
        ensureGreen();
        AtomicInteger totalNumDocs = new AtomicInteger(Integer.MAX_VALUE);
        assertNoSearchHits(client().prepareSearch().get());
        int numDocs = scaledRandomIntBetween(25, 100);
        totalNumDocs.set(numDocs);
        CountDownLatch indexingDone = new CountDownLatch(numDocs);
        client().prepareIndex("test").setId("0").setSource("{\"foo\" : \"bar\"}", XContentType.JSON).get();
        indexingDone.countDown(); // one doc is indexed above blocking
        IndexShard shard = indexService.getShard(0);
        PlainActionFuture<Boolean> future = PlainActionFuture.newFuture();
        shard.scheduledRefresh(future);
        boolean hasRefreshed = future.actionGet();
        if (randomTimeValue == TimeValue.ZERO) {
            // with ZERO we are guaranteed to see the doc since we will wait for a refresh in the background
            assertFalse(hasRefreshed);
            assertTrue(shard.isSearchIdle());
        } else {
            if (randomTimeValue == null) {
                assertFalse(shard.isSearchIdle());
            }
            // we can't assert on hasRefreshed since it might have been refreshed in the background on the shard concurrently.
            // and if the background refresh wins the refresh race (both call maybeRefresh), the document might not be visible
            // until the background refresh is done.
            if (hasRefreshed == false) {
                ensureNoPendingScheduledRefresh(indexService.getThreadPool());
            }
        }

        CountDownLatch started = new CountDownLatch(1);
        Thread t = new Thread(() -> {
            started.countDown();
            do {

            } while (count.applyAsLong(totalNumDocs.get()) != totalNumDocs.get());
        });
        t.start();
        started.await();
        assertThat(count.applyAsLong(totalNumDocs.get()), equalTo(1L));
        for (int i = 1; i < numDocs; i++) {
            client().prepareIndex("test")
                .setId("" + i)
                .setSource("{\"foo\" : \"bar\"}", XContentType.JSON)
                .execute(new ActionListener<IndexResponse>() {
                    @Override
                    public void onResponse(IndexResponse indexResponse) {
                        indexingDone.countDown();
                    }

                    @Override
                    public void onFailure(Exception e) {
                        indexingDone.countDown();
                        throw new AssertionError(e);
                    }
                });
        }
        indexingDone.await();
        t.join();
        final IndicesStatsResponse statsResponse = client().admin().indices().stats(new IndicesStatsRequest()).actionGet();
        for (ShardStats shardStats : statsResponse.getShards()) {
            if (randomTimeValue != null && shardStats.isSearchIdle()) {
                assertTrue(shardStats.getSearchIdleTime() >= randomTimeValue.millis());
            }
        }
    }

    public void testPendingRefreshWithIntervalChange() throws Exception {
        Settings.Builder builder = Settings.builder();
        builder.put(IndexSettings.INDEX_SEARCH_IDLE_AFTER.getKey(), TimeValue.ZERO);
        IndexService indexService = createIndex("test", builder.build());
        assertFalse(indexService.getIndexSettings().isExplicitRefresh());
        ensureGreen();
        client().prepareIndex("test").setId("0").setSource("{\"foo\" : \"bar\"}", XContentType.JSON).get();
        IndexShard shard = indexService.getShard(0);
        scheduleRefresh(shard, false);
        assertTrue(shard.isSearchIdle());
        CountDownLatch refreshLatch = new CountDownLatch(1);
        // async on purpose to make sure it happens concurrently
        client().admin().indices().prepareRefresh().execute(ActionListener.running(refreshLatch::countDown));
        assertHitCount(client().prepareSearch().get(), 1);
        client().prepareIndex("test").setId("1").setSource("{\"foo\" : \"bar\"}", XContentType.JSON).get();
        scheduleRefresh(shard, false);
        assertTrue(shard.hasRefreshPending());

        // now disable background refresh and make sure the refresh happens
        CountDownLatch updateSettingsLatch = new CountDownLatch(1);
        client().admin()
            .indices()
            .prepareUpdateSettings("test")
            .setSettings(Settings.builder().put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), -1).build())
            .execute(ActionListener.running(updateSettingsLatch::countDown));
        assertHitCount(client().prepareSearch().get(), 2);
        // wait for both to ensure we don't have in-flight operations
        updateSettingsLatch.await();
        refreshLatch.await();
        assertFalse(shard.hasRefreshPending());
        // We need to ensure a `scheduledRefresh` triggered by the internal refresh setting update is executed before we index a new doc;
        // otherwise, it will compete to call `Engine#maybeRefresh` with the `scheduledRefresh` that we are going to verify.
        ensureNoPendingScheduledRefresh(indexService.getThreadPool());
        client().prepareIndex("test").setId("2").setSource("{\"foo\" : \"bar\"}", XContentType.JSON).get();
        scheduleRefresh(shard, true);
        assertFalse(shard.hasRefreshPending());
        assertTrue(shard.isSearchIdle());
        assertHitCount(client().prepareSearch().get(), 3);
        final IndicesStatsResponse statsResponse = client().admin().indices().stats(new IndicesStatsRequest()).actionGet();
        for (ShardStats shardStats : statsResponse.getShards()) {
            if (shardStats.isSearchIdle()) {
                assertTrue(shardStats.getSearchIdleTime() >= TimeValue.ZERO.millis());
            }
        }
    }

    private static void scheduleRefresh(IndexShard shard, boolean expectRefresh) {
        PlainActionFuture<Boolean> future = PlainActionFuture.newFuture();
        shard.scheduledRefresh(future);
        if (expectRefresh) {
            assertTrue(future.actionGet());
        } else {
            assertFalse(future.actionGet());
        }
    }

    private void ensureNoPendingScheduledRefresh(ThreadPool threadPool) {
        // We can make sure that all scheduled refresh tasks are done by submitting *maximumPoolSize* blocking tasks,
        // then wait until all of them completed. Note that using ThreadPoolStats is not watertight as both queue and
        // active count can be 0 when ThreadPoolExecutor just takes a task out the queue but before marking it active.
        ThreadPoolExecutor refreshThreadPoolExecutor = (ThreadPoolExecutor) threadPool.executor(ThreadPool.Names.REFRESH);
        int maximumPoolSize = refreshThreadPoolExecutor.getMaximumPoolSize();
        Phaser barrier = new Phaser(maximumPoolSize + 1);
        for (int i = 0; i < maximumPoolSize; i++) {
            refreshThreadPoolExecutor.execute(barrier::arriveAndAwaitAdvance);
        }
        barrier.arriveAndAwaitAdvance();
    }

    public void testSearchIdleStats() throws InterruptedException {
        int searchIdleAfter = randomIntBetween(2, 5);
        final String indexName = randomAlphaOfLength(5).toLowerCase(Locale.ROOT);
        client().admin()
            .indices()
            .prepareCreate(indexName)
            .setSettings(
                Settings.builder()
                    .put(IndexSettings.INDEX_SEARCH_IDLE_AFTER.getKey(), searchIdleAfter + "s")
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, randomIntBetween(2, 7))
            )
            .get();
        waitUntil(
            () -> Arrays.stream(client().admin().indices().prepareStats(indexName).get().getShards()).allMatch(ShardStats::isSearchIdle),
            searchIdleAfter,
            TimeUnit.SECONDS
        );

        final IndicesStatsResponse statsResponse = client().admin().indices().prepareStats(indexName).get();
        assertTrue(Arrays.stream(statsResponse.getShards()).allMatch(ShardStats::isSearchIdle));
        assertTrue(Arrays.stream(statsResponse.getShards()).allMatch(x -> x.getSearchIdleTime() >= searchIdleAfter));
    }

    public void testSearchIdleBoolQueryMatchOneIndex() throws InterruptedException {
        // GIVEN
        final String idleIndex = "test1";
        final String activeIndex = "test2";
        // NOTE: we need many shards because shard pre-filtering and the "can match" phase
        // are executed only if we have enough shards.
        int idleIndexShardsCount = 3;
        int activeIndexShardsCount = 3;
        createIndex(
            idleIndex,
            Settings.builder()
                .put(IndexSettings.INDEX_SEARCH_IDLE_AFTER.getKey(), "500ms")
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, idleIndexShardsCount)
                .put(IndexSettings.MODE.getKey(), IndexMode.TIME_SERIES)
                .put(IndexMetadata.INDEX_ROUTING_PATH.getKey(), "routing_field")
                .put(IndexSettings.TIME_SERIES_START_TIME.getKey(), "2021-05-10T00:00:00.000Z")
                .put(IndexSettings.TIME_SERIES_END_TIME.getKey(), "2021-05-11T00:00:00.000Z")
                .build(),
            "doc",
            "keyword",
            "type=keyword",
            "@timestamp",
            "type=date",
            "routing_field",
            "type=keyword,time_series_dimension=true"
        );
        createIndex(
            activeIndex,
            Settings.builder()
                .put(IndexSettings.INDEX_SEARCH_IDLE_AFTER.getKey(), "500ms")
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, activeIndexShardsCount)
                .put(IndexSettings.MODE.getKey(), IndexMode.TIME_SERIES)
                .put(IndexMetadata.INDEX_ROUTING_PATH.getKey(), "routing_field")
                .put(IndexSettings.TIME_SERIES_START_TIME.getKey(), "2021-05-12T00:00:00.000Z")
                .put(IndexSettings.TIME_SERIES_END_TIME.getKey(), "2021-05-13T23:59:59.999Z")
                .build(),
            "doc",
            "keyword",
            "type=keyword",
            "@timestamp",
            "type=date",
            "routing_field",
            "type=keyword,time_series_dimension=true"
        );

        assertEquals(
            RestStatus.CREATED,
            client().prepareIndex(idleIndex)
                .setSource("keyword", "idle", "@timestamp", "2021-05-10T19:00:03.765Z", "routing_field", "aaa")
                .get()
                .status()
        );
        assertEquals(
            RestStatus.CREATED,
            client().prepareIndex(activeIndex)
                .setSource("keyword", "active", "@timestamp", "2021-05-12T20:07:12.112Z", "routing_field", "aaa")
                .get()
                .status()
        );
        assertEquals(RestStatus.OK, client().admin().indices().prepareRefresh(idleIndex, activeIndex).get().getStatus());

        waitUntil(
            () -> Arrays.stream(client().admin().indices().prepareStats(idleIndex, activeIndex).get().getShards())
                .allMatch(ShardStats::isSearchIdle),
            2,
            TimeUnit.SECONDS
        );

        final IndicesStatsResponse idleIndexStatsBefore = client().admin().indices().prepareStats("test1").get();
        assertIdleShard(idleIndexStatsBefore);

        final IndicesStatsResponse activeIndexStatsBefore = client().admin().indices().prepareStats("test2").get();
        assertIdleShard(activeIndexStatsBefore);

        // WHEN
        final SearchResponse searchResponse = client().prepareSearch("test*")
            .setQuery(new RangeQueryBuilder("@timestamp").from("2021-05-12T20:00:00.000Z").to("2021-05-12T21:00:00.000Z"))
            .setPreFilterShardSize(5)
            .get();

        // THEN
        assertEquals(RestStatus.OK, searchResponse.status());
        assertEquals(idleIndexShardsCount + activeIndexShardsCount - 1, searchResponse.getSkippedShards());
        assertEquals(0, searchResponse.getFailedShards());
        Arrays.stream(searchResponse.getHits().getHits()).forEach(searchHit -> assertEquals("test2", searchHit.getIndex()));
        // NOTE: we need an empty result from at least one shard
        assertEquals(1, searchResponse.getHits().getHits().length);
        final IndicesStatsResponse idleIndexStatsAfter = client().admin().indices().prepareStats(idleIndex).get();
        assertIdleShardsRefreshStats(idleIndexStatsBefore, idleIndexStatsAfter);
    }

    public void testSearchIdleExistsQueryMatchOneIndex() throws InterruptedException {
        // GIVEN
        final String idleIndex = "test1";
        final String activeIndex = "test2";
        // NOTE: we need many shards because shard pre-filtering and the "can match" phase
        // are executed only if we have enough shards.
        int idleIndexShardsCount = 3;
        int activeIndexShardsCount = 3;
        createIndex(
            idleIndex,
            Settings.builder()
                .put(IndexSettings.INDEX_SEARCH_IDLE_AFTER.getKey(), "500ms")
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, idleIndexShardsCount)
                .build(),
            "doc",
            "keyword",
            "type=keyword"
        );
        createIndex(
            activeIndex,
            Settings.builder()
                .put(IndexSettings.INDEX_SEARCH_IDLE_AFTER.getKey(), "500ms")
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, activeIndexShardsCount)
                .build(),
            "doc",
            "keyword",
            "type=keyword"
        );

        assertEquals(RestStatus.CREATED, client().prepareIndex(idleIndex).setSource("keyword", "idle").get().status());
        assertEquals(
            RestStatus.CREATED,
            client().prepareIndex(activeIndex).setSource("keyword", "active", "unmapped", "bbb").get().status()
        );
        assertEquals(RestStatus.OK, client().admin().indices().prepareRefresh(idleIndex, activeIndex).get().getStatus());

        waitUntil(
            () -> Arrays.stream(client().admin().indices().prepareStats(idleIndex, activeIndex).get().getShards())
                .allMatch(ShardStats::isSearchIdle),
            2,
            TimeUnit.SECONDS
        );

        final IndicesStatsResponse idleIndexStatsBefore = client().admin().indices().prepareStats("test1").get();
        assertIdleShard(idleIndexStatsBefore);

        final IndicesStatsResponse activeIndexStatsBefore = client().admin().indices().prepareStats("test2").get();
        assertIdleShard(activeIndexStatsBefore);

        // WHEN
        final SearchResponse searchResponse = client().prepareSearch("test*")
            .setQuery(new ExistsQueryBuilder("unmapped"))
            .setPreFilterShardSize(5)
            .get();

        // THEN
        assertEquals(RestStatus.OK, searchResponse.status());
        assertEquals(idleIndexShardsCount, searchResponse.getSkippedShards());
        assertEquals(0, searchResponse.getFailedShards());
        Arrays.stream(searchResponse.getHits().getHits()).forEach(searchHit -> assertEquals("test2", searchHit.getIndex()));
        // NOTE: we need an empty result from at least one shard
        assertEquals(1, searchResponse.getHits().getHits().length);
        final IndicesStatsResponse idleIndexStatsAfter = client().admin().indices().prepareStats(idleIndex).get();
        assertIdleShardsRefreshStats(idleIndexStatsBefore, idleIndexStatsAfter);
    }

    private static void assertIdleShard(final IndicesStatsResponse statsResponse) {
        Arrays.stream(statsResponse.getShards()).forEach(shardStats -> assertTrue(shardStats.isSearchIdle()));
        Arrays.stream(statsResponse.getShards()).forEach(shardStats -> assertTrue(shardStats.getSearchIdleTime() >= 100));
    }

    private static void assertIdleShardsRefreshStats(final IndicesStatsResponse before, final IndicesStatsResponse after) {
        assertNotEquals(0, before.getShards().length);
        assertNotEquals(0, after.getShards().length);
        final List<RefreshStats> refreshStatsBefore = Arrays.stream(before.getShards()).map(x -> x.getStats().refresh).toList();
        final List<RefreshStats> refreshStatsAfter = Arrays.stream(after.getShards()).map(x -> x.getStats().refresh).toList();
        assertEquals(refreshStatsBefore.size(), refreshStatsAfter.size());
        assertTrue(refreshStatsAfter.containsAll(refreshStatsBefore));
        assertTrue(refreshStatsBefore.containsAll(refreshStatsAfter));
    }
}
