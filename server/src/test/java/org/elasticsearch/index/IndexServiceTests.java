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

import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.TopDocs;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardTestCase;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.index.shard.IndexShardTestCase.getEngine;
import static org.elasticsearch.test.InternalSettingsPlugin.TRANSLOG_RETENTION_CHECK_INTERVAL_SETTING;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.core.IsEqual.equalTo;

/** Unit test(s) for IndexService */
public class IndexServiceTests extends ESSingleNodeTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return Collections.singleton(InternalSettingsPlugin.class);
    }

    public static CompressedXContent filter(QueryBuilder filterBuilder) throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder();
        filterBuilder.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.close();
        return new CompressedXContent(Strings.toString(builder));
    }

    public void testBaseAsyncTask() throws Exception {
        IndexService indexService = createIndex("test", Settings.EMPTY);
        AtomicReference<CountDownLatch> latch = new AtomicReference<>(new CountDownLatch(1));
        AtomicReference<CountDownLatch> latch2 = new AtomicReference<>(new CountDownLatch(1));
        final AtomicInteger count = new AtomicInteger();
        IndexService.BaseAsyncTask task = new IndexService.BaseAsyncTask(indexService, TimeValue.timeValueMillis(1)) {
            @Override
            protected void runInternal() {
                final CountDownLatch l1 = latch.get();
                final CountDownLatch l2 = latch2.get();
                count.incrementAndGet();
                assertTrue("generic threadpool is configured", Thread.currentThread().getName().contains("[generic]"));
                l1.countDown();
                try {
                    l2.await();
                } catch (InterruptedException e) {
                    fail("interrupted");
                }
                if (randomBoolean()) { // task can throw exceptions!!
                    if (randomBoolean()) {
                        throw new RuntimeException("foo");
                    } else {
                        throw new RuntimeException("bar");
                    }
                }
            }

            @Override
            protected String getThreadPool() {
                return ThreadPool.Names.GENERIC;
            }
        };

        latch.get().await();
        latch.set(new CountDownLatch(1));
        assertEquals(1, count.get());
        // here we need to swap first before we let it go otherwise threads might be very fast and run that task twice due to
        // random exception and the schedule interval is 1ms
        latch2.getAndSet(new CountDownLatch(1)).countDown();
        latch.get().await();
        assertEquals(2, count.get());
        task.close();
        latch2.get().countDown();
        assertEquals(2, count.get());

        task = new IndexService.BaseAsyncTask(indexService, TimeValue.timeValueMillis(1000000)) {
            @Override
            protected void runInternal() {

            }
        };
        assertTrue(task.mustReschedule());

        // now close the index
        final Index index = indexService.index();
        assertAcked(client().admin().indices().prepareClose(index.getName()));
        assertBusy(() -> assertTrue("Index not found: " + index.getName(), getInstanceFromNode(IndicesService.class).hasIndex(index)));

        final IndexService closedIndexService = getInstanceFromNode(IndicesService.class).indexServiceSafe(index);
        assertNotSame(indexService, closedIndexService);
        assertFalse(task.mustReschedule());
        assertFalse(task.isClosed());
        assertEquals(1000000, task.getInterval().millis());

        // now reopen the index
        assertAcked(client().admin().indices().prepareOpen(index.getName()));
        assertBusy(() -> assertTrue("Index not found: " + index.getName(), getInstanceFromNode(IndicesService.class).hasIndex(index)));
        indexService = getInstanceFromNode(IndicesService.class).indexServiceSafe(index);
        assertNotSame(closedIndexService, indexService);

        task = new IndexService.BaseAsyncTask(indexService, TimeValue.timeValueMillis(100000)) {
            @Override
            protected void runInternal() {

            }
        };
        assertTrue(task.mustReschedule());
        assertFalse(task.isClosed());
        assertTrue(task.isScheduled());

        indexService.close("simon says", false);
        assertFalse("no shards left", task.mustReschedule());
        assertTrue(task.isScheduled());
        task.close();
        assertFalse(task.isScheduled());
    }

    public void testRefreshTaskIsUpdated() throws Exception {
        IndexService indexService = createIndex("test", Settings.EMPTY);
        IndexService.AsyncRefreshTask refreshTask = indexService.getRefreshTask();
        assertEquals(1000, refreshTask.getInterval().millis());
        assertTrue(indexService.getRefreshTask().mustReschedule());

        // now disable
        client().admin().indices().prepareUpdateSettings("test")
            .setSettings(Settings.builder().put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), -1)).get();
        assertNotSame(refreshTask, indexService.getRefreshTask());
        assertTrue(refreshTask.isClosed());
        assertFalse(refreshTask.isScheduled());

        // set it to 100ms
        client().admin().indices().prepareUpdateSettings("test")
            .setSettings(Settings.builder().put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(),  "100ms")).get();
        assertNotSame(refreshTask, indexService.getRefreshTask());
        assertTrue(refreshTask.isClosed());

        refreshTask = indexService.getRefreshTask();
        assertTrue(refreshTask.mustReschedule());
        assertTrue(refreshTask.isScheduled());
        assertEquals(100, refreshTask.getInterval().millis());

        // set it to 200ms
        client().admin().indices().prepareUpdateSettings("test")
            .setSettings(Settings.builder().put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), "200ms")).get();
        assertNotSame(refreshTask, indexService.getRefreshTask());
        assertTrue(refreshTask.isClosed());

        refreshTask = indexService.getRefreshTask();
        assertTrue(refreshTask.mustReschedule());
        assertTrue(refreshTask.isScheduled());
        assertEquals(200, refreshTask.getInterval().millis());

        // set it to 200ms again
        client().admin().indices().prepareUpdateSettings("test")
            .setSettings(Settings.builder().put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), "200ms")).get();
        assertSame(refreshTask, indexService.getRefreshTask());
        assertTrue(indexService.getRefreshTask().mustReschedule());
        assertTrue(refreshTask.isScheduled());
        assertFalse(refreshTask.isClosed());
        assertEquals(200, refreshTask.getInterval().millis());

        // now close the index
        final Index index = indexService.index();
        assertAcked(client().admin().indices().prepareClose(index.getName()));
        assertBusy(() -> assertTrue("Index not found: " + index.getName(), getInstanceFromNode(IndicesService.class).hasIndex(index)));

        final IndexService closedIndexService = getInstanceFromNode(IndicesService.class).indexServiceSafe(index);
        assertNotSame(indexService, closedIndexService);
        assertNotSame(refreshTask, closedIndexService.getRefreshTask());
        assertFalse(closedIndexService.getRefreshTask().mustReschedule());
        assertFalse(closedIndexService.getRefreshTask().isClosed());
        assertEquals(200, closedIndexService.getRefreshTask().getInterval().millis());

        // now reopen the index
        assertAcked(client().admin().indices().prepareOpen(index.getName()));
        assertBusy(() -> assertTrue("Index not found: " + index.getName(), getInstanceFromNode(IndicesService.class).hasIndex(index)));
        indexService = getInstanceFromNode(IndicesService.class).indexServiceSafe(index);
        assertNotSame(closedIndexService, indexService);
        refreshTask = indexService.getRefreshTask();
        assertTrue(indexService.getRefreshTask().mustReschedule());
        assertTrue(refreshTask.isScheduled());
        assertFalse(refreshTask.isClosed());

        indexService.close("simon says", false);
        assertFalse(refreshTask.isScheduled());
        assertTrue(refreshTask.isClosed());
    }

    public void testFsyncTaskIsRunning() throws Exception {
        Settings settings = Settings.builder()
            .put(IndexSettings.INDEX_TRANSLOG_DURABILITY_SETTING.getKey(), Translog.Durability.ASYNC).build();
        IndexService indexService = createIndex("test", settings);
        IndexService.AsyncTranslogFSync fsyncTask = indexService.getFsyncTask();
        assertNotNull(fsyncTask);
        assertEquals(5000, fsyncTask.getInterval().millis());
        assertTrue(fsyncTask.mustReschedule());
        assertTrue(fsyncTask.isScheduled());

        // now close the index
        final Index index = indexService.index();
        assertAcked(client().admin().indices().prepareClose(index.getName()));
        assertBusy(() -> assertTrue("Index not found: " + index.getName(), getInstanceFromNode(IndicesService.class).hasIndex(index)));

        final IndexService closedIndexService = getInstanceFromNode(IndicesService.class).indexServiceSafe(index);
        assertNotSame(indexService, closedIndexService);
        assertNotSame(fsyncTask, closedIndexService.getFsyncTask());
        assertFalse(closedIndexService.getFsyncTask().mustReschedule());
        assertFalse(closedIndexService.getFsyncTask().isClosed());
        assertEquals(5000, closedIndexService.getFsyncTask().getInterval().millis());

        // now reopen the index
        assertAcked(client().admin().indices().prepareOpen(index.getName()));
        assertBusy(() -> assertTrue("Index not found: " + index.getName(), getInstanceFromNode(IndicesService.class).hasIndex(index)));
        indexService = getInstanceFromNode(IndicesService.class).indexServiceSafe(index);
        assertNotSame(closedIndexService, indexService);
        fsyncTask = indexService.getFsyncTask();
        assertTrue(indexService.getRefreshTask().mustReschedule());
        assertTrue(fsyncTask.isScheduled());
        assertFalse(fsyncTask.isClosed());

        indexService.close("simon says", false);
        assertFalse(fsyncTask.isScheduled());
        assertTrue(fsyncTask.isClosed());

        indexService = createIndex("test1", Settings.EMPTY);
        assertNull(indexService.getFsyncTask());
    }

    public void testRefreshActuallyWorks() throws Exception {
        IndexService indexService = createIndex("test", Settings.EMPTY);
        ensureGreen("test");
        IndexService.AsyncRefreshTask refreshTask = indexService.getRefreshTask();
        assertEquals(1000, refreshTask.getInterval().millis());
        assertTrue(indexService.getRefreshTask().mustReschedule());
        IndexShard shard = indexService.getShard(0);
        client().prepareIndex("test").setId("0").setSource("{\"foo\": \"bar\"}", XContentType.JSON).get();
        // now disable the refresh
        client().admin().indices().prepareUpdateSettings("test")
            .setSettings(Settings.builder().put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), -1)).get();
        // when we update we reschedule the existing task AND fire off an async refresh to make sure we make everything visible
        // before that this is why we need to wait for the refresh task to be unscheduled and the first doc to be visible
        assertTrue(refreshTask.isClosed());
        refreshTask = indexService.getRefreshTask();
        assertBusy(() -> {
            // this one either becomes visible due to a concurrently running scheduled refresh OR due to the force refresh
            // we are running on updateMetadata if the interval changes
            try (Engine.Searcher searcher = shard.acquireSearcher("test")) {
                TopDocs search = searcher.search(new MatchAllDocsQuery(), 10);
                assertEquals(1, search.totalHits.value);
            }
        });
        assertFalse(refreshTask.isClosed());
        // refresh every millisecond
        client().prepareIndex("test").setId("1").setSource("{\"foo\": \"bar\"}", XContentType.JSON).get();
        client().admin().indices().prepareUpdateSettings("test")
            .setSettings(Settings.builder().put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), "1ms")).get();
        assertTrue(refreshTask.isClosed());
        assertBusy(() -> {
            // this one becomes visible due to the force refresh we are running on updateMetadata if the interval changes
            try (Engine.Searcher searcher = shard.acquireSearcher("test")) {
                TopDocs search = searcher.search(new MatchAllDocsQuery(), 10);
                assertEquals(2, search.totalHits.value);
            }
        });
        client().prepareIndex("test").setId("2").setSource("{\"foo\": \"bar\"}", XContentType.JSON).get();
        assertBusy(() -> {
            // this one becomes visible due to the scheduled refresh
            try (Engine.Searcher searcher = shard.acquireSearcher("test")) {
                TopDocs search = searcher.search(new MatchAllDocsQuery(), 10);
                assertEquals(3, search.totalHits.value);
            }
        });
    }

    public void testAsyncFsyncActuallyWorks() throws Exception {
        Settings settings = Settings.builder()
            .put(IndexSettings.INDEX_TRANSLOG_SYNC_INTERVAL_SETTING.getKey(), "100ms") // very often :)
            .put(IndexSettings.INDEX_TRANSLOG_DURABILITY_SETTING.getKey(), Translog.Durability.ASYNC)
            .build();
        IndexService indexService = createIndex("test", settings);
        ensureGreen("test");
        assertTrue(indexService.getRefreshTask().mustReschedule());
        client().prepareIndex("test").setId("1").setSource("{\"foo\": \"bar\"}", XContentType.JSON).get();
        IndexShard shard = indexService.getShard(0);
        assertBusy(() -> assertFalse(shard.isSyncNeeded()));
    }

    public void testRescheduleAsyncFsync() throws Exception {
        final Settings settings = Settings.builder()
                .put(IndexSettings.INDEX_TRANSLOG_SYNC_INTERVAL_SETTING.getKey(), "100ms")
                .put(IndexSettings.INDEX_TRANSLOG_DURABILITY_SETTING.getKey(), Translog.Durability.REQUEST)
            .build();
        final IndexService indexService = createIndex("test", settings);
        ensureGreen("test");
        assertNull(indexService.getFsyncTask());

        client()
                .admin()
                .indices()
                .prepareUpdateSettings("test")
                .setSettings(Settings.builder().put(IndexSettings.INDEX_TRANSLOG_DURABILITY_SETTING.getKey(), Translog.Durability.ASYNC))
                .get();

        assertNotNull(indexService.getFsyncTask());
        assertTrue(indexService.getFsyncTask().mustReschedule());
        client().prepareIndex("test").setId("1").setSource("{\"foo\": \"bar\"}", XContentType.JSON).get();
        assertNotNull(indexService.getFsyncTask());
        final IndexShard shard = indexService.getShard(0);
        assertBusy(() -> assertFalse(shard.isSyncNeeded()));

        client()
                .admin()
                .indices()
                .prepareUpdateSettings("test")
                .setSettings(Settings.builder().put(IndexSettings.INDEX_TRANSLOG_DURABILITY_SETTING.getKey(), Translog.Durability.REQUEST))
                .get();
        assertNull(indexService.getFsyncTask());

        client()
                .admin()
                .indices()
                .prepareUpdateSettings("test")
                .setSettings(Settings.builder().put(IndexSettings.INDEX_TRANSLOG_DURABILITY_SETTING.getKey(), Translog.Durability.ASYNC))
                .get();
        assertNotNull(indexService.getFsyncTask());
    }

    public void testAsyncTranslogTrimActuallyWorks() throws Exception {
        Settings settings = Settings.builder()
            .put(TRANSLOG_RETENTION_CHECK_INTERVAL_SETTING.getKey(), "100ms") // very often :)
            .build();
        IndexService indexService = createIndex("test", settings);
        ensureGreen("test");
        assertTrue(indexService.getTrimTranslogTask().mustReschedule());
        client().prepareIndex("test").setId("1").setSource("{\"foo\": \"bar\"}", XContentType.JSON).get();
        client().admin().indices().prepareFlush("test").get();
        IndexShard shard = indexService.getShard(0);
        assertBusy(() -> assertThat(IndexShardTestCase.getTranslog(shard).totalOperations(), equalTo(0)));
    }

    public void testAsyncTranslogTrimTaskOnClosedIndex() throws Exception {
        final String indexName = "test";
        IndexService indexService = createIndex(indexName, Settings.builder()
            .put(TRANSLOG_RETENTION_CHECK_INTERVAL_SETTING.getKey(), "100ms")
            .build());
        Translog translog = IndexShardTestCase.getTranslog(indexService.getShard(0));

        int translogOps = 0;
        final int numDocs = scaledRandomIntBetween(10, 100);
        for (int i = 0; i < numDocs; i++) {
            client().prepareIndex().setIndex(indexName).setId(String.valueOf(i)).setSource("{\"foo\": \"bar\"}", XContentType.JSON).get();
            translogOps++;
            if (randomBoolean()) {
                client().admin().indices().prepareFlush(indexName).get();
                if (indexService.getIndexSettings().isSoftDeleteEnabled()) {
                    translogOps = 0;
                }
            }
        }
        assertThat(translog.totalOperations(), equalTo(translogOps));
        assertThat(translog.stats().estimatedNumberOfOperations(), equalTo(translogOps));
        assertAcked(client().admin().indices().prepareClose("test"));

        indexService = getInstanceFromNode(IndicesService.class).indexServiceSafe(indexService.index());
        assertTrue(indexService.getTrimTranslogTask().mustReschedule());

        final Engine readOnlyEngine = getEngine(indexService.getShard(0));
        assertBusy(() ->
            assertThat(readOnlyEngine.getTranslogStats().getTranslogSizeInBytes(), equalTo((long) Translog.DEFAULT_HEADER_SIZE_IN_BYTES)));

        assertAcked(client().admin().indices().prepareOpen("test"));

        indexService = getInstanceFromNode(IndicesService.class).indexServiceSafe(indexService.index());
        translog = IndexShardTestCase.getTranslog(indexService.getShard(0));
        assertThat(translog.totalOperations(), equalTo(0));
        assertThat(translog.stats().estimatedNumberOfOperations(), equalTo(0));
        assertThat(getEngine(indexService.getShard(0)).getTranslogStats().getTranslogSizeInBytes(),
            equalTo((long) Translog.DEFAULT_HEADER_SIZE_IN_BYTES));
    }

    public void testIllegalFsyncInterval() {
        Settings settings = Settings.builder()
            .put(IndexSettings.INDEX_TRANSLOG_SYNC_INTERVAL_SETTING.getKey(), "0ms") // disable
            .build();
        try {
            createIndex("test", settings);
            fail();
        } catch (IllegalArgumentException ex) {
            assertEquals("failed to parse value [0ms] for setting [index.translog.sync_interval], must be >= [100ms]", ex.getMessage());
        }
    }

    public void testUpdateSyncIntervalDynamically() {
        Settings settings = Settings.builder()
            .put(IndexSettings.INDEX_TRANSLOG_SYNC_INTERVAL_SETTING.getKey(), "10s") // very often :)
            .build();
        IndexService indexService = createIndex("test", settings);
        ensureGreen("test");
        assertNull(indexService.getFsyncTask());

        Settings.Builder builder = Settings.builder().put(IndexSettings.INDEX_TRANSLOG_SYNC_INTERVAL_SETTING.getKey(), "5s")
            .put(IndexSettings.INDEX_TRANSLOG_DURABILITY_SETTING.getKey(), Translog.Durability.ASYNC.name());

        client()
            .admin()
            .indices()
            .prepareUpdateSettings("test")
            .setSettings(builder)
            .get();

        assertNotNull(indexService.getFsyncTask());
        assertTrue(indexService.getFsyncTask().mustReschedule());

        IndexMetadata indexMetadata = client().admin().cluster().prepareState().execute().actionGet().getState().metadata().index("test");
        assertEquals("5s", indexMetadata.getSettings().get(IndexSettings.INDEX_TRANSLOG_SYNC_INTERVAL_SETTING.getKey()));

        client().admin().indices().prepareClose("test").get();
        client()
            .admin()
            .indices()
            .prepareUpdateSettings("test")
            .setSettings(Settings.builder().put(IndexSettings.INDEX_TRANSLOG_SYNC_INTERVAL_SETTING.getKey(), "20s"))
            .get();
        indexMetadata = client().admin().cluster().prepareState().execute().actionGet().getState().metadata().index("test");
        assertEquals("20s", indexMetadata.getSettings().get(IndexSettings.INDEX_TRANSLOG_SYNC_INTERVAL_SETTING.getKey()));
    }
}
