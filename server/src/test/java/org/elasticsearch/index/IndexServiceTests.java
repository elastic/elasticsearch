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

import static org.elasticsearch.test.InternalSettingsPlugin.TRANSLOG_RETENTION_CHECK_INTERVAL_SETTING;
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

    public void testBaseAsyncTask() throws InterruptedException, IOException {
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
        indexService.close("simon says", false);
        assertFalse("no shards left", task.mustReschedule());
        assertTrue(task.isScheduled());
        task.close();
        assertFalse(task.isScheduled());
    }

    public void testRefreshTaskIsUpdated() throws IOException {
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
        indexService.close("simon says", false);
        assertFalse(refreshTask.isScheduled());
        assertTrue(refreshTask.isClosed());
    }

    public void testFsyncTaskIsRunning() throws IOException {
        Settings settings = Settings.builder()
            .put(IndexSettings.INDEX_TRANSLOG_DURABILITY_SETTING.getKey(), Translog.Durability.ASYNC).build();
        IndexService indexService = createIndex("test", settings);
        IndexService.AsyncTranslogFSync fsyncTask = indexService.getFsyncTask();
        assertNotNull(fsyncTask);
        assertEquals(5000, fsyncTask.getInterval().millis());
        assertTrue(fsyncTask.mustReschedule());
        assertTrue(fsyncTask.isScheduled());

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
        client().prepareIndex("test", "test", "0").setSource("{\"foo\": \"bar\"}", XContentType.JSON).get();
        // now disable the refresh
        client().admin().indices().prepareUpdateSettings("test")
            .setSettings(Settings.builder().put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), -1)).get();
        // when we update we reschedule the existing task AND fire off an async refresh to make sure we make everything visible
        // before that this is why we need to wait for the refresh task to be unscheduled and the first doc to be visible
        assertTrue(refreshTask.isClosed());
        refreshTask = indexService.getRefreshTask();
        assertBusy(() -> {
            // this one either becomes visible due to a concurrently running scheduled refresh OR due to the force refresh
            // we are running on updateMetaData if the interval changes
            try (Engine.Searcher searcher = shard.acquireSearcher("test")) {
                TopDocs search = searcher.searcher().search(new MatchAllDocsQuery(), 10);
                assertEquals(1, search.totalHits.value);
            }
        });
        assertFalse(refreshTask.isClosed());
        // refresh every millisecond
        client().prepareIndex("test", "test", "1").setSource("{\"foo\": \"bar\"}", XContentType.JSON).get();
        client().admin().indices().prepareUpdateSettings("test")
            .setSettings(Settings.builder().put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), "1ms")).get();
        assertTrue(refreshTask.isClosed());
        assertBusy(() -> {
            // this one becomes visible due to the force refresh we are running on updateMetaData if the interval changes
            try (Engine.Searcher searcher = shard.acquireSearcher("test")) {
                TopDocs search = searcher.searcher().search(new MatchAllDocsQuery(), 10);
                assertEquals(2, search.totalHits.value);
            }
        });
        client().prepareIndex("test", "test", "2").setSource("{\"foo\": \"bar\"}", XContentType.JSON).get();
        assertBusy(() -> {
            // this one becomes visible due to the scheduled refresh
            try (Engine.Searcher searcher = shard.acquireSearcher("test")) {
                TopDocs search = searcher.searcher().search(new MatchAllDocsQuery(), 10);
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
        client().prepareIndex("test", "test", "1").setSource("{\"foo\": \"bar\"}", XContentType.JSON).get();
        IndexShard shard = indexService.getShard(0);
        assertBusy(() -> {
            assertFalse(shard.isSyncNeeded());
        });
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
        client().prepareIndex("test", "test", "1").setSource("{\"foo\": \"bar\"}", XContentType.JSON).get();
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
        assertTrue(indexService.getRefreshTask().mustReschedule());
        client().prepareIndex("test", "test", "1").setSource("{\"foo\": \"bar\"}", XContentType.JSON).get();
        client().admin().indices().prepareFlush("test").get();
        client().admin().indices().prepareUpdateSettings("test")
            .setSettings(Settings.builder()
                .put(IndexSettings.INDEX_TRANSLOG_RETENTION_SIZE_SETTING.getKey(), -1)
                .put(IndexSettings.INDEX_TRANSLOG_RETENTION_AGE_SETTING.getKey(), -1))
            .get();
        IndexShard shard = indexService.getShard(0);
        assertBusy(() -> assertThat(IndexShardTestCase.getTranslog(shard).totalOperations(), equalTo(0)));
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
}
