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

package org.elasticsearch.index.shard;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.MultiGetRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Phaser;
import java.util.concurrent.ThreadPoolExecutor;
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
        runTestAutomaticRefresh(
            numDocs -> {
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
        runTestAutomaticRefresh(
            numDocs -> {
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
        boolean hasRefreshed = shard.scheduledRefresh();
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
            client().prepareIndex("test").setId("" + i).setSource("{\"foo\" : \"bar\"}", XContentType.JSON)
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
    }


    public void testPendingRefreshWithIntervalChange() throws Exception {
        Settings.Builder builder = Settings.builder();
        builder.put(IndexSettings.INDEX_SEARCH_IDLE_AFTER.getKey(), TimeValue.ZERO);
        IndexService indexService = createIndex("test", builder.build());
        assertFalse(indexService.getIndexSettings().isExplicitRefresh());
        ensureGreen();
        client().prepareIndex("test").setId("0").setSource("{\"foo\" : \"bar\"}", XContentType.JSON).get();
        IndexShard shard = indexService.getShard(0);
        assertFalse(shard.scheduledRefresh());
        assertTrue(shard.isSearchIdle());
        CountDownLatch refreshLatch = new CountDownLatch(1);
        client().admin().indices().prepareRefresh()
            .execute(ActionListener.wrap(refreshLatch::countDown));// async on purpose to make sure it happens concurrently
        assertHitCount(client().prepareSearch().get(), 1);
        client().prepareIndex("test").setId("1").setSource("{\"foo\" : \"bar\"}", XContentType.JSON).get();
        assertFalse(shard.scheduledRefresh());

        // now disable background refresh and make sure the refresh happens
        CountDownLatch updateSettingsLatch = new CountDownLatch(1);
        client().admin().indices()
            .prepareUpdateSettings("test")
            .setSettings(Settings.builder().put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), -1).build())
            .execute(ActionListener.wrap(updateSettingsLatch::countDown));
        assertHitCount(client().prepareSearch().get(), 2);
        // wait for both to ensure we don't have in-flight operations
        updateSettingsLatch.await();
        refreshLatch.await();
        // We need to ensure a `scheduledRefresh` triggered by the internal refresh setting update is executed before we index a new doc;
        // otherwise, it will compete to call `Engine#maybeRefresh` with the `scheduledRefresh` that we are going to verify.
        ensureNoPendingScheduledRefresh(indexService.getThreadPool());
        client().prepareIndex("test").setId("2").setSource("{\"foo\" : \"bar\"}", XContentType.JSON).get();
        assertTrue(shard.scheduledRefresh());
        assertTrue(shard.isSearchIdle());
        assertHitCount(client().prepareSearch().get(), 3);
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

}
