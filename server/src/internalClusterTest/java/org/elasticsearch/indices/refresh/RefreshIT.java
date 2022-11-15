/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.indices.refresh;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.refresh.RefreshResponse;
import org.elasticsearch.action.admin.indices.stats.ShardStats;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.replication.ReplicationRequest;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.refresh.RefreshStats;
import org.elasticsearch.indices.IndexingMemoryController;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.xcontent.XContentType;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.action.DocWriteResponse.Result.CREATED;
import static org.elasticsearch.action.DocWriteResponse.Result.UPDATED;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.emptyIterable;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.oneOf;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 2)
public class RefreshIT extends ESIntegTestCase {
    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singletonList(InternalSettingsPlugin.class);
    }

    public void testWaitIfOngoing() throws InterruptedException {
        createIndex("test");
        ensureGreen("test");
        final int numIters = scaledRandomIntBetween(10, 30);

        for (int i = 0; i < numIters; i++) {
            for (int j = 0; j < 10; j++) {
                client().prepareIndex("test").setSource("{}", XContentType.JSON).get();
            }
            final CountDownLatch latch = new CountDownLatch(10);
            final CopyOnWriteArrayList<Throwable> errors = new CopyOnWriteArrayList<>();
            for (int j = 0; j < 10; j++) {
                client().admin().indices().prepareRefresh("test").execute(new ActionListener<RefreshResponse>() {
                    @Override
                    public void onResponse(RefreshResponse refreshResponse) {
                        try {
                            // don't use assertAllSuccessful it uses a randomized context that belongs to a different thread
                            assertThat(
                                "Unexpected ShardFailures: " + Arrays.toString(refreshResponse.getShardFailures()),
                                refreshResponse.getFailedShards(),
                                equalTo(0)
                            );
                            latch.countDown();
                        } catch (Exception ex) {
                            onFailure(ex);
                        }
                    }

                    @Override
                    public void onFailure(Exception e) {
                        errors.add(e);
                        latch.countDown();
                    }
                });
            }
            latch.await();
            assertThat(errors, emptyIterable());
        }
    }

    public void testRefreshWithBlocked() throws Exception {
        String index = "refresh_block";
        long shards = 3;
        long replicas = 1;
        final long totalDoc = 100;
        createIndex(
            index,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, shards)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, replicas)
                .put(IndexMetadata.SETTING_AUTO_EXPAND_REPLICAS, "false")
                .build()
        );
        ensureGreen(index);
        AtomicBoolean stopped = new AtomicBoolean();
         Thread[] threads = new Thread[between(2, 4)];
        AtomicInteger docID = new AtomicInteger();
        for (int i = 0; i < threads.length; i++) {
            threads[i] = new Thread(() -> {
                while (stopped.get() == false && docID.get() < totalDoc) {
                    String id = Integer.toString(docID.incrementAndGet());
                    try {
                        IndexResponse response = client().prepareIndex(index)
                            .setId(id)
                            .setSource(Map.of("f" + randomIntBetween(1, 10), randomNonNegativeLong()), XContentType.JSON)
                            .get();
                        assertThat(response.getResult(), is(oneOf(CREATED, UPDATED)));
                        logger.info("--> index id={} seq_no={}", response.getId(), response.getSeqNo());
                        RefreshResponse refreshResponse = client().admin().indices().prepareRefresh(index).setWaitIfOngoing(true).get();
                        assertEquals(refreshResponse.getShardFailures().length, 0);
                    } catch (ElasticsearchException ignore) {
                        logger.info("--> fail to index id={}", id);
                    }
                }
            });
            threads[i].start();
        }

        for (Thread thread : threads) {
            thread.join();
        }

        ensureGreen(index);
        assertBusy(() -> assertThat(docID.get(), greaterThanOrEqualTo(1)));
        RefreshStats refreshStats = client().admin().indices().prepareStats(index).clear().setRefresh(true).get().getTotal().refresh;

        assertThat(refreshStats.getTotal(), greaterThanOrEqualTo(totalDoc * shards * (1 + replicas)));

        assertAcked(client().admin().indices().prepareDelete(index));
        stopped.set(true);
        for (Thread thread : threads) {
            thread.join(ReplicationRequest.DEFAULT_TIMEOUT.millis() / 2);
            assertFalse(thread.isAlive());
        }

    }

    public void testRefreshWithNonBlocked() throws Exception {
        String index = "refresh_nonblock";
        long shards = 3;
        long replicas = 1;
        final long totalDoc = 100;
        createIndex(
            index,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, shards)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, replicas)
                .put(IndexMetadata.SETTING_AUTO_EXPAND_REPLICAS, "false")
                .build()
        );
        ensureGreen(index);
        AtomicBoolean stopped = new AtomicBoolean();
        Thread[] threads = new Thread[between(2, 4)];
        AtomicInteger docID = new AtomicInteger();
        for (int i = 0; i < threads.length; i++) {
            threads[i] = new Thread(() -> {
                while (stopped.get() == false && docID.get() < totalDoc) {
                    String id = Integer.toString(docID.incrementAndGet());
                    try {
                        IndexResponse response = client().prepareIndex(index)
                            .setId(id)
                            .setSource(Map.of("f" + randomIntBetween(1, 10), randomNonNegativeLong()), XContentType.JSON)
                            .get();
                        assertThat(response.getResult(), is(oneOf(CREATED, UPDATED)));
                        logger.info("--> index id={} seq_no={}", response.getId(), response.getSeqNo());
                        RefreshResponse refreshResponse = client().admin().indices().prepareRefresh(index).setWaitIfOngoing(false).get();
                        assertEquals(refreshResponse.getShardFailures().length, 0);
                    } catch (ElasticsearchException ignore) {
                        logger.info("--> fail to index id={}", id);
                    }
                }
            });
            threads[i].start();
        }

        for (Thread thread : threads) {
            thread.join();
        }

        ensureGreen(index);
        assertBusy(() -> assertThat(docID.get(), greaterThanOrEqualTo(1)));
        RefreshStats refreshStats = client().admin().indices().prepareStats(index).clear().setRefresh(true).get().getTotal().refresh;
        assertThat(refreshStats.getTotal(), lessThanOrEqualTo(totalDoc * shards * (1 + replicas)));

        assertAcked(client().admin().indices().prepareDelete(index));
        stopped.set(true);
        for (Thread thread : threads) {
            thread.join(ReplicationRequest.DEFAULT_TIMEOUT.millis() / 2);
            assertFalse(thread.isAlive());
        }
    }

    public void testRefreshOnInactive() throws Exception {
        final String indexName = "refresh_on_inactive";
        List<String> dataNodes = internalCluster().startDataOnlyNodes(
            2,
            Settings.builder()
                .put(IndexingMemoryController.SHARD_INACTIVE_TIME_SETTING.getKey(), randomTimeValue(10, 1000, "ms"))
                .put(IndexingMemoryController.SHARD_MEMORY_INTERVAL_TIME_SETTING.getKey(), randomTimeValue(10, 1000, "ms"))
                .build()
        );
        assertAcked(
            client().admin()
                .indices()
                .prepareCreate(indexName)
                .setSettings(
                    Settings.builder()
                        .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                        .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
                        .put(IndexSettings.INDEX_TRANSLOG_SYNC_INTERVAL_SETTING.getKey(), randomTimeValue(200, 500, "ms"))
                        .put(IndexService.GLOBAL_CHECKPOINT_SYNC_INTERVAL_SETTING.getKey(), randomTimeValue(50, 200, "ms"))
                        .put("index.routing.allocation.include._name", String.join(",", dataNodes))
                        .build()
                )
        );
        ensureGreen(indexName);
        int numDocs = randomIntBetween(1, 10);
        for (int i = 0; i < numDocs; i++) {
            client().prepareIndex(indexName).setSource("f", "v").get();
        }
        if (randomBoolean()) {
            internalCluster().restartNode(randomFrom(dataNodes), new InternalTestCluster.RestartCallback());
            ensureGreen(indexName);
        }

        assertBusy(() -> {
            for (ShardStats shardStats : client().admin().indices().prepareStats(indexName).get().getShards()) {
                assertThat(shardStats.getStats().getTranslog().getUncommittedOperations(), equalTo(0));
            }
        }, 30, TimeUnit.SECONDS);
    }
}
