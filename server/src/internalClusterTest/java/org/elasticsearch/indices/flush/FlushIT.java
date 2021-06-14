/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.indices.flush;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.flush.FlushRequest;
import org.elasticsearch.action.admin.indices.flush.FlushResponse;
import org.elasticsearch.action.admin.indices.stats.ShardStats;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.indices.IndexingMemoryController;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.elasticsearch.test.InternalTestCluster;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.emptyArray;
import static org.hamcrest.Matchers.emptyIterable;
import static org.hamcrest.Matchers.equalTo;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST)
public class FlushIT extends ESIntegTestCase {

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
                client().admin().indices().prepareFlush("test").execute(new ActionListener<FlushResponse>() {
                    @Override
                    public void onResponse(FlushResponse flushResponse) {
                        try {
                            // don't use assertAllSuccessful it uses a randomized context that belongs to a different thread
                            assertThat("Unexpected ShardFailures: " + Arrays.toString(flushResponse.getShardFailures()),
                                flushResponse.getFailedShards(), equalTo(0));
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

    public void testRejectIllegalFlushParameters() {
        createIndex("test");
        int numDocs = randomIntBetween(0, 10);
        for (int i = 0; i < numDocs; i++) {
            client().prepareIndex("test").setSource("{}", XContentType.JSON).get();
        }
        assertThat(expectThrows(ValidationException.class,
            () -> client().admin().indices().flush(new FlushRequest().force(true).waitIfOngoing(false)).actionGet()).getMessage(),
            containsString("wait_if_ongoing must be true for a force flush"));
        assertThat(client().admin().indices().flush(new FlushRequest().force(true).waitIfOngoing(true)).actionGet()
            .getShardFailures(), emptyArray());
        assertThat(client().admin().indices().flush(new FlushRequest().force(false).waitIfOngoing(randomBoolean()))
            .actionGet().getShardFailures(), emptyArray());
    }

    public void testFlushOnInactive() throws Exception {
        final String indexName = "flush_on_inactive";
        List<String> dataNodes = internalCluster().startDataOnlyNodes(
            2,
            Settings.builder()
                .put(IndexingMemoryController.SHARD_INACTIVE_TIME_SETTING.getKey(), randomTimeValue(10, 1000, "ms"))
                .put(IndexingMemoryController.SHARD_MEMORY_INTERVAL_TIME_SETTING.getKey(), randomTimeValue(10, 1000, "ms"))
                .build()
        );
        assertAcked(client().admin().indices().prepareCreate(indexName).setSettings(Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
            .put(IndexSettings.INDEX_TRANSLOG_SYNC_INTERVAL_SETTING.getKey(), randomTimeValue(200, 500, "ms"))
            .put(IndexService.GLOBAL_CHECKPOINT_SYNC_INTERVAL_SETTING.getKey(), randomTimeValue(50, 200, "ms"))
            .put("index.routing.allocation.include._name", String.join(",", dataNodes))
            .build()));
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
