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

import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.action.admin.indices.stats.IndexStats;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.routing.MutableShardRouting;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.test.ElasticsearchSingleNodeTest;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;

/**
 * Simple unit-test IndexShard related operations.
 */
public class IndexShardTests extends ElasticsearchSingleNodeTest {

    public void testFlushOnDeleteSetting() throws Exception {
        boolean initValue = randomBoolean();
        createIndex("test", settingsBuilder().put(IndexShard.INDEX_FLUSH_ON_CLOSE, initValue).build());
        ensureGreen();
        IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        IndexService test = indicesService.indexService("test");
        IndexShard shard = test.shard(0);
        assertEquals(initValue, shard.isFlushOnClose());
        final boolean newValue = !initValue;
        assertAcked(client().admin().indices().prepareUpdateSettings("test").setSettings(settingsBuilder().put(IndexShard.INDEX_FLUSH_ON_CLOSE, newValue).build()));
        assertEquals(newValue, shard.isFlushOnClose());

        try {
            assertAcked(client().admin().indices().prepareUpdateSettings("test").setSettings(settingsBuilder().put(IndexShard.INDEX_FLUSH_ON_CLOSE, "FOOBAR").build()));
            fail("exception expected");
        } catch (ElasticsearchIllegalArgumentException ex) {

        }
        assertEquals(newValue, shard.isFlushOnClose());

    }

    @Test
    public void testDeleteIndexDecreasesCounter() throws InterruptedException, ExecutionException, IOException {
        assertAcked(client().admin().indices().prepareCreate("test").setSettings(ImmutableSettings.builder().put("index.number_of_shards", 1).put("index.number_of_replicas", 0)).get());
        ensureGreen("test");
        IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        IndexService indexService = indicesService.indexServiceSafe("test");
        IndexShard indexShard = indexService.shard(0);
        client().admin().indices().prepareDelete("test").get();
        assertThat(indexShard.getOperationsCount(), equalTo(0));
        try {
            indexShard.incrementOperationCounter();
            fail("we should not be able to increment anymore");
        } catch (IndexShardClosedException e) {
            // expected
        }
    }

    @Test
    public void testIndexShardCounter() throws InterruptedException, ExecutionException, IOException {
        assertAcked(client().admin().indices().prepareCreate("test").setSettings(ImmutableSettings.builder().put("index.number_of_shards", 1).put("index.number_of_replicas", 0)).get());
        ensureGreen("test");
        IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        IndexService indexService = indicesService.indexServiceSafe("test");
        IndexShard indexShard = indexService.shard(0);
        indexShard.incrementOperationCounter();
        assertEquals(2, indexShard.getOperationsCount());
        indexShard.incrementOperationCounter();
        assertEquals(3, indexShard.getOperationsCount());
        indexShard.decrementOperationCounter();
        indexShard.decrementOperationCounter();
        assertEquals(1, indexShard.getOperationsCount());
    }

    @Test
    public void testMarkAsInactiveTriggersSyncedFlush() throws Exception {
        assertAcked(client().admin().indices().prepareCreate("test")
                .setSettings(SETTING_NUMBER_OF_SHARDS, 1, SETTING_NUMBER_OF_REPLICAS, 0));
        client().prepareIndex("test", "test").setSource("{}").get();
        ensureGreen("test");
        IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        indicesService.indexService("test").shard(0).markAsInactive();
        assertBusy(new Runnable() { // should be very very quick
            @Override
            public void run() {
                IndexStats indexStats = client().admin().indices().prepareStats("test").get().getIndex("test");
                assertNotNull(indexStats.getShards()[0].getCommitStats().getUserData().get(Engine.SYNC_COMMIT_ID));
            }
        });
        IndexStats indexStats = client().admin().indices().prepareStats("test").get().getIndex("test");
        assertNotNull(indexStats.getShards()[0].getCommitStats().getUserData().get(Engine.SYNC_COMMIT_ID));
    }

}
