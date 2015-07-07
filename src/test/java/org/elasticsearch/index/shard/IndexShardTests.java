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

import org.apache.lucene.index.CorruptIndexException;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.Version;
import org.elasticsearch.action.admin.indices.stats.IndexStats;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.gateway.local.state.shards.LocalGatewayShardsState;
import org.elasticsearch.gateway.local.state.shards.ShardStateInfo;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.query.QueryParsingException;
import org.elasticsearch.index.settings.IndexSettingsService;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.test.ElasticsearchSingleNodeTest;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_VERSION_CREATED;
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
        assertEquals(0, indexShard.getOperationsCount());
        indexShard.incrementOperationCounter();
        assertEquals(1, indexShard.getOperationsCount());
        indexShard.incrementOperationCounter();
        assertEquals(2, indexShard.getOperationsCount());
        indexShard.decrementOperationCounter();
        indexShard.decrementOperationCounter();
        assertEquals(0, indexShard.getOperationsCount());
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

    public void testDeleteByQueryBWC() {
        Version version = randomVersion();
        assertAcked(client().admin().indices().prepareCreate("test")
                .setSettings(SETTING_NUMBER_OF_SHARDS, 1, SETTING_NUMBER_OF_REPLICAS, 0, IndexMetaData.SETTING_VERSION_CREATED, version.id));
        ensureGreen("test");
        client().prepareIndex("test", "person").setSource("{ \"user\" : \"kimchy\" }").get();

        IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        IndexService test = indicesService.indexService("test");
        IndexShard shard = test.shard(0);
        int numDocs = 1;
        shard.state = IndexShardState.RECOVERING;
        try {
            shard.performRecoveryOperation(new Translog.DeleteByQuery(new Engine.DeleteByQuery(null, new BytesArray("{\"term\" : { \"user\" : \"kimchy\" }}"), null, null, null, Engine.Operation.Origin.RECOVERY, 0, "person")));
            assertTrue(version.onOrBefore(Version.V_1_0_0_Beta2));
            numDocs = 0;
        } catch (QueryParsingException ex) {
            assertTrue(version.after(Version.V_1_0_0_Beta2));
        } finally {
            shard.state = IndexShardState.STARTED;
        }
        shard.engine().refresh("foo");

        try (Engine.Searcher searcher = shard.engine().acquireSearcher("foo")) {
            assertEquals(numDocs, searcher.reader().numDocs());
        }
    }
    
    public void testMinimumCompatVersion() {
        Version versionCreated = randomVersion();
        assertAcked(client().admin().indices().prepareCreate("test")
                .setSettings(SETTING_NUMBER_OF_SHARDS, 1, SETTING_NUMBER_OF_REPLICAS, 0, SETTING_VERSION_CREATED, versionCreated.id));
        client().prepareIndex("test", "test").setSource("{}").get();
        ensureGreen("test");
        IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        IndexShard test = indicesService.indexService("test").shard(0);
        assertEquals(versionCreated.luceneVersion, test.minimumCompatibleVersion());
        client().prepareIndex("test", "test").setSource("{}").get();
        assertEquals(versionCreated.luceneVersion, test.minimumCompatibleVersion());
        test.engine().flush();
        assertEquals(Version.CURRENT.luceneVersion, test.minimumCompatibleVersion());
    }

    public void testFailShard() throws Exception {
        forceLocalGateway();
        createIndex("test");
        ensureGreen();
        IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        NodeEnvironment env = getInstanceFromNode(NodeEnvironment.class);
        IndexService test = indicesService.indexService("test");
        IndexShard shard = test.shard(0);
        // fail shard
        shard.failShard("test shard fail", new CorruptIndexException("corrupted"));
        // check state file still exists
        ShardStateInfo shardStateInfo = LocalGatewayShardsState.FORMAT.loadLatestState(logger, env.shardLocations(shard.shardId));
        assertNotNull(shardStateInfo);
        // but index can't be opened for a failed shard
        File[] shardLocations = env.shardDataLocations(shard.shardId, test.getIndexSettings());
        File[] shardIndexLocations = new File[shardLocations.length];
        for (int i = 0; i < shardLocations.length; i++) {
            shardIndexLocations[i] = new File(shardLocations[i], "index");
        }
        boolean exists = false;
        for (File shardIndexLocation : shardIndexLocations) {
            if (shardIndexLocation.exists()) {
                exists = true;
                break;
            }
        }
        assertTrue(exists);
        assertThat("store index should be corrupted", Store.canOpenIndex(logger, shardIndexLocations), equalTo(false));
    }

    public void testUpdatePriority() {
        assertAcked(client().admin().indices().prepareCreate("test")
                .setSettings(IndexMetaData.SETTING_PRIORITY, 200));
        IndexSettingsService indexSettingsService = getInstanceFromNode(IndicesService.class).indexService("test").settingsService();
        assertEquals(200, indexSettingsService.getSettings().getAsInt(IndexMetaData.SETTING_PRIORITY, 0).intValue());
        client().admin().indices().prepareUpdateSettings("test").setSettings(ImmutableSettings.builder().put(IndexMetaData.SETTING_PRIORITY, 400).build()).get();
        assertEquals(400, indexSettingsService.getSettings().getAsInt(IndexMetaData.SETTING_PRIORITY, 0).intValue());
    }
}
