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
import org.elasticsearch.index.IndexService;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.query.QueryParsingException;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.test.ElasticsearchSingleNodeTest;

import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;

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

    public void testDeleteByQueryBWC() {
        Version version = randomVersion();
        assertAcked(client().admin().indices().prepareCreate("test")
                .setSettings(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1, IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0, IndexMetaData.SETTING_VERSION_CREATED, version.id));
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
}
