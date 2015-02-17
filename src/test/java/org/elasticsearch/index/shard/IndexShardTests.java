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
}
