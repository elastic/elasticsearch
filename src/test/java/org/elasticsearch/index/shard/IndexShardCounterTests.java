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

import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.test.ElasticsearchSingleNodeTest;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;


public class IndexShardCounterTests extends ElasticsearchSingleNodeTest {

    @Test
    public void testDeleteIndexDecreasesCounter() throws InterruptedException, ExecutionException, IOException {
        assertAcked(client().admin().indices().prepareCreate("test").setSettings(ImmutableSettings.builder().put("index.number_of_shards", 1).put("index.number_of_replicas", 0)).get());
        ensureGreen("test");
        IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        IndexService indexService = indicesService.indexServiceSafe("test");
        IndexShard indexShard = indexService.shard(0);
        client().admin().indices().prepareDelete("test").get();
        assertThat(indexShard.getOperationCounter(), equalTo(0));
        try {
            indexShard.incrementOperationCounter();
            fail("we should not be able to increment anymore");
        } catch (IndexShardClosedException e) {
            // expected
        }
    }
}
