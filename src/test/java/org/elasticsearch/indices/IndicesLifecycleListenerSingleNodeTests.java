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
package org.elasticsearch.indices;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ElasticsearchSingleNodeTest;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;

public class IndicesLifecycleListenerSingleNodeTests extends ElasticsearchSingleNodeTest {

    @Override
    protected boolean resetNodeAfterTest() {
        return true;
    }

    @Test
    public void testCloseDeleteCallback() throws Throwable {

        final AtomicInteger counter = new AtomicInteger(1);
        assertAcked(client().admin().indices().prepareCreate("test")
                .setSettings(SETTING_NUMBER_OF_SHARDS, 1, SETTING_NUMBER_OF_REPLICAS, 0));
        ensureGreen();
        getInstanceFromNode(IndicesLifecycle.class).addListener(new IndicesLifecycle.Listener() {
            @Override
            public void afterIndexClosed(Index index, @IndexSettings Settings indexSettings) {
                assertEquals(counter.get(), 5);
                counter.incrementAndGet();
            }

            @Override
            public void beforeIndexClosed(IndexService indexService) {
                assertEquals(counter.get(), 1);
                counter.incrementAndGet();
            }

            @Override
            public void afterIndexDeleted(Index index, @IndexSettings Settings indexSettings) {
                assertEquals(counter.get(), 6);
                counter.incrementAndGet();
            }

            @Override
              public void beforeIndexDeleted(IndexService indexService) {
                assertEquals(counter.get(), 2);
                counter.incrementAndGet();
            }

            @Override
            public void beforeIndexShardDeleted(ShardId shardId, Settings indexSettings) {
                assertEquals(counter.get(), 3);
                counter.incrementAndGet();
            }

            @Override
            public void afterIndexShardDeleted(ShardId shardId, Settings indexSettings) {
                assertEquals(counter.get(), 4);
                counter.incrementAndGet();
            }
        });
        assertAcked(client().admin().indices().prepareDelete("test").get());
        assertEquals(7, counter.get());
    }

}
