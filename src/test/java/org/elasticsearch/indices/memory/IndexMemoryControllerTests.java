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

package org.elasticsearch.indices.memory;

import com.google.common.base.Predicate;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.index.engine.internal.InternalEngine;
import org.elasticsearch.index.shard.service.InternalIndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Test;


@ElasticsearchIntegrationTest.ClusterScope(scope = ElasticsearchIntegrationTest.Scope.TEST, numDataNodes = 0)
public class IndexMemoryControllerTests extends ElasticsearchIntegrationTest {

    @Test
    public void testIndexBufferSizeUpdateAfterShardCreation() throws InterruptedException {

        internalCluster().startNode(ImmutableSettings.builder()
                        .put("http.enabled", "false")
                        .put("discovery.type", "local")
                        .put("indices.memory.interval", "1s")
        );

        client().admin().indices().prepareCreate("test1")
                .setSettings(ImmutableSettings.builder()
                                .put("number_of_shards", 1)
                                .put("number_of_replicas", 0)
                ).get();

        ensureGreen();

        final InternalIndexShard shard1 = (InternalIndexShard) internalCluster().getInstance(IndicesService.class).indexService("test1").shard(0);

        client().admin().indices().prepareCreate("test2")
                .setSettings(ImmutableSettings.builder()
                                .put("number_of_shards", 1)
                                .put("number_of_replicas", 0)
                ).get();

        ensureGreen();

        final InternalIndexShard shard2 = (InternalIndexShard) internalCluster().getInstance(IndicesService.class).indexService("test2").shard(0);
        final long expectedShardSize = internalCluster().getInstance(IndexingMemoryController.class).indexingBufferSize().bytes() / 2;

        boolean success = awaitBusy(new Predicate<Object>() {
            @Override
            public boolean apply(Object input) {
                return ((InternalEngine) shard1.engine()).indexingBufferSize().bytes() <= expectedShardSize &&
                        ((InternalEngine) shard2.engine()).indexingBufferSize().bytes() <= expectedShardSize;
            }
        });

        if (!success) {
            fail("failed to update shard indexing buffer size. expected [" + expectedShardSize + "] shard1 [" +
                            ((InternalEngine) shard1.engine()).indexingBufferSize().bytes() + "] shard2  [" +
                            ((InternalEngine) shard1.engine()).indexingBufferSize().bytes() + "]"
            );
        }

    }
}
