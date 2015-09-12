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

import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.index.engine.EngineConfig;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.node.internal.InternalSettingsPreparer;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.Test;


@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class IndexingMemoryControllerIT extends ESIntegTestCase {

    @Test
    public void testIndexBufferSizeUpdateAfterCreationRemoval() throws InterruptedException {

        createNode(Settings.EMPTY);

        prepareCreate("test1").setSettings(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1, IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0).get();

        ensureGreen();

        final IndexShard shard1 = internalCluster().getInstance(IndicesService.class).indexService("test1").shard(0);

        prepareCreate("test2").setSettings(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1, IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0).get();

        ensureGreen();

        final IndexShard shard2 = internalCluster().getInstance(IndicesService.class).indexService("test2").shard(0);
        final long expected1ShardSize = internalCluster().getInstance(IndexingMemoryController.class).indexingBufferSize().bytes();
        final long expected2ShardsSize = expected1ShardSize / 2;

        boolean success = awaitBusy(() -> shard1.engine().config().getIndexingBufferSize().bytes() <= expected2ShardsSize &&
                        shard2.engine().config().getIndexingBufferSize().bytes() <= expected2ShardsSize
        );

        if (!success) {
            fail("failed to update shard indexing buffer size. expected [" + expected2ShardsSize + "] shard1 [" +
                            shard1.engine().config().getIndexingBufferSize().bytes() + "] shard2  [" +
                            shard2.engine().config().getIndexingBufferSize().bytes() + "]"
            );
        }

        client().admin().indices().prepareDelete("test2").get();
        success = awaitBusy(() -> shard1.engine().config().getIndexingBufferSize().bytes() >= expected1ShardSize);

        if (!success) {
            fail("failed to update shard indexing buffer size after deleting shards. expected [" + expected1ShardSize + "] got [" +
                            shard1.engine().config().getIndexingBufferSize().bytes() + "]"
            );
        }

    }

    @Test
    public void testIndexBufferSizeUpdateInactiveShard() throws InterruptedException {

        createNode(Settings.builder().put("indices.memory.shard_inactive_time", "100ms").build());

        prepareCreate("test1").setSettings(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1, IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0).get();

        ensureGreen();

        final IndexShard shard1 = internalCluster().getInstance(IndicesService.class).indexService("test1").shard(0);
        boolean success = awaitBusy(() -> shard1.engine().config().getIndexingBufferSize().bytes() == EngineConfig.INACTIVE_SHARD_INDEXING_BUFFER.bytes());
        if (!success) {
            fail("failed to update shard indexing buffer size due to inactive state. expected [" + EngineConfig.INACTIVE_SHARD_INDEXING_BUFFER + "] got [" +
                            shard1.engine().config().getIndexingBufferSize().bytes() + "]"
            );
        }

        index("test1", "type", "1", "f", 1);

        success = awaitBusy(() -> shard1.engine().config().getIndexingBufferSize().bytes() > EngineConfig.INACTIVE_SHARD_INDEXING_BUFFER.bytes());
        if (!success) {
            fail("failed to update shard indexing buffer size due to inactive state. expected something larger then [" + EngineConfig.INACTIVE_SHARD_INDEXING_BUFFER + "] got [" +
                            shard1.engine().config().getIndexingBufferSize().bytes() + "]"
            );
        }

        flush(); // clean translogs

        success = awaitBusy(() -> shard1.engine().config().getIndexingBufferSize().bytes() == EngineConfig.INACTIVE_SHARD_INDEXING_BUFFER.bytes());
        if (!success) {
            fail("failed to update shard indexing buffer size due to inactive state. expected [" + EngineConfig.INACTIVE_SHARD_INDEXING_BUFFER + "] got [" +
                            shard1.engine().config().getIndexingBufferSize().bytes() + "]"
            );
        }
    }

    private void createNode(Settings settings) {
        internalCluster().startNode(Settings.builder()
                        .put(ClusterName.SETTING, "IndexingMemoryControllerIT")
                        .put("node.name", "IndexingMemoryControllerIT")
                        .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
                        .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0)
                        .put(EsExecutors.PROCESSORS, 1) // limit the number of threads created
                        .put("http.enabled", false)
                        .put(InternalSettingsPreparer.IGNORE_SYSTEM_PROPERTIES_SETTING, true) // make sure we get what we set :)
                        .put("indices.memory.interval", "100ms")
                        .put(settings)
        );
    }
}
