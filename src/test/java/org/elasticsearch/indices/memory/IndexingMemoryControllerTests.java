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

import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.LoggingEvent;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsResponse;
import org.elasticsearch.action.admin.indices.recovery.RecoveryResponse;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.internal.InternalEngine;
import org.elasticsearch.index.shard.service.InternalIndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Test;
import com.google.common.base.Predicate;


@ElasticsearchIntegrationTest.ClusterScope(scope = ElasticsearchIntegrationTest.Scope.TEST, numDataNodes = 0)
public class IndexingMemoryControllerTests extends ElasticsearchIntegrationTest {

    @Test
    public void testIndexBufferSizeUpdateAfterCreationRemoval() throws InterruptedException {

        createNode(ImmutableSettings.EMPTY);

        prepareCreate("test1").setSettings(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1, IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0).get();

        ensureGreen();

        final InternalIndexShard shard1 = (InternalIndexShard) internalCluster().getInstance(IndicesService.class).indexService("test1").shard(0);

        prepareCreate("test2").setSettings(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1, IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0).get();

        ensureGreen();

        final InternalIndexShard shard2 = (InternalIndexShard) internalCluster().getInstance(IndicesService.class).indexService("test2").shard(0);
        final long expected1ShardSize = internalCluster().getInstance(IndexingMemoryController.class).indexingBufferSize().bytes();
        final long expected2ShardsSize = expected1ShardSize / 2;

        boolean success = awaitBusy(new Predicate<Object>() {
            @Override
            public boolean apply(Object input) {
                return ((InternalEngine) shard1.engine()).indexingBufferSize().bytes() <= expected2ShardsSize &&
                        ((InternalEngine) shard2.engine()).indexingBufferSize().bytes() <= expected2ShardsSize;
            }
        });

        if (!success) {
            fail("failed to update shard indexing buffer size. expected [" + expected2ShardsSize + "] shard1 [" +
                            ((InternalEngine) shard1.engine()).indexingBufferSize().bytes() + "] shard2  [" +
                            ((InternalEngine) shard2.engine()).indexingBufferSize().bytes() + "]"
            );
        }

        client().admin().indices().prepareDelete("test2").get();
        success = awaitBusy(new Predicate<Object>() {
            @Override
            public boolean apply(Object input) {
                return ((InternalEngine) shard1.engine()).indexingBufferSize().bytes() >= expected1ShardSize;
            }
        });

        if (!success) {
            fail("failed to update shard indexing buffer size after deleting shards. expected [" + expected1ShardSize + "] got [" +
                            ((InternalEngine) shard1.engine()).indexingBufferSize().bytes() + "]"
            );
        }

    }

    @Test
    public void testIndexBufferSizeUpdateInactiveShard() throws InterruptedException {

        createNode(ImmutableSettings.builder().put("indices.memory.shard_inactive_time", "100ms").build());

        prepareCreate("test1").setSettings(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1, IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0).get();

        ensureGreen();

        final InternalIndexShard shard1 = (InternalIndexShard) internalCluster().getInstance(IndicesService.class).indexService("test1").shard(0);
        boolean success = awaitBusy(new Predicate<Object>() {
            @Override
            public boolean apply(Object input) {
                return ((InternalEngine) shard1.engine()).indexingBufferSize().bytes() == Engine.INACTIVE_SHARD_INDEXING_BUFFER.bytes();
            }
        });
        if (!success) {
            fail("failed to update shard indexing buffer size due to inactive state. expected [" + Engine.INACTIVE_SHARD_INDEXING_BUFFER + "] got [" +
                            ((InternalEngine) shard1.engine()).indexingBufferSize().bytes() + "]"
            );
        }

        index("test1", "type", "1", "f", 1);

        success = awaitBusy(new Predicate<Object>() {
            @Override
            public boolean apply(Object input) {
                return ((InternalEngine) shard1.engine()).indexingBufferSize().bytes() > Engine.INACTIVE_SHARD_INDEXING_BUFFER.bytes();
            }
        });
        if (!success) {
            fail("failed to update shard indexing buffer size due to inactive state. expected something larger then [" + Engine.INACTIVE_SHARD_INDEXING_BUFFER + "] got [" +
                            ((InternalEngine) shard1.engine()).indexingBufferSize().bytes() + "]"
            );
        }

        flush(); // clean translogs

        success = awaitBusy(new Predicate<Object>() {
            @Override
            public boolean apply(Object input) {
                return ((InternalEngine) shard1.engine()).indexingBufferSize().bytes() == Engine.INACTIVE_SHARD_INDEXING_BUFFER.bytes();
            }
        });
        if (!success) {
            fail("failed to update shard indexing buffer size due to inactive state. expected [" + Engine.INACTIVE_SHARD_INDEXING_BUFFER + "] got [" +
                            ((InternalEngine) shard1.engine()).indexingBufferSize().bytes() + "]"
            );
        }
    }

    private void createNode(Settings settings) {
        internalCluster().startNode(ImmutableSettings.builder()
                        .put(ClusterName.SETTING, "IndexingMemoryControllerTests")
                        .put("node.name", "IndexingMemoryControllerTests")
                        .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
                        .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0)
                        .put(EsExecutors.PROCESSORS, 1) // limit the number of threads created
                        .put("http.enabled", false)
                        .put("index.store.type", "ram")
                        .put("config.ignore_system_properties", true) // make sure we get what we set :)
                        .put("gateway.type", "none")
                        .put("indices.memory.interval", "100ms")
                        .put(settings)
        );
    }

    private static class MockAppender extends AppenderSkeleton {
        boolean sawUsing;
        boolean sawRecalculating;
        boolean sawUpdatingIndexBuffer;
        boolean sawUpdatingIndexMinShardBuffer;
        boolean sawUpdatingIndexMaxShardBuffer;

        @Override
        protected void append(LoggingEvent event) {
            String message = event.getMessage().toString();
            if (event.getLoggerName().equals("indices.memory")) {
                if (message.contains("using indices.memory.index_buffer_size")) {
                    sawUsing = true;
                }
                if (message.contains("recalculating shard indexing buffer")) {
                    sawRecalculating = true;
                }
                if (message.contains("updating [" + IndexingMemoryController.INDEX_BUFFER_SIZE + "]") && message.contains(" to [52mb]")) {
                    sawUpdatingIndexBuffer = true;
                }
                if (message.contains("updating [" + IndexingMemoryController.MIN_SHARD_INDEX_BUFFER_SIZE + "]") && message.contains(" to [1mb]")) {
                    sawUpdatingIndexMinShardBuffer = true;
                }
                if (message.contains("updating [" + IndexingMemoryController.MAX_SHARD_INDEX_BUFFER_SIZE + "]") && message.contains("128mb")) {
                    sawUpdatingIndexMaxShardBuffer = true;
                }
            }
        }

        @Override
        public boolean requiresLayout() {
            return false;
        }

        @Override
        public void close() {
        }
    }

    // #7045
    public void testDynamicSettings() throws Exception {
        MockAppender mockAppender = new MockAppender();

        Logger rootLogger = Logger.getRootLogger();
        Level savedLevel = rootLogger.getLevel();
        rootLogger.addAppender(mockAppender);
        rootLogger.setLevel(Level.DEBUG);

        try {
            createNode(ImmutableSettings.EMPTY);

            //prepareCreate("test").setSettings(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1, IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0).get();
            prepareCreate("test").get();

            ensureGreen();

            // Should have seen "using index.buffer.size [...]:
            assertTrue(mockAppender.sawUsing);

            // Change indices.memory.index_buffer_size
            assertFalse(mockAppender.sawUpdatingIndexBuffer);
            mockAppender.sawRecalculating = false;
            Settings settings = ImmutableSettings.builder()
                .put(IndexingMemoryController.INDEX_BUFFER_SIZE, "52mb")
                .build();

            ClusterUpdateSettingsResponse response = client().admin().cluster().prepareUpdateSettings().setPersistentSettings(settings).get();
            assertEquals("52mb", response.getPersistentSettings().get(IndexingMemoryController.INDEX_BUFFER_SIZE));

            // Confirm we logged the change:
            assertTrue(mockAppender.sawUpdatingIndexBuffer);
            assertTrue(mockAppender.sawRecalculating);

            // Change indices.memory.min_shard_index_buffer_size
            assertFalse(mockAppender.sawUpdatingIndexMinShardBuffer);
            mockAppender.sawRecalculating = false;
            settings = ImmutableSettings.builder()
                .put(IndexingMemoryController.MIN_SHARD_INDEX_BUFFER_SIZE, "1mb")
                .build();
            response = client().admin().cluster().prepareUpdateSettings().setPersistentSettings(settings).get();
            assertEquals("1mb", response.getPersistentSettings().get(IndexingMemoryController.MIN_SHARD_INDEX_BUFFER_SIZE));

            // Confirm we logged the change:
            assertTrue(mockAppender.sawUpdatingIndexMinShardBuffer);
            assertTrue(mockAppender.sawRecalculating);

            // Change indices.memory.max_shard_index_buffer_size
            assertFalse(mockAppender.sawUpdatingIndexMaxShardBuffer);
            mockAppender.sawRecalculating = false;
            settings = ImmutableSettings.builder()
                .put(IndexingMemoryController.MAX_SHARD_INDEX_BUFFER_SIZE, "128mb")
                .build();
            response = client().admin().cluster().prepareUpdateSettings().setPersistentSettings(settings).get();
            assertEquals("128mb", response.getPersistentSettings().get(IndexingMemoryController.MAX_SHARD_INDEX_BUFFER_SIZE));
            assertTrue(mockAppender.sawRecalculating);
            
            // Confirm we logged the change:
            assertTrue(mockAppender.sawUpdatingIndexMaxShardBuffer);

            IndicesStatsResponse stats = client().admin().indices().prepareStats("test").get();

            // The total was divided between N shards and then summed up again so it may not match precisely:
            assertEquals(52.0, stats.getIndex("test").getTotal().getSegments().getIndexWriterMaxMemoryInBytes()/1024/1024., .001);

            // TODO: make sure settings survive cluster restart; this doesn't work for some reason (IndexMissingException "test"):
            //internalCluster().fullRestart();
            //ensureGreen();
            //RecoveryResponse recoveryResponse = client().admin().indices().prepareRecoveries("test").execute().actionGet();

        } finally {
            rootLogger.removeAppender(mockAppender);
            rootLogger.setLevel(savedLevel);
        }
    }
}
