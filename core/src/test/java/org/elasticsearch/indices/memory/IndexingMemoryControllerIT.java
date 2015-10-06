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
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.index.engine.EngineConfig;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.node.internal.InternalSettingsPreparer;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.Test;


@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class IndexingMemoryControllerIT extends ESIntegTestCase {

    private long getIWBufferSize(String indexName) {
        return client().admin().indices().prepareStats(indexName).get().getTotal().getSegments().getIndexWriterMaxMemoryInBytes();
    }

    @Test
    public void testIndexBufferPushedToEngine() throws InterruptedException {
        createNode(Settings.builder().put(IndexingMemoryController.SHARD_INACTIVE_TIME_SETTING, "100000h",
                                          IndexingMemoryController.INDEX_BUFFER_SIZE_SETTING, "32mb",
                                          IndexShard.INDEX_REFRESH_INTERVAL, "-1").build());

        // Create two active indices, sharing 32 MB indexing buffer:
        prepareCreate("test3").setSettings(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1, IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0).get();
        prepareCreate("test4").setSettings(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1, IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0).get();

        ensureGreen();

        index("test3", "type", "1", "f", 1);
        index("test4", "type", "1", "f", 1);

        // .. then make sure we really pushed the update (16 MB for each) down to the IndexWriter, even if refresh nor flush occurs:
        if (awaitBusy(() -> getIWBufferSize("test3") == 16*1024*1024) == false) {
            fail("failed to update shard indexing buffer size for test3 index to 16 MB; got: " + getIWBufferSize("test3"));
        }
        if (awaitBusy(() -> getIWBufferSize("test4") == 16*1024*1024) == false) {
            fail("failed to update shard indexing buffer size for test4 index to 16 MB; got: " + getIWBufferSize("test4"));
        }

        client().admin().indices().prepareDelete("test4").get();
        if (awaitBusy(() -> getIWBufferSize("test3") == 32 * 1024 * 1024) == false) {
            fail("failed to update shard indexing buffer size for test3 index to 32 MB; got: " + getIWBufferSize("test4"));
        }

    }

    @Test
    public void testInactivePushedToShard() throws InterruptedException {
        createNode(Settings.builder().put(IndexingMemoryController.SHARD_INACTIVE_TIME_SETTING, "100ms",
                IndexingMemoryController.SHARD_INACTIVE_INTERVAL_TIME_SETTING, "100ms",
                IndexShard.INDEX_REFRESH_INTERVAL, "-1").build());

        // Create two active indices, sharing 32 MB indexing buffer:
        prepareCreate("test1").setSettings(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1, IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0).get();

        ensureGreen();

        index("test1", "type", "1", "f", 1);

        // make shard the shard buffer was set to inactive size
        final ByteSizeValue inactiveBuffer = IndexingMemoryController.INACTIVE_SHARD_INDEXING_BUFFER;
        if (awaitBusy(() -> getIWBufferSize("test1") == inactiveBuffer.bytes()) == false) {
            fail("failed to update shard indexing buffer size for test1 index to [" + inactiveBuffer + "]; got: " + getIWBufferSize("test1"));
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
                        .put(IndexingMemoryController.SHARD_INACTIVE_INTERVAL_TIME_SETTING, "100ms")
                        .put(settings)
        );
    }
}
