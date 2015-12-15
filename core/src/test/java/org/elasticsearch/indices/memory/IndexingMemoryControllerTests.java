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

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.test.ESSingleNodeTestCase;

import java.util.*;

import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_SHARDS;
import static org.hamcrest.Matchers.equalTo;

public class IndexingMemoryControllerTests extends ESSingleNodeTestCase {

    static class MockController extends IndexingMemoryController {

        final Map<IndexShard, Long> indexBufferRAMBytesUsed = new HashMap<>();

        public MockController(Settings settings) {
            super(Settings.builder()
                            .put(SHARD_MEMORY_INTERVAL_TIME_SETTING, "200h") // disable it
                            .put(settings)
                            .build(),
                    null, null, 100 * 1024 * 1024); // fix jvm mem size to 100mb
        }

        public void deleteShard(IndexShard shard) {
            indexBufferRAMBytesUsed.remove(shard);
        }

        @Override
        protected List<IndexShard> availableShards() {
            return new ArrayList<>(indexBufferRAMBytesUsed.keySet());
        }

        @Override
        protected boolean shardAvailable(IndexShard shard) {
            return indexBufferRAMBytesUsed.containsKey(shard);
        }

        @Override
        protected long getIndexBufferRAMBytesUsed(IndexShard shard) {
            Long used = indexBufferRAMBytesUsed.get(shard);
            if (used == null) {
                return 0;
            } else {
                return used;
            }
        }

        @Override
        protected void checkIdle(IndexShard shard, long inactiveTimeNS) {
        }

        @Override
        public void writeIndexingBufferAsync(IndexShard shard) {
            indexBufferRAMBytesUsed.put(shard, 0L);
        }

        public void assertBuffer(IndexShard shard, ByteSizeValue expected) {
            Long actual = indexBufferRAMBytesUsed.get(shard);
            assertEquals(expected.bytes(), actual.longValue());
        }

        public void simulateIndexing(IndexShard shard) {
            Long bytes = indexBufferRAMBytesUsed.get(shard);
            if (bytes == null) {
                bytes = 0L;
            }
            // Each doc we index takes up a megabyte!
            bytes += 1024*1024;
            indexBufferRAMBytesUsed.put(shard, bytes);
            forceCheck();
        }
    }

    public void testShardAdditionAndRemoval() {
        createIndex("test", Settings.builder().put(SETTING_NUMBER_OF_SHARDS, 3).put(SETTING_NUMBER_OF_REPLICAS, 0).build());
        IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        IndexService test = indicesService.indexService("test");

        MockController controller = new MockController(Settings.builder()
                .put(IndexingMemoryController.INDEX_BUFFER_SIZE_SETTING, "4mb").build());
        IndexShard shard0 = test.getShard(0);
        controller.simulateIndexing(shard0);
        controller.assertBuffer(shard0, new ByteSizeValue(1, ByteSizeUnit.MB));

        // add another shard
        IndexShard shard1 = test.getShard(1);
        controller.simulateIndexing(shard1);
        controller.assertBuffer(shard0, new ByteSizeValue(1, ByteSizeUnit.MB));
        controller.assertBuffer(shard1, new ByteSizeValue(1, ByteSizeUnit.MB));

        // remove first shard
        controller.deleteShard(shard0);
        controller.forceCheck();
        controller.assertBuffer(shard1, new ByteSizeValue(1, ByteSizeUnit.MB));

        // remove second shard
        controller.deleteShard(shard1);
        controller.forceCheck();

        // add a new one
        IndexShard shard2 = test.getShard(2);
        controller.simulateIndexing(shard2);
        controller.assertBuffer(shard2, new ByteSizeValue(1, ByteSizeUnit.MB));
    }

    public void testActiveInactive() {

        createIndex("test", Settings.builder().put(SETTING_NUMBER_OF_SHARDS, 2).put(SETTING_NUMBER_OF_REPLICAS, 0).build());
        IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        IndexService test = indicesService.indexService("test");

        MockController controller = new MockController(Settings.builder()
                .put(IndexingMemoryController.INDEX_BUFFER_SIZE_SETTING, "5mb")
                .build());

        IndexShard shard0 = test.getShard(0);
        controller.simulateIndexing(shard0);
        IndexShard shard1 = test.getShard(1);
        controller.simulateIndexing(shard1);

        controller.assertBuffer(shard0, new ByteSizeValue(1, ByteSizeUnit.MB));
        controller.assertBuffer(shard1, new ByteSizeValue(1, ByteSizeUnit.MB));

        controller.simulateIndexing(shard0);
        controller.simulateIndexing(shard1);

        controller.assertBuffer(shard0, new ByteSizeValue(2, ByteSizeUnit.MB));
        controller.assertBuffer(shard1, new ByteSizeValue(2, ByteSizeUnit.MB));

        // index into one shard only, crosses the 5mb limit, so shard1 is refreshed
        controller.simulateIndexing(shard0);
        controller.simulateIndexing(shard0);
        controller.assertBuffer(shard0, new ByteSizeValue(0, ByteSizeUnit.MB));
        controller.assertBuffer(shard1, new ByteSizeValue(2, ByteSizeUnit.MB));

        controller.simulateIndexing(shard1);
        controller.simulateIndexing(shard1);
        controller.assertBuffer(shard1, new ByteSizeValue(4, ByteSizeUnit.MB));
        controller.simulateIndexing(shard1);
        controller.simulateIndexing(shard1);
        // shard1 crossed 5 mb and is now cleared:
        controller.assertBuffer(shard1, new ByteSizeValue(0, ByteSizeUnit.MB));
    }

    public void testMinBufferSizes() {
        MockController controller = new MockController(Settings.builder()
            .put(IndexingMemoryController.INDEX_BUFFER_SIZE_SETTING, "0.001%")
            .put(IndexingMemoryController.MIN_INDEX_BUFFER_SIZE_SETTING, "6mb").build());

        assertThat(controller.indexingBufferSize(), equalTo(new ByteSizeValue(6, ByteSizeUnit.MB)));
    }

    public void testMaxBufferSizes() {
        MockController controller = new MockController(Settings.builder()
                .put(IndexingMemoryController.INDEX_BUFFER_SIZE_SETTING, "90%")
                .put(IndexingMemoryController.MAX_INDEX_BUFFER_SIZE_SETTING, "6mb").build());

        assertThat(controller.indexingBufferSize(), equalTo(new ByteSizeValue(6, ByteSizeUnit.MB)));
    }
}
