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
import org.elasticsearch.index.engine.EngineConfig;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.translog.TranslogConfig;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

public class IndexingMemoryControllerTests extends ESTestCase {

    static class MockController extends IndexingMemoryController {

        final Map<ShardId, Long> indexBufferRAMBytesUsed = new HashMap<>();

        public MockController(Settings settings) {
            super(Settings.builder()
                            .put(SHARD_MEMORY_INTERVAL_TIME_SETTING, "200h") // disable it
                            .put(settings)
                            .build(),
                    null, null, 100 * 1024 * 1024); // fix jvm mem size to 100mb
        }

        public void deleteShard(ShardId shardId) {
            indexBufferRAMBytesUsed.remove(shardId);
        }

        @Override
        protected List<ShardId> availableShards() {
            return new ArrayList<>(indexBufferRAMBytesUsed.keySet());
        }

        @Override
        protected boolean shardAvailable(ShardId shardId) {
            return indexBufferRAMBytesUsed.containsKey(shardId);
        }

        @Override
        protected long getIndexBufferRAMBytesUsed(ShardId shardId) {
            Long used = indexBufferRAMBytesUsed.get(shardId);
            if (used == null) {
                return 0;
            } else {
                return used;
            }
        }

        @Override
        protected void checkIdle(ShardId shardId, long inactiveTimeNS) {
        }

        @Override
        public void refreshShardAsync(ShardId shardId) {
            indexBufferRAMBytesUsed.put(shardId, 0L);
        }

        public void assertBuffer(ShardId shardId, ByteSizeValue expected) {
            Long actual = indexBufferRAMBytesUsed.get(shardId);
            assertEquals(expected.bytes(), actual.longValue());
        }

        public void simulateIndexing(ShardId shardId) {
            Long bytes = indexBufferRAMBytesUsed.get(shardId);
            if (bytes == null) {
                bytes = 0L;
            }
            // Each doc we index takes up a megabyte!
            bytes += 1024*1024;
            indexBufferRAMBytesUsed.put(shardId, bytes);
            forceCheck();
        }
    }

    public void testShardAdditionAndRemoval() {
        MockController controller = new MockController(Settings.builder()
                .put(IndexingMemoryController.INDEX_BUFFER_SIZE_SETTING, "4mb").build());
        final ShardId shard1 = new ShardId("test", 1);
        controller.simulateIndexing(shard1);
        controller.assertBuffer(shard1, new ByteSizeValue(1, ByteSizeUnit.MB));

        // add another shard
        final ShardId shard2 = new ShardId("test", 2);
        controller.simulateIndexing(shard2);
        controller.assertBuffer(shard1, new ByteSizeValue(1, ByteSizeUnit.MB));
        controller.assertBuffer(shard2, new ByteSizeValue(1, ByteSizeUnit.MB));

        // remove first shard
        controller.deleteShard(shard1);
        controller.forceCheck();
        controller.assertBuffer(shard2, new ByteSizeValue(1, ByteSizeUnit.MB));

        // remove second shard
        controller.deleteShard(shard2);
        controller.forceCheck();

        // add a new one
        final ShardId shard3 = new ShardId("test", 3);
        controller.simulateIndexing(shard3);
        controller.assertBuffer(shard3, new ByteSizeValue(1, ByteSizeUnit.MB));
    }

    public void testActiveInactive() {
        MockController controller = new MockController(Settings.builder()
                .put(IndexingMemoryController.INDEX_BUFFER_SIZE_SETTING, "5mb")
                .build());

        final ShardId shard1 = new ShardId("test", 1);
        controller.simulateIndexing(shard1);
        final ShardId shard2 = new ShardId("test", 2);
        controller.simulateIndexing(shard2);
        controller.assertBuffer(shard1, new ByteSizeValue(1, ByteSizeUnit.MB));
        controller.assertBuffer(shard2, new ByteSizeValue(1, ByteSizeUnit.MB));

        controller.simulateIndexing(shard1);
        controller.simulateIndexing(shard2);

        controller.assertBuffer(shard1, new ByteSizeValue(2, ByteSizeUnit.MB));
        controller.assertBuffer(shard2, new ByteSizeValue(2, ByteSizeUnit.MB));

        // index into one shard only, crosses the 5mb limit, so shard1 is refreshed
        controller.simulateIndexing(shard1);
        controller.simulateIndexing(shard1);
        controller.assertBuffer(shard1, new ByteSizeValue(0, ByteSizeUnit.MB));
        controller.assertBuffer(shard2, new ByteSizeValue(2, ByteSizeUnit.MB));

        controller.simulateIndexing(shard2);
        controller.simulateIndexing(shard2);
        controller.assertBuffer(shard2, new ByteSizeValue(4, ByteSizeUnit.MB));
        controller.simulateIndexing(shard2);
        controller.simulateIndexing(shard2);
        // shard2 crossed 5 mb and is now cleared:
        controller.assertBuffer(shard2, new ByteSizeValue(0, ByteSizeUnit.MB));
    }

    public void testRelativeBufferSizes() {
        MockController controller = new MockController(Settings.builder()
                .put(IndexingMemoryController.INDEX_BUFFER_SIZE_SETTING, "50%")
                .build());

        assertThat(controller.indexingBufferSize(), equalTo(new ByteSizeValue(50, ByteSizeUnit.MB)));
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
