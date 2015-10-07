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

        final static ByteSizeValue INACTIVE = new ByteSizeValue(-1);

        final Map<ShardId, ByteSizeValue> indexingBuffers = new HashMap<>();
        final Map<ShardId, ByteSizeValue> translogBuffers = new HashMap<>();

        final Map<ShardId, Long> lastIndexTimeNanos = new HashMap<>();
        final Set<ShardId> activeShards = new HashSet<>();

        long currentTimeSec = TimeValue.timeValueNanos(System.nanoTime()).seconds();

        public MockController(Settings settings) {
            super(Settings.builder()
                            .put(SHARD_INACTIVE_INTERVAL_TIME_SETTING, "200h") // disable it
                            .put(SHARD_INACTIVE_TIME_SETTING, "1ms") // nearly immediate
                            .put(settings)
                            .build(),
                    null, null, 100 * 1024 * 1024); // fix jvm mem size to 100mb
        }

        public void deleteShard(ShardId id) {
            indexingBuffers.remove(id);
            translogBuffers.remove(id);
        }

        public void assertBuffers(ShardId id, ByteSizeValue indexing, ByteSizeValue translog) {
            assertThat(indexingBuffers.get(id), equalTo(indexing));
            assertThat(translogBuffers.get(id), equalTo(translog));
        }

        public void assertInActive(ShardId id) {
            assertThat(indexingBuffers.get(id), equalTo(INACTIVE));
            assertThat(translogBuffers.get(id), equalTo(INACTIVE));
        }

        @Override
        protected long currentTimeInNanos() {
            return TimeValue.timeValueSeconds(currentTimeSec).nanos();
        }

        @Override
        protected List<ShardId> availableShards() {
            return new ArrayList<>(indexingBuffers.keySet());
        }

        @Override
        protected boolean shardAvailable(ShardId shardId) {
            return indexingBuffers.containsKey(shardId);
        }

        @Override
        protected Boolean getShardActive(ShardId shardId) {
            return activeShards.contains(shardId);
        }

        @Override
        protected void updateShardBuffers(ShardId shardId, ByteSizeValue shardIndexingBufferSize, ByteSizeValue shardTranslogBufferSize) {
            indexingBuffers.put(shardId, shardIndexingBufferSize);
            translogBuffers.put(shardId, shardTranslogBufferSize);
        }

        @Override
        protected Boolean checkIdle(ShardId shardId, long inactiveTimeNS) {
            Long ns = lastIndexTimeNanos.get(shardId);
            if (ns == null) {
                return null;
            } else if (currentTimeInNanos() - ns >= inactiveTimeNS) {
                indexingBuffers.put(shardId, INACTIVE);
                translogBuffers.put(shardId, INACTIVE);
                activeShards.remove(shardId);
                return true;
            } else {
                return false;
            }
        }

        public void incrementTimeSec(int sec) {
            currentTimeSec += sec;
        }

        public void simulateIndexing(ShardId shardId) {
            lastIndexTimeNanos.put(shardId, currentTimeInNanos());
            if (indexingBuffers.containsKey(shardId) == false) {
                // First time we are seeing this shard; start it off with inactive buffers as IndexShard does:
                indexingBuffers.put(shardId, IndexingMemoryController.INACTIVE_SHARD_INDEXING_BUFFER);
                translogBuffers.put(shardId, IndexingMemoryController.INACTIVE_SHARD_TRANSLOG_BUFFER);
            }
            activeShards.add(shardId);
            forceCheck();
        }
    }

    public void testShardAdditionAndRemoval() {
        MockController controller = new MockController(Settings.builder()
                .put(IndexingMemoryController.INDEX_BUFFER_SIZE_SETTING, "10mb")
                .put(IndexingMemoryController.TRANSLOG_BUFFER_SIZE_SETTING, "100kb").build());
        final ShardId shard1 = new ShardId("test", 1);
        controller.simulateIndexing(shard1);
        controller.assertBuffers(shard1, new ByteSizeValue(10, ByteSizeUnit.MB), new ByteSizeValue(64, ByteSizeUnit.KB)); // translog is maxed at 64K

        // add another shard
        final ShardId shard2 = new ShardId("test", 2);
        controller.simulateIndexing(shard2);
        controller.assertBuffers(shard1, new ByteSizeValue(5, ByteSizeUnit.MB), new ByteSizeValue(50, ByteSizeUnit.KB));
        controller.assertBuffers(shard2, new ByteSizeValue(5, ByteSizeUnit.MB), new ByteSizeValue(50, ByteSizeUnit.KB));

        // remove first shard
        controller.deleteShard(shard1);
        controller.forceCheck();
        controller.assertBuffers(shard2, new ByteSizeValue(10, ByteSizeUnit.MB), new ByteSizeValue(64, ByteSizeUnit.KB)); // translog is maxed at 64K

        // remove second shard
        controller.deleteShard(shard2);
        controller.forceCheck();

        // add a new one
        final ShardId shard3 = new ShardId("test", 3);
        controller.simulateIndexing(shard3);
        controller.assertBuffers(shard3, new ByteSizeValue(10, ByteSizeUnit.MB), new ByteSizeValue(64, ByteSizeUnit.KB)); // translog is maxed at 64K
    }

    public void testActiveInactive() {
        MockController controller = new MockController(Settings.builder()
                .put(IndexingMemoryController.INDEX_BUFFER_SIZE_SETTING, "10mb")
                .put(IndexingMemoryController.TRANSLOG_BUFFER_SIZE_SETTING, "100kb")
                .put(IndexingMemoryController.SHARD_INACTIVE_TIME_SETTING, "5s")
                .build());

        final ShardId shard1 = new ShardId("test", 1);
        controller.simulateIndexing(shard1);
        final ShardId shard2 = new ShardId("test", 2);
        controller.simulateIndexing(shard2);
        controller.assertBuffers(shard1, new ByteSizeValue(5, ByteSizeUnit.MB), new ByteSizeValue(50, ByteSizeUnit.KB));
        controller.assertBuffers(shard2, new ByteSizeValue(5, ByteSizeUnit.MB), new ByteSizeValue(50, ByteSizeUnit.KB));

        // index into both shards, move the clock and see that they are still active
        controller.simulateIndexing(shard1);
        controller.simulateIndexing(shard2);

        controller.incrementTimeSec(10);
        controller.forceCheck();

        // both shards now inactive
        controller.assertInActive(shard1);
        controller.assertInActive(shard2);

        // index into one shard only, see it becomes active
        controller.simulateIndexing(shard1);
        controller.assertBuffers(shard1, new ByteSizeValue(10, ByteSizeUnit.MB), new ByteSizeValue(64, ByteSizeUnit.KB));
        controller.assertInActive(shard2);

        controller.incrementTimeSec(3); // increment but not enough to become inactive
        controller.forceCheck();
        controller.assertBuffers(shard1, new ByteSizeValue(10, ByteSizeUnit.MB), new ByteSizeValue(64, ByteSizeUnit.KB));
        controller.assertInActive(shard2);

        controller.incrementTimeSec(3); // increment some more
        controller.forceCheck();
        controller.assertInActive(shard1);
        controller.assertInActive(shard2);

        // index some and shard becomes immediately active
        controller.simulateIndexing(shard2);
        controller.assertInActive(shard1);
        controller.assertBuffers(shard2, new ByteSizeValue(10, ByteSizeUnit.MB), new ByteSizeValue(64, ByteSizeUnit.KB));
    }

    public void testMinShardBufferSizes() {
        MockController controller = new MockController(Settings.builder()
                .put(IndexingMemoryController.INDEX_BUFFER_SIZE_SETTING, "10mb")
                .put(IndexingMemoryController.TRANSLOG_BUFFER_SIZE_SETTING, "50kb")
                .put(IndexingMemoryController.MIN_SHARD_INDEX_BUFFER_SIZE_SETTING, "6mb")
                .put(IndexingMemoryController.MIN_SHARD_TRANSLOG_BUFFER_SIZE_SETTING, "40kb").build());

        assertTwoActiveShards(controller, new ByteSizeValue(6, ByteSizeUnit.MB), new ByteSizeValue(40, ByteSizeUnit.KB));
    }

    public void testMaxShardBufferSizes() {
        MockController controller = new MockController(Settings.builder()
                .put(IndexingMemoryController.INDEX_BUFFER_SIZE_SETTING, "10mb")
                .put(IndexingMemoryController.TRANSLOG_BUFFER_SIZE_SETTING, "50kb")
                .put(IndexingMemoryController.MAX_SHARD_INDEX_BUFFER_SIZE_SETTING, "3mb")
                .put(IndexingMemoryController.MAX_SHARD_TRANSLOG_BUFFER_SIZE_SETTING, "10kb").build());

        assertTwoActiveShards(controller, new ByteSizeValue(3, ByteSizeUnit.MB), new ByteSizeValue(10, ByteSizeUnit.KB));
    }

    public void testRelativeBufferSizes() {
        MockController controller = new MockController(Settings.builder()
                .put(IndexingMemoryController.INDEX_BUFFER_SIZE_SETTING, "50%")
                .put(IndexingMemoryController.TRANSLOG_BUFFER_SIZE_SETTING, "0.5%")
                .build());

        assertThat(controller.indexingBufferSize(), equalTo(new ByteSizeValue(50, ByteSizeUnit.MB)));
        assertThat(controller.translogBufferSize(), equalTo(new ByteSizeValue(512, ByteSizeUnit.KB)));
    }


    public void testMinBufferSizes() {
        MockController controller = new MockController(Settings.builder()
                .put(IndexingMemoryController.INDEX_BUFFER_SIZE_SETTING, "0.001%")
                .put(IndexingMemoryController.TRANSLOG_BUFFER_SIZE_SETTING, "0.001%")
                .put(IndexingMemoryController.MIN_INDEX_BUFFER_SIZE_SETTING, "6mb")
                .put(IndexingMemoryController.MIN_TRANSLOG_BUFFER_SIZE_SETTING, "512kb").build());

        assertThat(controller.indexingBufferSize(), equalTo(new ByteSizeValue(6, ByteSizeUnit.MB)));
        assertThat(controller.translogBufferSize(), equalTo(new ByteSizeValue(512, ByteSizeUnit.KB)));
    }

    public void testMaxBufferSizes() {
        MockController controller = new MockController(Settings.builder()
                .put(IndexingMemoryController.INDEX_BUFFER_SIZE_SETTING, "90%")
                .put(IndexingMemoryController.TRANSLOG_BUFFER_SIZE_SETTING, "90%")
                .put(IndexingMemoryController.MAX_INDEX_BUFFER_SIZE_SETTING, "6mb")
                .put(IndexingMemoryController.MAX_TRANSLOG_BUFFER_SIZE_SETTING, "512kb").build());

        assertThat(controller.indexingBufferSize(), equalTo(new ByteSizeValue(6, ByteSizeUnit.MB)));
        assertThat(controller.translogBufferSize(), equalTo(new ByteSizeValue(512, ByteSizeUnit.KB)));
    }

    protected void assertTwoActiveShards(MockController controller, ByteSizeValue indexBufferSize, ByteSizeValue translogBufferSize) {
        final ShardId shard1 = new ShardId("test", 1);
        controller.simulateIndexing(shard1);
        final ShardId shard2 = new ShardId("test", 2);
        controller.simulateIndexing(shard2);
        controller.assertBuffers(shard1, indexBufferSize, translogBufferSize);
        controller.assertBuffers(shard2, indexBufferSize, translogBufferSize);

    }

}
