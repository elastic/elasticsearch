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

import org.apache.lucene.index.DirectoryReader;
import org.elasticsearch.Version;
import org.elasticsearch.action.admin.indices.forcemerge.ForceMergeResponse;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.CheckedFunction;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardIT;
import org.elasticsearch.index.shard.IndexShardTestCase;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.threadpool.Scheduler.Cancellable;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.equalTo;

public class IndexingMemoryControllerTests extends ESSingleNodeTestCase {

    static class MockController extends IndexingMemoryController {

        // Size of each shard's indexing buffer
        final Map<IndexShard, Long> indexBufferRAMBytesUsed = new HashMap<>();

        // How many bytes this shard is currently moving to disk
        final Map<IndexShard, Long> writingBytes = new HashMap<>();

        // Shards that are currently throttled
        final Set<IndexShard> throttled = new HashSet<>();

        MockController(Settings settings) {
            super(Settings.builder()
                            .put("indices.memory.interval", "200h") // disable it
                            .put(settings)
                            .build(), null, null);
        }

        public void deleteShard(IndexShard shard) {
            indexBufferRAMBytesUsed.remove(shard);
            writingBytes.remove(shard);
        }

        @Override
        protected List<IndexShard> availableShards() {
            return new ArrayList<>(indexBufferRAMBytesUsed.keySet());
        }

        @Override
        protected long getIndexBufferRAMBytesUsed(IndexShard shard) {
            return indexBufferRAMBytesUsed.get(shard) + writingBytes.get(shard);
        }

        @Override
        protected long getShardWritingBytes(IndexShard shard) {
            Long bytes = writingBytes.get(shard);
            if (bytes == null) {
                return 0;
            } else {
                return bytes;
            }
        }

        @Override
        protected void checkIdle(IndexShard shard, long inactiveTimeNS) {
        }

        @Override
        public void writeIndexingBufferAsync(IndexShard shard) {
            long bytes = indexBufferRAMBytesUsed.put(shard, 0L);
            writingBytes.put(shard, writingBytes.get(shard) + bytes);
            indexBufferRAMBytesUsed.put(shard, 0L);
        }

        @Override
        public void activateThrottling(IndexShard shard) {
            assertTrue(throttled.add(shard));
        }

        @Override
        public void deactivateThrottling(IndexShard shard) {
            assertTrue(throttled.remove(shard));
        }

        public void doneWriting(IndexShard shard) {
            writingBytes.put(shard, 0L);
        }

        public void assertBuffer(IndexShard shard, int expectedMB) {
            Long actual = indexBufferRAMBytesUsed.get(shard);
            if (actual == null) {
                actual = 0L;
            }
            assertEquals(expectedMB * 1024 * 1024, actual.longValue());
        }

        public void assertThrottled(IndexShard shard) {
            assertTrue(throttled.contains(shard));
        }

        public void assertNotThrottled(IndexShard shard) {
            assertFalse(throttled.contains(shard));
        }

        public void assertWriting(IndexShard shard, int expectedMB) {
            Long actual = writingBytes.get(shard);
            if (actual == null) {
                actual = 0L;
            }
            assertEquals(expectedMB * 1024 * 1024, actual.longValue());
        }

        public void simulateIndexing(IndexShard shard) {
            Long bytes = indexBufferRAMBytesUsed.get(shard);
            if (bytes == null) {
                bytes = 0L;
                // First time we are seeing this shard:
                writingBytes.put(shard, 0L);
            }
            // Each doc we index takes up a megabyte!
            bytes += 1024*1024;
            indexBufferRAMBytesUsed.put(shard, bytes);
            forceCheck();
        }

        @Override
        protected Cancellable scheduleTask(ThreadPool threadPool) {
            return null;
        }
    }

    public void testShardAdditionAndRemoval() {
        createIndex("test", Settings.builder().put("index.number_of_shards", 3).put("index.number_of_replicas", 0).build());
        IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        IndexService test = indicesService.indexService(resolveIndex("test"));

        MockController controller = new MockController(Settings.builder()
                                                       .put("indices.memory.index_buffer_size", "4mb").build());
        IndexShard shard0 = test.getShard(0);
        controller.simulateIndexing(shard0);
        controller.assertBuffer(shard0, 1);

        // add another shard
        IndexShard shard1 = test.getShard(1);
        controller.simulateIndexing(shard1);
        controller.assertBuffer(shard0, 1);
        controller.assertBuffer(shard1, 1);

        // remove first shard
        controller.deleteShard(shard0);
        controller.forceCheck();
        controller.assertBuffer(shard1, 1);

        // remove second shard
        controller.deleteShard(shard1);
        controller.forceCheck();

        // add a new one
        IndexShard shard2 = test.getShard(2);
        controller.simulateIndexing(shard2);
        controller.assertBuffer(shard2, 1);
    }

    public void testActiveInactive() {

        createIndex("test", Settings.builder().put("index.number_of_shards", 2).put("index.number_of_replicas", 0).build());
        IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        IndexService test = indicesService.indexService(resolveIndex("test"));

        MockController controller = new MockController(Settings.builder()
                                                       .put("indices.memory.index_buffer_size", "5mb")
                                                       .build());

        IndexShard shard0 = test.getShard(0);
        controller.simulateIndexing(shard0);
        IndexShard shard1 = test.getShard(1);
        controller.simulateIndexing(shard1);

        controller.assertBuffer(shard0, 1);
        controller.assertBuffer(shard1, 1);

        controller.simulateIndexing(shard0);
        controller.simulateIndexing(shard1);

        controller.assertBuffer(shard0, 2);
        controller.assertBuffer(shard1, 2);

        // index into one shard only, crosses the 5mb limit, so shard1 is refreshed
        controller.simulateIndexing(shard0);
        controller.simulateIndexing(shard0);
        controller.assertBuffer(shard0, 0);
        controller.assertBuffer(shard1, 2);

        controller.simulateIndexing(shard1);
        controller.simulateIndexing(shard1);
        controller.assertBuffer(shard1, 4);
        controller.simulateIndexing(shard1);
        controller.simulateIndexing(shard1);
        // shard1 crossed 5 mb and is now cleared:
        controller.assertBuffer(shard1, 0);
    }

    public void testMinBufferSizes() {
        MockController controller = new MockController(Settings.builder()
                                                       .put("indices.memory.index_buffer_size", "0.001%")
                                                       .put("indices.memory.min_index_buffer_size", "6mb").build());

        assertThat(controller.indexingBufferSize(), equalTo(new ByteSizeValue(6, ByteSizeUnit.MB)));
    }

    public void testNegativeMinIndexBufferSize() {
        Exception e = expectThrows(IllegalArgumentException.class,
                                   () -> new MockController(Settings.builder()
                                                            .put("indices.memory.min_index_buffer_size", "-6mb").build()));
        assertEquals("failed to parse setting [indices.memory.min_index_buffer_size] with value [-6mb] as a size in bytes", e.getMessage());

    }

    public void testNegativeInterval() {
        Exception e = expectThrows(IllegalArgumentException.class,
                                   () -> new MockController(Settings.builder()
                                                            .put("indices.memory.interval", "-42s").build()));
        assertEquals("failed to parse value [-42s] for setting [indices.memory.interval], must be >= [0ms]", e.getMessage());

    }

    public void testNegativeShardInactiveTime() {
        Exception e = expectThrows(IllegalArgumentException.class,
                                   () -> new MockController(Settings.builder()
                                                            .put("indices.memory.shard_inactive_time", "-42s").build()));
        assertEquals("failed to parse value [-42s] for setting [indices.memory.shard_inactive_time], must be >= [0ms]", e.getMessage());

    }

    public void testNegativeMaxIndexBufferSize() {
        Exception e = expectThrows(IllegalArgumentException.class,
                                   () -> new MockController(Settings.builder()
                                                            .put("indices.memory.max_index_buffer_size", "-6mb").build()));
        assertEquals("failed to parse setting [indices.memory.max_index_buffer_size] with value [-6mb] as a size in bytes", e.getMessage());

    }

    public void testMaxBufferSizes() {
        MockController controller = new MockController(Settings.builder()
                                                       .put("indices.memory.index_buffer_size", "90%")
                                                       .put("indices.memory.max_index_buffer_size", "6mb").build());

        assertThat(controller.indexingBufferSize(), equalTo(new ByteSizeValue(6, ByteSizeUnit.MB)));
    }

    public void testThrottling() throws Exception {
        createIndex("test", Settings.builder().put("index.number_of_shards", 3).put("index.number_of_replicas", 0).build());
        IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        IndexService test = indicesService.indexService(resolveIndex("test"));

        MockController controller = new MockController(Settings.builder()
                                                       .put("indices.memory.index_buffer_size", "4mb").build());
        IndexShard shard0 = test.getShard(0);
        IndexShard shard1 = test.getShard(1);
        controller.simulateIndexing(shard0);
        controller.simulateIndexing(shard0);
        controller.simulateIndexing(shard0);
        controller.assertBuffer(shard0, 3);
        controller.simulateIndexing(shard1);
        controller.simulateIndexing(shard1);

        // We are now using 5 MB, so we should be writing shard0 since it's using the most heap:
        controller.assertWriting(shard0, 3);
        controller.assertWriting(shard1, 0);
        controller.assertBuffer(shard0, 0);
        controller.assertBuffer(shard1, 2);

        controller.simulateIndexing(shard0);
        controller.simulateIndexing(shard1);
        controller.simulateIndexing(shard1);

        // Now we are still writing 3 MB (shard0), and using 5 MB index buffers, so we should now 1) be writing shard1,
        // and 2) be throttling shard1:
        controller.assertWriting(shard0, 3);
        controller.assertWriting(shard1, 4);
        controller.assertBuffer(shard0, 1);
        controller.assertBuffer(shard1, 0);

        controller.assertNotThrottled(shard0);
        controller.assertThrottled(shard1);

        logger.info("--> Indexing more data");

        // More indexing to shard0
        controller.simulateIndexing(shard0);
        controller.simulateIndexing(shard0);
        controller.simulateIndexing(shard0);
        controller.simulateIndexing(shard0);

        // Now we are using 5 MB again, so shard0 should also be writing and now also be throttled:
        controller.assertWriting(shard0, 8);
        controller.assertWriting(shard1, 4);
        controller.assertBuffer(shard0, 0);
        controller.assertBuffer(shard1, 0);

        controller.assertThrottled(shard0);
        controller.assertThrottled(shard1);

        // Both shards finally finish writing, and throttling should stop:
        controller.doneWriting(shard0);
        controller.doneWriting(shard1);
        controller.forceCheck();
        controller.assertNotThrottled(shard0);
        controller.assertNotThrottled(shard1);
    }

    // #10312
    public void testDeletesAloneCanTriggerRefresh() throws Exception {
        createIndex("index",
                    Settings.builder().put("index.number_of_shards", 1)
                                      .put("index.number_of_replicas", 0)
                                      .put("index.refresh_interval", -1)
                                      .build());
        ensureGreen();

        IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        IndexService indexService = indicesService.indexService(resolveIndex("index"));
        IndexShard shard = indexService.getShardOrNull(0);
        assertNotNull(shard);

        for (int i = 0; i < 100; i++) {
            String id = Integer.toString(i);
            client().prepareIndex("index", "type", id).setSource("field", "value").get();
        }

        // Force merge so we know all merges are done before we start deleting:
        ForceMergeResponse r = client().admin().indices().prepareForceMerge().setMaxNumSegments(1).execute().actionGet();
        assertNoFailures(r);

        // Make a shell of an IMC to check up on indexing buffer usage:
        Settings settings = Settings.builder().put("indices.memory.index_buffer_size", "1kb").build();

        // TODO: would be cleaner if I could pass this 1kb setting to the single node this test created....
        IndexingMemoryController imc = new IndexingMemoryController(settings, null, null) {
            @Override
            protected List<IndexShard> availableShards() {
                return Collections.singletonList(shard);
            }

            @Override
            protected long getIndexBufferRAMBytesUsed(IndexShard shard) {
                return shard.getIndexBufferRAMBytesUsed();
            }

            @Override
            protected void writeIndexingBufferAsync(IndexShard shard) {
                // just do it sync'd for this test
                shard.writeIndexingBuffer();
            }

            @Override
            protected Cancellable scheduleTask(ThreadPool threadPool) {
                return null;
            }
        };

        for (int i = 0; i < 100; i++) {
            String id = Integer.toString(i);
            client().prepareDelete("index", "type", id).get();
        }

        final long indexingBufferBytes1 = shard.getIndexBufferRAMBytesUsed();

        imc.forceCheck();

        // We must assertBusy because the writeIndexingBufferAsync is done in background (REFRESH) thread pool:
        assertBusy(() -> {
            try (Engine.Searcher s2 = shard.acquireSearcher("index")) {
                // 100 buffered deletes will easily exceed our 1 KB indexing buffer so it should trigger a write:
                final long indexingBufferBytes2 = shard.getIndexBufferRAMBytesUsed();
                assertTrue(indexingBufferBytes2 < indexingBufferBytes1);
            }
        });
    }

    public void testTranslogRecoveryWorksWithIMC() throws IOException {
        createIndex("test");
        ensureGreen();
        IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        IndexService indexService = indicesService.indexService(resolveIndex("test"));
        IndexShard shard = indexService.getShardOrNull(0);
        for (int i = 0; i < 100; i++) {
            client().prepareIndex("test", "test", Integer.toString(i)).setSource("{\"foo\" : \"bar\"}", XContentType.JSON).get();
        }

        CheckedFunction<DirectoryReader, DirectoryReader, IOException> wrapper = directoryReader -> directoryReader;
        shard.close("simon says", false);
        AtomicReference<IndexShard> shardRef = new AtomicReference<>();
        Settings settings = Settings.builder().put("indices.memory.index_buffer_size", "50kb").build();
        Iterable<IndexShard> iterable = () -> (shardRef.get() == null) ? Collections.<IndexShard>emptyList().iterator()
            : Collections.singleton(shardRef.get()).iterator();
        AtomicInteger flushes = new AtomicInteger();
        IndexingMemoryController imc = new IndexingMemoryController(settings, client().threadPool(), iterable) {
            @Override
            protected void writeIndexingBufferAsync(IndexShard shard) {
                assertEquals(shard, shardRef.get());
                flushes.incrementAndGet();
                shard.writeIndexingBuffer();
            }
        };
        final IndexShard newShard = IndexShardIT.newIndexShard(indexService, shard, wrapper, new NoneCircuitBreakerService(), imc);
        shardRef.set(newShard);
        try {
            assertEquals(0, imc.availableShards().size());
            ShardRouting routing = newShard.routingEntry();
            DiscoveryNode localNode = new DiscoveryNode("foo", buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT);
            newShard.markAsRecovering("store", new RecoveryState(routing, localNode, null));

            assertEquals(1, imc.availableShards().size());
            assertTrue(newShard.recoverFromStore());
            assertTrue("we should have flushed in IMC at least once but did: " + flushes.get(), flushes.get() >= 1);
            IndexShardTestCase.updateRoutingEntry(newShard, routing.moveToStarted());
        } finally {
            newShard.close("simon says", false);
        }
    }

}
