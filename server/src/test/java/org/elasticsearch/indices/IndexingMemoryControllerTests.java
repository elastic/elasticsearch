/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.indices;

import org.apache.lucene.search.ReferenceManager;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.codec.CodecService;
import org.elasticsearch.index.engine.EngineConfig;
import org.elasticsearch.index.engine.InternalEngine;
import org.elasticsearch.index.refresh.RefreshStats;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardTestCase;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.threadpool.Scheduler.Cancellable;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.threadpool.ThreadPoolStats;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

public class IndexingMemoryControllerTests extends IndexShardTestCase {

    static class MockController extends IndexingMemoryController {

        // Size of each shard's indexing buffer
        final Map<IndexShard, Long> indexBufferRAMBytesUsed = new HashMap<>();

        // How many bytes this shard is currently moving to disk
        final Map<IndexShard, Long> writingBytes = new HashMap<>();

        // Shards that are currently throttled
        final Set<IndexShard> throttled = new HashSet<>();

        MockController(Settings settings) {
            super(
                Settings.builder()
                    .put("indices.memory.interval", "200h") // disable it
                    .put(settings)
                    .build(),
                null,
                null
            );
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
        protected void checkIdle(IndexShard shard, long inactiveTimeNS) {}

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
            bytes += 1024 * 1024;
            indexBufferRAMBytesUsed.put(shard, bytes);
            forceCheck();
        }

        @Override
        protected Cancellable scheduleTask(ThreadPool threadPool) {
            return null;
        }
    }

    public void testShardAdditionAndRemoval() throws IOException {

        MockController controller = new MockController(Settings.builder().put("indices.memory.index_buffer_size", "4mb").build());
        IndexShard shard0 = newStartedShard();
        controller.simulateIndexing(shard0);
        controller.assertBuffer(shard0, 1);

        // add another shard
        IndexShard shard1 = newStartedShard();
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
        IndexShard shard2 = newStartedShard();
        controller.simulateIndexing(shard2);
        controller.assertBuffer(shard2, 1);
        closeShards(shard0, shard1, shard2);
    }

    public void testActiveInactive() throws IOException {

        MockController controller = new MockController(Settings.builder().put("indices.memory.index_buffer_size", "5mb").build());

        IndexShard shard0 = newStartedShard();
        controller.simulateIndexing(shard0);
        IndexShard shard1 = newStartedShard();
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
        closeShards(shard0, shard1);
    }

    public void testMinBufferSizes() {
        MockController controller = new MockController(
            Settings.builder().put("indices.memory.index_buffer_size", "0.001%").put("indices.memory.min_index_buffer_size", "6mb").build()
        );

        assertThat(controller.indexingBufferSize(), equalTo(new ByteSizeValue(6, ByteSizeUnit.MB)));
    }

    public void testNegativeMinIndexBufferSize() {
        Exception e = expectThrows(
            IllegalArgumentException.class,
            () -> new MockController(Settings.builder().put("indices.memory.min_index_buffer_size", "-6mb").build())
        );
        assertEquals("failed to parse setting [indices.memory.min_index_buffer_size] with value [-6mb] as a size in bytes", e.getMessage());

    }

    public void testNegativeInterval() {
        Exception e = expectThrows(
            IllegalArgumentException.class,
            () -> new MockController(Settings.builder().put("indices.memory.interval", "-42s").build())
        );
        assertEquals(
            "failed to parse setting [indices.memory.interval] with value "
                + "[-42s] as a time value: negative durations are not supported",
            e.getMessage()
        );

    }

    public void testNegativeShardInactiveTime() {
        Exception e = expectThrows(
            IllegalArgumentException.class,
            () -> new MockController(Settings.builder().put("indices.memory.shard_inactive_time", "-42s").build())
        );
        assertEquals(
            "failed to parse setting [indices.memory.shard_inactive_time] with value "
                + "[-42s] as a time value: negative durations are not supported",
            e.getMessage()
        );

    }

    public void testNegativeMaxIndexBufferSize() {
        Exception e = expectThrows(
            IllegalArgumentException.class,
            () -> new MockController(Settings.builder().put("indices.memory.max_index_buffer_size", "-6mb").build())
        );
        assertEquals("failed to parse setting [indices.memory.max_index_buffer_size] with value [-6mb] as a size in bytes", e.getMessage());

    }

    public void testMaxBufferSizes() {
        MockController controller = new MockController(
            Settings.builder().put("indices.memory.index_buffer_size", "90%").put("indices.memory.max_index_buffer_size", "6mb").build()
        );

        assertThat(controller.indexingBufferSize(), equalTo(new ByteSizeValue(6, ByteSizeUnit.MB)));
    }

    public void testThrottling() throws Exception {

        MockController controller = new MockController(Settings.builder().put("indices.memory.index_buffer_size", "4mb").build());
        IndexShard shard0 = newStartedShard();
        IndexShard shard1 = newStartedShard();
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
        closeShards(shard0, shard1);
    }

    public void testTranslogRecoveryWorksWithIMC() throws IOException {
        IndexShard shard = newStartedShard(true);
        for (int i = 0; i < 100; i++) {
            indexDoc(shard, Integer.toString(i), "{\"foo\" : \"bar\"}", XContentType.JSON, null);
        }
        shard.close("simon says", false);
        AtomicReference<IndexShard> shardRef = new AtomicReference<>();
        Settings settings = Settings.builder().put("indices.memory.index_buffer_size", "50kb").build();
        Iterable<IndexShard> iterable = () -> (shardRef.get() == null)
            ? Collections.emptyIterator()
            : Collections.singleton(shardRef.get()).iterator();
        AtomicInteger flushes = new AtomicInteger();
        IndexingMemoryController imc = new IndexingMemoryController(settings, threadPool, iterable) {
            @Override
            protected void writeIndexingBufferAsync(IndexShard shard) {
                assertEquals(shard, shardRef.get());
                flushes.incrementAndGet();
                shard.writeIndexingBuffer();
            }
        };
        shard = reinitShard(shard, imc);
        shardRef.set(shard);
        assertEquals(0, imc.availableShards().size());
        DiscoveryNode localNode = new DiscoveryNode("foo", buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT);
        shard.markAsRecovering("store", new RecoveryState(shard.routingEntry(), localNode, null));

        assertEquals(1, imc.availableShards().size());
        assertTrue(recoverFromStore(shard));
        assertThat("we should have flushed in IMC at least once", flushes.get(), greaterThanOrEqualTo(1));
        closeShards(shard);
    }

    EngineConfig configWithRefreshListener(EngineConfig config, ReferenceManager.RefreshListener listener) {
        final List<ReferenceManager.RefreshListener> internalRefreshListener = new ArrayList<>(config.getInternalRefreshListener());
        ;
        internalRefreshListener.add(listener);
        return new EngineConfig(
            config.getShardId(),
            config.getThreadPool(),
            config.getIndexSettings(),
            config.getWarmer(),
            config.getStore(),
            config.getMergePolicy(),
            config.getAnalyzer(),
            config.getSimilarity(),
            new CodecService(null),
            config.getEventListener(),
            config.getQueryCache(),
            config.getQueryCachingPolicy(),
            config.getTranslogConfig(),
            config.getFlushMergesAfter(),
            config.getExternalRefreshListener(),
            internalRefreshListener,
            config.getIndexSort(),
            config.getCircuitBreakerService(),
            config.getGlobalCheckpointSupplier(),
            config.retentionLeasesSupplier(),
            config.getPrimaryTermSupplier(),
            config.getSnapshotCommitSupplier(),
            config.getLeafSorter()
        );
    }

    ThreadPoolStats.Stats getRefreshThreadPoolStats() {
        final ThreadPoolStats stats = threadPool.stats();
        for (ThreadPoolStats.Stats s : stats) {
            if (s.getName().equals(ThreadPool.Names.REFRESH)) {
                return s;
            }
        }
        throw new AssertionError("refresh thread pool stats not found [" + stats + "]");
    }

    public void testSkipRefreshIfShardIsRefreshingAlready() throws Exception {
        SetOnce<CountDownLatch> refreshLatch = new SetOnce<>();
        ReferenceManager.RefreshListener refreshListener = new ReferenceManager.RefreshListener() {
            @Override
            public void beforeRefresh() {
                if (refreshLatch.get() != null) {
                    try {
                        refreshLatch.get().await();
                    } catch (InterruptedException e) {
                        throw new AssertionError(e);
                    }
                }
            }

            @Override
            public void afterRefresh(boolean didRefresh) {

            }
        };
        IndexShard shard = newStartedShard(
            randomBoolean(),
            Settings.EMPTY,
            config -> new InternalEngine(configWithRefreshListener(config, refreshListener))
        );
        refreshLatch.set(new CountDownLatch(1)); // block refresh
        final RefreshStats refreshStats = shard.refreshStats();
        final IndexingMemoryController controller = new IndexingMemoryController(
            Settings.builder()
                .put("indices.memory.interval", "200h") // disable it
                .put("indices.memory.index_buffer_size", "1024b")
                .build(),
            threadPool,
            Collections.singleton(shard)
        ) {
            @Override
            protected long getIndexBufferRAMBytesUsed(IndexShard shard) {
                return randomLongBetween(1025, 10 * 1024 * 1024);
            }

            @Override
            protected long getShardWritingBytes(IndexShard shard) {
                return 0L;
            }
        };
        int iterations = randomIntBetween(10, 100);
        ThreadPoolStats.Stats beforeStats = getRefreshThreadPoolStats();
        for (int i = 0; i < iterations; i++) {
            controller.forceCheck();
        }
        assertBusy(() -> {
            ThreadPoolStats.Stats stats = getRefreshThreadPoolStats();
            assertThat(stats.getCompleted(), equalTo(beforeStats.getCompleted() + iterations - 1));
        });
        refreshLatch.get().countDown(); // allow refresh
        assertBusy(() -> {
            ThreadPoolStats.Stats stats = getRefreshThreadPoolStats();
            assertThat(stats.getCompleted(), equalTo(beforeStats.getCompleted() + iterations));
        });
        assertThat(shard.refreshStats().getTotal(), equalTo(refreshStats.getTotal() + 1));
        closeShards(shard);
    }
}
