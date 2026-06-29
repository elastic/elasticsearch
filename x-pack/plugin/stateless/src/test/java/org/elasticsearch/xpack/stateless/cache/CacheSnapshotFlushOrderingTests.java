/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.cache;

import org.elasticsearch.blobcache.BlobCacheMetrics;
import org.elasticsearch.blobcache.common.ByteRange;
import org.elasticsearch.blobcache.shared.DefaultEvictionPolicy;
import org.elasticsearch.blobcache.shared.SharedBlobCacheService;
import org.elasticsearch.blobcache.shared.SharedBytes;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.ThreadLocalDirectoryMetricHolder;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.stateless.StatelessPlugin;
import org.elasticsearch.xpack.stateless.lucene.BlobStoreCacheDirectoryMetrics;
import org.elasticsearch.xpack.stateless.lucene.FileCacheKey;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.elasticsearch.blobcache.shared.SharedBlobCacheService.SHARED_CACHE_REGION_SIZE_SETTING;
import static org.elasticsearch.blobcache.shared.SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING;
import static org.elasticsearch.blobcache.shared.SharedBytes.PAGE_SIZE;
import static org.elasticsearch.node.Node.NODE_NAME_SETTING;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class CacheSnapshotFlushOrderingTests extends ESTestCase {

    /**
     * Region size used across tests: four pages, which is the smallest meaningful size that
     * allows a non-trivial {@code effectiveRegionSize} to be stored in a
     * {@link SharedBlobCacheService.CacheIndexEntry}.
     */
    private static final int REGION_SIZE = PAGE_SIZE * 4;

    /**
     * Subclass that returns a fixed list of {@link SharedBlobCacheService.CacheIndexEntry} objects
     * from {@link #getOccupiedEntries()} and records every {@link #syncSlotRange} call so that
     * tests can assert on the ordering and flags of those calls.
     */
    private static final class TrackingSyncCacheService extends StatelessSharedBlobCacheService {

        /** Keys pre-loaded as the "occupied" entry set for this service instance. */
        private final List<FileCacheKey> storedKeys;

        /** Owned {@link NodeEnvironment} that must be closed when this service is closed. */
        private final NodeEnvironment nodeEnvironment;

        /**
         * Each recorded call is an {@code int[]{slot, flags}} pair, appended in call order.
         * {@link CopyOnWriteArrayList} is used so that reads from the test thread do not need
         * synchronization.
         */
        final CopyOnWriteArrayList<int[]> syncCalls = new CopyOnWriteArrayList<>();

        TrackingSyncCacheService(NodeEnvironment environment, Settings settings, ThreadPool threadPool, List<FileCacheKey> keys) {
            super(
                environment,
                settings,
                threadPool,
                new BlobCacheMetrics(MeterRegistry.NOOP),
                new DefaultEvictionPolicy<>(),
                System::nanoTime,
                new ThreadLocalDirectoryMetricHolder<>(BlobStoreCacheDirectoryMetrics::new)
            );
            this.nodeEnvironment = environment;
            this.storedKeys = List.copyOf(keys);
        }

        @Override
        public void close() {
            try {
                super.close();
            } finally {
                nodeEnvironment.close();
            }
        }

        @Override
        public List<SharedBlobCacheService.CacheIndexEntry<FileCacheKey>> getOccupiedEntries() {
            List<SharedBlobCacheService.CacheIndexEntry<FileCacheKey>> entries = new ArrayList<>(storedKeys.size());
            for (int i = 0; i < storedKeys.size(); i++) {
                // Full range — the entire region is considered downloaded.
                SortedSet<ByteRange> fullRange = new TreeSet<>(Comparator.comparingLong(ByteRange::start));
                fullRange.add(ByteRange.of(0, REGION_SIZE));
                entries.add(new SharedBlobCacheService.CacheIndexEntry<>(i, storedKeys.get(i), 0, REGION_SIZE, fullRange));
            }
            return entries;
        }

        @Override
        public void syncSlotRange(int slot, int flags) throws IOException {
            syncCalls.add(new int[] { slot, flags });
        }
    }

    private static Settings buildSettings(String tmpDir) {
        // The cache must be large enough to hold at least as many regions as we store keys.
        // We use REGION_SIZE * 8 to give plenty of headroom.
        return Settings.builder()
            .put(NODE_NAME_SETTING.getKey(), "test-node")
            .put(SHARED_CACHE_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes((long) REGION_SIZE * 8).getStringRep())
            .put(SHARED_CACHE_REGION_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(REGION_SIZE).getStringRep())
            .put("path.home", tmpDir)
            .build();
    }

    private static FileCacheKey makeKey(int index) {
        ShardId shardId = new ShardId(new Index("test-index-" + index, "_na_"), 0);
        return new FileCacheKey(shardId, 1L, "file_" + index + ".cfs");
    }

    private TrackingSyncCacheService createTrackingService(ThreadPool threadPool, List<FileCacheKey> keys) throws IOException {
        String tmp = createTempDir().toAbsolutePath().toString();
        Settings settings = buildSettings(tmp);
        NodeEnvironment env = new NodeEnvironment(settings, TestEnvironment.newEnvironment(settings));
        return new TrackingSyncCacheService(env, settings, threadPool, keys);
    }

    private CacheSnapshotService createSnapshotService(Path snapshotDir, String snapshotId) {
        return new CacheSnapshotService(snapshotDir.resolve("test.snapshot"), () -> snapshotId);
    }

    /**
     * All {@code SYNC_FILE_RANGE_WRITE} calls must be issued before any
     * {@code SYNC_FILE_RANGE_WAIT_AFTER} call. This ensures the kernel initiates
     * write-back for every occupied slot before we start waiting for completion,
     * which maximises I/O overlap and reduces total flush latency.
     */
    public void testAllWriteCallsBeforeAnyWaitAfterCalls() throws Exception {
        ThreadPool threadPool = new TestThreadPool("test", StatelessPlugin.statelessExecutorBuilders(Settings.EMPTY, true));
        try {
            List<FileCacheKey> keys = List.of(makeKey(0), makeKey(1), makeKey(2));
            try (TrackingSyncCacheService service = createTrackingService(threadPool, keys)) {
                Path snapshotDir = createTempDir();
                CacheSnapshotService snapshotService = createSnapshotService(snapshotDir, "snap-xyz");

                String returnedId = snapshotService.snapshot(service, "node-1");
                assertThat(returnedId, equalTo("snap-xyz"));

                List<int[]> calls = List.copyOf(service.syncCalls);
                // 3 WRITE calls + 3 WAIT_AFTER calls = 6 total
                assertThat(calls, hasSize(6));

                // First three must all be SYNC_FILE_RANGE_WRITE
                for (int i = 0; i < 3; i++) {
                    assertThat(
                        "call[" + i + "] must be SYNC_FILE_RANGE_WRITE",
                        calls.get(i)[1],
                        equalTo(SharedBytes.SYNC_FILE_RANGE_WRITE)
                    );
                }

                // Last three must all be SYNC_FILE_RANGE_WAIT_AFTER
                for (int i = 3; i < 6; i++) {
                    assertThat(
                        "call[" + i + "] must be SYNC_FILE_RANGE_WAIT_AFTER",
                        calls.get(i)[1],
                        equalTo(SharedBytes.SYNC_FILE_RANGE_WAIT_AFTER)
                    );
                }
            }
        } finally {
            terminate(threadPool);
        }
    }

    /**
     * When the cache has no occupied entries the snapshot must succeed with zero sync calls
     * and must still return the cloud provider's snapshot ID.
     */
    public void testZeroEntriesProducesZeroSyncCalls() throws Exception {
        ThreadPool threadPool = new TestThreadPool("test", StatelessPlugin.statelessExecutorBuilders(Settings.EMPTY, true));
        try {
            try (TrackingSyncCacheService service = createTrackingService(threadPool, List.of())) {
                Path snapshotDir = createTempDir();
                CacheSnapshotService snapshotService = createSnapshotService(snapshotDir, "snap-empty");

                String returnedId = snapshotService.snapshot(service, "node-1");

                assertThat(returnedId, equalTo("snap-empty"));
                assertThat(service.syncCalls, hasSize(0));

                // The snapshot metadata file must also have been written.
                assertTrue(Files.exists(snapshotDir.resolve("test.snapshot")));
            }
        } finally {
            terminate(threadPool);
        }
    }
}
