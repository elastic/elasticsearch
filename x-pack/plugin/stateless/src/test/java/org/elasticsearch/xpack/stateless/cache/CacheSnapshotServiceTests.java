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
import org.elasticsearch.xpack.stateless.lucene.BlobStoreCacheDirectoryMetrics;
import org.elasticsearch.xpack.stateless.lucene.FileCacheKey;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

import static org.elasticsearch.blobcache.shared.SharedBlobCacheService.SHARED_CACHE_REGION_SIZE_SETTING;
import static org.elasticsearch.blobcache.shared.SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING;
import static org.elasticsearch.blobcache.shared.SharedBytes.PAGE_SIZE;
import static org.elasticsearch.node.Node.NODE_NAME_SETTING;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.sameInstance;

public class CacheSnapshotServiceTests extends ESTestCase {

    /** Region size used across all tests: 4 pages. */
    private static final int REGION_SIZE = PAGE_SIZE * 4;

    /**
     * Minimal subclass of {@link StatelessSharedBlobCacheService} that uses the protected test
     * constructor so that no real I/O against the underlying cache file is required. The caller
     * supplies a fixed list of occupied entries; {@link #syncSlotRange} is a no-op so that the
     * {@code sync_file_range} syscall is never issued during tests.
     */
    private static final class TestCacheService extends StatelessSharedBlobCacheService {

        private final List<SharedBlobCacheService.CacheIndexEntry<FileCacheKey>> occupiedEntries;

        TestCacheService(
            NodeEnvironment environment,
            Settings settings,
            ThreadPool threadPool,
            List<SharedBlobCacheService.CacheIndexEntry<FileCacheKey>> occupiedEntries
        ) {
            super(
                environment,
                settings,
                threadPool,
                new BlobCacheMetrics(MeterRegistry.NOOP),
                new DefaultEvictionPolicy<>(),
                System::nanoTime,
                new ThreadLocalDirectoryMetricHolder<>(BlobStoreCacheDirectoryMetrics::new)
            );
            this.occupiedEntries = occupiedEntries;
        }

        @Override
        public List<SharedBlobCacheService.CacheIndexEntry<FileCacheKey>> getOccupiedEntries() {
            return occupiedEntries;
        }

        @Override
        public void syncSlotRange(int slot, int flags) throws IOException {
            // no-op — avoids real sync_file_range calls in tests
        }
    }

    /**
     * Creates a {@link SharedBlobCacheService.CacheIndexEntry} for the given physical slot and
     * region, with a single completed range covering the full region.
     */
    private static SharedBlobCacheService.CacheIndexEntry<FileCacheKey> makeTestEntry(int slot, int region) {
        FileCacheKey key = new FileCacheKey(new ShardId(new Index("test-index", "_na_"), 0), 1L, "test_file.cfs");
        SortedSet<ByteRange> ranges = new TreeSet<>((a, b) -> Long.compare(a.start(), b.start()));
        ranges.add(ByteRange.of(0, REGION_SIZE));
        return new SharedBlobCacheService.CacheIndexEntry<>(slot, key, region, REGION_SIZE, ranges);
    }

    /**
     * Builds the {@link Settings} required to construct a {@link TestCacheService} with
     * {@code numRegions} regions of {@link #REGION_SIZE} bytes each, storing cache files
     * under {@code dataDir}.
     */
    private static Settings buildSettings(Path homeDir, Path dataDir, int numRegions) {
        return Settings.builder()
            .put(NODE_NAME_SETTING.getKey(), "test")
            .put(SHARED_CACHE_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes((long) REGION_SIZE * numRegions).getStringRep())
            .put(SHARED_CACHE_REGION_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(REGION_SIZE).getStringRep())
            .put("path.home", homeDir.toAbsolutePath())
            .put("path.data", dataDir.toAbsolutePath())
            .build();
    }

    // -------------------------------------------------------------------------
    // readSnapshotFile — edge cases that do not require a running cache service
    // -------------------------------------------------------------------------

    /**
     * When the snapshot metadata file does not exist, {@link CacheSnapshotService#readSnapshotFile}
     * must return {@link CacheSnapshotService#EMPTY}.
     */
    public void testReadSnapshotFileMissingFileReturnsEmpty() {
        Path missingPath = createTempDir().resolve("no-such-file.snapshot");
        CacheSnapshotService.SnapshotMetadata result = CacheSnapshotService.readSnapshotFile(missingPath, 4, REGION_SIZE);
        assertThat(result, sameInstance(CacheSnapshotService.EMPTY));
    }

    /**
     * A snapshot file that contains arbitrary non-JSON bytes must be handled gracefully; the method
     * must return {@link CacheSnapshotService#EMPTY} rather than propagating the parse exception.
     */
    public void testReadSnapshotFileMalformedJsonReturnsEmpty() throws IOException {
        Path snapshotFile = createTempDir().resolve("malformed.snapshot");
        Files.writeString(snapshotFile, "not valid json");
        CacheSnapshotService.SnapshotMetadata result = CacheSnapshotService.readSnapshotFile(snapshotFile, 4, REGION_SIZE);
        assertThat(result, sameInstance(CacheSnapshotService.EMPTY));
    }

    /**
     * A snapshot file whose {@code version} field does not match
     * {@link CacheSnapshotService#SNAPSHOT_FORMAT_VERSION} must be rejected and
     * {@link CacheSnapshotService#EMPTY} returned.
     */
    public void testReadSnapshotFileVersionMismatchReturnsEmpty() throws IOException {
        Path snapshotFile = createTempDir().resolve("version-mismatch.snapshot");
        // Write a structurally valid document but with an unrecognised version.
        String json = """
            {"version":99,"num_regions":4,"region_size":%d,"node_id":"node-1","entries":[]}
            """.formatted(REGION_SIZE);
        Files.writeString(snapshotFile, json);
        CacheSnapshotService.SnapshotMetadata result = CacheSnapshotService.readSnapshotFile(snapshotFile, 4, REGION_SIZE);
        assertThat(result, sameInstance(CacheSnapshotService.EMPTY));
    }

    /**
     * A snapshot file written with a larger cache ({@code num_regions=2}) is read back with
     * {@code numRegions=1}. The single entry with {@code slot=1} falls outside the valid slot range
     * {@code [0, numRegions)} and is silently dropped by the parser. The resulting entry list is
     * empty; since the caller must check {@link CacheSnapshotService.SnapshotMetadata#entries()}
     * before using the snapshot, this is the only assertion needed.
     */
    public void testReadSnapshotFileNumRegionsMismatchReturnsEmpty() throws IOException {
        Path snapshotFile = createTempDir().resolve("region-mismatch.snapshot");
        // Entry in slot 1 is out-of-range when numRegions=1.
        String json = """
            {
              "version": %d,
              "num_regions": 2,
              "region_size": %d,
              "node_id": "source-node",
              "entries": [
                {
                  "slot": 1,
                  "key": {"index":"idx","shard":0,"primary_term":1,"file_name":"f.cfs"},
                  "region": 0,
                  "effective_region_size": %d,
                  "ranges": [{"start":0,"end":%d}]
                }
              ]
            }
            """.formatted(CacheSnapshotService.SNAPSHOT_FORMAT_VERSION, REGION_SIZE, REGION_SIZE, REGION_SIZE);
        Files.writeString(snapshotFile, json);
        // Only 1 region — slot 1 is out-of-range and must be dropped, leaving an empty entry list.
        CacheSnapshotService.SnapshotMetadata result = CacheSnapshotService.readSnapshotFile(snapshotFile, 1, REGION_SIZE);
        assertThat(result.entries(), hasSize(0));
    }

    // -------------------------------------------------------------------------
    // snapshot + readSnapshotFile round-trip tests
    // -------------------------------------------------------------------------

    /**
     * Full round-trip: {@link CacheSnapshotService#snapshot} must write the metadata file and
     * return the snapshot ID supplied by the cloud provider. A subsequent
     * {@link CacheSnapshotService#readSnapshotFile} call must recover the source node ID and
     * the single occupied entry.
     */
    public void testSnapshotRoundTrip() throws Exception {
        int numRegions = 4;
        Path homeDir = createTempDir();
        Path dataDir = createTempDir();
        Settings settings = buildSettings(homeDir, dataDir, numRegions);

        try (
            TestThreadPool threadPool = new TestThreadPool(getTestName());
            NodeEnvironment env = new NodeEnvironment(settings, TestEnvironment.newEnvironment(settings));
            TestCacheService cacheService = new TestCacheService(env, settings, threadPool, List.of(makeTestEntry(0, 0)))
        ) {
            Path snapshotFile = createTempDir().resolve("cache.snapshot");
            CloudVolumeSnapshotProvider provider = () -> "snap-123";
            CacheSnapshotService service = new CacheSnapshotService(snapshotFile, provider);

            String snapshotId = service.snapshot(cacheService, "source-node-id");

            assertThat(snapshotId, equalTo("snap-123"));
            assertTrue("snapshot metadata file must exist after snapshot()", Files.exists(snapshotFile));

            CacheSnapshotService.SnapshotMetadata metadata = CacheSnapshotService.readSnapshotFile(snapshotFile, numRegions, REGION_SIZE);
            assertThat(metadata.sourceNodeId(), equalTo("source-node-id"));
            assertThat(metadata.entries(), hasSize(1));
        }
    }

    /**
     * When the cloud provider throws during {@link CacheSnapshotService#snapshot}, the method
     * must propagate the {@link IOException} and must also delete the partially-written snapshot
     * metadata file so that the next startup does not attempt to restore from a snapshot that was
     * never committed to the cloud.
     */
    public void testSnapshotDeletesFileOnCloudProviderFailure() throws Exception {
        int numRegions = 4;
        Path homeDir = createTempDir();
        Path dataDir = createTempDir();
        Settings settings = buildSettings(homeDir, dataDir, numRegions);

        try (
            TestThreadPool threadPool = new TestThreadPool(getTestName());
            NodeEnvironment env = new NodeEnvironment(settings, TestEnvironment.newEnvironment(settings));
            TestCacheService cacheService = new TestCacheService(env, settings, threadPool, List.of(makeTestEntry(0, 0)))
        ) {
            Path snapshotFile = createTempDir().resolve("cache.snapshot");
            CloudVolumeSnapshotProvider failingProvider = () -> { throw new IOException("simulated failure"); };
            CacheSnapshotService service = new CacheSnapshotService(snapshotFile, failingProvider);

            expectThrows(IOException.class, () -> service.snapshot(cacheService, "source-node-id"));

            assertFalse("snapshot metadata file must be cleaned up after a cloud provider failure", Files.exists(snapshotFile));
        }
    }

    /**
     * After a successful snapshot/restore round-trip, every field of the recovered entry must
     * match the original: key (index name, shard ID, primary term, file name), region, physical
     * slot, and a non-empty set of completed ranges.
     */
    public void testReadSnapshotFileRoundTripPreservesEntryFields() throws Exception {
        int numRegions = 4;
        Path homeDir = createTempDir();
        Path dataDir = createTempDir();
        Settings settings = buildSettings(homeDir, dataDir, numRegions);

        try (
            TestThreadPool threadPool = new TestThreadPool(getTestName());
            NodeEnvironment env = new NodeEnvironment(settings, TestEnvironment.newEnvironment(settings));
            TestCacheService cacheService = new TestCacheService(env, settings, threadPool, List.of(makeTestEntry(0, 0)))
        ) {
            Path snapshotFile = createTempDir().resolve("cache.snapshot");
            CloudVolumeSnapshotProvider provider = () -> "snap-xyz";
            CacheSnapshotService service = new CacheSnapshotService(snapshotFile, provider);

            service.snapshot(cacheService, "origin-node");

            CacheSnapshotService.SnapshotMetadata metadata = CacheSnapshotService.readSnapshotFile(snapshotFile, numRegions, REGION_SIZE);

            assertThat(metadata.entries(), hasSize(1));
            SharedBlobCacheService.CacheIndexEntry<FileCacheKey> entry = metadata.entries().get(0);

            assertThat("physical slot must be preserved", entry.physicalSlot(), equalTo(0));
            assertThat("region must be preserved", entry.region(), equalTo(0));

            FileCacheKey key = entry.key();
            assertThat("key must not be null", key, notNullValue());
            assertThat("index name must be preserved", key.shardId().getIndexName(), equalTo("test-index"));
            assertThat("shard ID must be preserved", key.shardId().id(), equalTo(0));
            assertThat("primary term must be preserved", key.primaryTerm(), equalTo(1L));
            assertThat("file name must be preserved", key.fileName(), equalTo("test_file.cfs"));

            assertFalse("completed ranges must not be empty", entry.completedRanges().isEmpty());
        }
    }

    public void testReadSnapshotFileRejectsNumRegionsMismatch() throws Exception {
        int numRegions = 4;
        Path homeDir = createTempDir();
        Path dataDir = createTempDir();
        Settings settings = buildSettings(homeDir, dataDir, numRegions);

        try (
            TestThreadPool threadPool = new TestThreadPool(getTestName());
            NodeEnvironment env = new NodeEnvironment(settings, TestEnvironment.newEnvironment(settings));
            TestCacheService cacheService = new TestCacheService(env, settings, threadPool, List.of(makeTestEntry(0, 0)))
        ) {
            Path snapshotFile = createTempDir().resolve("cache.snapshot");
            new CacheSnapshotService(snapshotFile, () -> "snap-123").snapshot(cacheService, "origin-node");

            CacheSnapshotService.SnapshotMetadata metadata = CacheSnapshotService.readSnapshotFile(
                snapshotFile,
                numRegions + 1,
                REGION_SIZE
            );
            assertThat(metadata, sameInstance(CacheSnapshotService.EMPTY));
            assertNull(metadata.sourceNodeId());
        }
    }

    public void testReadSnapshotFileRejectsRegionSizeMismatch() throws Exception {
        int numRegions = 4;
        Path homeDir = createTempDir();
        Path dataDir = createTempDir();
        Settings settings = buildSettings(homeDir, dataDir, numRegions);

        try (
            TestThreadPool threadPool = new TestThreadPool(getTestName());
            NodeEnvironment env = new NodeEnvironment(settings, TestEnvironment.newEnvironment(settings));
            TestCacheService cacheService = new TestCacheService(env, settings, threadPool, List.of(makeTestEntry(0, 0)))
        ) {
            Path snapshotFile = createTempDir().resolve("cache.snapshot");
            new CacheSnapshotService(snapshotFile, () -> "snap-123").snapshot(cacheService, "origin-node");

            CacheSnapshotService.SnapshotMetadata metadata = CacheSnapshotService.readSnapshotFile(
                snapshotFile,
                numRegions,
                REGION_SIZE + PAGE_SIZE
            );
            assertThat(metadata, sameInstance(CacheSnapshotService.EMPTY));
            assertNull(metadata.sourceNodeId());
        }
    }
}
