/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.searchablesnapshots;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.store.ByteBuffersIndexInput;
import org.apache.lucene.store.ByteBuffersIndexOutput;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.lucene.store.ESIndexInputTestCase;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.snapshots.Snapshot;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.threadpool.ThreadPoolStats;
import org.elasticsearch.xpack.searchablesnapshots.cache.common.ByteRange;
import org.elasticsearch.xpack.searchablesnapshots.cache.common.CacheFile;
import org.elasticsearch.xpack.searchablesnapshots.cache.common.CacheKey;
import org.elasticsearch.xpack.searchablesnapshots.cache.full.CacheService;
import org.elasticsearch.xpack.searchablesnapshots.cache.full.PersistentCache;
import org.elasticsearch.xpack.searchablesnapshots.cache.shared.FrozenCacheService;
import org.elasticsearch.xpack.searchablesnapshots.recovery.SearchableSnapshotRecoveryState;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.SortedSet;
import java.util.concurrent.TimeUnit;

import static com.carrotsearch.randomizedtesting.RandomizedTest.randomAsciiLettersOfLengthBetween;
import static org.elasticsearch.xpack.searchablesnapshots.cache.common.TestUtils.randomPopulateAndReads;
import static org.elasticsearch.xpack.searchablesnapshots.cache.shared.SharedBytes.PAGE_SIZE;
import static org.elasticsearch.xpack.searchablesnapshots.cache.shared.SharedBytes.pageAligned;

public abstract class AbstractSearchableSnapshotsTestCase extends ESIndexInputTestCase {

    private static final ClusterSettings CLUSTER_SETTINGS = new ClusterSettings(
        Settings.EMPTY,
        Sets.union(
            ClusterSettings.BUILT_IN_CLUSTER_SETTINGS,
            Set.of(
                CacheService.SNAPSHOT_CACHE_RANGE_SIZE_SETTING,
                CacheService.SNAPSHOT_CACHE_SYNC_INTERVAL_SETTING,
                CacheService.SNAPSHOT_CACHE_MAX_FILES_TO_SYNC_AT_ONCE_SETTING
            )
        )
    );

    protected ThreadPool threadPool;
    protected ClusterService clusterService;
    protected NodeEnvironment nodeEnvironment;
    protected NodeEnvironment singlePathNodeEnvironment;

    @Before
    public void setUpTest() throws Exception {
        final DiscoveryNode node = new DiscoveryNode(
            "node",
            ESTestCase.buildNewFakeTransportAddress(),
            Collections.emptyMap(),
            DiscoveryNodeRole.roles(),
            Version.CURRENT
        );
        threadPool = new TestThreadPool(getTestName(), SearchableSnapshots.executorBuilders(Settings.EMPTY));
        clusterService = ClusterServiceUtils.createClusterService(threadPool, node, CLUSTER_SETTINGS);
        nodeEnvironment = newNodeEnvironment();
        singlePathNodeEnvironment = newSinglePathNodeEnvironment();
    }

    @After
    public void tearDownTest() throws Exception {
        IOUtils.close(nodeEnvironment, clusterService);
        IOUtils.close(singlePathNodeEnvironment, clusterService);
        assertTrue(ThreadPool.terminate(threadPool, 30L, TimeUnit.SECONDS));
    }

    /**
     * @return a new {@link CacheService} instance configured with default settings
     */
    protected CacheService defaultCacheService() {
        return new CacheService(Settings.EMPTY, clusterService, threadPool, new PersistentCache(nodeEnvironment));
    }

    /**
     * @return a new {@link CacheService} instance configured with random cache size and cache range size settings
     */
    protected CacheService randomCacheService() {
        final Settings.Builder cacheSettings = Settings.builder();
        if (randomBoolean()) {
            cacheSettings.put(CacheService.SNAPSHOT_CACHE_RANGE_SIZE_SETTING.getKey(), randomCacheRangeSize());
        }
        if (randomBoolean()) {
            cacheSettings.put(CacheService.SNAPSHOT_CACHE_RECOVERY_RANGE_SIZE_SETTING.getKey(), randomCacheRangeSize());
        }
        if (randomBoolean()) {
            cacheSettings.put(
                CacheService.SNAPSHOT_CACHE_SYNC_INTERVAL_SETTING.getKey(),
                TimeValue.timeValueSeconds(scaledRandomIntBetween(1, 120))
            );
        }
        return new CacheService(cacheSettings.build(), clusterService, threadPool, new PersistentCache(nodeEnvironment));
    }

    /**
     * @return a new {@link FrozenCacheService} instance configured with default settings
     */
    protected FrozenCacheService defaultFrozenCacheService() {
        return new FrozenCacheService(nodeEnvironment, Settings.EMPTY, threadPool);
    }

    protected FrozenCacheService randomFrozenCacheService() {
        final Settings.Builder cacheSettings = Settings.builder();
        if (randomBoolean()) {
            cacheSettings.put(FrozenCacheService.SNAPSHOT_CACHE_SIZE_SETTING.getKey(), randomFrozenCacheSize());
        }
        if (randomBoolean()) {
            cacheSettings.put(FrozenCacheService.SNAPSHOT_CACHE_REGION_SIZE_SETTING.getKey(), pageAligned(randomFrozenCacheSize()));
        }
        if (randomBoolean()) {
            cacheSettings.put(FrozenCacheService.SHARED_CACHE_RANGE_SIZE_SETTING.getKey(), randomFrozenCacheRangeSize());
        }
        if (randomBoolean()) {
            cacheSettings.put(FrozenCacheService.FROZEN_CACHE_RECOVERY_RANGE_SIZE_SETTING.getKey(), randomFrozenCacheRangeSize());
        }
        return new FrozenCacheService(singlePathNodeEnvironment, cacheSettings.build(), threadPool);
    }

    /**
     * @return a new {@link CacheService} instance configured with the given cache range size setting
     */
    protected CacheService createCacheService(final ByteSizeValue cacheRangeSize) {
        return new CacheService(
            Settings.builder().put(CacheService.SNAPSHOT_CACHE_RANGE_SIZE_SETTING.getKey(), cacheRangeSize).build(),
            clusterService,
            threadPool,
            new PersistentCache(nodeEnvironment)
        );
    }

    protected FrozenCacheService createFrozenCacheService(final ByteSizeValue cacheSize, final ByteSizeValue cacheRangeSize) {
        return new FrozenCacheService(
            singlePathNodeEnvironment,
            Settings.builder()
                .put(FrozenCacheService.SNAPSHOT_CACHE_SIZE_SETTING.getKey(), cacheSize)
                .put(FrozenCacheService.SHARED_CACHE_RANGE_SIZE_SETTING.getKey(), cacheRangeSize)
                .build(),
            threadPool
        );
    }

    private NodeEnvironment newSinglePathNodeEnvironment() throws IOException {
        Settings build = Settings.builder()
            .put(buildEnvSettings(Settings.EMPTY))
            .put(Environment.PATH_DATA_SETTING.getKey(), createTempDir().toAbsolutePath().toString())
            .build();
        return new NodeEnvironment(build, TestEnvironment.newEnvironment(build));
    }

    /**
     * Returns a random shard data path for the specified {@link ShardId}. The returned path can be located on any of the data node paths.
     */
    protected Path shardPath(ShardId shardId) {
        return nodeEnvironment.availableShardPath(shardId);
    }

    protected static ByteSizeValue randomFrozenCacheSize() {
        return new ByteSizeValue(randomLongBetween(0, 10_000_000));
    }

    /**
     * @return a random {@link ByteSizeValue} that can be used to set {@link CacheService#SNAPSHOT_CACHE_RANGE_SIZE_SETTING}
     */
    protected static ByteSizeValue randomCacheRangeSize() {
        return pageAlignedBetween(CacheService.MIN_SNAPSHOT_CACHE_RANGE_SIZE, CacheService.MAX_SNAPSHOT_CACHE_RANGE_SIZE);
    }

    protected static ByteSizeValue randomFrozenCacheRangeSize() {
        return pageAlignedBetween(FrozenCacheService.MIN_SNAPSHOT_CACHE_RANGE_SIZE, FrozenCacheService.MAX_SNAPSHOT_CACHE_RANGE_SIZE);
    }

    private static ByteSizeValue pageAlignedBetween(ByteSizeValue min, ByteSizeValue max) {
        ByteSizeValue aligned = pageAligned(new ByteSizeValue(randomLongBetween(min.getBytes(), max.getBytes())));
        if (aligned.compareTo(max) > 0) {
            // minus one page in case page alignment moved us past the max setting value
            return new ByteSizeValue(aligned.getBytes() - PAGE_SIZE);
        }
        return aligned;
    }

    protected static SearchableSnapshotRecoveryState createRecoveryState(boolean finalizedDone) {
        ShardRouting shardRouting = TestShardRouting.newShardRouting(
            new ShardId(randomAlphaOfLength(10), randomAlphaOfLength(10), 0),
            randomAlphaOfLength(10),
            true,
            ShardRoutingState.INITIALIZING,
            new RecoverySource.SnapshotRecoverySource(
                UUIDs.randomBase64UUID(),
                new Snapshot("repo", new SnapshotId(randomAlphaOfLength(8), UUIDs.randomBase64UUID())),
                Version.CURRENT,
                new IndexId("some_index", UUIDs.randomBase64UUID(random()))
            )
        );
        DiscoveryNode targetNode = new DiscoveryNode("local", buildNewFakeTransportAddress(), Version.CURRENT);
        SearchableSnapshotRecoveryState recoveryState = new SearchableSnapshotRecoveryState(shardRouting, targetNode, null);

        recoveryState.setStage(RecoveryState.Stage.INIT)
            .setStage(RecoveryState.Stage.INDEX)
            .setStage(RecoveryState.Stage.VERIFY_INDEX)
            .setStage(RecoveryState.Stage.TRANSLOG);
        recoveryState.getIndex().setFileDetailsComplete();
        if (finalizedDone) {
            recoveryState.setStage(RecoveryState.Stage.FINALIZE).setStage(RecoveryState.Stage.DONE);
        }
        return recoveryState;
    }

    /**
     * Wait for all operations on the threadpool to complete
     */
    protected static void assertThreadPoolNotBusy(ThreadPool threadPool) throws Exception {
        assertBusy(() -> {
            for (ThreadPoolStats.Stats stat : threadPool.stats()) {
                assertEquals(stat.getActive(), 0);
                assertEquals(stat.getQueue(), 0);
            }
        }, 30L, TimeUnit.SECONDS);
    }

    /**
     * Generates one or more cache files using the specified {@link CacheService}. Each cache files have been written at least once.
     */
    protected List<CacheFile> randomCacheFiles(CacheService cacheService) throws Exception {
        final byte[] buffer = new byte[1024];
        Arrays.fill(buffer, (byte) 0xff);

        final List<CacheFile> cacheFiles = new ArrayList<>();
        for (int snapshots = 0; snapshots < between(1, 2); snapshots++) {
            final String snapshotUUID = UUIDs.randomBase64UUID(random());
            for (int indices = 0; indices < between(1, 2); indices++) {
                IndexId indexId = new IndexId(randomAlphaOfLength(5).toLowerCase(Locale.ROOT), UUIDs.randomBase64UUID(random()));
                for (int shards = 0; shards < between(1, 2); shards++) {
                    ShardId shardId = new ShardId(indexId.getName(), indexId.getId(), shards);

                    final Path cacheDir = Files.createDirectories(
                        CacheService.resolveSnapshotCache(shardPath(shardId)).resolve(snapshotUUID)
                    );

                    for (int files = 0; files < between(1, 2); files++) {
                        final CacheKey cacheKey = new CacheKey(snapshotUUID, indexId.getName(), shardId, "file_" + files);
                        final CacheFile cacheFile = cacheService.get(cacheKey, randomLongBetween(100L, buffer.length), cacheDir);

                        final CacheFile.EvictionListener listener = evictedCacheFile -> {};
                        cacheFile.acquire(listener);
                        try {
                            SortedSet<ByteRange> ranges = Collections.emptySortedSet();
                            while (ranges.isEmpty()) {
                                ranges = randomPopulateAndReads(cacheFile, (channel, from, to) -> {
                                    try {
                                        channel.write(ByteBuffer.wrap(buffer, Math.toIntExact(from), Math.toIntExact(to)));
                                    } catch (IOException e) {
                                        throw new AssertionError(e);
                                    }
                                });
                            }
                            cacheFiles.add(cacheFile);
                        } finally {
                            cacheFile.release(listener);
                        }
                    }
                }
            }
        }
        return cacheFiles;
    }

    public static Tuple<String, byte[]> randomChecksumBytes(int length) throws IOException {
        return randomChecksumBytes(randomUnicodeOfLength(length).getBytes(StandardCharsets.UTF_8));
    }

    public static Tuple<String, byte[]> randomChecksumBytes(byte[] bytes) throws IOException {
        final ByteBuffersDataOutput out = new ByteBuffersDataOutput();
        try (IndexOutput output = new ByteBuffersIndexOutput(out, "randomChecksumBytes()", "randomChecksumBytes()")) {
            CodecUtil.writeHeader(output, randomAsciiLettersOfLengthBetween(0, 127), randomNonNegativeByte());
            output.writeBytes(bytes, bytes.length);
            CodecUtil.writeFooter(output);
        }
        final String checksum;
        try (IndexInput input = new ByteBuffersIndexInput(out.toDataInput(), "checksumEntireFile()")) {
            checksum = Store.digestToString(CodecUtil.checksumEntireFile(input));
        }
        return Tuple.tuple(checksum, out.toArrayCopy());
    }

    /**
     * @return a random {@link IOContext} that corresponds to a default, read or read_once usage.
     *
     * It's important that the context returned by this method is not a "merge" once as {@link org.apache.lucene.store.BufferedIndexInput}
     * uses a different buffer size for them.
     */
    public static IOContext randomIOContext() {
        final IOContext ioContext = randomFrom(IOContext.DEFAULT, IOContext.READ, IOContext.READONCE);
        assert ioContext.context != IOContext.Context.MERGE;
        return ioContext;
    }
}
