/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.searchablesnapshots.store.input;

import org.apache.lucene.store.IndexInput;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardPath;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot.FileInfo;
import org.elasticsearch.index.store.StoreFileMetadata;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.snapshots.SearchableSnapshotsSettings;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.xpack.searchablesnapshots.AbstractSearchableSnapshotsTestCase;
import org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshots;
import org.elasticsearch.xpack.searchablesnapshots.cache.common.TestUtils;
import org.elasticsearch.xpack.searchablesnapshots.cache.full.CacheService;
import org.elasticsearch.xpack.searchablesnapshots.cache.shared.FrozenCacheService;
import org.elasticsearch.xpack.searchablesnapshots.cache.shared.SharedBytes;
import org.elasticsearch.xpack.searchablesnapshots.store.SearchableSnapshotDirectory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.elasticsearch.xpack.searchablesnapshots.cache.full.CacheService.resolveSnapshotCache;
import static org.hamcrest.Matchers.instanceOf;

public class FrozenIndexInputTests extends AbstractSearchableSnapshotsTestCase {

    private static final ShardId SHARD_ID = new ShardId(new Index("_index_name", "_index_id"), 0);

    public void testRandomReads() throws IOException {
        final String fileName = randomAlphaOfLength(5) + randomFileExtension();
        final Tuple<String, byte[]> bytes = randomChecksumBytes(randomIntBetween(1, 100_000));

        final byte[] fileData = bytes.v2();
        final String checksum = bytes.v1();

        final FileInfo fileInfo = new FileInfo(
            randomAlphaOfLength(10),
            new StoreFileMetadata(fileName, fileData.length, checksum, Version.CURRENT.luceneVersion.toString()),
            ByteSizeValue.ofBytes(fileData.length)
        );

        final ByteSizeValue rangeSize;
        if (rarely()) {
            rangeSize = FrozenCacheService.SHARED_CACHE_RANGE_SIZE_SETTING.get(Settings.EMPTY);
        } else if (randomBoolean()) {
            rangeSize = ByteSizeValue.ofBytes(randomIntBetween(1, 16) * SharedBytes.PAGE_SIZE);
        } else {
            rangeSize = ByteSizeValue.ofBytes(randomIntBetween(1, 16000) * SharedBytes.PAGE_SIZE);
        }

        final ByteSizeValue regionSize;
        if (rarely()) {
            regionSize = FrozenCacheService.SHARED_CACHE_REGION_SIZE_SETTING.get(Settings.EMPTY);
        } else {
            regionSize = ByteSizeValue.ofBytes(randomIntBetween(1, 16) * SharedBytes.PAGE_SIZE);
        }

        final ByteSizeValue cacheSize;
        if (rarely()) {
            cacheSize = regionSize;
        } else {
            cacheSize = ByteSizeValue.ofBytes(randomLongBetween(1L, 10L) * regionSize.getBytes() + randomIntBetween(0, 100));
        }

        final Settings settings = Settings.builder()
            .put(FrozenCacheService.SHARED_CACHE_REGION_SIZE_SETTING.getKey(), regionSize)
            .put(FrozenCacheService.SHARED_CACHE_RANGE_SIZE_SETTING.getKey(), rangeSize)
            .put(FrozenCacheService.SHARED_CACHE_SIZE_SETTING.getKey(), cacheSize)
            .put("path.home", createTempDir())
            .build();
        final Environment environment = TestEnvironment.newEnvironment(settings);
        for (Path path : environment.dataFiles()) {
            Files.createDirectories(path);
        }
        SnapshotId snapshotId = new SnapshotId("_name", "_uuid");
        final Path shardDir = randomShardPath(SHARD_ID);
        final ShardPath shardPath = new ShardPath(false, shardDir, shardDir, SHARD_ID);
        final Path cacheDir = Files.createDirectories(resolveSnapshotCache(shardDir).resolve(snapshotId.getUUID()));
        try (
            NodeEnvironment nodeEnvironment = new NodeEnvironment(settings, environment);
            FrozenCacheService frozenCacheService = new FrozenCacheService(nodeEnvironment, settings, threadPool);
            CacheService cacheService = randomCacheService();
            TestSearchableSnapshotDirectory directory = new TestSearchableSnapshotDirectory(
                frozenCacheService,
                cacheService,
                fileInfo,
                snapshotId,
                fileData,
                shardPath,
                cacheDir
            )
        ) {
            cacheService.start();
            directory.loadSnapshot(createRecoveryState(true), ActionListener.noop());

            // TODO does not test using the recovery range size
            final IndexInput indexInput = directory.openInput(fileName, randomIOContext());
            assertThat(indexInput, instanceOf(FrozenIndexInput.class));
            assertEquals(fileData.length, indexInput.length());
            assertEquals(0, indexInput.getFilePointer());

            final byte[] result = randomReadAndSlice(indexInput, fileData.length);
            assertArrayEquals(fileData, result);
            indexInput.close();
        }
    }

    private class TestSearchableSnapshotDirectory extends SearchableSnapshotDirectory {

        TestSearchableSnapshotDirectory(
            FrozenCacheService service,
            CacheService cacheService,
            FileInfo fileInfo,
            SnapshotId snapshotId,
            byte[] fileData,
            ShardPath shardPath,
            Path cacheDir
        ) {
            super(
                () -> TestUtils.singleBlobContainer(fileInfo.partName(0), fileData),
                () -> new BlobStoreIndexShardSnapshot("_snapshot_id", 0L, List.of(fileInfo), 0L, 0L, 0, 0L),
                new TestUtils.SimpleBlobStoreCacheService(),
                "_repository",
                snapshotId,
                new IndexId(SHARD_ID.getIndex().getName(), SHARD_ID.getIndex().getUUID()),
                SHARD_ID,
                Settings.builder()
                    .put(SearchableSnapshotsSettings.SNAPSHOT_PARTIAL_SETTING.getKey(), true)
                    .put(SearchableSnapshots.SNAPSHOT_CACHE_ENABLED_SETTING.getKey(), true)
                    .build(),
                System::currentTimeMillis,
                cacheService,
                cacheDir,
                shardPath,
                threadPool,
                service
            );
        }
    }
}
