/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.index.store.cache;

import org.apache.lucene.store.IndexInput;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardPath;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot.FileInfo;
import org.elasticsearch.index.store.SearchableSnapshotDirectory;
import org.elasticsearch.index.store.StoreFileMetadata;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.snapshots.SnapshotsService;
import org.elasticsearch.xpack.searchablesnapshots.AbstractSearchableSnapshotsTestCase;
import org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshots;
import org.elasticsearch.xpack.searchablesnapshots.cache.CacheService;
import org.elasticsearch.xpack.searchablesnapshots.cache.FrozenCacheService;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Locale;

import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.Mockito.mock;

public class FrozenIndexInputTests extends AbstractSearchableSnapshotsTestCase {

    private static final ShardId SHARD_ID = new ShardId(new Index("_index_name", "_index_id"), 0);

    public void testRandomReads() throws IOException {
        final String fileName = randomAlphaOfLengthBetween(5, 10).toLowerCase(Locale.ROOT);
        final byte[] fileData = randomUnicodeOfLength(randomIntBetween(1, 100_000)).getBytes(StandardCharsets.UTF_8);

        final Path tempDir = createTempDir().resolve(SHARD_ID.getIndex().getUUID()).resolve(String.valueOf(SHARD_ID.getId()));
        final FileInfo fileInfo = new FileInfo(
            randomAlphaOfLength(10),
            new StoreFileMetadata(fileName, fileData.length, "_na", Version.CURRENT.luceneVersion),
            new ByteSizeValue(fileData.length)
        );

        final ByteSizeValue rangeSize;
        if (rarely()) {
            rangeSize = SnapshotsService.SHARED_CACHE_RANGE_SIZE_SETTING.get(Settings.EMPTY);
        } else if (randomBoolean()) {
            rangeSize = new ByteSizeValue(
                randomLongBetween(CacheService.MIN_SNAPSHOT_CACHE_RANGE_SIZE.getBytes(), ByteSizeValue.ofKb(8L).getBytes())
            );
        } else {
            rangeSize = new ByteSizeValue(
                randomLongBetween(CacheService.MIN_SNAPSHOT_CACHE_RANGE_SIZE.getBytes(), ByteSizeValue.ofMb(64L).getBytes())
            );
        }

        final ByteSizeValue regionSize;
        if (rarely()) {
            regionSize = SnapshotsService.SNAPSHOT_CACHE_REGION_SIZE_SETTING.get(Settings.EMPTY);
        } else if (randomBoolean()) {
            regionSize = new ByteSizeValue(randomLongBetween(ByteSizeValue.ofKb(1L).getBytes(), ByteSizeValue.ofKb(8L).getBytes()));
        } else {
            regionSize = new ByteSizeValue(randomLongBetween(ByteSizeValue.ofKb(1L).getBytes(), ByteSizeValue.ofMb(64L).getBytes()));
        }

        final ByteSizeValue cacheSize;
        if (rarely()) {
            cacheSize = regionSize;
        } else {
            cacheSize = new ByteSizeValue(randomLongBetween(1L, 10L) * regionSize.getBytes() + randomIntBetween(0, 100));
        }

        final Settings settings = Settings.builder()
            .put(SnapshotsService.SNAPSHOT_CACHE_REGION_SIZE_SETTING.getKey(), regionSize)
            .put(SnapshotsService.SHARED_CACHE_RANGE_SIZE_SETTING.getKey(), rangeSize)
            .put(SnapshotsService.SNAPSHOT_CACHE_SIZE_SETTING.getKey(), cacheSize)
            .put("path.home", createTempDir())
            .build();
        final Environment environment = TestEnvironment.newEnvironment(settings);
        for (Path path : environment.dataFiles()) {
            Files.createDirectories(path);
        }

        try (
            FrozenCacheService cacheService = new FrozenCacheService(environment, threadPool);
            TestSearchableSnapshotDirectory directory = new TestSearchableSnapshotDirectory(cacheService, tempDir, fileInfo, fileData)
        ) {
            directory.loadSnapshot(createRecoveryState(true), ActionListener.wrap(() -> {}));

            // TODO does not test the checksum shortcut, does not test using the recovery range size
            final IndexInput indexInput = directory.openInput(fileName, newIOContext(random()));
            assertThat(indexInput, instanceOf(FrozenIndexInput.class));
            assertEquals(fileData.length, indexInput.length());
            assertEquals(0, indexInput.getFilePointer());

            final byte[] result = randomReadAndSlice(indexInput, fileData.length);
            assertArrayEquals(fileData, result);
            indexInput.close();
        }
    }

    private class TestSearchableSnapshotDirectory extends SearchableSnapshotDirectory {

        TestSearchableSnapshotDirectory(FrozenCacheService service, Path tempDir, FileInfo fileInfo, byte[] fileData) {
            super(
                () -> TestUtils.singleBlobContainer(fileInfo.partName(0), fileData),
                () -> new BlobStoreIndexShardSnapshot("_snapshot_id", 0L, List.of(fileInfo), 0L, 0L, 0, 0L),
                new TestUtils.SimpleBlobStoreCacheService(),
                "_repository",
                new SnapshotId("_snapshot_name", "_snapshot_id"),
                new IndexId(SHARD_ID.getIndex().getName(), SHARD_ID.getIndex().getUUID()),
                SHARD_ID,
                Settings.builder()
                    .put(SearchableSnapshots.SNAPSHOT_PARTIAL_SETTING.getKey(), true)
                    .put(SearchableSnapshots.SNAPSHOT_CACHE_ENABLED_SETTING.getKey(), true)
                    .build(),
                System::currentTimeMillis,
                mock(CacheService.class),
                tempDir,
                new ShardPath(false, tempDir, tempDir, SHARD_ID),
                threadPool,
                service
            );
        }
    }
}
