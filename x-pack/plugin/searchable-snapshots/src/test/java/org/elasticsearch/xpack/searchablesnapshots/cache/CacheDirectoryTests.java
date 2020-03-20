/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.searchablesnapshots.cache;

import org.apache.lucene.store.IndexInput;
import org.elasticsearch.Version;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.fs.FsBlobContainer;
import org.elasticsearch.common.blobstore.fs.FsBlobStore;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot;
import org.elasticsearch.index.store.StoreFileMetaData;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matcher;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class CacheDirectoryTests extends ESTestCase {

    public void testClearCache() throws Exception {
        try (CacheService cacheService = new CacheService(Settings.EMPTY)) {
            cacheService.start();

            final int nbRandomFiles = randomIntBetween(3, 10);
            final List<BlobStoreIndexShardSnapshot.FileInfo> randomFiles = new ArrayList<>(nbRandomFiles);

            final Path shardSnapshotDir = createTempDir();
            for (int i = 0; i < nbRandomFiles; i++) {
                final String fileName = "file_" + randomAlphaOfLength(10);
                final byte[] fileContent = randomUnicodeOfLength(randomIntBetween(1, 100_000)).getBytes(StandardCharsets.UTF_8);
                final String blobName = randomAlphaOfLength(15);
                Files.write(shardSnapshotDir.resolve(blobName), fileContent, StandardOpenOption.CREATE_NEW);
                randomFiles.add(new BlobStoreIndexShardSnapshot.FileInfo(blobName,
                    new StoreFileMetaData(fileName, fileContent.length, "_check", Version.CURRENT.luceneVersion),
                    new ByteSizeValue(fileContent.length)));
            }

            final BlobStoreIndexShardSnapshot snapshot = new BlobStoreIndexShardSnapshot("_snapshot", 0L, randomFiles, 0L, 0L, 0, 0L);
            final BlobContainer blobContainer = new FsBlobContainer(new FsBlobStore(Settings.EMPTY, shardSnapshotDir, true),
                BlobPath.cleanPath(), shardSnapshotDir);

            final Path cacheDir = createTempDir();
            try (CacheDirectory cacheDirectory = newCacheDirectory(snapshot, blobContainer, cacheService, cacheDir)) {
                final byte[] buffer = new byte[1024];
                for (int i = 0; i < randomIntBetween(10, 50); i++) {
                    final BlobStoreIndexShardSnapshot.FileInfo fileInfo = randomFrom(randomFiles);
                    final int fileLength = Math.toIntExact(fileInfo.length());

                    try (IndexInput input = cacheDirectory.openInput(fileInfo.physicalName(), newIOContext(random()))) {
                        assertThat(input.length(), equalTo((long) fileLength));
                        final int start = between(0, fileLength - 1);
                        final int end = between(start + 1, fileLength);

                        input.seek(start);
                        while (input.getFilePointer() < end) {
                            input.readBytes(buffer, 0, Math.toIntExact(Math.min(buffer.length, end - input.getFilePointer())));
                        }
                    }
                    assertListOfFiles(cacheDir, allOf(greaterThan(0), lessThanOrEqualTo(nbRandomFiles)), greaterThan(0L));
                    if (randomBoolean()) {
                        cacheDirectory.clearCache();
                        assertListOfFiles(cacheDir, equalTo(0), equalTo(0L));
                    }
                }
            }
        }
    }

    private CacheDirectory newCacheDirectory(BlobStoreIndexShardSnapshot snapshot, BlobContainer container,
                                             CacheService cacheService, Path cacheDir) throws IOException {
        return new CacheDirectory(snapshot, container, cacheService, cacheDir, new SnapshotId("_na","_na"), new IndexId("_na", "_na"),
            new ShardId("_na", "_na", 0), () -> 0L);
    }

    private void assertListOfFiles(Path cacheDir, Matcher<Integer> matchNumberOfFiles, Matcher<Long> matchSizeOfFiles) throws IOException {
        final Map<String, Long> files = new HashMap<>();
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(cacheDir)) {
            for (Path file : stream) {
                final String fileName = file.getFileName().toString();
                if (fileName.equals("write.lock") || fileName.startsWith("extra")) {
                    continue;
                }
                try {
                    if (Files.isRegularFile(file)) {
                        final BasicFileAttributes fileAttributes = Files.readAttributes(file, BasicFileAttributes.class);
                        files.put(fileName, fileAttributes.size());
                    }
                } catch (FileNotFoundException | NoSuchFileException e) {
                    // ignoring as the cache file might be evicted
                }
            }
        }
        assertThat("Number of files (" + files.size() + ") mismatch, got : " + files.keySet(), files.size(), matchNumberOfFiles);
        assertThat("Sum of file sizes mismatch, got: " + files, files.values().stream().mapToLong(Long::longValue).sum(), matchSizeOfFiles);
    }
}
