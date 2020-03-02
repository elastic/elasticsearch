/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.searchablesnapshots.cache;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.shard.ShardId;
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
import java.nio.file.attribute.BasicFileAttributes;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class CacheDirectoryTests extends ESTestCase {

    public void testClearCache() throws Exception {
        try (CacheService cacheService = new CacheService(Settings.EMPTY)) {
            cacheService.start();
            try (Directory directory = newDirectory()) {
                final int nbRandomFiles = randomIntBetween(3, 10);
                final Map<String, Integer> randomFiles = new HashMap<>(nbRandomFiles);

                for (int i = 0; i < nbRandomFiles; i++) {
                    final String fileName = randomAlphaOfLength(10);
                    final byte[] fileContent = randomUnicodeOfLength(randomIntBetween(1, 100_000)).getBytes(StandardCharsets.UTF_8);

                    final IndexOutput indexOutput = directory.createOutput(fileName, newIOContext(random()));
                    indexOutput.writeBytes(fileContent, fileContent.length);
                    indexOutput.close();
                    randomFiles.put(fileName, fileContent.length);
                }

                final Path cacheDir = createTempDir();
                try (CacheDirectory cacheDirectory = newCacheDirectory(directory, cacheService, cacheDir)) {
                    final byte[] buffer = new byte[1024];
                    for (int i = 0; i < randomIntBetween(10, 50); i++) {
                        final String fileName = randomFrom(randomFiles.keySet());
                        final int fileLength = randomFiles.get(fileName);

                        try (IndexInput input = cacheDirectory.openInput(fileName, newIOContext(random()))) {
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
    }

    private CacheDirectory newCacheDirectory(Directory directory, CacheService cacheService, Path cacheDir) throws IOException {
        return new CacheDirectory(directory, cacheService, cacheDir, new SnapshotId("_na","_na"), new IndexId("_na", "_na"),
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
