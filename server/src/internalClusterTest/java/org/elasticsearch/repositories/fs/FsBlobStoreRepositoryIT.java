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
package org.elasticsearch.repositories.fs;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.fs.FsBlobStore;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.repositories.blobstore.ESBlobStoreRepositoryIntegTestCase;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.stream.Stream;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.instanceOf;

public class FsBlobStoreRepositoryIT extends ESBlobStoreRepositoryIntegTestCase {

    @Override
    protected String repositoryType() {
        return FsRepository.TYPE;
    }

    @Override
    protected Settings repositorySettings() {
        final Settings.Builder settings = Settings.builder();
        settings.put(super.repositorySettings());
        settings.put("location", randomRepoPath());
        if (randomBoolean()) {
            long size = 1 << randomInt(10);
            settings.put("chunk_size", new ByteSizeValue(size, ByteSizeUnit.KB));
        }
        return settings.build();
    }

    public void testMissingDirectoriesNotCreatedInReadonlyRepository() throws IOException, InterruptedException {
        final String repoName = randomName();
        final Path repoPath = randomRepoPath();

        logger.info("--> creating repository {} at {}", repoName, repoPath);

        assertAcked(client().admin().cluster().preparePutRepository(repoName).setType("fs").setSettings(Settings.builder()
            .put("location", repoPath)
            .put("compress", randomBoolean())
            .put("chunk_size", randomIntBetween(100, 1000), ByteSizeUnit.BYTES)));

        final String indexName = randomName();
        int docCount = iterations(10, 1000);
        logger.info("-->  create random index {} with {} records", indexName, docCount);
        addRandomDocuments(indexName, docCount);
        assertHitCount(client().prepareSearch(indexName).setSize(0).get(), docCount);

        final String snapshotName = randomName();
        logger.info("-->  create snapshot {}:{}", repoName, snapshotName);
        assertSuccessfulSnapshot(client().admin().cluster().prepareCreateSnapshot(repoName, snapshotName)
            .setWaitForCompletion(true).setIndices(indexName));

        assertAcked(client().admin().indices().prepareDelete(indexName));
        assertAcked(client().admin().cluster().prepareDeleteRepository(repoName));

        final Path deletedPath;
        try (Stream<Path> contents = Files.list(repoPath.resolve("indices"))) {
            //noinspection OptionalGetWithoutIsPresent because we know there's a subdirectory
            deletedPath = contents.filter(Files::isDirectory).findAny().get();
            IOUtils.rm(deletedPath);
        }
        assertFalse(Files.exists(deletedPath));

        assertAcked(client().admin().cluster().preparePutRepository(repoName).setType("fs").setSettings(Settings.builder()
            .put("location", repoPath).put("readonly", true)));

        final ElasticsearchException exception = expectThrows(ElasticsearchException.class, () ->
            client().admin().cluster().prepareRestoreSnapshot(repoName, snapshotName).setWaitForCompletion(randomBoolean()).get());
        assertThat(exception.getRootCause(), instanceOf(NoSuchFileException.class));

        assertFalse("deleted path is not recreated in readonly repository", Files.exists(deletedPath));
    }

    public void testReadOnly() throws Exception {
        Path tempDir = createTempDir();
        Path path = tempDir.resolve("bar");

        try (FsBlobStore store = new FsBlobStore(randomIntBetween(1, 8) * 1024, path, true)) {
            assertFalse(Files.exists(path));
            BlobPath blobPath = BlobPath.cleanPath().add("foo");
            store.blobContainer(blobPath);
            Path storePath = store.path();
            for (String d : blobPath) {
                storePath = storePath.resolve(d);
            }
            assertFalse(Files.exists(storePath));
        }

        try (FsBlobStore store = new FsBlobStore(randomIntBetween(1, 8) * 1024, path, false)) {
            assertTrue(Files.exists(path));
            BlobPath blobPath = BlobPath.cleanPath().add("foo");
            BlobContainer container = store.blobContainer(blobPath);
            Path storePath = store.path();
            for (String d : blobPath) {
                storePath = storePath.resolve(d);
            }
            assertTrue(Files.exists(storePath));
            assertTrue(Files.isDirectory(storePath));

            byte[] data = randomBytes(randomIntBetween(10, scaledRandomIntBetween(1024, 1 << 16)));
            writeBlob(container, "test", new BytesArray(data));
            assertArrayEquals(readBlobFully(container, "test", data.length), data);
            assertTrue(container.blobExists("test"));
        }
    }
}
