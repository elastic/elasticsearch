/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.repositories.blobstore;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.BlobStore;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.repositories.fs.FsRepository;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.stream.Stream;

import static org.elasticsearch.repositories.blobstore.BlobStoreRepository.READONLY_SETTING_KEY;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.instanceOf;

public abstract class ESFsBasedRepositoryIntegTestCase extends ESBlobStoreRepositoryIntegTestCase {

    @Override
    protected String repositoryType() {
        return FsRepository.TYPE;
    }

    public void testMissingDirectoriesNotCreatedInReadonlyRepository() throws IOException, InterruptedException {
        final String repoName = randomRepositoryName();
        final Path repoPath = randomRepoPath();

        final Settings repoSettings = Settings.builder()
                .put(repositorySettings(repoName))
                .put("location", repoPath)
                .build();
        createRepository(repoName, repoSettings, randomBoolean());

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

        createRepository(repoName, Settings.builder().put(repoSettings).put(READONLY_SETTING_KEY, true).build(), randomBoolean());

        final ElasticsearchException exception = expectThrows(ElasticsearchException.class, () ->
            client().admin().cluster().prepareRestoreSnapshot(repoName, snapshotName).setWaitForCompletion(randomBoolean()).get());
        assertThat(exception.getRootCause(), instanceOf(NoSuchFileException.class));

        assertFalse("deleted path is not recreated in readonly repository", Files.exists(deletedPath));
    }

    public void testReadOnly() throws Exception {
        final String repoName = randomRepositoryName();
        final Path repoPath = randomRepoPath();
        final Settings repoSettings = Settings.builder()
                .put(repositorySettings(repoName))
                .put(READONLY_SETTING_KEY, true)
                .put(FsRepository.LOCATION_SETTING.getKey(), repoPath)
                .put(BlobStoreRepository.BUFFER_SIZE_SETTING.getKey(), String.valueOf(randomIntBetween(1, 8) * 1024) + "kb")
                .build();
        createRepository(repoName, repoSettings, false);

        try (BlobStore store = newBlobStore(repoName)) {
            assertFalse(Files.exists(repoPath));
            BlobPath blobPath = BlobPath.EMPTY.add("foo");
            store.blobContainer(blobPath);
            Path storePath = repoPath;
            for (String d : blobPath.parts()) {
                storePath = storePath.resolve(d);
            }
            assertFalse(Files.exists(storePath));
        }

        createRepository(repoName, Settings.builder().put(repoSettings).put(READONLY_SETTING_KEY, false).build(), false);

        try (BlobStore store = newBlobStore(repoName)) {
            assertTrue(Files.exists(repoPath));
            BlobPath blobPath = BlobPath.EMPTY.add("foo");
            BlobContainer container = store.blobContainer(blobPath);
            Path storePath = repoPath;
            for (String d : blobPath.parts()) {
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
